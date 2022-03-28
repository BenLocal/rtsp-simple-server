package core

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/h264"
	"github.com/aler9/gortsplib/pkg/ringbuffer"
	"github.com/aler9/gortsplib/pkg/rtpaac"
	"github.com/aler9/gortsplib/pkg/rtph264"
	"github.com/aler9/rtsp-simple-server/internal/logger"
	"github.com/notedit/rtmp/av"
	"github.com/pion/rtcp"
	"github.com/pion/rtp/v2"
)

const (
	closeFlvCheckPeriod = 1 * time.Second
)

type flvMuxerTrackIDPayloadPair struct {
	trackID int
	packet  *rtp.Packet
}

type flvMuxerPathManager interface {
	onReaderSetupPlay(req pathReaderSetupPlayReq) pathReaderSetupPlayRes
}

type flvMuxerParent interface {
	Log(logger.Level, string, ...interface{})
	onMuxerClose(*flvMuxer)
}

type flvMuxer struct {
	wg                        *sync.WaitGroup
	externalAuthenticationURL string
	pathName                  string
	pathManager               flvMuxerPathManager
	parent                    flvMuxerParent
	readBufferCount           int

	ctx        context.Context
	ctxCancel  func()
	ringBuffer *ringbuffer.RingBuffer
	path       *path
	videoTrack *gortsplib.TrackH264
	audioTrack *gortsplib.TrackAAC
	mutex      sync.Mutex

	requests []flvSession
	queues   map[chan av.Packet]struct{}

	// in
	request chan flvSession
	closed  chan flvSession
}

func newFlvMuxer(
	parentCtx context.Context,
	wg *sync.WaitGroup,
	pathName string,
	pathManager flvMuxerPathManager,
	parent flvMuxerParent,
	readBufferCount int,
	externalAuthenticationURL string,
) *flvMuxer {

	ctx, ctxCancel := context.WithCancel(parentCtx)

	r := &flvMuxer{
		pathName:                  pathName,
		pathManager:               pathManager,
		parent:                    parent,
		ctx:                       ctx,
		ctxCancel:                 ctxCancel,
		readBufferCount:           readBufferCount,
		externalAuthenticationURL: externalAuthenticationURL,
		wg:                        wg,
		request:                   make(chan flvSession),
		closed:                    make(chan flvSession),
		queues:                    make(map[chan av.Packet]struct{}),
	}

	r.log(logger.Info, "created")

	r.wg.Add(1)
	go r.run()
	return r
}

func (r *flvMuxer) log(level logger.Level, format string, args ...interface{}) {
	r.parent.Log(level, "[flv muxer %s] "+format, append([]interface{}{r.pathName}, args...)...)
}

func (r *flvMuxer) Path() string {
	return r.pathName
}

func (r *flvMuxer) run() {
	defer r.wg.Done()
	defer r.log(logger.Info, "destroyed")

	innerCtx, innerCtxCancel := context.WithCancel(context.Background())
	innerReady := make(chan struct{})
	innerErr := make(chan error)
	go func() {
		innerErr <- r.runInner(innerCtx, innerReady)
	}()

	isReady := false
	err := func() error {
		for {
			select {
			case <-r.ctx.Done():
				innerCtxCancel()
				<-innerErr
				return errors.New("terminated")

			case req := <-r.request:
				if isReady {
					r.handleRequest(req)
				} else {
					r.requests = append(r.requests, req)
				}
			case <-innerReady:
				isReady = true
				for _, req := range r.requests {
					r.handleRequest(req)
				}
				r.requests = nil

			case req := <-r.closed:
				r.mutex.Lock()
				delete(r.queues, req.Queue)
				r.mutex.Unlock()

			case err := <-innerErr:
				innerCtxCancel()
				return err
			}
		}
	}()

	r.ctxCancel()
	for _, req := range r.requests {
		req.setHeaderFunc(flvResponse{Status: http.StatusNotFound})
	}
	r.parent.onMuxerClose(r)

	r.log(logger.Info, "closed (%v)", err)
}

func (r *flvMuxer) runInner(innerCtx context.Context, innerReady chan struct{}) error {
	res := r.pathManager.onReaderSetupPlay(pathReaderSetupPlayReq{
		author:       r,
		pathName:     r.pathName,
		authenticate: nil,
	})

	if res.err != nil {
		return res.err
	}

	r.path = res.path
	defer func() {
		r.path.onReaderRemove(pathReaderRemoveReq{author: r})
	}()

	var videoTrack *gortsplib.TrackH264
	videoTrackID := -1
	var h264Decoder *rtph264.Decoder
	var audioTrack *gortsplib.TrackAAC
	audioTrackID := -1
	var aacDecoder *rtpaac.Decoder

	for i, track := range res.stream.tracks() {
		switch tt := track.(type) {
		case *gortsplib.TrackH264:
			if videoTrack != nil {
				return fmt.Errorf("can't encode track %d with FLV: too many tracks", i+1)
			}

			videoTrack = tt
			videoTrackID = i
			h264Decoder = rtph264.NewDecoder()

		case *gortsplib.TrackAAC:
			if audioTrack != nil {
				return fmt.Errorf("can't encode track %d with FLV: too many tracks", i+1)
			}

			audioTrack = tt
			audioTrackID = i
			aacDecoder = rtpaac.NewDecoder(track.ClockRate())
		}
	}

	if videoTrack == nil && audioTrack == nil {
		return fmt.Errorf("the stream doesn't contain an H264 track or an AAC track")
	}

	r.videoTrack = videoTrack
	r.audioTrack = audioTrack

	innerReady <- struct{}{}
	r.ringBuffer = ringbuffer.New(uint64(r.readBufferCount))

	r.path.onReaderPlay(pathReaderPlayReq{
		author: r,
	})

	var videoStartPTS time.Duration
	videoDTSEst := h264.NewDTSEstimator()
	writerDone := make(chan error)
	videoFirstIDRFound := false

	go func() {
		writerDone <- func() error {
			for {
				data, ok := r.ringBuffer.Pull()
				if !ok {
					return fmt.Errorf("terminated")
				}
				pair := data.(flvMuxerTrackIDPayloadPair)

				if videoTrack != nil && pair.trackID == videoTrackID {
					nalus, pts, err := h264Decoder.DecodeUntilMarker(pair.packet)
					if err != nil {
						if err != rtph264.ErrMorePacketsNeeded &&
							err != rtph264.ErrNonStartingPacketAndNoPrevious {
							r.log(logger.Warn, "unable to decode video track: %v", err)
						}
						continue
					}

					var nalusFiltered [][]byte

					for _, nalu := range nalus {
						typ := h264.NALUType(nalu[0] & 0x1F)
						switch typ {
						case h264.NALUTypeSPS, h264.NALUTypePPS, h264.NALUTypeAccessUnitDelimiter:
							continue
						}

						nalusFiltered = append(nalusFiltered, nalu)
					}

					idrPresent := func() bool {
						for _, nalu := range nalus {
							typ := h264.NALUType(nalu[0] & 0x1F)
							if typ == h264.NALUTypeIDR {
								return true
							}
						}
						return false
					}()

					// wait until we receive an IDR
					if !videoFirstIDRFound {
						if !idrPresent {
							continue
						}

						videoFirstIDRFound = true
						videoStartPTS = pts
						videoDTSEst = h264.NewDTSEstimator()
					}
					data, err := h264.EncodeAVCC(nalusFiltered)
					if err != nil {
						return err
					}

					pts -= videoStartPTS
					dts := videoDTSEst.Feed(pts)
					pkg := av.Packet{
						Type:  av.H264,
						Data:  data,
						Time:  dts,
						CTime: pts - dts,
					}
					for q := range r.queues {
						select {
						case q <- pkg:
						default:
							delete(r.queues, q)
						}
					}

				} else if audioTrack != nil && pair.trackID == audioTrackID {
					aus, pts, err := aacDecoder.Decode(pair.packet)
					if err != nil {
						if err != rtpaac.ErrMorePacketsNeeded {
							r.log(logger.Warn, "unable to decode audio track: %v", err)
						}
						continue
					}

					if videoTrack != nil && !videoFirstIDRFound {
						continue
					}

					pts -= videoStartPTS
					if pts < 0 {
						continue
					}

					for _, au := range aus {
						pkg := av.Packet{
							Type: av.AAC,
							Data: au,
							Time: pts,
						}
						for q := range r.queues {
							select {
							case q <- pkg:
							default:
								delete(r.queues, q)
							}
						}

						pts += 1000 * time.Second / time.Duration(audioTrack.ClockRate())
					}
				}
			}
		}()
	}()
	closeCheckTicker := time.NewTicker(closeFlvCheckPeriod)
	defer closeCheckTicker.Stop()
	for {
		select {
		case <-closeCheckTicker.C:
			if len(r.queues) <= 0 {
				r.ringBuffer.Close()
				<-writerDone
				return fmt.Errorf("not used anymore")
			}

		case err := <-writerDone:
			r.ringBuffer.Close()
			return err

		case <-innerCtx.Done():
			r.ringBuffer.Close()
			<-writerDone
			return nil
		}
	}
}

func (r *flvMuxer) authenticate(req *http.Request) error {
	pathConf := r.path.Conf()
	pathIPs := pathConf.ReadIPs
	pathUser := pathConf.ReadUser
	pathPass := pathConf.ReadPass

	if r.externalAuthenticationURL != "" {
		tmp, _, _ := net.SplitHostPort(req.RemoteAddr)
		ip := net.ParseIP(tmp)
		user, pass, _ := req.BasicAuth()

		err := externalAuth(
			r.externalAuthenticationURL,
			ip.String(),
			user,
			pass,
			r.pathName,
			"read",
			req.URL.RawQuery)
		if err != nil {
			return pathErrAuthCritical{
				message: fmt.Sprintf("external authentication failed: %s", err),
			}
		}
	}

	if pathIPs != nil {
		tmp, _, _ := net.SplitHostPort(req.RemoteAddr)
		ip := net.ParseIP(tmp)

		if !ipEqualOrInRange(ip, pathIPs) {
			return pathErrAuthCritical{
				message: fmt.Sprintf("IP '%s' not allowed", ip),
			}
		}
	}

	if pathUser != "" {
		user, pass, ok := req.BasicAuth()
		if !ok {
			return pathErrAuthNotCritical{}
		}

		if user != string(pathUser) || pass != string(pathPass) {
			return pathErrAuthCritical{
				message: "invalid credentials",
			}
		}
	}
	return nil
}

func (r *flvMuxer) handleRequest(req flvSession) {
	err := r.authenticate(req.Req)
	if err != nil {
		if terr, ok := err.(pathErrAuthCritical); ok {
			r.log(logger.Info, "authentication error: %s", terr.message)
			err = req.setHeaderFunc(flvResponse{
				Status: http.StatusUnauthorized,
			})
			if err != nil {
				req.colsed()
			}
			return
		}

		err = req.setHeaderFunc(flvResponse{
			Status: http.StatusUnauthorized,
			Header: map[string]string{
				"WWW-Authenticate": `Basic realm="rtsp-simple-server"`,
			},
		})
		if err != nil {
			req.colsed()
		}
		return
	}
	err = req.setHeaderFunc(flvResponse{
		Status: http.StatusOK,
	})
	if err != nil {
		req.colsed()
		return
	}

	err = req.WriteMetadata(r.videoTrack, r.audioTrack)
	if err != nil {
		req.colsed()
		return
	}

	r.mutex.Lock()
	r.queues[req.Queue] = struct{}{}
	r.mutex.Unlock()
}

func (r *flvMuxer) close() {
	r.ctxCancel()
}

// onRequest is called by hlsserver.Server (forwarded from ServeHTTP).
func (m *flvMuxer) onRequest(req flvSession) {
	select {
	case m.request <- req:
	case <-m.ctx.Done():
		req.setHeaderFunc(flvResponse{Status: http.StatusNotFound})
	}
}

// onReaderAccepted implements reader.
func (r *flvMuxer) onReaderAccepted() {
	r.log(logger.Info, "is converting into FLV")
}

// onReaderPacketRTP implements reader.
func (r *flvMuxer) onReaderPacketRTP(trackID int, pkt *rtp.Packet) {
	r.ringBuffer.Push(flvMuxerTrackIDPayloadPair{trackID, pkt})
}

// onReaderPacketRTCP implements reader.
func (r *flvMuxer) onReaderPacketRTCP(trackID int, pkt rtcp.Packet) {
}

func (r *flvMuxer) onSessionClose(s flvSession) {
	select {
	case r.closed <- s:
	case <-r.ctx.Done():
	}
}

// onReaderAPIDescribe implements reader.
func (r *flvMuxer) onReaderAPIDescribe() interface{} {
	return struct {
		Type string `json:"type"`
	}{"flvMuxer"}
}
