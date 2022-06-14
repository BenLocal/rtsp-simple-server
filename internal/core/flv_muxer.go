package core

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/aac"
	"github.com/aler9/gortsplib/pkg/h264"
	"github.com/aler9/gortsplib/pkg/ringbuffer"
	"github.com/aler9/gortsplib/pkg/rtpaac"
	"github.com/aler9/rtsp-simple-server/internal/logger"
	"github.com/notedit/rtmp/av"
	nh264 "github.com/notedit/rtmp/codec/h264"
)

const (
	closeFlvCheckPeriod = 60 * time.Second
)

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

	requests []*flvSession
	sessions map[string]*flvSession

	// in
	request       chan *flvSession
	requestClosed chan *flvSession
	notUsed       bool
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
		request:                   make(chan *flvSession),
		requestClosed:             make(chan *flvSession),
		sessions:                  make(map[string]*flvSession),
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

func (r *flvMuxer) NotUsed() bool {
	return r.notUsed
}

func (r *flvMuxer) run() {
	defer r.wg.Done()
	defer r.log(logger.Info, "destroyed")

	innerCtx, innerCtxCancel := context.WithCancel(r.ctx)
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

			case req := <-r.requestClosed:
				if _, ok := r.sessions[req.Id()]; !ok {
					continue
				}
				delete(r.sessions, req.Id())

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
		author:   r,
		pathName: r.pathName,
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
	var audioTrack *gortsplib.TrackAAC
	audioTrackID := -1
	var aacDecoder *rtpaac.Decoder

	for i, track := range res.stream.tracks() {
		switch tt := track.(type) {
		case *gortsplib.TrackH264:
			if videoTrack != nil {
				return fmt.Errorf("can't encode track %d with HLS: too many tracks", i+1)
			}

			videoTrack = tt
			videoTrackID = i

		case *gortsplib.TrackAAC:
			if audioTrack != nil {
				return fmt.Errorf("can't encode track %d with HLS: too many tracks", i+1)
			}

			audioTrack = tt
			audioTrackID = i
			audioTrack = tt
			audioTrackID = i
			aacDecoder = &rtpaac.Decoder{
				SampleRate:       tt.ClockRate(),
				SizeLength:       tt.SizeLength(),
				IndexLength:      tt.IndexLength(),
				IndexDeltaLength: tt.IndexDeltaLength(),
			}
			aacDecoder.Init()
		}
	}

	if videoTrack == nil && audioTrack == nil {
		return fmt.Errorf("the stream doesn't contain an H264 track or an AAC track")
	}

	innerReady <- struct{}{}
	r.ringBuffer = ringbuffer.New(uint64(r.readBufferCount))

	r.path.onReaderPlay(pathReaderPlayReq{
		author: r,
	})

	var videoInitialPTS *time.Duration
	videoFirstIDRFound := false
	var videoStartDTS time.Duration
	var videoDTSExtractor *h264.DTSExtractor

	writerDone := make(chan error)

	go func() {
		writerDone <- func() error {
			for {
				item, ok := r.ringBuffer.Pull()
				if !ok {
					return fmt.Errorf("terminated")
				}
				data := item.(*data)

				if videoTrack != nil && data.trackID == videoTrackID {
					if data.h264NALUs == nil {
						continue
					}

					// video is decoded in another routine,
					// while audio is decoded in this routine:
					// we have to sync their PTS.
					if videoInitialPTS == nil {
						v := data.h264PTS
						videoInitialPTS = &v
					}
					pts := data.h264PTS - *videoInitialPTS
					idrPresent := false
					nonIDRPresent := false

					for _, nalu := range data.h264NALUs {
						typ := h264.NALUType(nalu[0] & 0x1F)
						switch typ {
						case h264.NALUTypeIDR:
							idrPresent = true

						case h264.NALUTypeNonIDR:
							nonIDRPresent = true
						}
					}

					var dts time.Duration

					// wait until we receive an IDR
					if !videoFirstIDRFound {
						if !idrPresent {
							continue
						}

						videoFirstIDRFound = true
						videoDTSExtractor = h264.NewDTSExtractor()

						var err error
						dts, err = videoDTSExtractor.Extract(data.h264NALUs, pts)
						if err != nil {
							return err
						}

						videoStartDTS = dts
						dts = 0
						pts -= videoStartDTS
					} else {
						if !idrPresent && !nonIDRPresent {
							continue
						}

						var err error
						dts, err = videoDTSExtractor.Extract(data.h264NALUs, pts)
						if err != nil {
							return err
						}

						dts -= videoStartDTS
						pts -= videoStartDTS
					}

					if h264.IDRPresent(data.h264NALUs) {
						codec := nh264.Codec{
							SPS: map[int][]byte{
								0: videoTrack.SPS(),
							},
							PPS: map[int][]byte{
								0: videoTrack.PPS(),
							},
						}
						b := make([]byte, 128)
						var n int
						codec.ToConfig(b, &n)
						b = b[:n]
						pkg := av.Packet{
							Type: av.H264DecoderConfig,
							Data: b,
						}
						for _, q := range r.sessions {
							q.Push(videoTrack, audioTrack, pkg)
						}
					}

					avcc, err := h264.AVCCEncode(data.h264NALUs)
					if err != nil {
						return err
					}

					pkg := av.Packet{
						Type:  av.H264,
						Data:  avcc,
						Time:  dts,
						CTime: pts - dts,
					}
					for _, q := range r.sessions {
						q.Push(videoTrack, audioTrack, pkg)
					}

				} else if audioTrack != nil && data.trackID == audioTrackID {
					aus, pts, err := aacDecoder.Decode(data.rtp)
					if err != nil {
						if err != rtpaac.ErrMorePacketsNeeded {
							r.log(logger.Warn, "unable to decode audio track: %v", err)
						}
						continue
					}

					if videoTrack != nil && !videoFirstIDRFound {
						continue
					}

					pts -= videoStartDTS
					if pts < 0 {
						continue
					}

					for i, au := range aus {
						pkg := av.Packet{
							Type: av.AAC,
							Data: au,
							Time: pts + time.Duration(i)*aac.SamplesPerAccessUnit*time.Second/time.Duration(audioTrack.ClockRate()),
						}
						for _, q := range r.sessions {
							q.Push(videoTrack, audioTrack, pkg)
						}
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
			if len(r.sessions) <= 0 {
				r.ringBuffer.Close()
				r.notUsed = true
				return fmt.Errorf("not used anymore")
			}

		case err := <-writerDone:
			r.ringBuffer.Close()
			return err

		case <-innerCtx.Done():
			r.ringBuffer.Close()
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

func (r *flvMuxer) handleRequest(req *flvSession) {
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

	err = req.WriteHeader()
	if err != nil {
		req.colsed()
		return
	}

	r.sessions[req.Id()] = req
}

func (s *flvMuxer) createSession(req *flvRequest, m *flvMuxer) *flvSession {
	id, _ := s.newConnID()
	return newFlvSession(
		id,
		req.path,
		req.r,
		req.w,
		req.wait,
		m,
		req.setHeaderFunc,
		s.readBufferCount,
		s.ctx,
	)
}

func (r *flvMuxer) close() {
	r.ctxCancel()
}

// onRequest is called by hlsserver.Server (forwarded from ServeHTTP).
func (m *flvMuxer) onRequest(req *flvSession) {
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
func (r *flvMuxer) onReaderData(data *data) {
	r.ringBuffer.Push(data)
}

func (r *flvMuxer) onSessionClose(s *flvSession) {
	select {
	case r.requestClosed <- s:
	case <-r.ctx.Done():
	}
}

// onReaderAPIDescribe implements reader.
func (r *flvMuxer) onReaderAPIDescribe() interface{} {
	return struct {
		Type string `json:"type"`
	}{"flvMuxer"}
}

func (s *flvMuxer) newConnID() (string, error) {
	for {
		b := make([]byte, 4)
		_, err := rand.Read(b)
		if err != nil {
			return "", err
		}

		u := binary.LittleEndian.Uint32(b)
		u %= 899999999
		u += 100000000

		id := strconv.FormatUint(uint64(u), 10)

		alreadyPresent := func() bool {
			for _, c := range s.sessions {
				if c.Id() == id {
					return true
				}
			}
			return false
		}()
		if !alreadyPresent {
			return id, nil
		}
	}
}
