package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/aac"
	"github.com/aler9/gortsplib/pkg/h264"
	"github.com/aler9/gortsplib/pkg/ringbuffer"
	"github.com/aler9/gortsplib/pkg/rtpaac"
	"github.com/aler9/rtsp-simple-server/internal/logger"
	"github.com/notedit/rtmp/av"
	nh264 "github.com/notedit/rtmp/codec/h264"
	"github.com/notedit/rtmp/format/flv"
	"github.com/notedit/rtmp/format/flv/flvio"
)

const (
	codecH264 = 7
	codecAAC  = 10
)

type flvSessionParent interface {
	log(logger.Level, string, ...interface{})
	onSessionClose(*flvSession)
}

type flvSession struct {
	path            string
	Req             *http.Request
	wait            chan struct{}
	ringBuffer      *ringbuffer.RingBuffer
	muxer           *flv.Muxer
	parent          flvSessionParent
	setHeaderFunc   setHeaderFunc
	readBufferCount int
	id              string

	ctx                context.Context
	ctxCancel          func()
	isWriteMetadata    bool
	writeMetadataError chan error
	mute               bool

	videoTrack   *gortsplib.TrackH264
	audioTrack   *gortsplib.TrackAAC
	videoTrackID int
	audioTrackID int
	aacDecoder   *rtpaac.Decoder
}

type setHeaderFunc func(
	flvResponse flvResponse,
) error

type flvResponse struct {
	Status int
	Header map[string]string
}

func newFlvSession(
	id string,
	path string,
	req *http.Request,
	w io.Writer,
	wait chan struct{},
	parent flvSessionParent,
	setHeaderFunc setHeaderFunc,
	readBufferCount int,
	parentCtx context.Context,
	mute bool,
) *flvSession {
	ctx, ctxCancel := context.WithCancel(parentCtx)

	s := &flvSession{
		id:              id,
		path:            path,
		Req:             req,
		muxer:           flv.NewMuxer(w),
		wait:            wait,
		parent:          parent,
		setHeaderFunc:   setHeaderFunc,
		readBufferCount: readBufferCount,
		ringBuffer:      ringbuffer.New(uint64(readBufferCount)),
		mute:            mute,

		ctx:                ctx,
		ctxCancel:          ctxCancel,
		isWriteMetadata:    false,
		writeMetadataError: make(chan error),
	}

	s.log(logger.Info, "created")

	go s.run()
	return s
}

func (s *flvSession) Id() string {
	return s.id
}

func (s *flvSession) Push(videoTrack *gortsplib.TrackH264,
	audioTrack *gortsplib.TrackAAC,
	videoTrackID int,
	audioTrackID int,
	data *data) {
	if s.videoTrack == nil {
		s.videoTrack = videoTrack
		s.videoTrackID = videoTrackID
	}

	if !s.mute && s.audioTrack == nil {
		s.audioTrack = audioTrack
		s.audioTrackID = audioTrackID
		s.aacDecoder = &rtpaac.Decoder{
			SampleRate:       audioTrack.ClockRate(),
			SizeLength:       audioTrack.SizeLength(),
			IndexLength:      audioTrack.IndexLength(),
			IndexDeltaLength: audioTrack.IndexDeltaLength(),
		}
		s.aacDecoder.Init()
	}

	s.ringBuffer.Push(data)
}

func (s *flvSession) run() {
	defer s.log(logger.Info, "destroyed")

	err := make(chan error)

	go func() {
		err <- func() error {

			var videoInitialPTS *time.Duration
			videoFirstIDRFound := false
			var videoStartDTS time.Duration
			var videoDTSExtractor *h264.DTSExtractor

			for {
				item, ok := s.ringBuffer.Pull()
				if !ok {
					return fmt.Errorf("RingBuffer terminated")
				}

				if !s.isWriteMetadata {
					s.isWriteMetadata = true
					// s.mutex.Lock()
					// defer s.mutex.Unlock()
					if s.videoTrack == nil && s.audioTrack == nil {
						s.isWriteMetadata = false
						continue
					}

					s.log(logger.Info, "send metadata")
					err := s.writeMetadata(s.videoTrack, s.audioTrack)
					if err != nil {
						s.writeMetadataError <- err
						s.isWriteMetadata = false
						continue
					}
				}

				data := item.(*data)
				if s.videoTrack != nil && data.trackID == s.videoTrackID {
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
								0: s.videoTrack.SPS(),
							},
							PPS: map[int][]byte{
								0: s.videoTrack.PPS(),
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
						err := s.muxer.WritePacket(pkg)
						if err != nil {
							return err
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

					err = s.muxer.WritePacket(pkg)
					if err != nil {
						return err
					}

				} else if s.audioTrack != nil && data.trackID == s.audioTrackID {
					aus, pts, err := s.aacDecoder.Decode(data.rtp)
					if err != nil {
						if err != rtpaac.ErrMorePacketsNeeded {
							s.log(logger.Warn, "unable to decode audio track: %v", err)
						}
						continue
					}

					if s.videoTrack != nil && !videoFirstIDRFound {
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
							Time: pts + time.Duration(i)*aac.SamplesPerAccessUnit*time.Second/time.Duration(s.audioTrack.ClockRate()),
						}

						err = s.muxer.WritePacket(pkg)
						if err != nil {
							return err
						}
					}
				}
			}
		}()
	}()
outer:
	for {
		select {
		case err := <-err:
			if err != nil {
				s.log(logger.Info, "error: %v", err)
				break outer
			}
		case <-s.ctx.Done():
			break outer
		case err := <-s.writeMetadataError:
			s.log(logger.Info, "error: %v", err)
			break outer
		}
	}
	s.colsed()
}

func (s *flvSession) log(level logger.Level, format string, args ...interface{}) {
	s.parent.log(level, "[flv session %s] "+format, append([]interface{}{s.id}, args...)...)
}

func (s *flvSession) writeMetadata(videoTrack *gortsplib.TrackH264, audioTrack *gortsplib.TrackAAC) error {
	pkt := av.Packet{
		Type: av.Metadata,
		Data: flvio.FillAMF0ValMalloc(flvio.AMFMap{
			{
				K: "videodatarate",
				V: float64(0),
			},
			{
				K: "videocodecid",
				V: func() float64 {
					if videoTrack != nil {
						return codecH264
					}
					return 0
				}(),
			},
			{
				K: "audiodatarate",
				V: float64(0),
			},
			{
				K: "audiocodecid",
				V: func() float64 {
					if audioTrack != nil {
						return codecAAC
					}
					return 0
				}(),
			},
		}),
	}

	// write metadata
	err := s.muxer.WritePacket(pkt)
	if err != nil {
		return err
	}

	if videoTrack != nil && videoTrack.SPS() != nil && videoTrack.PPS() != nil {
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

		err = s.muxer.WritePacket(av.Packet{
			Type: av.H264DecoderConfig,
			Data: b,
		})
		if err != nil {
			return err
		}
	}

	if audioTrack != nil {
		enc, err := aac.MPEG4AudioConfig{
			Type:              aac.MPEG4AudioType(audioTrack.Type()),
			SampleRate:        audioTrack.ClockRate(),
			ChannelCount:      audioTrack.ChannelCount(),
			AOTSpecificConfig: audioTrack.AOTSpecificConfig(),
		}.Encode()
		if err != nil {
			return err
		}

		err = s.muxer.WritePacket(av.Packet{
			Type: av.AACDecoderConfig,
			Data: enc,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *flvSession) WriteHeader() error {
	return s.muxer.WriteFileHeader()
}

func (s *flvSession) colsed() {
	s.ctxCancel()
	s.parent.onSessionClose(s)
	s.ringBuffer.Close()
	s.wait <- struct{}{}
}
