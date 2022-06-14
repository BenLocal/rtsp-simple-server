package core

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/aac"
	"github.com/aler9/gortsplib/pkg/ringbuffer"
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

	videoTrack *gortsplib.TrackH264
	audioTrack *gortsplib.TrackAAC
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

func (s *flvSession) Push(videoTrack *gortsplib.TrackH264, audioTrack *gortsplib.TrackAAC, pkg av.Packet) {
	if s.videoTrack == nil {
		s.videoTrack = videoTrack
	}

	if s.audioTrack == nil {
		s.audioTrack = audioTrack
	}

	s.ringBuffer.Push(pkg)
}

func (s *flvSession) run() {
	defer s.log(logger.Info, "destroyed")

	err := make(chan error)

	go func() {
		err <- func() error {
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

				p, ok := item.(av.Packet)
				if !ok {
					continue
				}

				err := s.muxer.WritePacket(p)
				if err != nil {
					return err
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
