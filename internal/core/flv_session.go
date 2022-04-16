package core

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/aac"
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
	onSessionClose(flvSession)
}

type flvSession struct {
	wg            sync.WaitGroup
	Path          string
	Req           *http.Request
	wait          chan struct{}
	Queue         chan av.Packet
	muxer         *flv.Muxer
	parent        flvSessionParent
	setHeaderFunc setHeaderFunc
}

type setHeaderFunc func(
	flvResponse flvResponse,
) error

type flvResponse struct {
	Status int
	Header map[string]string
}

func newFlvSession(
	wg sync.WaitGroup,
	path string,
	req *http.Request,
	w io.Writer,
	wait chan struct{},
	parent flvSessionParent,
	setHeaderFunc setHeaderFunc,
) flvSession {
	s := flvSession{
		wg:            wg,
		Path:          path,
		Req:           req,
		muxer:         flv.NewMuxer(w),
		wait:          wait,
		Queue:         make(chan av.Packet, 100),
		parent:        parent,
		setHeaderFunc: setHeaderFunc,
	}

	s.log(logger.Info, "created")

	wg.Add(1)
	go s.run()
	return s
}

func (s *flvSession) run() {
	defer s.wg.Done()
	defer s.log(logger.Info, "destroyed")
outer:
	for {
		select {
		case pkg, ok := <-s.Queue:
			if !ok {
				s.log(logger.Info, "muxer queue closed")
				break outer
			}

			err := s.muxer.WritePacket(pkg)
			if err != nil {
				s.log(logger.Info, "error: %v", err)
				break outer
			}
		case <-time.After(30 * time.Second):
			s.log(logger.Info, "muxer queue timeout")
			break outer
		}
	}

	s.colsed()
}

func (s flvSession) log(level logger.Level, format string, args ...interface{}) {
	s.parent.log(level, "[flv session %s] "+format, append([]interface{}{s.Path}, args...)...)
}

func (s flvSession) WriteMetadata(videoTrack *gortsplib.TrackH264, audioTrack *gortsplib.TrackAAC) error {
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

	if videoTrack != nil {
		if videoTrack.SPS() == nil || videoTrack.PPS() == nil {
			return fmt.Errorf("invalid H264 track: SPS or PPS not provided into the SDP")
		}
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

func (s flvSession) colsed() {
	s.parent.onSessionClose(s)
	s.wait <- struct{}{}
}
