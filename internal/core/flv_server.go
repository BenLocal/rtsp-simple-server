package core

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/aler9/rtsp-simple-server/internal/logger"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type WebSocketWriter struct {
	c *websocket.Conn
}

func (w *WebSocketWriter) Write(p []byte) (n int, err error) {
	if err = w.c.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

type flvServerParent interface {
	Log(logger.Level, string, ...interface{})
}

type flvServer struct {
	parent                    flvServerParent
	ctx                       context.Context
	ctxCancel                 func()
	readBufferCount           int
	wg                        sync.WaitGroup
	ln                        net.Listener
	pathManager               *pathManager
	muxers                    map[string]*flvMuxer
	externalAuthenticationURL string

	// in
	request    chan *flvRequest
	muxerClose chan *flvMuxer
}

type flvRequest struct {
	path          string
	r             *http.Request
	w             io.Writer
	wait          chan struct{}
	setHeaderFunc setHeaderFunc
	mute          bool
}

func newFlvServer(
	parentCtx context.Context,
	address string,
	pathManager *pathManager,
	parent flvServerParent,
	readBufferCount int,
	externalAuthenticationURL string,
) (*flvServer, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(parentCtx)

	s := &flvServer{
		ln:                        ln,
		parent:                    parent,
		pathManager:               pathManager,
		ctx:                       ctx,
		ctxCancel:                 ctxCancel,
		readBufferCount:           readBufferCount,
		muxers:                    make(map[string]*flvMuxer),
		request:                   make(chan *flvRequest),
		muxerClose:                make(chan *flvMuxer),
		externalAuthenticationURL: externalAuthenticationURL,
	}

	s.Log(logger.Info, "listener opened on "+address)

	s.wg.Add(1)
	go s.run()

	return s, nil
}

// Log is the main logging function.
func (s *flvServer) Log(level logger.Level, format string, args ...interface{}) {
	s.parent.Log(level, "[FLV] "+format, append([]interface{}{}, args...)...)
}

func (s *flvServer) close() {
	s.ctxCancel()
	s.wg.Wait()
	s.Log(logger.Info, "closed")
}

func (s *flvServer) run() {
	defer s.wg.Done()

	router := gin.New()
	router.NoRoute(s.onRequest)

	hs := &http.Server{Handler: router}
	go hs.Serve(s.ln)

outer:
	for {
		select {
		case req := <-s.request:
			m := s.findOrCreateMuxer(req.path)
			r := m.createSession(req, m)
			m.onRequest(r)

		case c := <-s.muxerClose:
			if c2, ok := s.muxers[c.Path()]; !ok || c2 != c {
				continue
			}
			delete(s.muxers, c.Path())

		case <-s.ctx.Done():
			break outer
		}
	}

	s.ctxCancel()

	hs.Shutdown(context.Background())
}

func (s *flvServer) onRequest(ctx *gin.Context) {
	s.Log(logger.Info, "[conn %v] %s %s", ctx.Request.RemoteAddr, ctx.Request.Method, ctx.Request.URL.Path)

	var hreq *flvRequest
	wait := make(chan struct{})
	if ctx.IsWebsocket() {
		webSocketKey := ctx.Request.Header.Get("Sec-WebSocket-Key")
		hreq = s.onWebSocketRequest(ctx, webSocketKey, wait)
	} else {
		hreq = s.onHttpRequest(ctx, wait)
	}

	if hreq == nil {
		return
	}
	s.request <- hreq
	select {
	case <-wait:
	case <-s.ctx.Done():
	}
}

func (s *flvServer) onWebSocketRequest(ctx *gin.Context, webSocketKey string, wait chan struct{}) *flvRequest {
	w := ctx.Writer
	r := ctx.Request
	pa := ctx.Request.URL.Path[1:]
	muted := ctx.Request.URL.Query().Get("mute") == "1"
	w.Header().Set("Content-Type", "video/x-flv")
	w.Header().Set("Transfer-Encoding", "chunked")
	conn, err := (&websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}).Upgrade(
		w,
		r,
		nil,
	)
	if err != nil {
		http.NotFound(w, r)
		return nil
	}

	var writer WebSocketWriter
	writer.c = conn

	dir := strings.TrimSuffix(pa, "/")

	return &flvRequest{
		path: dir,
		r:    r,
		w:    &writer,
		wait: wait,
		mute: muted,
		setHeaderFunc: func(r flvResponse) error {
			for k, v := range r.Header {
				ctx.Writer.Header().Set(k, v)
			}
			ctx.Writer.WriteHeader(r.Status)
			if r.Status != 200 {
				return errors.New("fail status code")
			}
			return nil
		},
	}
}

func (s *flvServer) onHttpRequest(ctx *gin.Context, wait chan struct{}) *flvRequest {
	w := ctx.Writer
	r := ctx.Request
	pa := ctx.Request.URL.Path[1:]
	muted := ctx.Request.URL.Query().Get("mute") == "1"
	w.Header().Set("Server", "rtsp-simple-server")
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Access-Control-Allow-Credentials", "true")

	switch r.Method {
	case http.MethodGet:

	case http.MethodOptions:
		w.Header().Add("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Add("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
		w.WriteHeader(http.StatusOK)
		return nil

	default:
		w.WriteHeader(http.StatusNotFound)
		return nil
	}

	switch pa {
	case "", "favicon.ico":
		w.WriteHeader(http.StatusNotFound)
		return nil
	}

	dir := strings.TrimSuffix(pa, "/")
	w.Header().Set("Content-Type", "video/x-flv")
	w.Header().Set("Transfer-Encoding", "chunked")
	return &flvRequest{
		path: dir,
		r:    r,
		w:    w,
		wait: wait,
		mute: muted,
		setHeaderFunc: func(r flvResponse) error {
			for k, v := range r.Header {
				ctx.Writer.Header().Set(k, v)
			}
			ctx.Writer.WriteHeader(r.Status)
			if r.Status != 200 {
				return errors.New("fail status code")
			}
			return nil
		},
	}
}

func (s *flvServer) findOrCreateMuxer(path string) *flvMuxer {
	fm, ok := s.muxers[path]
	if !ok || fm.NotUsed() {
		fm = newFlvMuxer(
			s.ctx,
			&s.wg,
			path,
			s.pathManager,
			s,
			s.readBufferCount,
			s.externalAuthenticationURL,
		)
		s.muxers[path] = fm
		s.Log(logger.Info, "muxer (%s) created", path)
	}

	return fm
}

// onMuxerClose is called by flvMuxer.
func (s *flvServer) onMuxerClose(c *flvMuxer) {
	select {
	case s.muxerClose <- c:
	case <-s.ctx.Done():
	}
}
