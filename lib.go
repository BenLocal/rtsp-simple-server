package main

import "github.com/aler9/rtsp-simple-server/internal/core"

type serve struct {
	core *core.Core
}

func New(args []string, options ...func(*core.Core) error) (*serve, bool) {
	core, ok := core.New(args, options...)
	if !ok {
		return nil, false
	}
	return &serve{
		core: core,
	}, true
}

func (s *serve) Wait() {
	s.core.Wait()
}
