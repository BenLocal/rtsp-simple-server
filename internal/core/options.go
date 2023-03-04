package core

import "github.com/aler9/rtsp-simple-server/internal/store"

func UsePathStore(pathStore store.PathStore) func(*Core) error {
	return func(c *Core) error {
		c.pathStore = pathStore
		return nil
	}
}
