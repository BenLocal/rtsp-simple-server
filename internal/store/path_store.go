package store

import "github.com/aler9/rtsp-simple-server/internal/conf"

type PathStore interface {
	All() ([]*conf.PathConf, error)
	Insert(path *conf.PathConf) error
	Update(path *conf.PathConf) error
	Delete(pathSource string) error
}
