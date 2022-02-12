package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/aler9/gortsplib"
	"github.com/aler9/rtsp-simple-server/internal/conf"
	"github.com/aler9/rtsp-simple-server/internal/logger"

	_ "github.com/mattn/go-sqlite3"
)

const (
	default_db_file  = "./data/server.db"
	select_by_id_sql = `select id
		,source
		,sourceProtocol
		,sourceOnDemand
		from paths where id = ?`
	insert_sql = `insert into paths(id
		,source
		,sourceProtocol
		,sourceOnDemand)
		values(?, ?, ?, ?)`
	update_sql = `update paths set
		source = ?
		,sourceProtocol = ?
		,sourceOnDemand = ?
		where id = ?`
	delete_sql      = `delete from paths where id = ?`
	select_list_sql = `select id
		,source
		,sourceProtocol
		,sourceOnDemand
		from paths`
)

type PathInfo struct {
	Path           string
	Source         string
	SourceProtocol string
	SourceOnDemand bool
}

type pathStoreParent interface {
	Log(logger.Level, string, ...interface{})
}

type pathStore struct {
	conf      *conf.Conf
	parent    pathStoreParent
	storePath string
}

func newStore(
	c *conf.Conf,
	parent pathStoreParent,
) (s *pathStore) {
	s = &pathStore{
		parent:    parent,
		conf:      c,
		storePath: default_db_file,
	}

	if len(s.conf.StorePath) != 0 {
		s.storePath = s.conf.StorePath
	}

	ext := filepath.Ext(s.storePath)
	if ext != ".db" {
		s.log(logger.Error, "StorePath must end with .db suffix ")
		return nil
	}

	dir, _ := filepath.Split(s.storePath)
	if _, err := os.Stat(dir); err != nil {
		err := os.Mkdir(dir, fs.ModeDir)
		if err != nil {
			s.log(logger.Error, "%s ", err)
			return nil
		}
	}

	_, err := os.Stat(s.storePath)
	if err != nil && os.IsNotExist(err) {
		db, err := sql.Open("sqlite3", s.storePath)
		if err != nil {
			s.log(logger.Error, "%s ", err)
			return nil
		}
		defer db.Close()
		sqlStmt := `CREATE TABLE paths (
			id             VARCHAR (50) NOT NULL
										PRIMARY KEY,
			source         TEXT,
			sourceProtocol VARCHAR (20) DEFAULT automatic,
			sourceOnDemand BOOLEAN      DEFAULT (1) 
		);
		`
		_, err = db.Exec(sqlStmt)
		if err != nil {
			s.log(logger.Error, "%s ", err)
			return nil
		}
	}

	paths, err := s.pathList()
	if err != nil {
		s.log(logger.Error, "%s ", err)
		return s
	}
	var newConf conf.Conf
	s.cloneStruct(&newConf, c)
	for _, path := range paths {
		sp, err := s.getSourceProtocol(path.SourceProtocol)
		if err != nil {
			continue
		}
		newConf.Paths[path.Path] = &conf.PathConf{
			Source:         path.Source,
			SourceProtocol: sp,
			SourceOnDemand: path.SourceOnDemand,
		}
	}

	err = newConf.CheckAndFillMissing()
	if err != nil {
		s.log(logger.Error, "%s ", err)
		return
	}

	s.conf = &newConf
	return s
}

func (s *pathStore) pathList() (info []*PathInfo, err error) {
	db, err := sql.Open("sqlite3", s.storePath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(select_list_sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var a []*PathInfo
	for rows.Next() {
		ps := &PathInfo{}
		err = rows.Scan(
			&ps.Path,
			&ps.Source,
			&ps.SourceProtocol,
			&ps.SourceOnDemand,
		)
		if err != nil {
			continue
		}
		a = append(a, ps)
	}

	return a, nil
}

func (s *pathStore) onAddOrUpdatePath(info *PathInfo) error {
	db, err := sql.Open("sqlite3", s.storePath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// get path info from db
	stmt, err := tx.Prepare(select_by_id_sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	ps := &PathInfo{}
	err = stmt.QueryRow(info.Path).Scan(
		&ps.Path,
		&ps.Source,
		&ps.SourceProtocol,
		&ps.SourceOnDemand,
	)

	if err == sql.ErrNoRows {
		stmt, err = tx.Prepare(insert_sql)
		if err != nil {
			return err
		}
		defer stmt.Close()

		_, err = stmt.Exec(
			info.Path,
			info.Source,
			info.SourceProtocol,
			info.SourceOnDemand,
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	} else if err == nil {
		stmt, err = tx.Prepare(update_sql)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		_, err = stmt.Exec(
			info.Source,
			info.SourceProtocol,
			info.SourceOnDemand,

			info.Path,
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	tx.Commit()
	return nil
}

func (s *pathStore) onDeletePath(path string) error {
	db, err := sql.Open("sqlite3", s.storePath)
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare("delete from paths where id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(path)
	if err != nil {
		return err
	}

	return nil
}

func (a *pathStore) log(level logger.Level, format string, args ...interface{}) {
	a.parent.Log(level, "[PATH DB] "+format, args...)
}

func (a *pathStore) cloneStruct(dest interface{}, source interface{}) {
	enc, _ := json.Marshal(source)
	_ = json.Unmarshal(enc, dest)
}

func (a *pathStore) getSourceProtocol(in string) (sp conf.SourceProtocol, err error) {
	switch in {
	case "udp":
		v := gortsplib.TransportUDP
		return conf.SourceProtocol{Transport: &v}, nil

	case "multicast":
		v := gortsplib.TransportUDPMulticast
		return conf.SourceProtocol{Transport: &v}, nil

	case "tcp":
		v := gortsplib.TransportTCP
		return conf.SourceProtocol{Transport: &v}, nil

	case "automatic":

	default:
		return conf.SourceProtocol{}, fmt.Errorf("invalid protocol '%s'", in)
	}

	return conf.SourceProtocol{}, nil
}

func (a *pathStore) onStrSourceProtocol(in conf.SourceProtocol) string {
	var out string

	if in.Transport == nil {
		out = "automatic"
	} else {
		switch *in.Transport {
		case gortsplib.TransportUDP:
			out = "udp"

		case gortsplib.TransportUDPMulticast:
			out = "multicast"

		default:
			out = "tcp"
		}
	}

	return out
}
