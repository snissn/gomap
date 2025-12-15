package treedb

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/db"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = db.ErrLocked

	// ErrClosed indicates the DB handle has been closed.
	ErrClosed = errors.New("treedb: db is closed")
)
