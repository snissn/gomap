package treedb

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = db.ErrLocked

	// ErrClosed indicates the DB handle has been closed.
	ErrClosed = errors.New("treedb: db is closed")

	// ErrKeyNotFound indicates the key does not exist.
	ErrKeyNotFound = tree.ErrKeyNotFound
)
