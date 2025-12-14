package treedb

import "github.com/snissn/gomap/TreeDB/db"

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = db.ErrLocked
)
