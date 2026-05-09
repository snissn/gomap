package treedb

import (
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = db.ErrLocked
	// ErrMemtableFull indicates the cached memtable has reached its hard cap.
	ErrMemtableFull = caching.ErrMemtableFull

	// ErrClosed indicates the DB handle has been closed.
	ErrClosed = db.ErrClosed
	// ErrRecoveryRequired indicates the DB must be opened read-write for recovery
	// before the requested read-only or offline-maintenance operation can run.
	ErrRecoveryRequired = db.ErrRecoveryRequired

	// ErrKeyNotFound indicates the key does not exist.
	ErrKeyNotFound = tree.ErrKeyNotFound
)
