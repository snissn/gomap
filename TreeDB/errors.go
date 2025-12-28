package treedb

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = db.ErrLocked
	// ErrUnsafeOptions indicates unsafe durability/integrity options were set without acknowledgement.
	ErrUnsafeOptions = db.ErrUnsafeOptions
	// ErrMemtableFull indicates the cached memtable has reached its hard cap.
	ErrMemtableFull = caching.ErrMemtableFull
	// ErrMemtableValueLogPointers indicates memtable value-log pointers require WAL/value-log enabled.
	ErrMemtableValueLogPointers = caching.ErrMemtableValueLogPointers

	// ErrClosed indicates the DB handle has been closed.
	ErrClosed = errors.New("treedb: db is closed")

	// ErrKeyNotFound indicates the key does not exist.
	ErrKeyNotFound = tree.ErrKeyNotFound
)
