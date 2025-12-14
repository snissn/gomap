package treedb

import (
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
)

// Options configures TreeDB. It is re-exported from TreeDB/db for convenience.
type Options = db.Options

// DB is the primary TreeDB entrypoint: the cached DB wrapper.
type DB = caching.DB

// Open opens TreeDB using the cached DB wrapper (recommended default).
func Open(opts Options) (*DB, error) {
	return db.OpenCached(opts)
}

// OpenCached is an explicit alias for Open.
func OpenCached(opts Options) (*DB, error) {
	return Open(opts)
}

// OpenBackend opens the underlying uncached DB directly.
func OpenBackend(opts Options) (*db.DB, error) {
	return db.Open(opts)
}
