package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = lockfile.ErrLocked
	// ErrReadOnly indicates a write was attempted on a read-only DB handle.
	ErrReadOnly = errors.New("treedb: read-only")
	// ErrClosed indicates the DB handle is closed or closing for reads.
	ErrClosed = errors.New("treedb: db is closed")
	// ErrRecoveryRequired indicates the DB must be opened read-write for recovery
	// before the requested read-only or offline-maintenance operation can run.
	ErrRecoveryRequired = collectionwal.ErrCollectionWALRecoveryRequired
	// ErrUnsupportedRequiredFeature indicates format.json requires a storage
	// feature this binary does not understand.
	ErrUnsupportedRequiredFeature = errors.New("treedb: unsupported required storage feature")
	// ErrCommandWALUnsupported is returned while command_wal_v1 durable
	// execution is still gated behind later implementation PRs.
	ErrCommandWALUnsupported = errors.New("treedb: command_wal_v1 execution is not enabled")
)
