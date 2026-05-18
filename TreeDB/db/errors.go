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
	// ErrConcurrentModification indicates a publish or maintenance operation
	// observed that a root changed after its validation point.
	ErrConcurrentModification = errors.New("treedb: concurrent modification")
	// ErrRecoveryRequired indicates the DB must be opened read-write for recovery
	// before the requested read-only or offline-maintenance operation can run.
	ErrRecoveryRequired = collectionwal.ErrCollectionWALRecoveryRequired
	// ErrUnsupportedRequiredFeature indicates format.json requires a storage
	// feature this binary does not understand.
	ErrUnsupportedRequiredFeature = errors.New("treedb: unsupported required storage feature")
	// ErrCommandWALUnsupported is returned while command_wal_v1 durable
	// execution is still gated behind later implementation PRs.
	ErrCommandWALUnsupported = errors.New("treedb: command_wal_v1 execution is not enabled")
	// ErrCommandWALRejected is the stable public sentinel for commands that are
	// intentionally rejected while command_wal_v1 is active.
	ErrCommandWALRejected = errors.New("treedb: command_wal_v1 command rejected")
	// ErrCommandWALSegmentSeqExhausted indicates the command WAL segment sequence
	// number space has no strictly higher segment available.
	ErrCommandWALSegmentSeqExhausted = errors.New("treedb: command wal segment sequence exhausted")
)
