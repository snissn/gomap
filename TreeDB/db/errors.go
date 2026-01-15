package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = lockfile.ErrLocked
	// ErrUnsafeOptions indicates unsafe durability/integrity options were set without acknowledgement.
	ErrUnsafeOptions = errors.New("treedb: unsafe options require AllowUnsafe")
	// ErrReadOnly indicates a write was attempted on a read-only DB handle.
	ErrReadOnly = errors.New("treedb: read-only")
	// ErrClosed indicates an operation was attempted on a closed DB handle.
	ErrClosed = errors.New("treedb: closed")
)
