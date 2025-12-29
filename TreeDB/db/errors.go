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
)
