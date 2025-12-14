package db

import "github.com/snissn/gomap/TreeDB/internal/lockfile"

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = lockfile.ErrLocked
)
