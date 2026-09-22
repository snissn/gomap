package hashdb

import "github.com/snissn/gomap/HashDB/internal/lockfile"

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = lockfile.ErrLocked
)
