//go:build !linux

package commitlog

import (
	"github.com/snissn/gomap/TreeDB/internal/stableio"
	"os"
)

func syncWALData(file *os.File) error { return stableio.SyncFile(file) }
