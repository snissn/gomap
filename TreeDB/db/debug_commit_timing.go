package db

import (
	"fmt"
	"os"
	"sync/atomic"
)

const envDebugCommitTiming = "TREEDB_DEBUG_COMMIT_TIMING"

var debugCommitTiming atomic.Bool

func init() {
	if os.Getenv(envDebugCommitTiming) != "" {
		debugCommitTiming.Store(true)
	}
}

func commitTimingEnabled() bool {
	return debugCommitTiming.Load()
}

func commitTimingPrintf(format string, args ...any) {
	if !commitTimingEnabled() {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
}
