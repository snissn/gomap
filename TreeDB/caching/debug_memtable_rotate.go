package caching

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var (
	debugMemtableRotateEnabled atomic.Bool
	debugMemtableRotateBudget  atomic.Int64
)

func init() {
	if os.Getenv("TREEDB_DEBUG_MEMTABLE_ROTATE") == "" {
		return
	}
	debugMemtableRotateEnabled.Store(true)

	budget := int64(2000)
	if s := os.Getenv("TREEDB_DEBUG_MEMTABLE_ROTATE_BUDGET"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			budget = v
		}
	}
	debugMemtableRotateBudget.Store(budget)
}

func debugMemtableRotateOn() bool {
	return debugMemtableRotateEnabled.Load() && debugMemtableRotateBudget.Load() > 0
}

func debugMemtableModeLabel(mt memtable.Table) string {
	switch mt.(type) {
	case *memtable.Memtable:
		return "skiplist"
	case *memtable.HashSorted:
		return "hash_sorted"
	case *memtable.BTree:
		return "btree"
	case *memtable.AppendOnly:
		return "append_only"
	default:
		return "unknown"
	}
}

func (db *DB) debugMemtableRotatef(format string, args ...any) {
	if !debugMemtableRotateOn() {
		return
	}
	if debugMemtableRotateBudget.Add(-1) < 0 {
		return
	}
	walDir := ""
	if db != nil {
		walDir = db.dir
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	if walDir != "" {
		a := make([]any, 0, 3+len(args))
		a = append(a, ts, filepath.Base(filepath.Dir(walDir)), walDir)
		a = append(a, args...)
		fmt.Fprintf(os.Stderr, "ts=%s treedb debug memtable_rotate db=%s wal_dir=%s "+format+"\n", a...)
		return
	}
	fmt.Fprintf(os.Stderr, "ts=%s treedb debug memtable_rotate "+format+"\n", append([]any{ts}, args...)...)
}
