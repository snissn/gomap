package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
)

func BenchmarkCommandWALPublishAppliedLSN(b *testing.B) {
	db, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	state := db.State()
	root, sysRoot := state.RootPageID, state.SystemRootPageID

	b.ReportAllocs()
	b.ResetTimer()
	var covered [1]CommandWALLSNRange
	for i := 0; i < b.N; i++ {
		lsn := uint64(i + 1)
		covered[0] = CommandWALLSNRange{First: lsn, Last: lsn}
		if err := db.publishCommandWALRoots(root, sysRoot, lsn, covered[:], false); err != nil {
			b.Fatalf("publishCommandWALRoots: %v", err)
		}
	}
}

func BenchmarkFinalizeCommitSameRoots(b *testing.B) {
	db, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	state := db.State()
	root, sysRoot := state.RootPageID, state.SystemRootPageID

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := benchmarkFinalizeCommitLocked(db, root, sysRoot); err != nil {
			b.Fatalf("finalizeCommit: %v", err)
		}
	}
}

func benchmarkFinalizeCommitLocked(db *DB, root, sysRoot uint64) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	return db.finalizeCommit(root, sysRoot, nil, false, adaptiveMetricsZero(), nil, false, nil, nil, nil)
}

func adaptiveMetricsZero() adaptive.Metrics {
	return adaptive.Metrics{}
}
