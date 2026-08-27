package treedb

import (
	"testing"
	"time"
)

func TestBeginPublicOperationWithDeferNoAlloc(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	allocs := testing.AllocsPerRun(1000, func() {
		if err := beginPublicOperationWithDeferForAllocTest(db); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("beginPublicOperation with direct deferred RUnlock allocs = %v, want 0", allocs)
	}
}

func BenchmarkBeginPublicOperationWithDefer(b *testing.B) {
	db, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := beginPublicOperationWithDeferForAllocTest(db); err != nil {
			b.Fatal(err)
		}
	}
}

func beginPublicOperationWithDeferForAllocTest(db *DB) error {
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	return nil
}

func TestVacuumOnlineStatsSharesCloseLifecycleLock(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	db.lifecycleMu.Lock()
	statsDone := make(chan struct{})
	go func() {
		_ = db.VacuumOnlineStats()
		close(statsDone)
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case <-statsDone:
		db.lifecycleMu.Unlock()
		<-closeDone
		t.Fatal("VacuumOnlineStats bypassed lifecycle lock")
	case <-time.After(25 * time.Millisecond):
	}
	db.lifecycleMu.Unlock()
	select {
	case <-statsDone:
	case <-time.After(time.Second):
		t.Fatal("VacuumOnlineStats did not complete after lifecycle unlock")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not complete after lifecycle unlock")
	}
}
