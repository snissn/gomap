package db

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestNativeGenerationPublishedFailurePredicates(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	id := page.ValueLogFileID(1)
	w, err := valuelog.NewWriterWithStableResourcePinRegistry(valuelog.SegmentPath(ValueLogDirPath(dir), id), id, d.valueLogIdentityPins)
	if err != nil {
		t.Fatal(err)
	}
	ptr, err := w.Append(0, nil, 1, bytes.Repeat([]byte("v"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	setValueLogPointer(t, d, []byte("original-key"), ptr)
	s := d.AcquireSnapshot()
	if s == nil {
		t.Fatal("missing actual DATA-only Snapshot")
	}
	g := s.idx
	var file *valuelog.File
	for _, f := range s.state.ValueLogSet.Files {
		file = f
		break
	}
	if file == nil {
		t.Fatal("missing original value-log physical file")
	}
	if err = g.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err == nil {
		t.Fatal("actual DATA final Close refusal missing")
	}
	published := d.backgroundError()
	var receipt *originalDBCleanupFailure
	if !errors.As(published, &receipt) || receipt != &g.closeFailure {
		t.Fatalf("lost same original generation: %v", published)
	}
	if err = file.File.Close(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var snapshotErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			_ = errors.Is(published, os.ErrClosed)
			var found *originalDBCleanupFailure
			_ = errors.As(published, &found)
			var late *valuelog.ManagerCleanupFailure
			_ = errors.As(published, &late)
		}
	}()
	go func() { defer wg.Done(); <-start; snapshotErr = s.Close() }()
	close(start)
	wg.Wait()
	var late *valuelog.ManagerCleanupFailure
	t.Logf("actual predicates: snapshotManager=%v backgroundManager=%v failedSnapshot=%v borrows=%d", errors.As(snapshotErr, &late), errors.As(d.backgroundError(), &late), g.failedSnapshots == s, g.wrapperBorrowCountV6())
	if !errors.As(snapshotErr, &late) || !errors.As(d.backgroundError(), &late) || g.failedSnapshots != s || g.wrapperBorrowCountV6() != 1 {
		t.Fatalf("late physical completion lost from published original receipt: %v", snapshotErr)
	}
	missing := errors.New("unrelated missing cause")
	if errors.Is(published, missing) || errors.Is(d.backgroundError(), missing) {
		t.Fatal("original error graph fabricated an unrelated cause")
	}
}
