package caching

import (
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestFlushValueLogLane_VlogWriterUnavailableErrorIncludesLane(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = backend.Close() }()

	db, err := Open(dir, backend, Options{})
	if err != nil {
		t.Fatalf("open caching db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if len(db.lanes) == 0 {
		t.Fatalf("expected at least one lane")
	}

	l := &db.lanes[0]
	l.vlogMu.Lock()
	if l.vlog != nil {
		_ = l.vlog.Close()
		l.vlog = nil
	}
	l.vlogDirty.Store(false)
	l.vlogMu.Unlock()

	err = db.flushValueLogLane(l)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "value log writer unavailable") {
		t.Fatalf("expected value-log writer error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "lane=0") {
		t.Fatalf("expected lane id in error, got: %v", err)
	}
}

