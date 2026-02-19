package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
)

func TestSnapshotPoolPutRetainsDecodeLeasesWhileDBOpen(t *testing.T) {
	pool := NewSnapshotPool()
	s := pool.Get()
	openDB := &DB{}
	s.db = openDB
	s.reader.reconfigure(nil, outerleaf.ModeV2FencePtr, false, nil, nil)
	if s.reader.fenceDecodeLeases == nil {
		t.Fatalf("fenceDecodeLeases=nil want initialized")
	}
	want := s.reader.fenceDecodeLeases
	pool.Put(s)

	s2 := pool.Get()
	if s2.reader.fenceDecodeLeases == nil {
		t.Fatalf("fenceDecodeLeases=nil after pooled reuse; want retained")
	}
	if s2.reader.fenceDecodeLeases != want {
		t.Fatalf("fenceDecodeLeases pointer changed across pooled reuse")
	}
	closingDB := &DB{}
	closingDB.closing.Store(true)
	s2.db = closingDB
	pool.Put(s2)
}

func TestSnapshotPoolPutReleasesDecodeLeasesWhenDBClosing(t *testing.T) {
	pool := NewSnapshotPool()
	s := pool.Get()
	closingDB := &DB{}
	closingDB.closing.Store(true)
	s.db = closingDB
	s.reader.reconfigure(nil, outerleaf.ModeV2FencePtr, false, nil, nil)
	if s.reader.fenceDecodeLeases == nil {
		t.Fatalf("fenceDecodeLeases=nil want initialized")
	}
	pool.Put(s)

	s2 := pool.Get()
	if s2.reader.fenceDecodeLeases != nil {
		t.Fatalf("fenceDecodeLeases=%v want nil after closing Put", s2.reader.fenceDecodeLeases)
	}
}
