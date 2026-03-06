package caching

import (
	"context"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type rewriteRecordingBackend struct {
	*backenddb.DB

	mu             sync.Mutex
	protectedPaths []string
	reservedStarts []uint64
}

func (b *rewriteRecordingBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	b.protectedPaths = append([]string(nil), opts.ProtectedPaths...)
	b.mu.Unlock()
	if opts.ReserveRIDs != nil {
		start, err := opts.ReserveRIDs(3)
		if err != nil {
			return backenddb.ValueLogRewriteStats{}, err
		}
		b.mu.Lock()
		b.reservedStarts = append(b.reservedStarts, start)
		b.mu.Unlock()
	}
	return backenddb.ValueLogRewriteStats{}, nil
}

func (b *rewriteRecordingBackend) recordedProtectedPaths() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.protectedPaths...)
}

func (b *rewriteRecordingBackend) recordedReservedStarts() []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint64(nil), b.reservedStarts...)
}

func TestVlogGenerationRewrite_ProtectedPathsIncludeCurrentValueLogPaths(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteRecordingBackend{DB: backend}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	db.maybeRunVlogGenerationMaintenance(false)

	got := recorder.recordedProtectedPaths()
	if len(got) == 0 {
		t.Fatalf("expected rewrite to be invoked and to record protected paths")
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, path := range got {
		if path == "" {
			continue
		}
		gotSet[path] = struct{}{}
	}

	want := db.currentValueLogPaths()
	if len(want) == 0 {
		t.Fatalf("missing current value-log paths")
	}
	for _, path := range want {
		if path == "" {
			continue
		}
		if _, ok := gotSet[path]; !ok {
			t.Fatalf("protected paths missing current value-log segment: %s", path)
		}
	}
}

func TestVlogGenerationRewrite_UsesSharedRIDAllocator(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteRecordingBackend{DB: backend}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
		ValueLogPointerThreshold:         1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("k1"), []byte("value-1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	before := db.nextRID.Load()
	if before == 0 {
		t.Fatalf("expected nextRID to be seeded by pointer write")
	}

	db.maybeRunVlogGenerationMaintenance(false)

	got := recorder.recordedReservedStarts()
	if len(got) != 1 {
		t.Fatalf("expected one ReserveRIDs call, got %d", len(got))
	}
	if got[0] != before+1 {
		t.Fatalf("expected ReserveRIDs to start at %d, got %d", before+1, got[0])
	}
	if after := db.nextRID.Load(); after != before+3 {
		t.Fatalf("expected nextRID advanced to %d after reserve, got %d", before+3, after)
	}
}
