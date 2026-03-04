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
}

func (b *rewriteRecordingBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	b.protectedPaths = append([]string(nil), opts.ProtectedPaths...)
	b.mu.Unlock()
	return backenddb.ValueLogRewriteStats{}, nil
}

func (b *rewriteRecordingBackend) recordedProtectedPaths() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.protectedPaths...)
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
