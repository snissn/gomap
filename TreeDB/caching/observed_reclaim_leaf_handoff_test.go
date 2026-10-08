package caching

import (
	"context"
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type observedReclaimHandoffBackend struct {
	*backenddb.DB
	cache      *DB
	leafCalls  int
	valueError error
}

func (b *observedReclaimHandoffBackend) ValueLogGC(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
	if b.valueError != nil {
		return backenddb.ValueLogGCStats{}, b.valueError
	}
	return b.DB.ValueLogGC(ctx, opts)
}

func (b *observedReclaimHandoffBackend) LeafGenerationGC(ctx context.Context, opts backenddb.LeafGenerationGCOptions) (backenddb.LeafGenerationGCStats, error) {
	b.leafCalls++
	if !b.cache.writeMu.TryLock() {
		return backenddb.LeafGenerationGCStats{}, errors.New("leaf GC inherited observed-value write fence")
	}
	b.cache.writeMu.Unlock()
	return b.DB.LeafGenerationGC(ctx, opts)
}

func TestReclaimObservedValueLogSourcesReleasesFenceBeforeLeafHandoff(t *testing.T) {
	for _, failValue := range []bool{false, true} {
		t.Run(map[bool]string{false: "leaf-handoff", true: "value-error"}[failValue], func(t *testing.T) {
			dir := t.TempDir()
			raw, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &observedReclaimHandoffBackend{DB: raw}
			cache, err := Open(dir, wrapped, Options{IndexOuterLeavesInValueLog: true, FlushApplyConcurrency: 2, RelaxedSync: true, AllowUnsafe: true})
			if err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			defer cache.Close()
			wrapped.cache = cache
			var injected = errors.New("injected observed-value revalidation failure")
			if failValue {
				wrapped.valueError = injected
			}
			id, err := valuelog.EncodeFileID(0, 1)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = cache.ReclaimObservedValueLogSources(ctx, []uint32{id})
			if failValue {
				if !errors.Is(err, injected) || wrapped.leafCalls != 0 {
					t.Fatalf("error=%v leaf calls=%d", err, wrapped.leafCalls)
				}
			} else if err != nil || wrapped.leafCalls != 1 {
				t.Fatalf("error=%v leaf calls=%d", err, wrapped.leafCalls)
			}
			if !cache.writeMu.TryLock() {
				t.Fatal("reclaim retained write fence")
			}
			cache.writeMu.Unlock()
			if !cache.flushMu.TryLock() {
				t.Fatal("leaf handoff retained flush owner")
			}
			cache.flushMu.Unlock()
		})
	}
}
