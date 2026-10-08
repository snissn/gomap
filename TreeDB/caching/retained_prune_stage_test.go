package caching

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/pager"
)

func retainedPruneStageFixture(t *testing.T) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := Open(dir, backend, Options{DisableWAL: true, RelaxedSync: true, AllowUnsafe: true, FlushThreshold: 1 << 20, ValueLogPointerThreshold: 1})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	cache.testSkipRetainedPrune = true
	for i := 0; i < 8; i++ {
		if err := cache.Set([]byte(fmt.Sprintf("live/%02d", i)), []byte("persistent-value")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := cache.Set([]byte("slot-advance"), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	return cache, backend
}

func TestRetainedPruneStageCachedPartialCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "budget"} {
		t.Run(mode, func(t *testing.T) {
			cache, backend := retainedPruneStageFixture(t)
			path, id := seedRetainedPruneSegment(t, cache, 406, 128)
			snap := backend.AcquireSnapshot()
			cache.mu.Lock()
			cache.rootPublishedSet = &publishedRootSet{generation: 1, iterator: publishedRootRef{rootID: snap.State().RootPageID}}
			cache.mu.Unlock()
			_ = snap.Close()
			t.Cleanup(func() { cache.mu.Lock(); cache.rootPublishedSet = nil; cache.mu.Unlock() })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := error(context.Canceled)
			if mode == "budget" {
				cause = errRetainedPruneScanBudgetExceeded
			}
			seen := 0
			cache.testRetainedPruneScanHook = func(ctx context.Context, phase string) error {
				if phase == "iterator_record" {
					seen++
					if seen == 2 {
						cancel(cause)
						return ctx.Err()
					}
				}
				return nil
			}
			var out retainedValueLogPruneStats
			if !cache.pruneRetainedValueLogsBatch(ctx, []retainedPruneCandidate{{path: path, id: id, hasID: true, size: 128}}, false, nil, retainedValueLogPruneRunOptions{}, &out) {
				t.Fatal("batch path not used")
			}
			if out.ScanStats.ProofStage != "cached_projection" || out.ScanStats.Records != 1 || out.GCCalls != 0 || !cache.valueLogRetained(path) {
				t.Fatalf("partial cache proof lost stage/work or mutated: %+v", out)
			}
			if mode == "budget" && !out.AbortedScanBudget || mode == "cancel" && !out.ScanError {
				t.Fatalf("incorrect abort classification: %+v", out)
			}
			cache.observeRetainedPruneMembership(out)
			stats := cache.Stats()
			if stats["treedb.cache.vlog_retained_prune.membership.last_proof_stage"] != "cached_projection" || stats["treedb.cache.vlog_retained_prune.membership.last_cache_records"] != "1" || stats["treedb.cache.vlog_retained_prune.membership.last_gc_records"] != "0" {
				t.Fatalf("partial proof publication: %+v", cache.retainedPruneMembershipLast)
			}
		})
	}
}

func TestRetainedPruneStageBackendCancellation(t *testing.T) {
	cache, _ := retainedPruneStageFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var work valueLogLiveIDScanStats
	_, err := cache.collectRetainedPruneProtectedValueLogIDs(ctx, 0, &work)
	if !errors.Is(err, context.Canceled) || work.ProofStage != "backend_membership" || work.Records != 0 || work.Membership.RecordsScanned != 0 {
		t.Fatalf("backend entry cancellation: work=%+v err=%v", work, err)
	}
}

type retainedPruneStageBackend struct {
	*backenddb.DB
	gc func(context.Context, backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error)
}

func (b *retainedPruneStageBackend) ValueLogGC(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
	return b.gc(ctx, opts)
}

func TestRetainedPruneStageGCBoundaries(t *testing.T) {
	for _, mode := range []string{"partial-cancel", "mutation-fence", "completed"} {
		t.Run(mode, func(t *testing.T) {
			cache, backend := retainedPruneStageFixture(t)
			path, id := seedRetainedPruneSegment(t, cache, 406, 128)
			if err := backend.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var out retainedValueLogPruneStats
			wrapped := &retainedPruneStageBackend{DB: backend}
			wrapped.gc = func(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
				if out.ScanStats.ProofStage != "gc_recovery" {
					t.Fatalf("GC entered at %s", out.ScanStats.ProofStage)
				}
				if mode == "partial-cancel" {
					// A backend error must retain the exact returned partial work.
					cancel()
					return backenddb.ValueLogGCStats{Membership: backenddb.RecoverableValueLogMembershipStats{RecordsScanned: 7}}, ctx.Err()
				}
				before := opts.BeforeMutation
				opts.BeforeMutation = func(ctx context.Context, token backenddb.StateToken, p *pager.Pager) (func(), error) {
					if mode == "mutation-fence" {
						cancel()
					}
					release, err := before(ctx, token, p)
					if out.ScanStats.ProofStage != "mutation_fence" {
						t.Fatalf("callback entered at %s", out.ScanStats.ProofStage)
					}
					return release, err
				}
				return backend.ValueLogGC(ctx, opts)
			}
			cache.backend = wrapped
			if !cache.pruneRetainedValueLogsBatch(ctx, []retainedPruneCandidate{{path: path, id: id, hasID: true, size: 128}}, false, nil, retainedValueLogPruneRunOptions{}, &out) {
				t.Fatal("batch path not used")
			}
			want := map[string]string{"partial-cancel": "gc_recovery", "mutation-fence": "mutation_fence", "completed": "completed"}[mode]
			if out.ScanStats.ProofStage != want || out.GCCalls != 1 {
				t.Fatalf("GC result: stage=%s calls=%d want=%s", out.ScanStats.ProofStage, out.GCCalls, want)
			}
			if mode != "completed" && (!out.ScanError || len(out.GCStats.ZombieMarkedFileIDs) != 0 || !cache.valueLogRetained(path)) {
				t.Fatalf("canceled proof mutated candidate: %+v", out)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("ordinary recent candidate disappeared: %v", err)
			}
			cache.observeRetainedPruneMembership(out)
			if mode == "partial-cancel" && cache.retainedPruneMembershipLast.GCRecords != 7 {
				t.Fatalf("lost partial GC work: %+v", cache.retainedPruneMembershipLast)
			}
		})
	}
}

func TestRetainedPruneStageNoSelection(t *testing.T) {
	cache, backend := retainedPruneStageFixture(t)
	snap := backend.AcquireSnapshot()
	entry, err := snap.GetEntry([]byte("live/00"))
	_ = snap.Close()
	if err != nil || entry.ValuePtr.FileID == 0 {
		t.Fatalf("missing live pointer: %+v %v", entry, err)
	}
	var out retainedValueLogPruneStats
	if !cache.pruneRetainedValueLogsBatch(context.Background(), []retainedPruneCandidate{{id: entry.ValuePtr.FileID, hasID: true}}, false, nil, retainedValueLogPruneRunOptions{}, &out) || out.ScanStats.ProofStage != "no_selection" || out.GCCalls != 0 || out.LiveSkippedSegments != 1 {
		t.Fatalf("live candidate no-selection: %+v", out)
	}
}

func TestRetainedPruneStageSeparateWorkStats(t *testing.T) {
	cache := &DB{}
	var out retainedValueLogPruneStats
	out.Mode = retainedPruneModeCertifiedMembership
	out.ScanStats.ProofStage = "backend_membership"
	out.ScanStats.Records = 3
	out.ScanStats.Membership.RecordsScanned = 5
	out.GCStats.Membership.RecordsScanned = 7
	cache.observeRetainedPruneMembership(out)
	out.ScanStats.ProofStage = "gc_recovery"
	cache.observeRetainedPruneMembership(out)
	stats := make(map[string]string)
	cache.retainedPruneMembershipLast.appendStats(stats, "last_")
	cache.retainedPruneMembershipTotals.appendStats(stats, "total_")
	for key, want := range map[string]string{"last_backend_records": "5", "last_cache_records": "3", "last_gc_records": "7", "last_fallback_records": "12", "total_backend_records": "10", "total_cache_records": "6", "total_gc_records": "14", "total_fallback_records": "24", "last_proof_stage": "gc_recovery"} {
		if got := stats["treedb.cache.vlog_retained_prune.membership."+key]; got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
	cache.observeRetainedPruneMembership(retainedValueLogPruneStats{})
	if cache.retainedPruneMembershipLast.ProofStage != "not_started" || cache.retainedPruneMembershipLast.BackendRecords != 0 || cache.retainedPruneMembershipLast.CacheRecords != 0 || cache.retainedPruneMembershipLast.GCRecords != 0 {
		t.Fatalf("new attempt inherited work: %+v", cache.retainedPruneMembershipLast)
	}
	legacy := retainedValueLogPruneStats{Mode: retainedPruneModeFullLiveIDScan}
	legacy.ScanStats.Records = 99
	cache.observeRetainedPruneMembership(legacy)
	if cache.retainedPruneMembershipLast.CacheRecords != 0 {
		t.Fatal("legacy whole-root scan mislabeled as cached projection")
	}
}
