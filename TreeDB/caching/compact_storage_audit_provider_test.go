package caching

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type compactStorageRealProviderFixture struct {
	backend *backenddb.DB
	cached  *DB
	sets    [2]*publishedRootSet
}

func openCompactStorageRealProviderFixture(t *testing.T) *compactStorageRealProviderFixture {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		FlushThreshold:             1 << 30,
		MemtableShards:             1,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cached DB: %v", err)
	}
	publish := func(label string, baseRoot uint64) (uint64, uint64) {
		t.Helper()
		ordinary := newRootDomainTestTable(t, rootDomainTestOp{key: "ordinary/" + label, value: label})
		ordinaryRoot, err := backend.PublishOrderedRootIterator(baseRoot, ordinary.NewIterator(nil, nil))
		if err != nil {
			t.Fatalf("publish ordinary root %s: %v", label, err)
		}
		system := newRootDomainTestTable(t, rootDomainTestOp{key: "system/" + label, value: label})
		systemRoot, err := backend.PublishSystemRootIterator(system.NewIterator(nil, nil))
		if err != nil {
			t.Fatalf("publish system root %s: %v", label, err)
		}
		return ordinaryRoot, systemRoot
	}
	ordinaryA, systemA := publish("a", 0)
	pin := backend.AcquireSnapshot()
	if pin == nil {
		_ = cached.Close()
		t.Fatal("acquire first-root lifetime pin")
	}
	ordinaryB, systemB := publish("b", ordinaryA)
	fixture := &compactStorageRealProviderFixture{
		backend: backend,
		cached:  cached,
		sets: [2]*publishedRootSet{
			{
				generation:  1,
				pointShards: []publishedRootRef{{rootID: ordinaryA}},
				system:      publishedRootRef{rootID: systemA},
				iterator:    publishedRootRef{rootID: ordinaryA},
			},
			{
				generation:  2,
				pointShards: []publishedRootRef{{rootID: ordinaryB}},
				system:      publishedRootRef{rootID: systemB},
				iterator:    publishedRootRef{rootID: ordinaryB},
			},
		},
	}
	fixture.installProviderSet(0)
	t.Cleanup(func() {
		if err := pin.Close(); err != nil {
			t.Errorf("close first-root pin: %v", err)
		}
		if err := cached.Close(); err != nil {
			t.Errorf("close cached DB: %v", err)
		}
	})
	return fixture
}

func (f *compactStorageRealProviderFixture) installProviderSet(index int) {
	f.cached.mu.Lock()
	f.cached.applyPublishedRootSetLocked(clonePublishedRootSet(f.sets[index]))
	f.cached.publishMemtablesLocked()
	f.cached.mu.Unlock()
}

func (f *compactStorageRealProviderFixture) advanceProviderVersion() {
	f.cached.mu.Lock()
	f.cached.publishMemtablesLocked()
	f.cached.mu.Unlock()
}

type compactStorageProviderMutationLatch struct {
	trigger chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newCompactStorageProviderMutationLatch(mutate func()) *compactStorageProviderMutationLatch {
	latch := &compactStorageProviderMutationLatch{
		trigger: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		<-latch.trigger
		mutate()
		close(latch.done)
	}()
	return latch
}

func (l *compactStorageProviderMutationLatch) wait() {
	l.once.Do(func() { close(l.trigger) })
	<-l.done
}

func compactStoragePlanWithTimeout(t *testing.T, backend *backenddb.DB, opts backenddb.CompactStorageOptions) (backenddb.CompactStorageStats, error) {
	t.Helper()
	type result struct {
		stats backenddb.CompactStorageStats
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		stats, err := backend.CompactStoragePlan(context.Background(), opts)
		resultCh <- result{stats: stats, err: err}
	}()
	select {
	case result := <-resultCh:
		return result.stats, result.err
	case <-time.After(5 * time.Second):
		t.Fatal("CompactStoragePlan deadlocked while the real cached provider advanced")
		return backenddb.CompactStorageStats{}, context.DeadlineExceeded
	}
}

func TestCompactStorageAudit_RealCachedProviderRootDriftRetriesAndInvalidates(t *testing.T) {
	tests := []struct {
		name        string
		captureCall uint64
		wantCalls   uint64
		wantScans   uint64
	}{
		{name: "between acquisition captures", captureCall: 2, wantCalls: 6, wantScans: 1},
		{name: "between revalidation captures", captureCall: 4, wantCalls: 8, wantScans: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := openCompactStorageRealProviderFixture(t)
			beforeRoots, beforeSystemRoots := fixture.cached.ProtectedLeafGenerationRootIDPair()
			if len(beforeRoots) == 0 || len(beforeSystemRoots) == 0 {
				t.Fatalf("real provider missing ordinary/system roots: ordinary=%v system=%v", beforeRoots, beforeSystemRoots)
			}
			latch := newCompactStorageProviderMutationLatch(func() { fixture.installProviderSet(1) })
			var calls atomic.Uint64
			stats, err := compactStoragePlanWithTimeout(t, fixture.backend, backenddb.CompactStorageOptions{
				LeafGenerationProtectedRootIDPairFunc: func() ([]uint64, []uint64) {
					if calls.Add(1) == tt.captureCall {
						latch.wait()
					}
					return nil, nil
				},
			})
			if err != nil {
				t.Fatalf("CompactStoragePlan: %v", err)
			}
			if got := calls.Load(); got != tt.wantCalls {
				t.Fatalf("protected-basis captures=%d want %d", got, tt.wantCalls)
			}
			afterRoots, afterSystemRoots := fixture.cached.ProtectedLeafGenerationRootIDPair()
			if reflect.DeepEqual(beforeRoots, afterRoots) || reflect.DeepEqual(beforeSystemRoots, afterSystemRoots) {
				t.Fatalf("provider roots did not advance: ordinary %v -> %v, system %v -> %v", beforeRoots, afterRoots, beforeSystemRoots, afterSystemRoots)
			}
			if stats.Audit.SharedScans != tt.wantScans || stats.Audit.RevalidationRetries != 1 {
				t.Fatalf("audit counters=%+v want scans=%d retries=1", stats.Audit, tt.wantScans)
			}
			if stats.Audit.StructuralReuseHits != 0 || stats.Audit.StructuralReuseMisses != tt.wantScans {
				t.Fatalf("stale provider basis was reused: %+v", stats.Audit)
			}
		})
	}
}

func TestCompactStorageAudit_RealCachedProviderSameIDABAIsVersioned(t *testing.T) {
	fixture := openCompactStorageRealProviderFixture(t)
	beforeRoots, beforeSystemRoots := fixture.cached.ProtectedLeafGenerationRootIDPair()
	latch := newCompactStorageProviderMutationLatch(fixture.advanceProviderVersion)
	var calls atomic.Uint64
	stats, err := compactStoragePlanWithTimeout(t, fixture.backend, backenddb.CompactStorageOptions{
		LeafGenerationProtectedRootIDPairFunc: func() ([]uint64, []uint64) {
			if calls.Add(1) == 2 {
				latch.wait()
			}
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	afterRoots, afterSystemRoots := fixture.cached.ProtectedLeafGenerationRootIDPair()
	if !reflect.DeepEqual(beforeRoots, afterRoots) || !reflect.DeepEqual(beforeSystemRoots, afterSystemRoots) {
		t.Fatalf("same-ID ABA changed IDs: ordinary %v -> %v, system %v -> %v", beforeRoots, afterRoots, beforeSystemRoots, afterSystemRoots)
	}
	if calls.Load() != 6 || stats.Audit.SharedScans != 1 || stats.Audit.RevalidationRetries != 1 {
		t.Fatalf("same-ID provider ABA was accepted: calls=%d audit=%+v", calls.Load(), stats.Audit)
	}
}

func TestCompactStorageAudit_RealCachedProviderRepeatedDriftReturnsStale(t *testing.T) {
	fixture := openCompactStorageRealProviderFixture(t)
	first := newCompactStorageProviderMutationLatch(func() { fixture.installProviderSet(1) })
	second := newCompactStorageProviderMutationLatch(func() { fixture.installProviderSet(0) })
	var calls atomic.Uint64
	_, err := compactStoragePlanWithTimeout(t, fixture.backend, backenddb.CompactStorageOptions{
		LeafGenerationProtectedRootIDPairFunc: func() ([]uint64, []uint64) {
			switch calls.Add(1) {
			case 4:
				first.wait()
			case 8:
				second.wait()
			}
			return nil, nil
		},
	})
	if !errors.Is(err, backenddb.ErrCompactStorageAuditStale) {
		t.Fatalf("CompactStoragePlan error=%v want ErrCompactStorageAuditStale", err)
	}
	if calls.Load() != 8 {
		t.Fatalf("protected-basis captures=%d want 8", calls.Load())
	}
}
