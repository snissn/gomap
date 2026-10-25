package db

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestVacuumPreparedSystemAllowsWriterAndReopens(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, walOff := range []bool{false, true} {
		t.Run(fmt.Sprintf("wal-off=%t", walOff), func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), ChunkSize: 64 << 10}
			if walOff {
				opts.ResolvedProfile = ProfileNoWALFast
				opts.Durability = DurabilityWALOffRelaxed
			}
			d, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close() }()
			if _, err := d.PublishSystemRootIterator(mustFrozenSystemMemtable(t, "catalog/prepared", "retained").NewIterator(nil, nil)); err != nil {
				t.Fatal(err)
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			reached, release := make(chan struct{}), make(chan struct{})
			var holdOnce, releaseOnce sync.Once
			var lockHeld atomic.Bool
			d.vacuumSystemTreePageHook = func(uint64) {
				holdOnce.Do(func() {
					if d.writeMu.TryLock() {
						d.writeMu.Unlock()
					} else {
						lockHeld.Store(true)
					}
					close(reached)
					<-release
				})
			}
			ctx, cancel := context.WithCancel(context.Background())
			vacuumDone, writerDone := make(chan error, 1), make(chan error, 1)
			var vacuumJoined, writerStarted, writerJoined bool
			// Release and join before Close even when a progress assertion fails.
			defer func() {
				cancel()
				releaseOnce.Do(func() { close(release) })
				if !vacuumJoined {
					select {
					case <-vacuumDone:
					case <-time.After(5 * time.Second):
						t.Error("vacuum did not join after releasing preparation")
					}
				}
				if writerStarted && !writerJoined {
					select {
					case <-writerDone:
					case <-time.After(5 * time.Second):
						t.Error("writer did not join after releasing preparation")
					}
				}
			}()
			go func() { vacuumDone <- d.VacuumIndexOnline(ctx) }()
			waitVacuumTestSignal(t, reached, "actual prepared system page allocation")
			writerStarted = true
			go func() { writerDone <- d.SetSync([]byte("during-system-prepare"), []byte("writer")) }()
			select {
			case err := <-writerDone:
				writerJoined = true
				if err != nil {
					t.Fatalf("writer during preparation: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("writer blocked by private system-tree preparation")
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-vacuumDone:
				vacuumJoined = true
				if err != nil {
					t.Fatalf("vacuum: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("vacuum did not finish after preparation release")
			}
			if lockHeld.Load() {
				t.Fatal("system page allocation held writeMu")
			}
			if stats := d.VacuumOnlineStats(); stats.DeferredCutovers == 0 || stats.RecoverableSetRecaptures == 0 {
				t.Fatalf("concurrent writer was not reconciled: %+v", stats)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			if value, err := reopened.Get([]byte("during-system-prepare")); err != nil || string(value) != "writer" {
				t.Fatalf("reopened writer value=%q err=%v", value, err)
			}
			snap := reopened.AcquireSnapshot()
			if snap == nil {
				t.Fatal("missing reopened snapshot")
			}
			defer func() { _ = snap.Close() }()
			if value, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte("catalog/prepared")); err != nil || string(value) != "retained" {
				t.Fatalf("reopened catalog=%q err=%v", value, err)
			}
		})
	}
}

func TestVacuumPreparedSystemAcceptsOrInvalidatesCatalog(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, invalidate := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalidate=%t", invalidate), func(t *testing.T) {
			opts := vacuumSnapshotTestOptions(t.TempDir())
			d, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close() }()
			root, err := publishVacuumSnapshotCollectionVersion(d, 0, 0, 64)
			if err != nil {
				t.Fatal(err)
			}
			root, err = publishVacuumSnapshotCollectionVersion(d, root, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			var pages, attempts int
			var hookErr error
			d.vacuumSystemTreePageHook = func(uint64) { pages++ }
			d.vacuumBeforeCutoverHook = func(int) {
				attempts++
				if pages == 0 {
					hookErr = fmt.Errorf("attempt %d has no prepared system pages", attempts)
					return
				}
				pages = 0
				if !invalidate || attempts != 1 {
					return
				}
				before := d.systemRootPublishEpoch.Load()
				root, hookErr = publishVacuumSnapshotCollectionVersion(d, root, 2, 0)
				if hookErr == nil && d.systemRootPublishEpoch.Load() <= before {
					hookErr = fmt.Errorf("catalog publication did not advance basis epoch")
				}
				if hookErr != nil {
					return
				}
				// This hook runs after vacuum's drain. Settle the injected ordinary
				// publication before recapture so this case isolates catalog/epoch
				// invalidation, not a later independent durable-capability change.
				publication := d.rootPublication
				if publication == nil || publication.coordinator == nil {
					hookErr = fmt.Errorf("missing catalog publication coordinator")
					return
				}
				drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
				hookErr = publication.coordinator.Drain(drainCtx)
				cancelDrain()
				if hookErr != nil {
					return
				}
				settled := publication.coordinator.Stats()
				if settled.PendingCommits != 0 || settled.VisibleCommitSeq == 0 || settled.DurableCommitSeq != settled.VisibleCommitSeq {
					hookErr = fmt.Errorf("catalog publication not settled before recapture: pending=%d visible=%d durable=%d", settled.PendingCommits, settled.VisibleCommitSeq, settled.DurableCommitSeq)
				}
			}
			if err := d.VacuumIndexOnline(context.Background()); err != nil {
				t.Fatal(err)
			}
			if hookErr != nil {
				t.Fatal(hookErr)
			}
			if pages != 0 {
				t.Fatalf("system tree rebuilt after prepared catalog validation: %d pages", pages)
			}
			wantAttempts, wantVersion := 1, 1
			if invalidate {
				wantAttempts, wantVersion = 2, 2
			}
			stats := d.VacuumOnlineStats()
			if attempts != wantAttempts || stats.DeferredCutovers != uint64(wantAttempts-1) {
				t.Fatalf("attempts=%d deferred=%d want %d/%d", attempts, stats.DeferredCutovers, wantAttempts, wantAttempts-1)
			}
			if invalidate && (stats.RecoverableSetRecaptures == 0 || stats.DirtyDescriptors == 0) {
				t.Fatalf("stale catalog was not reconciled: %+v", stats)
			}
			verifyVacuumSnapshotCollectionVersion(t, d, root, wantVersion)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			verifyVacuumSnapshotCollectionVersion(t, reopened, 0, wantVersion)
		})
	}
}

// Build real multi-page catalogs through the same builder/recording allocator,
// then reclaim and reuse every ID. A one-page reservation forces both reserved
// and spill ownership without modifying production reservation geometry.
func TestVacuumPreparedSystemReclaimsReservedAndSpilledPages(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), ChunkSize: 64 << 10, ValueLog: ValueLogOptions{PointerThreshold: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	catalog, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		catalog.Set([]byte(fmt.Sprintf("catalog/%06d", i)), bytes.Repeat([]byte{byte(i)}, 128))
	}
	catalog.Freeze()
	if _, err := d.PublishSystemRootIterator(catalog.NewIterator(nil, nil)); err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing source snapshot")
	}
	defer func() { _ = snap.Close() }()
	p, err := pager.Open(filepath.Join(t.TempDir(), "private-index"), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	alloc := freelist.New(p, 0)
	guard, err := alloc.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	guardImage := bytes.Repeat([]byte{0x7d}, page.PageSize)
	if err := p.Write(guard, guardImage); err != nil {
		t.Fatal(err)
	}
	start, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	reserved := newVacuumReservedAllocator(alloc, start, 1)
	var highWater uint64
	for attempt := 0; attempt <= vacuumCutoverMaxDefers; attempt++ {
		recorded := newVacuumRecordingAllocator(reserved)
		root, _, err := d.vacuumBuildSystemRoot(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID, recorded, p, bulk.BuildOptions{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(recorded.pages) < 2 || (attempt == 0 && !slices.Contains(recorded.pages, start)) {
			t.Fatalf("attempt %d pages=%v lack reserved/spill coverage", attempt, recorded.pages)
		}
		it := tree.New(p, nil, root).Iterator(nil, nil)
		count := 0
		for ; it.Valid(); it.Next() {
			if !bytes.Equal(it.UnsafeKey(), []byte(fmt.Sprintf("catalog/%06d", count))) || !bytes.Equal(it.UnsafeValue(), bytes.Repeat([]byte{byte(count)}, 128)) {
				_ = it.Close()
				t.Fatalf("attempt %d reconstructed catalog differs at row %d", attempt, count)
			}
			count++
		}
		if err := it.Close(); err != nil || count != 256 {
			t.Fatalf("attempt %d catalog rows=%d close=%v", attempt, count, err)
		}
		if err := reserved.ReleaseUnused(); err != nil {
			t.Fatal(err)
		}
		if err := freeVacuumRetired(alloc, recorded.pages); err != nil {
			t.Fatal(err)
		}
		if got := alloc.Counters().ReclaimablePages(); got != uint64(len(recorded.pages)) {
			t.Fatalf("attempt %d reclaimable=%d want %d", attempt, got, len(recorded.pages))
		}
		got, err := alloc.AllocMany(len(recorded.pages), 0)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		want := slices.Clone(recorded.pages)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("attempt %d reused IDs=%v want exactly %v", attempt, got, want)
		}
		if err := freeVacuumRetired(alloc, got); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			highWater = p.PageCount()
		} else if p.PageCount() != highWater {
			t.Fatalf("retry %d grew private pager: %d want %d", attempt, p.PageCount(), highWater)
		}
		if data, err := p.Get(guard); err != nil || !bytes.Equal(data, guardImage) {
			t.Fatalf("attempt %d changed retained non-system page: %v", attempt, err)
		}
	}
}
