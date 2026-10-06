package caching

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var errCOWMaintenanceAcceptedReport = errors.New("test: error reported after real maintenance acceptance")

// Only heuristic input and post-acceptance fault placement are controlled.
// Vacuum, snapshot capture, cut installation, and finite admission are native.
type cowMaintenanceLifetimeBackend struct {
	*backenddb.DB
	cache                                      *DB
	t                                          *testing.T
	indexPath                                  string
	before, after                              os.FileInfo
	beforeRoot, afterRoot, beforeSeq, afterSeq string
	pressure                                   *memtable.COWExternalLease
	armed                                      bool
	installPressure                            bool
	accepted                                   atomic.Int64
	captures                                   atomic.Int64
}

func (b *cowMaintenanceLifetimeBackend) exhaustInFlight() {
	b.t.Helper()
	stats, limits := b.cache.cow.budget.Stats(), b.cache.cow.budget.Limits()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if stats.ReservedBytes+overhead >= limits.MaxInFlightBytes {
		b.t.Fatal("fixture already has no admission headroom")
	}
	lease, err := b.cache.cow.budget.AcquireExternal(limits.MaxInFlightBytes - stats.ReservedBytes - overhead)
	if err != nil {
		b.t.Fatalf("real external reservation: %v", err)
	}
	b.pressure = lease
	if got := b.cache.cow.budget.Stats().ReservedBytes; got != limits.MaxInFlightBytes {
		b.t.Fatalf("reserved=%d want finite limit%d", got, limits.MaxInFlightBytes)
	}
}

func (b *cowMaintenanceLifetimeBackend) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	snapshot, err := b.DB.AcquireSnapshotWithAllocationAdmission(admit)
	if err == nil {
		b.captures.Add(1)
	}
	if err == nil && b.armed && b.installPressure && b.accepted.Load() > 0 {
		b.armed = false
		b.exhaustInFlight() // Snapshot admitted; the following cut admission must refuse.
	}
	return snapshot, err
}

func (b *cowMaintenanceLifetimeBackend) FragmentationReport() (map[string]string, error) {
	report, err := b.DB.FragmentationReport()
	if err == nil && b.armed {
		// Force only the sparse-index heuristic on a small real tree. This does
		// not replace its vacuum, checkpoint fence or refresh protocol.
		report["treedb.user.pages"] = "128"
		report["treedb.user.internal_fill_ppm_p50"] = "100000"
		report["treedb.user.internal_fill_ppm_avg"] = "100000"
	}
	return report, err
}

func (b *cowMaintenanceLifetimeBackend) VacuumIndexOnline(ctx context.Context) error {
	b.t.Helper()
	before, err := os.Stat(b.indexPath)
	if err != nil {
		return err
	}
	stats, err := b.DB.VacuumIndexOnlineWithStats(ctx)
	if err != nil {
		return err
	}
	after, err := os.Stat(b.indexPath)
	if err != nil {
		return err
	}
	if !stats.WorkCompleted || os.SameFile(before, after) {
		b.t.Fatalf("vacuum not physically accepted: completed%t sameIndex%t", stats.WorkCompleted, os.SameFile(before, after))
	}
	b.before, b.after = before, after
	b.accepted.Add(1)
	if !b.installPressure {
		b.armed = false
		b.exhaustInFlight()
	}
	return errCOWMaintenanceAcceptedReport
}

func (b *cowMaintenanceLifetimeBackend) compactIndexWithAcceptedReport() error {
	b.t.Helper()
	before := b.DB.Stats()
	if err := b.DB.CompactIndex(); err != nil {
		return err
	}
	after := b.DB.Stats()
	b.beforeRoot, b.afterRoot = before["treedb.root_page"], after["treedb.root_page"]
	b.beforeSeq, b.afterSeq = before["treedb.commit_seq"], after["treedb.commit_seq"]
	if b.beforeRoot == "" || b.afterRoot == "" || b.beforeRoot == b.afterRoot || b.beforeSeq == "" || b.afterSeq == "" || b.beforeSeq == b.afterSeq {
		b.t.Fatalf("CompactIndex did not publish a real successor root: root%s->%s sequence%s->%s", b.beforeRoot, b.afterRoot, b.beforeSeq, b.afterSeq)
	}
	b.accepted.Add(1)
	if !b.installPressure {
		b.armed = false
		b.exhaustInFlight()
	}
	return errCOWMaintenanceAcceptedReport
}

func cowMaintenanceLifetimeFixture(t *testing.T) (*DB, *cowMaintenanceLifetimeBackend) {
	t.Helper()
	dir := t.TempDir()
	backendDir := filepath.Join(dir, "backend")
	native, err := backenddb.Open(backenddb.Options{Dir: backendDir, KeepRecent: 1, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := Open(dir, native, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", MemtableShards: 2, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = native.Close()
		t.Fatal(err)
	}
	backend := &cowMaintenanceLifetimeBackend{DB: native, cache: cached, t: t, indexPath: filepath.Join(backendDir, "index.db")}
	cached.backend = backend
	t.Cleanup(func() {
		if backend.pressure != nil {
			backend.pressure.Close()
		}
		if err := cached.Close(); err != nil {
			t.Errorf("cache Close: %v", err)
		}
		if err := native.Close(); err != nil {
			t.Errorf("backend Close: %v", err)
		}
	})
	return cached, backend
}

func TestCOWMaintenanceAcceptedRefreshPressureLifetime(t *testing.T) {
	routes := []string{"central-maintenance", "checkpoint-vacuum", "central-index-compaction"}
	if runtime.GOOS == "windows" {
		testCOWMaintenanceUnsupportedVacuumPreservesLifetime(t)
		// Windows has no online vacuum. Keep both post-acceptance admission
		// stages using native CompactIndex, which publishes a new tree root.
		routes = []string{"central-index-compaction"}
	}
	for _, route := range routes {
		for _, stage := range []string{"capture", "install"} {
			t.Run(route+"/"+stage, func(t *testing.T) {
				central := route != "checkpoint-vacuum"
				cached, backend := cowMaintenanceLifetimeFixture(t)
				beforeValue := bytes.Repeat([]byte("old-pointer/"), 512)
				afterValue := bytes.Repeat([]byte("new-pointer/"), 512)
				for _, key := range []string{"a", "deleted"} {
					if err := cached.Set([]byte(key), beforeValue); err != nil {
						t.Fatal(err)
					}
				}
				if err := cached.Set([]byte("empty"), []byte{}); err != nil {
					t.Fatal(err)
				}
				if err := cached.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				// Exercise the real cadence, rather than setting checkpointing/cutover
				// booleans or pretending a vacuum publication occurred.
				if route == "checkpoint-vacuum" {
					for cached.checkpointRuns.Load() < checkpointSparseIndexCheckEveryNoops {
						if err := cached.Checkpoint(); err != nil {
							t.Fatal(err)
						}
					}
				}
				old, err := cached.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				oldValue, oldRevision, err := old.GetVersioned([]byte("a"))
				if err != nil || !bytes.Equal(oldValue, beforeValue) {
					t.Fatalf("old point before vacuum: len%d err%v", len(oldValue), err)
				}
				basis := cached.cow.cut.basis
				if central {
					if err := cached.Set([]byte("a"), afterValue); err != nil {
						t.Fatal(err)
					}
					if err := cached.Delete([]byte("deleted")); err != nil {
						t.Fatal(err)
					}
					if err := cached.Set([]byte("late"), []byte("later")); err != nil {
						t.Fatal(err)
					}
				}
				backend.armed = true
				backend.installPressure = stage == "install"
				capturesBefore := backend.captures.Load()
				if central {
					maintenance := func() error { return backend.VacuumIndexOnline(context.Background()) }
					if route == "central-index-compaction" {
						maintenance = backend.compactIndexWithAcceptedReport
					}
					err = cached.RunBackendMaintenance(maintenance)
				} else {
					err = cached.Checkpoint()
				}
				if !errors.Is(err, errCOWMaintenanceAcceptedReport) || !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("accepted report plus real refresh refusal: %v", err)
				}
				if backend.accepted.Load() != 1 {
					t.Fatal("native maintenance acceptance lost")
				}
				if route == "central-index-compaction" {
					if backend.beforeRoot == backend.afterRoot || backend.beforeSeq == backend.afterSeq {
						t.Fatal("native index-compaction root publication lost")
					}
				} else if backend.before == nil || backend.after == nil || os.SameFile(backend.before, backend.after) {
					t.Fatal("physical vacuum acceptance lost")
				}
				if !cached.cow.refreshRequired.Load() || cached.cow.cut.basis != basis {
					t.Fatal("refusal cleared gate or replaced retained basis")
				}
				if stage == "install" && backend.captures.Load() <= capturesBefore {
					t.Fatal("install fixture did not complete native snapshot capture")
				}
				backend.pressure.Close()
				backend.pressure = nil
				// Pressure is now gone. This refusal therefore proves the persistent
				// coherence gate, not generic exhaustion in NewBatch.
				if err := cached.Set([]byte("denied-point"), []byte("bad")); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("following Set=%v", err)
				}
				batch := cached.NewBatch()
				if err := batch.Set([]byte("denied-batch"), []byte("bad")); err != nil {
					t.Fatal(err)
				}
				if err := batch.Write(); !errors.Is(err, memtable.ErrCOWCapacity) {
					_ = batch.Close()
					t.Fatalf("following Write=%v", err)
				}
				if err := batch.Close(); err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{"denied-point", "denied-batch"} {
					if has, err := cached.Has([]byte(key)); err != nil || has {
						t.Fatalf("refused %s visible=%t err%v", key, has, err)
					}
				}
				got, revision, err := old.GetVersioned([]byte("a"))
				if err != nil || !bytes.Equal(got, beforeValue) || revision != oldRevision {
					t.Fatalf("old pinned point changed: len%d revision%d want%d err%v", len(got), revision, oldRevision, err)
				}
				if got, err := old.Get([]byte("deleted")); err != nil || !bytes.Equal(got, beforeValue) {
					t.Fatalf("old pinned deleted value len%d err%v", len(got), err)
				}
				if got, err := old.Get([]byte("empty")); err != nil || got == nil || len(got) != 0 {
					t.Fatalf("old empty=%v err%v", got, err)
				}
				want := beforeValue
				if central {
					want = afterValue
					if has, err := cached.Has([]byte("deleted")); err != nil || has {
						t.Fatalf("newer tombstone has%t err%v", has, err)
					}
				}
				if got, err := cached.Get([]byte("a")); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("current cut lost newer source len%d err%v", len(got), err)
				}
				if err := cached.Checkpoint(); err != nil {
					t.Fatalf("refresh retry: %v", err)
				}
				if cached.cow.refreshRequired.Load() || cached.cow.cut.basis == basis {
					t.Fatal("retry did not install coherent basis and release gate")
				}
				if backend.accepted.Load() != 1 {
					t.Fatalf("retry repeated native maintenance %d times", backend.accepted.Load())
				}
				if got, err := backend.Get([]byte("a")); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("retry backend len%d err%v", len(got), err)
				}
				if central {
					if has, err := cached.Has([]byte("deleted")); err != nil || has {
						t.Fatalf("retried tombstone has%t err%v", has, err)
					}
					if got, err := cached.Get([]byte("late")); err != nil || string(got) != "later" {
						t.Fatalf("retried late=%q err%v", got, err)
					}
				}
				if err := cached.Set([]byte("resumed"), []byte("ok")); err != nil {
					t.Fatalf("resumed writer: %v", err)
				}
				if got, err := old.Get([]byte("a")); err != nil || !bytes.Equal(got, beforeValue) {
					t.Fatalf("old after retry len%d err%v", len(got), err)
				}
				t.Logf("real accepted maintenance1; %s refusal then retry; captures%d oldRevision%d", stage, backend.captures.Load(), oldRevision)
			})
		}
	}
}

func testCOWMaintenanceUnsupportedVacuumPreservesLifetime(t *testing.T) {
	t.Helper()
	for _, route := range []string{"central-maintenance", "checkpoint-vacuum"} {
		t.Run(route+"/unsupported-native-vacuum", func(t *testing.T) {
			cached, backend := cowMaintenanceLifetimeFixture(t)
			beforeValue := bytes.Repeat([]byte("old-pointer/"), 512)
			afterValue := bytes.Repeat([]byte("new-pointer/"), 512)
			for _, key := range []string{"a", "deleted"} {
				if err := cached.Set([]byte(key), beforeValue); err != nil {
					t.Fatal(err)
				}
			}
			if err := cached.Set([]byte("empty"), []byte{}); err != nil {
				t.Fatal(err)
			}
			if err := cached.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for route == "checkpoint-vacuum" && cached.checkpointRuns.Load() < checkpointSparseIndexCheckEveryNoops {
				if err := cached.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			old, err := cached.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			_, oldRevision, err := old.GetVersioned([]byte("a"))
			if err != nil {
				t.Fatal(err)
			}
			if err := cached.Set([]byte("a"), afterValue); err != nil {
				t.Fatal(err)
			}
			if err := cached.Delete([]byte("deleted")); err != nil {
				t.Fatal(err)
			}
			backend.armed = true
			if route == "central-maintenance" {
				err = cached.RunBackendMaintenance(func() error { return backend.VacuumIndexOnline(context.Background()) })
				if !errors.Is(err, backenddb.ErrVacuumUnsupported) {
					t.Fatalf("Windows native vacuum refusal: %v", err)
				}
			} else if err = cached.Checkpoint(); err != nil {
				t.Fatalf("checkpoint without unsupported auto-vacuum: %v", err)
			}
			if backend.accepted.Load() != 0 || backend.pressure != nil || cached.cow.refreshRequired.Load() {
				t.Fatal("unsupported vacuum fabricated acceptance, pressure or a persistent coherence gate")
			}
			if got, revision, err := old.GetVersioned([]byte("a")); err != nil || !bytes.Equal(got, beforeValue) || revision != oldRevision {
				t.Fatalf("old cut after refusal: len%d revision%d want%d error%v", len(got), revision, oldRevision, err)
			}
			if got, err := old.Get([]byte("deleted")); err != nil || !bytes.Equal(got, beforeValue) {
				t.Fatalf("old deleted pointer len%d error%v", len(got), err)
			}
			if got, err := old.Get([]byte("empty")); err != nil || got == nil || len(got) != 0 {
				t.Fatalf("old empty%v error%v", got, err)
			}
			if got, err := cached.Get([]byte("a")); err != nil || !bytes.Equal(got, afterValue) {
				t.Fatalf("current pointer after refusal len%d error%v", len(got), err)
			}
			if has, err := cached.Has([]byte("deleted")); err != nil || has {
				t.Fatalf("current tombstone after refusal has%t error%v", has, err)
			}
			if err := cached.Set([]byte("resumed"), []byte("ok")); err != nil {
				t.Fatalf("writer after unsupported vacuum: %v", err)
			}
			if err := cached.Checkpoint(); err != nil {
				t.Fatalf("later checkpoint: %v", err)
			}
			if got, err := backend.Get([]byte("a")); err != nil || !bytes.Equal(got, afterValue) {
				t.Fatalf("later native pointer len%d error%v", len(got), err)
			}
			if has, err := backend.Has([]byte("deleted")); err != nil || has {
				t.Fatalf("later native tombstone has%t error%v", has, err)
			}
			if got, err := old.Get([]byte("a")); err != nil || !bytes.Equal(got, beforeValue) {
				t.Fatalf("old cut after later checkpoint len%d error%v", len(got), err)
			}
		})
	}
}
