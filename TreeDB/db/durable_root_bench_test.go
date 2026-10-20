package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// BenchmarkSelectDurableRootV1 is the committed matched recovery fixture for
// the V1 format cutover. Both cases retain two independently complete slots;
// only the newest slot's deterministic dependency inventory varies. Page reads
// are counted at the PageSource boundary, so the benchmark will expose any
// accidental recursive tree walk as additional validation work.
func BenchmarkSelectDurableRootV1(b *testing.B) {
	for _, resourceCount := range []int{0, 64} {
		b.Run(fmt.Sprintf("resources=%d", resourceCount), func(b *testing.B) {
			manifest := benchmarkDurableRootManifestV1(b, resourceCount)
			fixture := newDurableRootFixtureV1(b)
			fixture.addCandidate(b, 0)
			newest := fixture.addCandidateWithManifest(b, 1, manifest)

			var resourcesValidated uint64
			validator := func(candidate *rootpublication.DependencyManifestV1) (*rootpublication.StableResourceSet, error) {
				resourcesValidated += uint64(len(candidate.Entries()))
				return nil, nil
			}
			fixture.store.Reads = 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				selected, err := selectDurableRootV1(fixture.store, newest.Record.TotalPages, validator)
				if err != nil {
					b.Fatal(err)
				}
				if selected.Slot != 1 || selected.Record.CommitSeq != newest.Record.CommitSeq {
					b.Fatalf("selected slot=%d commit=%d, want slot=1 commit=%d", selected.Slot, selected.Record.CommitSeq, newest.Record.CommitSeq)
				}
				for _, resources := range selected.SlotResources {
					resources.Release()
				}
			}
			b.StopTimer()
			if b.N == 0 {
				return
			}
			operations := float64(b.N)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/operations, "open-ns/op")
			b.ReportMetric(float64(resourcesValidated)/operations, "resources-validated/op")
			b.ReportMetric(float64(fixture.store.Reads)/operations, "pages-validated/op")
			b.ReportMetric(float64(newest.Record.Manifest.ByteLength), "manifest-bytes")
			b.ReportMetric(float64(page.PageSize), "record-bytes")
			b.ReportMetric(float64(fixture.lastPublicationPages*page.PageSize), "publication-bytes")
			b.ReportMetric(2, "recoverable-slots")
		})
	}
}

// BenchmarkPublishDurableRootV1 records the stable-call shape and wall time of
// the synchronous root transaction. The two cases are identical key/value
// workloads; only the inline threshold changes whether the root manifest owns
// an external value-log dependency.
func BenchmarkPublishDurableRootV1(b *testing.B) { benchmarkPublishDurableRootV1(b) }

// The opt-in case measures the same complete SetSync/WriteSync operations and
// stable-call observers, with setup outside the timer and real DATA output.
// It does not assign a new performance threshold or native qualification.

func benchmarkPublishDurableRootV1(b *testing.B) {
	for _, fixture := range []struct {
		name          string
		valueLog      bool
		inlineCutover int
	}{
		{name: "inline", inlineCutover: 4096},
		{name: "value-log", valueLog: true, inlineCutover: 1},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			dir := b.TempDir()
			value := bytes.Repeat([]byte("v"), 256)
			var pointers []page.ValuePtr
			if fixture.valueLog {
				pointers = appendPointersInNewSegmentBench(b, dir, 0, 1, 1, b.N, func(int) []byte { return value })
			}
			database, err := Open(Options{
				Dir: dir,

				DisableBackgroundPrune: true,
				ValueLog: ValueLogOptions{
					PointerThreshold: fixture.inlineCutover,
					ForcePointers:    fixture.valueLog,
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := database.Close(); err != nil {
					b.Errorf("Close: %v", err)
				}
			})

			keys := make([][]byte, b.N)
			for i := range keys {
				keys[i] = strconv.AppendInt([]byte("durable-root-bench/"), int64(i), 10)
			}
			stable := newDurableRootStableCallAccumulator()
			restore := durabilitycut.Install(stable.observe)
			b.Cleanup(restore)
			var resourceStats durableRootResourceCallTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if fixture.valueLog {
					batch := database.NewBatch().(*Batch)
					if err := batch.SetPointer(keys[i], pointers[i]); err != nil {
						_ = batch.Close()
						b.Fatalf("SetPointer(%d): %v", i, err)
					}
					if err := batch.WriteSync(); err != nil {
						_ = batch.Close()
						b.Fatalf("WriteSync(%d): %v", i, err)
					}
					if err := batch.Close(); err != nil {
						b.Fatalf("Close batch %d: %v", i, err)
					}
				} else if err := database.SetSync(keys[i], value); err != nil {
					b.Fatalf("SetSync(%d): %v", i, err)
				}
				resourceStats.addSelected(database)
			}
			b.StopTimer()
			if b.N == 0 {
				return
			}

			database.durablePublishMu.Lock()
			record := database.durableRoot.record
			slotCommits := database.durableRoot.slotCommit
			database.durablePublishMu.Unlock()
			recoverableSlots := 0
			for _, commit := range slotCommits {
				if commit != 0 {
					recoverableSlots++
				}
			}

			operations := float64(b.N)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/operations, "publish-ns/op")
			b.ReportMetric(float64(record.Manifest.EntryCount), "manifest-entries")
			b.ReportMetric(float64(record.Manifest.ByteLength), "manifest-bytes")
			b.ReportMetric(float64(recoverableSlots), "recoverable-slots")
			resourceStats.report(b, operations)
			stable.report(b, operations)
		})
	}
}

type durableRootResourceCallTotals struct {
	flushes               uint64
	flushDuration         time.Duration
	syncs                 uint64
	syncDuration          time.Duration
	physicalFileSyncs     uint64
	physicalFileSyncNanos time.Duration
	namespaceSyncs        uint64
	namespaceSyncNanos    time.Duration
}

func (totals *durableRootResourceCallTotals) addSelected(database *DB) {
	if totals == nil || database == nil {
		return
	}
	database.durablePublishMu.Lock()
	resources := database.durableRoot.slotResources[database.durableRoot.slot]
	stats := resources.Stats(time.Now())
	database.durablePublishMu.Unlock()
	for _, kind := range stats {
		totals.flushes += kind.Flushes
		totals.flushDuration += kind.FlushDuration
		totals.syncs += kind.Syncs
		totals.syncDuration += kind.SyncDuration
		totals.physicalFileSyncs += kind.PhysicalFileSyncs
		totals.physicalFileSyncNanos += kind.PhysicalFileSyncDuration
		totals.namespaceSyncs += kind.NamespaceSyncs
		totals.namespaceSyncNanos += kind.NamespaceSyncDuration
	}
}

func (totals durableRootResourceCallTotals) report(b *testing.B, operations float64) {
	b.Helper()
	b.ReportMetric(float64(totals.flushes)/operations, "resource-flush-calls/op")
	b.ReportMetric(float64(totals.flushDuration.Nanoseconds())/operations, "resource-flush-ns/op")
	b.ReportMetric(float64(totals.syncs)/operations, "resource-sync-calls/op")
	b.ReportMetric(float64(totals.syncDuration.Nanoseconds())/operations, "resource-sync-ns/op")
	b.ReportMetric(float64(totals.physicalFileSyncs)/operations, "resource-file-stable-calls/op")
	b.ReportMetric(float64(totals.physicalFileSyncNanos.Nanoseconds())/operations, "resource-file-stable-ns/op")
	b.ReportMetric(float64(totals.namespaceSyncs)/operations, "resource-namespace-stable-calls/op")
	b.ReportMetric(float64(totals.namespaceSyncNanos.Nanoseconds())/operations, "resource-namespace-stable-ns/op")
}

type durableRootStableCallAccumulator struct {
	mu                        sync.Mutex
	started                   map[string]time.Time
	calls                     map[string]uint64
	durations                 map[string]time.Duration
	metaWrites                uint64
	metaBytes                 uint64
	observedCallerGoroutine   uint64
	observedCallerStableCalls uint64
}

func newDurableRootStableCallAccumulator() *durableRootStableCallAccumulator {
	return &durableRootStableCallAccumulator{
		started: make(map[string]time.Time), calls: make(map[string]uint64), durations: make(map[string]time.Duration),
	}
}

func (accumulator *durableRootStableCallAccumulator) observeCaller(goroutineID uint64) {
	accumulator.mu.Lock()
	accumulator.observedCallerGoroutine = goroutineID
	accumulator.mu.Unlock()
}

func (accumulator *durableRootStableCallAccumulator) callerStableCalls() uint64 {
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	return accumulator.observedCallerStableCalls
}

func (accumulator *durableRootStableCallAccumulator) observe(event durabilitycut.Event) error {
	if event.Point == durabilitycut.BeforeMetaWrite {
		accumulator.mu.Lock()
		accumulator.metaWrites++
		if event.Length > 0 {
			accumulator.metaBytes += uint64(event.Length)
		}
		accumulator.mu.Unlock()
		return nil
	}
	phase, before, ok := durableRootStablePhase(event.Point)
	if !ok {
		return nil
	}
	key := phase + "|" + string(event.Resource) + "|" + event.Path + "|" + strings.Join(event.Paths, "\x00")
	now := time.Now()
	caller := uint64(0)
	if before && phase != "userspace-flush" {
		caller = currentGoroutineID()
	}
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	if before {
		if caller != 0 && caller == accumulator.observedCallerGoroutine {
			accumulator.observedCallerStableCalls++
		}
		accumulator.started[key] = now
		return nil
	}
	started, exists := accumulator.started[key]
	if !exists {
		return nil
	}
	delete(accumulator.started, key)
	accumulator.calls[phase]++
	accumulator.durations[phase] += now.Sub(started)
	return nil
}

func durableRootStablePhase(point durabilitycut.Point) (phase string, before bool, ok bool) {
	switch point {
	case durabilitycut.BeforeUserspaceFlush:
		return "userspace-flush", true, true
	case durabilitycut.AfterUserspaceFlush:
		return "userspace-flush", false, true
	case durabilitycut.BeforeDependencyFileSync:
		return "dependency-stable", true, true
	case durabilitycut.AfterDependencyFileSync:
		return "dependency-stable", false, true
	case durabilitycut.BeforeNewFileDirectorySync:
		return "namespace-stable", true, true
	case durabilitycut.AfterNewFileDirectorySync:
		return "namespace-stable", false, true
	case durabilitycut.BeforeIndexDataSync:
		return "index-stable", true, true
	case durabilitycut.AfterIndexDataSync:
		return "index-stable", false, true
	case durabilitycut.BeforeMetaSync:
		return "meta-stable", true, true
	case durabilitycut.AfterMetaSync:
		return "meta-stable", false, true
	default:
		return "", false, false
	}
}

func (accumulator *durableRootStableCallAccumulator) report(b *testing.B, operations float64) {
	b.Helper()
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	for _, phase := range []string{"userspace-flush", "dependency-stable", "namespace-stable", "index-stable", "meta-stable"} {
		b.ReportMetric(float64(accumulator.calls[phase])/operations, phase+"-calls/op")
		b.ReportMetric(float64(accumulator.durations[phase].Nanoseconds())/operations, phase+"-ns/op")
	}
	b.ReportMetric(float64(accumulator.metaWrites)/operations, "meta-writes/op")
	b.ReportMetric(float64(accumulator.metaBytes)/operations, "meta-write-B/op")
}

func benchmarkDurableRootManifestV1(tb testing.TB, resourceCount int) *rootpublication.DependencyManifestV1 {
	tb.Helper()
	entries := make([]rootpublication.DependencyManifestEntryV1, resourceCount)
	for i := range entries {
		generation := uint64(i + 1)
		resourceID := fmt.Sprintf("resource-%03d", i)
		var objectID [16]byte
		binary.LittleEndian.PutUint64(objectID[:8], generation)
		binary.LittleEndian.PutUint64(objectID[8:], generation^0x9e3779b97f4a7c15)
		entries[i] = rootpublication.DependencyManifestEntryV1{
			Kind:           rootpublication.ResourceValueLog,
			LogicalLane:    "benchmark/value-log",
			ResourceID:     resourceID,
			DiagnosticPath: "value-log/" + resourceID + ".vlog",
			Identity: rootpublication.StableIdentity{
				Platform: "benchmark", VolumeID: 1, ObjectID: objectID, Generation: generation,
			},
			Generation:   generation,
			Digest:       sha256.Sum256([]byte(resourceID)),
			Frontier:     rootpublication.DurableFrontier{Bytes: 4096 + generation},
			Reachability: []rootpublication.ReachabilityField{rootpublication.ReachabilityValueLogPointer},
		}
	}
	manifest, err := rootpublication.NewDependencyManifestV1(entries)
	if err != nil {
		tb.Fatal(err)
	}
	return manifest
}

// BenchmarkDATABorrowedValueLogCohort measures actual selected kind adoption
// and scoped borrowing at the publication bridge. Fixture creation is excluded;
// each timed operation captures, imports/clones the selected source kind, reads
// one original value and releases its original Snapshot and selected wrapper.
// After timing, overwrite/Vacuum/GC expose held versus drained physical cost.
func BenchmarkDATABorrowedValueLogCohort(b *testing.B) {
	for _, files := range []int{8, 16} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			fixture := newDATABorrowedCohort(b, files)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loan := fixture.capture(b)
				err := loan.resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
					loan.resources.Release() // Scoped descriptor remains the real owner.
					if len(tokens) != files {
						return fmt.Errorf("selected cohort tokens=%d want%d", len(tokens), files)
					}
					value, err := loan.snapshot.Get(fixture.keys[0])
					if err != nil || !bytes.Equal(value, fixture.value) {
						return fmt.Errorf("original cohort read: %w", err)
					}
					return loan.snapshot.Close()
				})
				if err != nil {
					b.Fatal(err)
				}
				loan.enrollment.Close()
				loan.wrapper.Close()
				if fixture.budget.Stats().ExternalBytes != 0 {
					b.Fatal("completed borrow retained temporary governor")
				}
			}
			b.StopTimer()
			cost := fixture.retention(b)
			for unit, value := range cost {
				b.ReportMetric(float64(value), unit)
			}
		})
	}
}

type dataBorrowedCohortFixture struct {
	db       *DB
	budget   *memtable.COWBudget
	keys     [][]byte
	value    []byte
	files    int
	pointers map[uint32]page.ValuePtr
}
type dataBorrowedCohortLoan struct {
	snapshot   *Snapshot
	resources  *rootpublication.StableResourceSet
	wrapper    *memtable.COWExternalLease
	enrollment *retainedalloc.Enrollment
}

func newDATABorrowedCohort(tb testing.TB, files int) *dataBorrowedCohortFixture {
	tb.Helper()
	dir := tb.TempDir()
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatal(err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		tb.Fatal(err)
	}
	f := &dataBorrowedCohortFixture{db: d, budget: budget, files: files, value: bytes.Repeat([]byte("cohort"), 128), keys: make([][]byte, files), pointers: make(map[uint32]page.ValuePtr, files)}
	tb.Cleanup(func() {
		if err := d.Close(); err != nil {
			tb.Error(err)
		}
		if budget.Stats().ExternalBytes != 0 {
			tb.Error("cohort final cleanup retained temporary governor")
		}
		budget.Close()
	})
	pending := d.NewBatch().(*Batch)
	defer pending.Close()
	for i := 0; i < files; i++ {
		f.keys[i] = []byte(fmt.Sprintf("cohort/%02d", i))
		pointers := appendPointersInNewSegmentBench(tb, dir, 0, uint32(i+1), uint64(i+1)*100, 1, func(int) []byte { return f.value })
		f.pointers[pointers[0].FileID] = pointers[0]
		if err := pending.SetPointer(f.keys[i], pointers[0]); err != nil {
			tb.Fatal(err)
		}
	}
	if err := d.RefreshValueLogSet(); err != nil {
		tb.Fatal(err)
	}
	if err := pending.WriteSync(); err != nil {
		tb.Fatal(err)
	}

	return f
}
func (f *dataBorrowedCohortFixture) capture(tb testing.TB) dataBorrowedCohortLoan {
	tb.Helper()
	var enrollment *retainedalloc.Enrollment
	var wrapper *memtable.COWExternalLease
	snapshot, err := f.db.AcquireSnapshotWithAllocationAdmission(func(sizes SnapshotAllocationSizes) error {
		var e error
		enrollment, e = sizes.RegistryMetadata.Enroll(f.budget)
		if e != nil {
			return e
		}
		wrapper, e = f.budget.AcquireExternal(retainedalloc.AllocationCharge(sizes.Wrapper))
		return e
	})
	if err != nil {
		enrollment.Close()
		wrapper.Close()
		tb.Fatal(err)
	}

	// The live ordinary publication has already adopted each original provider's
	// descriptor/operation callback. Select its actual source-kind backing, not a
	// synthetic token or a generic Snapshot-only retention proxy.
	visible, err := f.db.rootPublication.cloneVisibleResources()
	if err != nil {
		tb.Fatal(err)
	}
	resources, err := rootpublication.CloneStableResourceSetExcludingKinds(visible, rootpublication.ResourceIndex)
	visible.Release()
	if err != nil {
		tb.Fatal(err)
	}
	if resources.Len() != f.files {
		tb.Fatalf("cohort selected resources=%d want%d", resources.Len(), f.files)
	}
	return dataBorrowedCohortLoan{snapshot: snapshot, resources: resources, wrapper: wrapper, enrollment: enrollment}
}
func (f *dataBorrowedCohortFixture) retention(tb testing.TB) map[string]int64 {
	tb.Helper()
	loan := f.capture(tb)
	old := loan.snapshot.idx
	cost := make(map[string]int64)
	var paths []string
	originalFiles := make(map[*rootpublication.StableResourceToken]os.FileInfo, f.files)
	err := loan.resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
		loan.resources.Release()
		for _, token := range tokens {
			if token.Kind() != rootpublication.ResourceValueLog {
				return fmt.Errorf("unexpected cohort kind %s", token.Kind())
			}
			if err := token.WithPinnedFile(func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				cost["held-cohort-bytes"] += info.Size()
				cost["held-cohort-files"]++
				originalFiles[token] = info
				// Stable duplicated handles have synthetic Names. Bind residual
				// filesystem observations to the original descriptor path/identity.
				path := filepath.Join(f.db.dir, token.DiagnosticPath())
				original, err := os.Stat(path)
				if err != nil || !os.SameFile(info, original) {
					return fmt.Errorf("cohort original path identity: %w", err)
				}
				paths = append(paths, path)
				return nil
			}); err != nil {
				return err
			}
		}
		pending := f.db.NewBatch().(*Batch)
		for _, key := range f.keys {
			if err := pending.Set(key, []byte("new-inline")); err != nil {
				return err
			}
		}
		if err := pending.WriteSync(); err != nil {
			return err
		}
		if err := pending.Close(); err != nil {
			return err
		}
		// Replace both eligible slots and their one-hop proof frontiers before GC.
		if err := f.db.SetSync([]byte("cohort/advance-a"), []byte("a")); err != nil {
			return err
		}
		if err := f.db.SetSync([]byte("cohort/advance-b"), []byte("b")); err != nil {
			return err
		}
		beforeVacuum := f.db.state.Load()
		vacuumErr := f.db.VacuumIndexOnline(context.Background())
		if runtime.GOOS == "windows" {
			// The maintained platform refusal precedes replacement capture. Keep
			// exercising original cohort custody without claiming a retired DATA.
			if !errors.Is(vacuumErr, ErrVacuumUnsupported) || f.db.idx.Load() != old || f.db.state.Load() != beforeVacuum {
				return fmt.Errorf("cohort unsupported vacuum changed DATA or returned %v", vacuumErr)
			}
			cost["vacuum-unsupported"] = 1
		} else {
			if vacuumErr != nil {
				return vacuumErr
			}
			if f.db.idx.Load() == old {
				return fmt.Errorf("cohort vacuum did not replace actual DATA generation")
			}
		}
		record := func(file *os.File, unit string) error {
			info, err := file.Stat()
			if err == nil {
				cost[unit] = info.Size()
			}
			return err
		}
		if runtime.GOOS != "windows" {
			if err := old.pager.WithStableResourceFile(func(file *os.File) error { return record(file, "held-old-data-bytes") }); err != nil {
				return err
			}
		}

		cost["held-governor-bytes"] = int64(f.budget.Stats().ExternalBytes)
		if value, err := loan.snapshot.Get(f.keys[0]); err != nil || !bytes.Equal(value, f.value) {
			return fmt.Errorf("held old cohort read: %w", err)
		}
		if err := loan.snapshot.beginRead(); err != nil {
			return err
		}
		if err := loan.snapshot.Close(); err != nil {
			return err
		}

		gc, err := f.db.ValueLogGC(context.Background(), ValueLogGCOptions{})
		if err != nil {
			return err
		}
		cost["held-gc-deleted-files"] = int64(gc.SegmentsDeleted)
		for _, token := range tokens {
			if err := token.WithPinnedFile(func(file *os.File) error { _, err := file.Stat(); return err }); err != nil {
				return err
			}
		}
		start := time.Now()
		loan.snapshot.endRead()
		cost["last-reader-cleanup-ns"] = time.Since(start).Nanoseconds()
		// The descriptor scope is still a real governor/physical callback edge.
		cost["scope-after-reader-governor-bytes"] = int64(f.budget.Stats().ExternalBytes)
		if loan.snapshot.treePager != nil {
			return fmt.Errorf("original reader cleanup did not complete")
		}
		gc, err = f.db.ValueLogGC(context.Background(), ValueLogGCOptions{})
		if err != nil {
			return err
		}
		cost["descriptor-only-gc-deleted-files"] = int64(gc.SegmentsDeleted)
		for _, token := range tokens {
			if err := f.verifyOriginalCohortToken(token, originalFiles[token]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	loan.enrollment.Close()
	loan.wrapper.Close()
	// Scope completion releases private descriptor aliases. After a successful
	// replacement, the retired arena remains governed through its existing
	// generation ghost lifetime; no age is forced and residuals are measured.
	cost["scope-complete-governor-bytes"] = int64(f.budget.Stats().ExternalBytes)
	gc, err := f.db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	cost["drained-gc-deleted-files"] = int64(gc.SegmentsDeleted)
	for _, path := range paths {
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			tb.Fatal(err)
		}
		cost["drained-cohort-files"]++
		cost["drained-cohort-bytes"] += info.Size()
	}
	if err := loan.snapshot.Close(); err != nil {
		tb.Fatal(err)
	}
	loan.resources.Release()
	loan.enrollment.Close()
	loan.wrapper.Close()
	// Actual DB teardown drains the existing ghost manager and closes original
	// physical owners. This terminal action, not public Snapshot.Close return,
	// must end every successful allocation/operation edge exactly once.
	if err := f.db.Close(); err != nil {
		tb.Fatal(err)
	}
	cost["closed-governor-bytes"] = int64(f.budget.Stats().ExternalBytes)
	if cost["closed-governor-bytes"] != 0 || f.budget.Stats().ExternalLeases != 0 {
		tb.Fatalf("actual cohort teardown retained original custody: cost=%v", cost)
	}
	if err := f.db.Close(); err != nil || f.budget.Stats().ExternalBytes != 0 {
		tb.Fatalf("repeated actual cleanup changed terminal outcome: %v", err)
	}
	return cost
}

// Called after original Snapshot finalization, while only the selected
// descriptor scope retains the original provider's physical operation edge.
// An unlinked pinned inode is valid physical custody; a surviving name must
// still identify that same original file. CRC-verified payload reads, rather
// than Stat alone, prove the retained handle remains usable.
func (f *dataBorrowedCohortFixture) verifyOriginalCohortToken(token *rootpublication.StableResourceToken, original os.FileInfo) error {
	fileID, err := strconv.ParseUint(token.ResourceID(), 10, 32)
	if err != nil {
		return err
	}
	pointer, ok := f.pointers[uint32(fileID)]
	if !ok || original == nil {
		return errors.New("cohort token does not identify an original producer frame")
	}
	return token.WithPinnedFile(func(file *os.File) error {
		info, err := file.Stat()
		if err != nil || !os.SameFile(original, info) {
			return fmt.Errorf("descriptor-only original inode: %w", err)
		}
		if named, err := os.Stat(filepath.Join(f.db.dir, token.DiagnosticPath())); err == nil {
			if !os.SameFile(info, named) {
				return errors.New("surviving cohort path changed original identity")
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		value, err := valuelog.ReadAt(file, pointer, true)
		if err != nil || !bytes.Equal(value, f.value) {
			return fmt.Errorf("descriptor-only original frame/payload: %w", err)
		}
		return nil
	})
}

func TestDATABorrowedValueLogCohortUsesOriginalCleanup(t *testing.T) {
	for _, files := range []int{2, 8, 16} {
		t.Run(fmt.Sprintf("files=%d", files), func(t *testing.T) {
			f := newDATABorrowedCohort(t, files)

			cost := f.retention(t)
			if cost["held-cohort-files"] != int64(files) || cost["held-cohort-bytes"] == 0 {
				t.Fatal("cohort did not observe original physical owners", cost)
			}
			if runtime.GOOS == "windows" {
				if cost["vacuum-unsupported"] != 1 || cost["held-old-data-bytes"] != 0 {
					t.Fatal("unsupported vacuum reported retired DATA custody", cost)
				}
			} else if cost["held-old-data-bytes"] == 0 {
				t.Fatal("cohort did not observe original retired DATA owner", cost)
			}
			t.Logf("actual retained/residual cohort: %v", cost)
		})
	}
}

func TestDATABorrowedCohortRebuildFailureReleasesPrivateManifests(t *testing.T) {
	f := newDATABorrowedCohort(t, 2)
	loan := f.capture(t)
	want := errors.New("reject rebuilt runtime before cutover")
	wantRefusal := want
	if runtime.GOOS == "windows" {
		wantRefusal = ErrVacuumUnsupported
	}
	hookCalled := false
	f.db.vacuumReplacementRuntimeHook = func(*rootPublicationRuntimeV1) error {
		hookCalled = true
		return want
	}
	err := loan.resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
		loan.resources.Release()
		originalFiles := make(map[*rootpublication.StableResourceToken]os.FileInfo, len(tokens))
		for _, token := range tokens {
			if err := token.WithPinnedFile(func(file *os.File) error {
				info, err := file.Stat()
				originalFiles[token] = info
				return err
			}); err != nil {
				return err
			}
		}

		beforeVacuum, originalGen := f.db.state.Load(), f.db.idx.Load()
		if err := f.db.VacuumIndexOnline(context.Background()); !errors.Is(err, wantRefusal) {
			return fmt.Errorf("rebuild refusal=%v want %v", err, wantRefusal)
		}
		if hookCalled != (runtime.GOOS != "windows") {
			return fmt.Errorf("replacement runtime hook called=%t on %s", hookCalled, runtime.GOOS)
		}
		if runtime.GOOS == "windows" && (f.db.state.Load() != beforeVacuum || f.db.idx.Load() != originalGen) {
			return errors.New("unsupported vacuum changed original DATA publication")
		}

		if value, err := loan.snapshot.Get(f.keys[0]); err != nil || !bytes.Equal(value, f.value) {
			return fmt.Errorf("refused rebuild changed original reader: %w", err)
		}
		if err := loan.snapshot.beginRead(); err != nil {
			return err
		}
		if err := loan.snapshot.Close(); err != nil {
			return err
		}
		if err := f.db.Close(); err != nil {
			return err
		}

		for _, token := range tokens {
			if err := token.WithPinnedFile(func(file *os.File) error { _, err := file.Stat(); return err }); err != nil {
				return err
			}
		}
		loan.snapshot.endRead()
		for _, token := range tokens {
			if err := f.verifyOriginalCohortToken(token, originalFiles[token]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		var original *originalDBCleanupFailure
		if errors.As(err, &original) {
			original.mu.Lock()
			causes := original.causes
			original.mu.Unlock()
			for i, cause := range causes {
				t.Logf("original cleanup cause[%d]=%T %v", i, cause, cause)
			}
		}
		t.Fatalf("refused rebuild cleanup: ownership=%t closed=%t: %v", errors.Is(err, rootpublication.ErrResourceOwnership), errors.Is(err, os.ErrClosed), err)
	}
	loan.enrollment.Close()
	loan.wrapper.Close()
	if f.budget.Stats().ExternalBytes != 0 || f.budget.Stats().ExternalLeases != 0 {
		t.Fatalf("refused rebuild retained completed private manifests: stats=%+v", f.budget.Stats())
	}
	if err := loan.snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	loan.resources.Release()
	loan.enrollment.Close()
	loan.wrapper.Close()
}
