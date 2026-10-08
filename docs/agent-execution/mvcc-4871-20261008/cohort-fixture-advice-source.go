package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
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
func BenchmarkPublishDurableRootV1(b *testing.B) { benchmarkPublishDurableRootV1(b, false) }

// The opt-in case measures the same complete SetSync/WriteSync operations and
// stable-call observers, with setup outside the timer and real PRIMARY output.
// It does not assign a new performance threshold or native qualification.
func BenchmarkPublishPrimaryDurableRootV1(b *testing.B) { benchmarkPublishDurableRootV1(b, true) }

func benchmarkPublishDurableRootV1(b *testing.B, primary bool) {
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
				Dir:                    dir,
				IndexPrimaryDirectory:  primary,
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
			if primary {
				idx := database.idx.Load()
				if idx.primary == nil || !primaryarena.IsPage(database.State().RootPageID) {
					b.Fatal("PRIMARY benchmark did not select PRIMARY root")
				}
				b.ReportMetric(float64(idx.primary.MetadataOwner().Bytes()), "primary-retained-bytes")
				b.ReportMetric(float64(database.valueLogIdentityPins.MetadataOwner().Bytes()), "registry-retained-bytes")
				b.ReportMetric(float64(idx.primary.Pager().PageCount()*page.PageSize), "primary-extent-bytes")
				b.ReportMetric(float64(idx.pager.PageCount()*page.PageSize), "data-extent-bytes")
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

// BenchmarkPrimaryBorrowedValueLogCohort measures actual selected kind adoption
// and scoped borrowing at the publication bridge. Fixture creation is excluded;
// each timed operation captures, imports/clones the selected source kind, reads
// one original value and releases its original Snapshot and selected wrapper.
// After timing, overwrite/Vacuum/GC expose held versus drained physical cost.
func BenchmarkPrimaryBorrowedValueLogCohort(b *testing.B) {
	for _, files := range []int{8, 16} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			fixture := newPrimaryBorrowedCohort(b, files)
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

type primaryBorrowedCohortFixture struct {
	db     *DB
	budget *memtable.COWBudget
	keys   [][]byte
	value  []byte
	files  int
}
type primaryBorrowedCohortLoan struct {
	snapshot  *Snapshot
	resources *rootpublication.StableResourceSet
	wrapper   *memtable.COWExternalLease
}

func newPrimaryBorrowedCohort(tb testing.TB, files int) *primaryBorrowedCohortFixture {
	tb.Helper()
	dir := tb.TempDir()
	d, err := Open(Options{Dir: dir, IndexPrimaryDirectory: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatal(err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		tb.Fatal(err)
	}
	f := &primaryBorrowedCohortFixture{db: d, budget: budget, files: files, value: bytes.Repeat([]byte("cohort"), 128), keys: make([][]byte, files)}
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
	if !primaryarena.IsPage(d.State().RootPageID) {
		tb.Fatal("cohort fixture did not select PRIMARY")
	}
	return f
}
func (f *primaryBorrowedCohortFixture) capture(tb testing.TB) primaryBorrowedCohortLoan {
	tb.Helper()
	var enrollment *retainedalloc.Enrollment
	var wrapper *memtable.COWExternalLease
	snapshot, err := f.db.AcquireSnapshotWithAllocationAdmission(func(sizes SnapshotAllocationSizes) error {
		var e error
		enrollment, e = retainedalloc.EnrollPair(sizes.PrimaryMetadata, sizes.RegistryMetadata, f.budget)
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
	if err := snapshot.AdoptPrimaryMetadataEnrollment(enrollment); err != nil {
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
	return primaryBorrowedCohortLoan{snapshot: snapshot, resources: resources, wrapper: wrapper}
}
func (f *primaryBorrowedCohortFixture) retention(tb testing.TB) map[string]int64 {
	tb.Helper()
	loan := f.capture(tb)
	old := loan.snapshot.idx
	oldArena := old.primary
	tb.Logf("original metadata owner=%p", oldArena.MetadataOwner())
	cost := make(map[string]int64)
	var paths []string
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
				paths = append(paths, file.Name())
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
		if err := f.db.VacuumIndexOnline(context.Background()); err != nil {
			return err
		}
		if f.db.idx.Load() == old {
			return fmt.Errorf("cohort vacuum did not replace actual DATA generation")
		}
		record := func(file *os.File, unit string) error {
			info, err := file.Stat()
			if err == nil {
				cost[unit] = info.Size()
			}
			return err
		}
		if err := old.pager.WithStableResourceFile(func(file *os.File) error { return record(file, "held-old-data-bytes") }); err != nil {
			return err
		}
		if err := oldArena.Pager().WithStableResourceFile(func(file *os.File) error { return record(file, "held-old-primary-bytes") }); err != nil {
			return err
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
		if oldArena.MetadataOwner().PhysicalClosed() {
			return fmt.Errorf("Close stole active cohort reader mapping")
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
		if loan.snapshot.primaryOwner != nil || loan.snapshot.treePager != nil {
			return fmt.Errorf("original reader cleanup did not complete")
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	loan.wrapper.Close()
	cost["drained-governor-bytes"] = int64(f.budget.Stats().ExternalBytes)
	if cost["drained-governor-bytes"] != 0 {
		f.db.ghostManager.mu.Lock()
		tb.Logf("before Close original generation refs=%d ghosts=%d", old.refs.Load(), len(f.db.ghostManager.ghosts))
		f.db.ghostManager.mu.Unlock()
		tb.Logf("actual DB Close=%v", f.db.Close())
		f.db.ghostManager.mu.Lock()
		tb.Logf("after Close original generation refs=%d ghosts=%d stats=%+v oldArenaBytes=%d closed=%v ownerRefs=%d cleanup=%v", old.refs.Load(), len(f.db.ghostManager.ghosts), f.budget.Stats(), oldArena.MetadataOwner().Bytes(), oldArena.MetadataOwner().PhysicalClosed(), old.primaryOwner.refs, old.primaryOwner.cleanupErr)
		f.db.ghostManager.mu.Unlock()
		tb.Fatalf("completed cohort retained temporary governor: stats=%+v oldArenaBytes=%d oldArenaClosed=%v ownerRefs=%d cleanup=%v cost=%v",f.budget.Stats(),oldArena.MetadataOwner().Bytes(),oldArena.MetadataOwner().PhysicalClosed(),old.primaryOwner.refs,old.primaryOwner.cleanupErr,cost)
	}
	if !oldArena.MetadataOwner().PhysicalClosed() {
		tb.Fatal("cohort original mapping did not complete physical cleanup")
	}
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
	loan.wrapper.Close()
	return cost
}

func TestPrimaryBorrowedValueLogCohortUsesSelectedBridgeAndOriginalCleanup(t *testing.T) {
	f := newPrimaryBorrowedCohort(t, 2)
	cost := f.retention(t)
	if cost["held-cohort-files"] != 2 || cost["held-cohort-bytes"] == 0 || cost["held-old-data-bytes"] == 0 || cost["held-old-primary-bytes"] == 0 {
		t.Fatal("cohort did not observe original physical owners", cost)
	}
	t.Logf("actual retained/residual cohort: %v", cost)
}
