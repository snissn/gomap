package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
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
func BenchmarkPublishDurableRootV1(b *testing.B) {
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
