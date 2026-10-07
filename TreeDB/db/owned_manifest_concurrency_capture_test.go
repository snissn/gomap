package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The same diagnostic source can compile against frozen797 with an independently
// frozen test-only overlay. Public calls and assertions are identical; only the
// explicitly selected admission/revision constructor differs.
func TestR1ManifestConcurrentACK5095(t *testing.T) {
	layout, n := r1ManifestCaptureDimensions(t)
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	r1SelectOwnedManifestCapture(t, &opts, layout)
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer leafLog.Close()
	d.SetLeafPageLog(leafLog)
	initial := d.NewPhysicalBatch().(*Batch)
	for i := 0; i < 32; i++ {
		if err = initial.Set([]byte(fmt.Sprintf("concurrent/%02d", i)), []byte("initial complete row")); err != nil {
			t.Fatal(err)
		}
	}
	if err = initial.WriteSync(); err != nil {
		t.Fatal(err)
	}
	initial.Close()
	held := d.AcquireSnapshot()
	// Pin and verify the complete original logical view independently of later ACKs.
	defer held.Close()
	r1PopulateManifestCaptureRevisions(t, d, layout, n)
	ackDurations := make([]time.Duration, 0, 32)
	phaseDurations := make([]time.Duration, 0, 4096)
	var phaseStart time.Time
	begin := make(chan struct{})
	attemptReady := make(chan struct{})
	var gcActive, fenceActive, rendezvousFailed atomic.Bool
	var startsDuringGC, completionsDuringGC, startsDuringFence atomic.Int64
	var once sync.Once
	unregister := registerLeafGenerationGCExclusivePhaseHook(func(entering bool) {
		if entering {
			phaseStart = time.Now()
			fenceActive.Store(true)
			// The first public request marks its attempt and rendezvous here
			// before WriteSync. Both layouts charge this bounded orchestration.
			once.Do(func() {
				close(begin)
				timer := time.NewTimer(30 * time.Second)
				defer timer.Stop()
				select {
				case <-attemptReady:
				case <-timer.C:
					rendezvousFailed.Store(true)
				}
			})
		} else {
			fenceActive.Store(false)
			phaseDurations = append(phaseDurations, time.Since(phaseStart))
		}
	})
	defer unregister()
	writerDone := make(chan error, 1)
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	start := time.Now()
	gcActive.Store(true)
	go func() {
		<-begin
		for i := 0; i < 32; i++ {
			now := time.Now()
			batch := d.NewPhysicalBatch().(*Batch)
			e := batch.Set([]byte(fmt.Sprintf("concurrent/%02d", i)), []byte("acknowledged complete row"))
			if e == nil {
				if gcActive.Load() {
					startsDuringGC.Add(1)
				}
				if fenceActive.Load() {
					startsDuringFence.Add(1)
				}
				if i == 0 {
					close(attemptReady)
				}
				e = batch.WriteSync()
				if gcActive.Load() {
					completionsDuringGC.Add(1)
				}
			} else if i == 0 {
				close(attemptReady)
			}
			batch.Close()
			ackDurations = append(ackDurations, time.Since(now))
			if e != nil {
				writerDone <- e
				return
			}
		}
		writerDone <- nil
	}()
	stats, gcErr := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	firstStats, firstGCError := stats, gcErr
	gcActive.Store(false)
	gcElapsed := time.Since(start)
	once.Do(func() { close(begin) })
	if err = <-writerDone; err != nil {
		t.Fatal(err)
	}
	if rendezvousFailed.Load() || startsDuringGC.Load() == 0 || startsDuringFence.Load() == 0 {
		t.Fatalf("no measured request-attempt overlap: rendezvous_failed=%t gc_starts=%d fence_starts=%d completions=%d", rendezvousFailed.Load(), startsDuringGC.Load(), startsDuringFence.Load(), completionsDuringGC.Load())
	}
	gcCalls := 1
	// A stale capture may defer reclamation; after the finite writer completes,
	// repeat the public operation once and charge its complete cost.
	if errors.Is(gcErr, ErrRecoverableRootSetStale) {
		gcCalls++
		stats, gcErr = d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&b)
	if gcErr != nil {
		t.Fatal(gcErr)
	}
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("concurrent/%02d", i))
		value, e := d.Get(key)
		if e != nil || !bytes.Equal(value, []byte("acknowledged complete row")) {
			t.Fatalf("acknowledged row %d: %q %v", i, value, e)
		}
		original, e := held.Get(key)
		if e != nil || !bytes.Equal(original, []byte("initial complete row")) {
			t.Fatalf("held row %d: %q %v", i, original, e)
		}
	}
	t.Logf("layout=%s revisions=%d public_acks=%d ack_starts_during_gc=%d ack_completions_during_gc=%d ack_starts_during_fence=%d whole_elapsed_ns=%d first_gc_elapsed_ns=%d gc_calls=%d allocated_bytes=%d ack_p95_ns=%d ack_p99_ns=%d fence_samples=%d fence_p95_ns=%d fence_p99_ns=%d first_gc_error=%v first_gc_stats=%+v final_gc_stats=%+v", layout, n, len(ackDurations), startsDuringGC.Load(), completionsDuringGC.Load(), startsDuringFence.Load(), elapsed.Nanoseconds(), gcElapsed.Nanoseconds(), gcCalls, b.TotalAlloc-a.TotalAlloc, leafGenerationGCBenchmarkPercentile(ackDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(ackDurations, 99).Nanoseconds(), len(phaseDurations), leafGenerationGCBenchmarkPercentile(phaseDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(phaseDurations, 99).Nanoseconds(), firstGCError, firstStats, stats)
	// Legacy hook attribution excludes its revision writeMu hold. Its reported
	// segment samples are never called complete legacy revision-fence evidence.
}

// Shared selectors keep control/candidate dimensions and public calls identical.
// Reflection is limited to this test-only optional feature selector.
func r1ManifestCaptureDimensions(t *testing.T) (string, int) {
	t.Helper()
	layout := os.Getenv("GOMAP_L_CONCURRENT_LAYOUT")
	if layout == "" {
		t.Skip("opt-in source-bound public manifest capture")
	}
	n, err := strconv.Atoi(os.Getenv("GOMAP_L_GC_REVISIONS"))
	if err != nil || (n != 128 && n != 512) {
		t.Fatal("requires frozen 128/512 dimensions")
	}
	if layout != "owned" && layout != "legacy" {
		t.Fatal("unknown explicit layout")
	}
	return layout, n
}

func r1SelectOwnedManifestCapture(t *testing.T, opts *Options, layout string) {
	t.Helper()
	if layout == "owned" {
		field := reflect.ValueOf(opts).Elem().FieldByName("OwnedLeafManifests")
		if !field.IsValid() || field.Kind() != reflect.Bool || !field.CanSet() {
			t.Fatal("owned layout unavailable in this binary")
		}
		field.SetBool(true)
	}
}

func r1PopulateManifestCaptureRevisions(t *testing.T, d *DB, layout string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if layout == "owned" {
			checkpoint, ok := any(d).(interface{ CheckpointOwnedLeafManifest() (uint64, error) })
			if !ok {
				t.Fatal("owned checkpoint unavailable")
			}
			if _, err := checkpoint.CheckpointOwnedLeafManifest(); err != nil {
				t.Fatal(err)
			}
		} else {
			closure, err := d.PrepareLeafGenerationManifestStableClosure()
			if err != nil {
				t.Fatal(err)
			}
			closure.Release()
		}
	}
}

// Preserve the original empty-store N-revision GC workload, while additionally
// reporting setup cost and allocator work. Internal page reuse is distinct from
// standalone manifest-file deletion.
func TestR1ManifestWholeGC5095(t *testing.T) {
	layout, n := r1ManifestCaptureDimensions(t)
	opts := Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	r1SelectOwnedManifestCapture(t, &opts, layout)
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var setupA, setupB runtime.MemStats
	runtime.ReadMemStats(&setupA)
	setupStart := time.Now()
	r1PopulateManifestCaptureRevisions(t, d, layout, n)
	setupElapsed := time.Since(setupStart)
	runtime.ReadMemStats(&setupB)
	before := d.idx.Load().allocator.COWPrepareProfileV1()
	phaseDurations := make([]time.Duration, 0, 4096)
	var phaseStart time.Time
	unregister := registerLeafGenerationGCExclusivePhaseHook(func(entering bool) {
		if entering {
			phaseStart = time.Now()
		} else {
			phaseDurations = append(phaseDurations, time.Since(phaseStart))
		}
	})
	defer unregister()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	start := time.Now()
	stats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	elapsed := time.Since(start)
	runtime.ReadMemStats(&b)
	if err != nil {
		t.Fatal(err)
	}
	if layout == "legacy" && stats.ManifestRevisionsDeleted != n {
		t.Fatalf("legacy revision oracle: %+v", stats)
	}
	if layout == "owned" && (stats.ManifestRevisionsDeleted != 0 || stats.ManifestRevisionBytesDeleted != 0) {
		t.Fatal("internal reuse mislabeled as unlinked files")
	}
	after := d.idx.Load().allocator.COWPrepareProfileV1()
	t.Logf("layout=%s revisions=%d elapsed_ns=%d allocated_bytes=%d allocations=%d setup_elapsed_ns=%d setup_allocated_bytes=%d fence_samples=%d fence_p95_ns=%d fence_p99_ns=%d allocator_before=%+v allocator_after=%+v stats=%+v", layout, n, elapsed.Nanoseconds(), b.TotalAlloc-a.TotalAlloc, b.Mallocs-a.Mallocs, setupElapsed.Nanoseconds(), setupB.TotalAlloc-setupA.TotalAlloc, len(phaseDurations), leafGenerationGCBenchmarkPercentile(phaseDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(phaseDurations, 99).Nanoseconds(), before, after, stats)
}
