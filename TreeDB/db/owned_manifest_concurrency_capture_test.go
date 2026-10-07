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
	writerReady := make(chan string, 1)
	var writerID string
	var stackSamples int
	var stackBuffer []byte
	var stackTruncated bool
	var gcActive, fenceActive, rendezvousFailed atomic.Bool
	var startsDuringGC, completionsDuringGC, startsDuringFence atomic.Int64
	var once sync.Once
	unregister := registerLeafGenerationGCExclusivePhaseHook(func(entering bool) {
		if entering {
			phaseStart = time.Now()
			fenceActive.Store(true)
			// Observe the identified goroutine inside public WriteSync, waiting
			// for this writeMu fence. A signal before the call cannot prove overlap.
			once.Do(func() {
				close(begin)
				timer := time.NewTimer(30 * time.Second)
				defer timer.Stop()
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					n := runtime.Stack(stackBuffer, true)
					stackSamples++
					if n == len(stackBuffer) {
						stackTruncated = true
						rendezvousFailed.Store(true)
						return
					}
					if r1PublicWriteWaitingAtFence(stackBuffer[:n], writerID) {
						if gcActive.Load() {
							startsDuringGC.Add(1)
						}
						if fenceActive.Load() {
							startsDuringFence.Add(1)
						}
						return
					}
					select {
					case <-tick.C:
					case <-timer.C:
						rendezvousFailed.Store(true)
						return
					}
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
	// Fixed diagnostic storage and every stack sample are within the measured
	// time/allocation window in both layouts; truncation rejects the packet.
	stackBuffer = make([]byte, 128<<10)
	go func() {
		var own [64]byte
		n := runtime.Stack(own[:], false)
		fields := bytes.Fields(own[:n])
		if len(fields) < 2 || string(fields[0]) != "goroutine" {
			writerReady <- ""
		} else {
			writerReady <- string(fields[1])
		}
		<-begin
		for i := 0; i < 32; i++ {
			now := time.Now()
			batch := d.NewPhysicalBatch().(*Batch)
			e := batch.Set([]byte(fmt.Sprintf("concurrent/%02d", i)), []byte("acknowledged complete row"))
			if e == nil {
				e = batch.WriteSync()
				if gcActive.Load() {
					completionsDuringGC.Add(1)
				}
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
	writerID = <-writerReady
	stats, gcErr := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	firstStats, firstGCError := stats, gcErr
	gcActive.Store(false)
	gcElapsed := time.Since(start)
	once.Do(func() { close(begin) })
	if err = <-writerDone; err != nil {
		t.Fatal(err)
	}
	if rendezvousFailed.Load() || startsDuringGC.Load() == 0 || startsDuringFence.Load() == 0 {
		t.Fatalf("no observed public lock-wait overlap: rendezvous_failed=%t gc_calls_observed=%d fence_calls_observed=%d completions=%d", rendezvousFailed.Load(), startsDuringGC.Load(), startsDuringFence.Load(), completionsDuringGC.Load())
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
	t.Logf("layout=%s revisions=%d public_acks=%d ack_calls_observed_during_gc=%d ack_completions_during_gc=%d ack_calls_observed_during_fence=%d stack_buffer_bytes=%d stack_samples=%d stack_truncated=%t whole_elapsed_ns=%d first_gc_elapsed_ns=%d gc_calls=%d allocated_bytes=%d ack_p95_ns=%d ack_p99_ns=%d fence_samples=%d fence_p95_ns=%d fence_p99_ns=%d first_gc_error=%v first_gc_stats=%+v final_gc_stats=%+v", layout, n, len(ackDurations), startsDuringGC.Load(), completionsDuringGC.Load(), startsDuringFence.Load(), len(stackBuffer), stackSamples, stackTruncated, elapsed.Nanoseconds(), gcElapsed.Nanoseconds(), gcCalls, b.TotalAlloc-a.TotalAlloc, leafGenerationGCBenchmarkPercentile(ackDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(ackDurations, 99).Nanoseconds(), len(phaseDurations), leafGenerationGCBenchmarkPercentile(phaseDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(phaseDurations, 99).Nanoseconds(), firstGCError, firstStats, stats)
	// Legacy hook attribution excludes its revision writeMu hold. Its reported
	// segment samples are never called complete legacy revision-fence evidence.
}

// r1PublicWriteWaitingAtFence rejects pre-call scheduling and unrelated writers.
// Stack capture is test-only; production runtime and lock paths are unchanged.
func r1PublicWriteWaitingAtFence(stacks []byte, writerID string) bool {
	if writerID == "" {
		return false
	}
	header := []byte("goroutine " + writerID + " ")
	start := bytes.Index(stacks, header)
	if start < 0 || (start > 0 && stacks[start-1] != '\n') {
		return false
	}
	writer := stacks[start:]
	if end := bytes.Index(writer, []byte("\n\ngoroutine ")); end >= 0 {
		writer = writer[:end]
	}
	return bytes.Contains(writer, []byte("sync.(*RWMutex).RLock(")) &&
		bytes.Contains(writer, []byte(".(*Batch).writeOptimistic(")) &&
		bytes.Contains(writer, []byte(".(*Batch).WriteSync("))
}

func TestR1PublicWriteOverlapRejectsPrecallAndOtherGoroutines(t *testing.T) {
	waiting := "goroutine 7 [sync.RWMutex.RLock]:\nsync.(*RWMutex).RLock(...)\nexample/db.(*Batch).writeOptimistic(...)\nexample/db.(*Batch).WriteSync(...)\n"
	if !r1PublicWriteWaitingAtFence([]byte(waiting), "7") {
		t.Fatal("identified public write lock wait was not observed")
	}
	for _, stack := range []string{
		"goroutine 7 [runnable]:\nexample/db.TestR1ManifestConcurrentACK5095.func2(...)\n",
		"goroutine 7 [chan receive]:\nexample/db.wait(...)\n\n" + string(bytes.ReplaceAll([]byte(waiting), []byte("goroutine 7 "), []byte("goroutine 8 "))),
		"goroutine 7 [runnable]:\nexample/db.(*Batch).WriteSync(...)\n",
	} {
		if r1PublicWriteWaitingAtFence([]byte(stack), "7") {
			t.Fatal("pre-call or unrelated goroutine accepted as public lock-wait overlap")
		}
	}
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
