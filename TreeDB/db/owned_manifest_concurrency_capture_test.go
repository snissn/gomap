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
	"testing"
	"time"
)

// The same diagnostic source can compile against frozen797 with an independently
// frozen test-only overlay. Public calls and assertions are identical; only the
// explicitly selected admission/revision constructor differs.
func TestR1ManifestConcurrentACK5095(t *testing.T) {
	layout := os.Getenv("GOMAP_L_CONCURRENT_LAYOUT")
	if layout == "" {
		t.Skip("opt-in source-bound public concurrency capture")
	}
	n, err := strconv.Atoi(os.Getenv("GOMAP_L_GC_REVISIONS"))
	if err != nil || (n != 128 && n != 512) {
		t.Fatal("requires frozen 128/512 dimensions")
	}
	if layout != "owned" && layout != "legacy" {
		t.Fatal("unknown explicit layout")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	if layout == "owned" {
		field := reflect.ValueOf(&opts).Elem().FieldByName("OwnedLeafManifests")
		if !field.IsValid() {
			t.Fatal("owned layout unavailable in this binary")
		}
		field.SetBool(true)
	}
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
	for i := 0; i < n; i++ {
		if layout == "owned" {
			checkpoint, ok := any(d).(interface{ CheckpointOwnedLeafManifest() (uint64, error) })
			if !ok {
				t.Fatal("owned checkpoint unavailable")
			}
			_, err = checkpoint.CheckpointOwnedLeafManifest()
		} else {
			closure, e := d.PrepareLeafGenerationManifestStableClosure()
			err = e
			if e == nil {
				closure.Release()
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	ackDurations := make([]time.Duration, 0, 32)
	phaseDurations := make([]time.Duration, 0, 4096)
	var phaseStart time.Time
	begin := make(chan struct{})
	var once sync.Once
	unregister := registerLeafGenerationGCExclusivePhaseHook(func(entering bool) {
		if entering {
			phaseStart = time.Now()
			once.Do(func() { close(begin) })
		} else {
			phaseDurations = append(phaseDurations, time.Since(phaseStart))
		}
	})
	defer unregister()
	writerDone := make(chan error, 1)
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	start := time.Now()
	go func() {
		<-begin
		for i := 0; i < 32; i++ {
			now := time.Now()
			batch := d.NewPhysicalBatch().(*Batch)
			e := batch.Set([]byte(fmt.Sprintf("concurrent/%02d", i)), []byte("acknowledged complete row"))
			if e == nil {
				e = batch.WriteSync()
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
	gcElapsed := time.Since(start)
	once.Do(func() { close(begin) })
	if err = <-writerDone; err != nil {
		t.Fatal(err)
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
	t.Logf("layout=%s revisions=%d public_acks=%d whole_elapsed_ns=%d first_gc_elapsed_ns=%d gc_calls=%d allocated_bytes=%d ack_p95_ns=%d ack_p99_ns=%d fence_samples=%d fence_p95_ns=%d fence_p99_ns=%d stats=%+v", layout, n, len(ackDurations), elapsed.Nanoseconds(), gcElapsed.Nanoseconds(), gcCalls, b.TotalAlloc-a.TotalAlloc, leafGenerationGCBenchmarkPercentile(ackDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(ackDurations, 99).Nanoseconds(), len(phaseDurations), leafGenerationGCBenchmarkPercentile(phaseDurations, 95).Nanoseconds(), leafGenerationGCBenchmarkPercentile(phaseDurations, 99).Nanoseconds(), stats)
	// Legacy hook attribution excludes its revision writeMu hold. Its reported
	// segment samples are never called complete legacy revision-fence evidence.
}
