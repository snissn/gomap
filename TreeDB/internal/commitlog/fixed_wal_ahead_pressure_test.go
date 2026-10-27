package commitlog

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Explicit opt-in throughput/rotation diagnostic, not a timing assertion in CI.
// Use TMPDIR on the target persistent filesystem. This file deliberately has
// no policy-field dependencies and can be overlaid unchanged on the base.
// GOMAP_WAL_AHEAD_PRESSURE=steady go test ./TreeDB/internal/commitlog -run '^TestFixedWALAheadPressureDiagnostic$' -count=1 -v -timeout=5m
// burst adds one 2s idle and periodic larger groups; PACE_US paces each group.
// CAP=1 stops after six pressure rotations (two at the 64 MiB capacity cap),
// rather than three rotations (4->8->16->32 MiB). Opening/recovery are untimed.
// Initializer timing covers the first header write through full zero-fill+Sync,
// excluding private name creation and subsequent canonical/namespace work.
func TestFixedWALAheadPressureDiagnostic(t *testing.T) {
	shape := os.Getenv("GOMAP_WAL_AHEAD_PRESSURE")
	if shape == "" {
		t.Skip("set GOMAP_WAL_AHEAD_PRESSURE=steady or burst")
	}
	if shape != "steady" && shape != "burst" {
		t.Fatal("invalid pressure shape")
	}
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	pace, err := strconv.Atoi(os.Getenv("GOMAP_WAL_AHEAD_PACE_US"))
	if err != nil && os.Getenv("GOMAP_WAL_AHEAD_PACE_US") != "" {
		t.Fatal(err)
	}
	rotationsNeeded := 3
	if os.Getenv("GOMAP_WAL_AHEAD_CAP") == "1" {
		rotationsNeeded = 6
	}
	var foreground, ahead atomic.Int64
	var initMu sync.Mutex
	starts := make(map[string]time.Time)
	var initMax time.Duration
	var initTotal time.Duration
	oldSync := fixedWALInitializeSync
	fixedWALInitializeSync = func(f *os.File) error {
		err := oldSync(f)
		initMu.Lock()
		if began, ok := starts[f.Name()]; ok {
			elapsed := time.Since(began)
			initTotal += elapsed
			initMax = max(initMax, elapsed)
			delete(starts, f.Name())
		}
		initMu.Unlock()
		return err
	}
	oldWrite := fixedWALInitializeWrite
	fixedWALInitializeWrite = func(w io.WriterAt, p []byte, off int64) error {
		if off == 0 {
			if f, ok := w.(*os.File); ok {
				initMu.Lock()
				starts[f.Name()] = time.Now()
				initMu.Unlock()
				if strings.HasPrefix(filepath.Base(f.Name()), fixedWALAheadPrefix) {
					ahead.Add(1)
				} else {
					foreground.Add(1)
				}
			}
		}
		return oldWrite(w, p, off)
	}
	defer func() { fixedWALInitializeWrite = oldWrite; fixedWALInitializeSync = oldSync }()
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.writer.fixed == nil {
		t.Fatal("persistent fixed-container filesystem required")
	}
	foreground.Store(0) // Separate the mandatory opening container from successors.
	initMu.Lock()
	initTotal, initMax = 0, 0
	initMu.Unlock()
	key, value := []byte("pressure-key"), bytes.Repeat([]byte("v"), 1024)
	var latencies []time.Duration
	var lastLSN uint64
	seq := j.segmentSeq
	rotations := 0
	idled := false
	start := time.Now()
	for groups := 0; rotations < rotationsNeeded; groups++ {
		if groups >= 100000 {
			t.Fatal("pressure diagnostic did not rotate")
		}
		if shape == "burst" && rotations >= 1 && !idled {
			time.Sleep(2 * time.Second)
			idled = true
		}
		count := 16
		if shape == "burst" && groups%16 == 0 {
			count = 128
		}
		began := time.Now()
		for n := 0; n < count; n++ {
			lastLSN, err = j.AppendRawKVPointCommandTrusted(0, RawKVOpSet, key, value)
			if err != nil {
				t.Fatal(err)
			}
		}
		pending, e := j.TakePendingStableRotations()
		if e != nil {
			t.Fatal(e)
		}
		var tokens []*rootpublication.StableResourceToken
		for _, r := range pending {
			tokens = append(tokens, r.Closed, r.Active)
		}
		captured, e := j.CaptureSync(tokens)
		if e == nil {
			e = captured.Sync()
			captured.Release()
		}
		for _, r := range pending {
			r.Release()
		}
		if e != nil {
			t.Fatal(e)
		}
		latencies = append(latencies, time.Since(began))
		j.mu.Lock()
		newSeq, capacity, physical := j.segmentSeq, j.writer.fixed.capacity, j.writer.fixed.physical
		j.mu.Unlock()
		if newSeq != seq {
			rotations += int(newSeq - seq)
			seq = newSeq
			initMu.Lock()
			total, maximum := initTotal, initMax
			initMu.Unlock()
			t.Logf("rotation=%d capacity=%d physical=%d foreground_initializers=%d ahead_starts=%d initialize_write_sync_total=%s initialize_write_sync_max=%s", rotations, capacity, physical, foreground.Load(), ahead.Load(), total, maximum)
		}
		if pace > 0 {
			time.Sleep(time.Duration(pace) * time.Microsecond)
		}
	}
	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("shape=%s groups=%d accepted=%d rotations=%d foreground_initializers=%d inferred_adoptions=%d ahead_starts=%d elapsed=%s commands_per_sec=%.1f group_p99=%s group_max=%s", shape, len(latencies), lastLSN, rotations, foreground.Load(), int64(rotations)-foreground.Load(), ahead.Load(), elapsed, float64(lastLSN)/elapsed.Seconds(), latencies[(len(latencies)-1)*99/100], latencies[len(latencies)-1])
	if rotations < int(foreground.Load()) {
		t.Fatal("initializer count exceeds pressure transitions")
	}
	// End-to-end prefix proof: close joins the worker, then normal recovery scans
	// the accepted commands. The timing above ends before this verification.
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, e := OpenCommandJournal(dir, CommandJournalOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if reopened.NextLSN() != lastLSN+1 {
		t.Fatalf("reopen next=%d expected=%d", reopened.NextLSN(), lastLSN+1)
	}
}
