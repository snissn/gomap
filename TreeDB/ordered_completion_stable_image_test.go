package treedb_test

// Stable-image recovery coverage for ordered durable acknowledgements.
// Reuses cowOrderedDurabilityOptions/Open, cowOrderedCapturePhysical,
// cowOrderedRequireVersions, and cowEventBelongsToDB from existing tests.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// Run the potentially deadlocking old-head reproduction in a bounded child.
// The child never invokes Close on a failure with undrained accepted handles.
func TestOrderedCompletionStableImageAtAcknowledgement(t *testing.T) {
	if scenario := os.Getenv("GOMAP_ORDERED_ACK_IMAGE_CHILD"); scenario != "" {
		proposalOrderedACKImageChild(t, scenario)
		return
	}
	for _, scenario := range []string{"fifo-1", "fifo-70", "later-driver"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOrderedCompletionStableImageAtAcknowledgement$", "-test.timeout=15s", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_ORDERED_ACK_IMAGE_CHILD="+scenario)
			started := time.Now()
			out, err := cmd.CombinedOutput()
			// Context state is an independent observation, not a causal verdict.
			t.Logf("ACK child scenario=%s process=%v context=%v elapsed=%s state=%v\n%s", scenario, err, ctx.Err(), time.Since(started), cmd.ProcessState, out)
			if err != nil {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}

func proposalOrderedACKImageChild(t *testing.T, scenario string) {
	var witness *orderedACKPhaseWitness
	if deadline, ok := t.Deadline(); ok {
		witness = startOrderedACKPhaseWitness(scenario, deadline.Add(-3*time.Second), os.Stderr)
		defer witness.stopAndJoin()
	}
	opts := cowOrderedDurabilityOptions(treedb.ProfileCommandWALDurable, t.TempDir())
	// Observe snapshots every file at each staging hook. Keep unused index
	// capacity small while retaining every observation and the progress deadlines.
	opts.ChunkSize = 64 << 10
	// Bound the fixed WAL container copied by every full observation. The
	// default starts near 4 MiB even when the index chunk is much smaller.
	opts.CommandWALSegmentTargetBytes = 256 << 10
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	database := cowOrderedDurabilityOpen(t, opts)
	drained := false
	defer func() {
		if drained {
			// Only after the immutable ACK image was frozen.
			witness.mark("original-close")
			if err := closeOrderedACKImage("original close", database.Close); err != nil {
				t.Error(err)
			}
		}
	}()
	store := mvcc.New(database)
	if err := store.CommitAt(1, []mvcc.Mutation{{Key: []byte("anchor"), Value: []byte("stable")}}, mvcc.CommitDurable); err != nil {
		t.Fatal(err)
	}
	witness.mark("baseline-checkpoint")
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	baselineStats := database.Stats()
	baselineWALPath := baselineStats["treedb.command_wal.writer.path"]
	if baselineWALPath == "" {
		t.Fatal("baseline command-WAL writer path unavailable")
	}
	walInfo, err := os.Stat(baselineWALPath)
	if err != nil {
		t.Fatal(err)
	}
	// Fixed-container header, witnesses and alignment fit within this 8 KiB
	// allowance. Stream fallback may have an empty new active segment.
	if !walInfo.Mode().IsRegular() || walInfo.Size() > opts.CommandWALSegmentTargetBytes+(8<<10) {
		t.Fatalf("observer WAL fixture escaped budget: path=%q mode=%s bytes=%d target=%d", baselineWALPath, walInfo.Mode(), walInfo.Size(), opts.CommandWALSegmentTargetBytes)
	}
	t.Logf("observer WAL fixture: path=%q container=%s bytes=%d target=%d", baselineWALPath, baselineStats["treedb.command_wal.writer.container"], walInfo.Size(), opts.CommandWALSegmentTargetBytes)
	baselineLSN, err := strconv.ParseUint(baselineStats["treedb.applied_command_lsn"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	witness.mark("baseline-capture")
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var observationMu sync.Mutex
	active := true
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	restoreObserver := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !cowEventBelongsToDB(opts.Dir, e) {
			return nil
		}
		observationMu.Lock()
		var err error
		if active {
			err = model.Observe(opts.Dir, e)
		}
		observationMu.Unlock()
		if err != nil {
			return err
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			// Never hold observationMu while the test stages the successor cut.
			enterOnce.Do(func() { close(entered); <-release })
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreObserver) }
	defer restore()
	defer unblock()

	versions := []cowOrderedVersion{{key: []byte("anchor"), ts: 1, state: mvcc.Present, value: []byte("stable")}}
	submit := func(c *mvcc.CommitCompletion, ts uint64, key string, value []byte) {
		t.Helper()
		if err := store.SubmitCommitGroupAt([]mvcc.CommitGroup{{Timestamp: ts, Mutations: []mvcc.Mutation{{Key: []byte(key), Value: value}}}}, mvcc.CommitDurable, c); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, cowOrderedVersion{key: []byte(key), ts: ts, state: mvcc.Present, value: bytes.Clone(value)})
	}
	await := func(done <-chan error, label string) {
		t.Helper()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s did not progress", label)
		}
	}
	waitForSync := func() {
		t.Helper()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("driver did not reach physical command sync")
		}
	}
	start := func(c *mvcc.CommitCompletion) <-chan error {
		done := make(chan error, 1)
		go func() { done <- c.Complete() }()
		return done
	}

	witness.mark("ACK-staging-and-completion")
	var a mvcc.CommitCompletion
	large := bytes.Repeat([]byte("persistent-history/"), 512)
	switch scenario {
	case "fifo-1", "fifo-70":
		var p mvcc.CommitCompletion
		submit(&p, 2, "predecessor", []byte("p"))
		pdone := start(&p)
		waitForSync()
		// A joins the pending ledger while P owns the physical target.
		submit(&a, 3, "a", large)
		unblock()
		await(pdone, "P")
		n := 1
		if scenario == "fifo-70" {
			n = 70 // Beyond the default 64-participant collection trigger.
		}
		bs := make([]mvcc.CommitCompletion, n)
		for i := range bs {
			submit(&bs[i], uint64(i+4), fmt.Sprintf("b/%d", i), []byte("successor"))
		}
		// No B completion exists until A returns. This is the original FIFO
		// dependency-cycle reproducer, rather than a scheduler-timed overlap.
		await(start(&a), "FIFO A")
		for i := range bs {
			await(start(&bs[i]), fmt.Sprintf("B/%d", i))
		}
	case "later-driver":
		var b mvcc.CommitCompletion
		submit(&a, 2, "a", large)
		submit(&b, 3, "b", []byte("successor"))
		bdone := start(&b)
		waitForSync() // Proves B, the only completion owner running, drives.
		select {
		case err := <-bdone:
			t.Fatalf("B ACK before held physical sync: %v", err)
		default:
		}
		unblock()
		await(start(&a), "earlier publication A")
		await(bdone, "later driver B")
	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
	drained = true

	// Freeze immediately after the final ACK, before physical inspection,
	// Checkpoint, Close, GC, or any other potentially persistent operation.
	witness.mark("ACK-freeze")
	observationMu.Lock()
	active = false
	stable := model.Clone()
	observationMu.Unlock()
	restore()
	// Segment sequences only advance. A smaller target must not introduce a
	// rotation while staging or completing this same FIFO reproduction.
	if got := database.Stats()["treedb.command_wal.writer.path"]; got != baselineWALPath {
		t.Fatalf("command WAL rotated during ACK fixture: path=%q baseline=%q", got, baselineWALPath)
	}
	witness.mark("physical-capture")
	physical := cowOrderedCapturePhysical(t, database, versions)
	witness.mark("writable-open")
	result, reopened, closeReopened, err := powerlossreopen.Stable(stable, opts, false)
	if err != nil || result.Rejected || reopened == nil {
		t.Fatalf("ACK stable image rejected: %+v %v", result, err)
	}
	defer func() {
		witness.mark("reopened-close")
		if err := closeOrderedACKImage("reopened close/cleanup", closeReopened); err != nil {
			t.Error(err)
		}
	}()
	witness.mark("validation")
	if result.AppliedLSN <= baselineLSN {
		t.Fatalf("recovery did not advance beyond baseline: applied=%d baseline=%d", result.AppliedLSN, baselineLSN)
	}
	cowOrderedRequireVersions(t, mvcc.New(reopened), versions)
	for _, want := range physical {
		got, revision, err := reopened.GetVersioned(want.key)
		// Recovery may rematerialize pointers; file placement is not identity.
		if err != nil || revision != want.revision || !bytes.Equal(got, want.value) {
			t.Fatalf("recovered history %x: revision=%d want=%d equal=%t err=%v", want.key, revision, want.revision, bytes.Equal(got, want.value), err)
		}
	}
}
