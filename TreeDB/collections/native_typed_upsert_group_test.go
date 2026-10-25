package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func nativeTypedGroupOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	return opts
}

func nativeTypedGroupBatch(id string) ([][]byte, [][]byte, []TypedColumnBatch) {
	return [][]byte{[]byte(id)}, [][]byte{[]byte(`{"id":"` + id + `","payload":"original"}`)}, []TypedColumnBatch{
		{Name: "label", Strings: []string{"label-" + id}},
		{Name: "embedding", Float32Vectors: [][]float32{{1, 2, 3}}},
	}
}

// This fixture uses the ordinary factory and the exact eligible public schema.
// The returned original allocator is observed after all public owners close.
func openNativeTypedGroup(t *testing.T, dir string, seed bool) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	database, err := treedb.Open(nativeTypedGroupOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	if seed {
		if _, err := manager.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true, Columns: []ColumnStoreColumn{
				{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset},
				{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3, Owner: TypedStorageOwnerColumnPart},
			}, RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if seed {
		if _, _, err := col.InsertTypedBatchWithStats(nativeTypedGroupBatch("seed")); err != nil {
			t.Fatal(err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	return database, handle, manager, col
}

// Formation is controlled by the original package hook and original queue
// condition. A deadline only breaks a failed assertion; it never schedules a
// participant or polls for a cohort.
func formNativeTypedGroup(t *testing.T, handle *backenddb.DB, col *Collection, ids ...string) (<-chan typedUpsertGroupResult, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	hook := func() { enteredOnce.Do(func() { close(entered); <-release }) }
	if !typedUpsertGroupBeforeSealTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("group formation hook already installed")
	}
	t.Cleanup(func() { unblock(); typedUpsertGroupBeforeSealTestHook.CompareAndSwap(&hook, nil) })
	coord := collectionCommandWALCoordinatorForDB(handle)
	done := make(chan typedUpsertGroupResult, len(ids))
	var finished atomic.Int32
	write := func(c *Collection, id string) {
		input, residual, columns := nativeTypedGroupBatch(id)
		updated, handled, stats, err := c.TryUpsertTypedBatchGroup(input, residual, columns)
		clear(input[0])
		clear(residual[0])
		clear(columns[0].Strings)
		clear(columns[1].Float32Vectors[0])
		done <- typedUpsertGroupResult{updated: updated, handled: handled, stats: stats, err: err}
		finished.Add(1)
		coord.mu.Lock()
		coord.condLocked().Broadcast()
		coord.mu.Unlock()
	}
	go write(col, ids[0])
	select {
	case <-entered:
	case result := <-done:
		t.Fatalf("native participant did not queue: %+v", result)
	case <-time.After(15 * time.Second):
		t.Fatal("native leader did not enter original queue")
	}
	for i, id := range ids[1:] {
		other, err := col.manager.OpenCollection("events")
		if err != nil || other == col {
			t.Fatalf("independent handle: %v", err)
		}
		go write(other, id)
		var expired atomic.Bool
		timer := time.AfterFunc(15*time.Second, func() { expired.Store(true); coord.mu.Lock(); coord.condLocked().Broadcast(); coord.mu.Unlock() })
		coord.mu.Lock()
		for coord.typedUpsertGroup != nil && len(coord.typedUpsertGroup.requests) != i+2 && finished.Load() == 0 && !expired.Load() {
			coord.condLocked().Wait()
		}
		joined := coord.typedUpsertGroup != nil && len(coord.typedUpsertGroup.requests) == i+2
		coord.mu.Unlock()
		timer.Stop()
		if !joined {
			t.Fatalf("original queue did not retain participant %d (finished=%d deadline=%v)", i+2, finished.Load(), expired.Load())
		}
	}
	return done, unblock
}

func checkNativeTypedGroupDocument(t *testing.T, col *Collection, id string, found bool) {
	t.Helper()
	got, err := col.Get([]byte(id))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		if got != nil {
			t.Fatalf("unaccepted row %s=%s", id, got)
		}
		return
	}
	var doc struct {
		ID, Payload, Label string
		Embedding          []float32
	}
	if err := json.Unmarshal(got, &doc); err != nil || doc.ID != id || doc.Payload != "original" || doc.Label != "label-"+id || len(doc.Embedding) != 3 || doc.Embedding[0] != 1 || doc.Embedding[1] != 2 || doc.Embedding[2] != 3 {
		t.Fatalf("complete owned row %s=%s: %v", id, got, err)
	}
}

func finishNativeTypedGroup(t *testing.T, database *treedb.DB, handle *backenddb.DB, manager *CollectionManager, col *Collection) {
	t.Helper()
	read, err := col.nativeJSON.Capture("events")
	if err != nil {
		t.Fatal(err)
	}
	type memoryStats interface{ Stats() memtable.COWStats }
	budget := read.capture.NativeAllocator().(memoryStats)
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 || s.Sources != 0 || s.Views != 0 || s.Generations != 0 {
		t.Fatalf("original allocator after Close=%+v", s)
	}
	pins := handle.ValueLogIdentityPinRegistry()
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
		t.Fatalf("original stable custody after Close=%+v: %v", s, pins.CleanupError())
	}
}

func TestNativeTypedUpsertGroupOneCutAndLateDecline(t *testing.T) {
	dir := t.TempDir()
	database, handle, manager, col := openNativeTypedGroup(t, dir, true)
	t.Cleanup(func() {
		if database != nil {
			_ = manager.Close()
			_ = database.Close()
		}
	})
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if held != nil {
			_ = held.Close()
		}
	}()
	before, ok := handle.StateToken()
	if !ok {
		t.Fatal("missing physical basis")
	}
	beforeNext, beforeStats := handle.CommandWALNextLSN(), database.Stats()
	installed, ack := make(chan struct{}), make(chan struct{})
	var ackOnce sync.Once
	unblockAck := func() { ackOnce.Do(func() { close(ack) }) }
	hook := func() { close(installed); <-ack }
	if !typedUpsertGroupAfterInstallTestHook.CompareAndSwap(nil, &hook) {
		t.Fatal("install hook already set")
	}
	defer func() { unblockAck(); typedUpsertGroupAfterInstallTestHook.CompareAndSwap(&hook, nil) }()
	done, release := formNativeTypedGroup(t, handle, col, "a", "b")
	release()
	select {
	case <-installed:
	case result := <-done:
		t.Fatalf("group completed without one install: %+v", result)
	case <-time.After(20 * time.Second):
		t.Fatal("group did not install")
	}
	for _, id := range []string{"seed", "a", "b"} {
		checkNativeTypedGroupDocument(t, col, id, true)
	}
	old, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("a"), []byte("b")}, DocumentFetchOptions{})
	if err != nil || len(old.Results) != 3 || !old.Results[0].Found || old.Results[1].Found || old.Results[2].Found {
		t.Fatalf("held predecessor changed: %+v %v", old.Results, err)
	}
	if now, ok := handle.StateToken(); !ok || now != before {
		t.Fatalf("logical group moved physical basis: %+v", now)
	}
	if handle.CommandWALNextLSN() != beforeNext+3 {
		t.Fatalf("actual two frames + barrier next=%d", handle.CommandWALNextLSN())
	}
	if got := typedGroupStatUint64(t, database.Stats(), "treedb.cache.cow.txn.logical_seq"); got != typedGroupStatUint64(t, beforeStats, "treedb.cache.cow.txn.logical_seq")+1 {
		t.Fatalf("group logical sequence=%d", got)
	}
	if got := typedGroupStatUint64(t, database.Stats(), "treedb.command_wal.file_sync.calls_total"); got != typedGroupStatUint64(t, beforeStats, "treedb.command_wal.file_sync.calls_total")+1 {
		t.Fatalf("group sync count=%d", got)
	}
	// The ordinary factory places the backend beneath the public root. Read
	// that actual backend directory, as opposed to a legacy flat DB directory.
	frames := collectionCommandWALFrames(t, handle.Dir())
	first := 0
	for first < len(frames) && frames[first].LSN < beforeNext {
		first++
	}
	frames = frames[first:]
	if len(frames) != 3 {
		t.Fatalf("actual two participants + barrier frames=%d want=3", len(frames))
	}
	read, err := col.nativeJSON.Capture("events")
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	actual := make(map[uint64]bool)
	for i, frame := range frames {
		if frame.LSN != beforeNext+uint64(i) {
			t.Fatalf("actual frame range=%+v", frames)
		}
		if i == 2 {
			if frame.Kind != commitlog.CommandKindDurablePrefixBarrier {
				t.Fatal("missing actual barrier")
			}
			continue
		}
		if frame.Kind != commitlog.CommandKindCompositeMutation {
			t.Fatalf("participant kind=%d", frame.Kind)
		}
		members := 0
		if err := commitlog.ScanCompositeMutationPayload(frame.Payload, func(m commitlog.CompositeMutationMember) error {
			members++
			if m.Incarnation == 0 || m.Kind != commitlog.CommandKindCollectionReplaceSourceByID || m.PayloadFormat != commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
				t.Fatalf("participant grammar=%+v", m)
			}
			return nil
		}); err != nil || members != 1 {
			t.Fatalf("not a singleton frame: %d %v", members, err)
		}
		actual[frame.LSN] = true
	}
	for d := read.domain.domain.typed; d != nil; d = d.previous {
		if d.appliedLSN >= beforeNext {
			if !actual[d.appliedLSN] || d.memberOrdinal != 0 {
				t.Fatalf("staged identity bound to nonparticipant/barrier: %d/%d", d.appliedLSN, d.memberOrdinal)
			}
			delete(actual, d.appliedLSN)
		}
	}
	if len(actual) != 0 {
		t.Fatal("participant did not bind its actual identity")
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		t.Fatalf("ack escaped installed hook: %+v", result)
	default:
	}
	unblockAck()
	for range 2 {
		result := <-done
		if !result.handled || result.updated != 0 || result.err != nil || result.stats.Documents != 1 {
			t.Fatalf("group caller=%+v", result)
		}
	}
	typedUpsertGroupAfterInstallTestHook.CompareAndSwap(&hook, nil)
	// Restore only the formation hook before starting a second deterministic
	// cohort; this is still the same original coordinator and public entry.
	typedUpsertGroupBeforeSealTestHook.Store(nil)
	done, release = formNativeTypedGroup(t, handle, col, "late")
	if _, _, err := col.InsertTypedBatchWithStats(nativeTypedGroupBatch("late")); err != nil {
		t.Fatal(err)
	}
	next := handle.CommandWALNextLSN()
	release()
	if result := <-done; result.handled || result.err != nil || handle.CommandWALNextLSN() != next {
		t.Fatalf("late predecessor decline assigned identity: %+v next=%d", result, handle.CommandWALNextLSN())
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	finishNativeTypedGroup(t, database, handle, manager, col)
	database = nil
	database, handle, manager, col = openNativeTypedGroup(t, dir, false)
	for _, id := range []string{"seed", "a", "b", "late"} {
		checkNativeTypedGroupDocument(t, col, id, true)
	}
	finishNativeTypedGroup(t, database, handle, manager, col)
	database = nil
}

// Userspace-flushed prefixes survive this process boundary. This is ordinary
// crash replay, not a power-loss durability claim for an unsuccessful sync.
func TestNativeTypedUpsertGroupPartialPrefixRecovery(t *testing.T) {
	for _, scenario := range []struct {
		name                 string
		point                durabilitycut.Point
		occurrence, assigned int
	}{
		{"zero-assigned", durabilitycut.BeforeDependencyAppend, 1, 0},
		{"first-assigned", durabilitycut.AfterUserspaceFlush, 1, 1},
		{"middle-prefix", durabilitycut.BeforeDependencyAppend, 3, 2},
		{"barrier-assigned", durabilitycut.AfterDependencyFileSync, 1, 4},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			const env = "GOMAP_NATIVE_TYPED_GROUP_PREFIX_DIR"
			const marker = "ORIGINAL GROUP PREFIX RETAINED"
			if dir := os.Getenv(env); dir == "" {
				dir = t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
				cmd.Env = append(os.Environ(), env+"="+dir)
				output, err := cmd.CombinedOutput()
				t.Logf("actual prefix child:\n%s", output)
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("prefix child: %v\n%s", err, output)
				}
				database, handle, manager, col := openNativeTypedGroup(t, dir, false)
				defer func() {
					if database != nil {
						_ = manager.Close()
						_ = database.Close()
					}
				}()
				for i, id := range []string{"a", "b", "c"} {
					checkNativeTypedGroupDocument(t, col, id, i < scenario.assigned)
				}
				checkNativeTypedGroupDocument(t, col, "seed", true)
				var seedNext uint64
				for _, line := range bytes.Split(output, []byte("\n")) {
					if _, err := fmt.Sscanf(string(line), "ORIGINAL GROUP SEED NEXT %d", &seedNext); err == nil {
						break
					}
				}
				if seedNext == 0 {
					t.Fatal("child did not preserve seed identity")
				}
				if got := handle.CommandWALNextLSN(); got != seedNext+uint64(scenario.assigned) {
					t.Fatalf("replay reassigned prefix: next=%d", got)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				finishNativeTypedGroup(t, database, handle, manager, col)
				database = nil
				return
			}
			dir := os.Getenv(env)
			database, handle, manager, col := openNativeTypedGroup(t, dir, true)
			held, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			read, err := col.nativeJSON.Capture("events")
			if err != nil {
				t.Fatal(err)
			}
			type memoryStats interface{ Stats() memtable.COWStats }
			budget := read.capture.NativeAllocator().(memoryStats)
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			before, ok := handle.StateToken()
			if !ok {
				t.Fatal("missing basis")
			}
			beforeNext := handle.CommandWALNextLSN()
			if before.AppliedCommandLSN == 0 || beforeNext != before.AppliedCommandLSN+1 {
				t.Fatalf("seed checkpoint frontier=%d next=%d", before.AppliedCommandLSN, beforeNext)
			}
			fmt.Printf("ORIGINAL GROUP SEED NEXT %d\n", beforeNext)
			// Checkpoint can reclaim the already materialized catalog and seed
			// frames. Preserve its actual retained baseline, rather than assuming
			// those historical frames are still physically present.
			beforeFrames := collectionCommandWALFrames(t, handle.Dir())
			for _, frame := range beforeFrames {
				if frame.LSN >= beforeNext {
					t.Fatalf("checkpoint retained an unapplied frame: %+v", frame)
				}
			}
			sequence := database.Stats()["treedb.cache.cow.txn.logical_seq"]
			var installs atomic.Int32
			installHook := func() { installs.Add(1) }
			if !typedUpsertGroupAfterInstallTestHook.CompareAndSwap(nil, &installHook) {
				t.Fatal("install hook already set")
			}
			participantIDs := []string{"a", "b", "c"}
			done, release := formNativeTypedGroup(t, handle, col, participantIDs...)
			injected := errors.New("actual typed group prefix cut")
			var calls int
			var actual []uint64
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				if e.Point == durabilitycut.AfterDependencyAppend {
					actual = append(actual, e.LSN)
				}
				if e.Point == scenario.point {
					calls++
					if calls == scenario.occurrence {
						return injected
					}
				}
				return nil
			})
			release()
			for range 3 {
				result := <-done
				if !result.handled || result.err == nil || !errors.Is(result.err, injected) {
					t.Fatalf("partial group caller=%+v", result)
				}
			}
			restore()
			if installs.Load() != 0 {
				t.Fatal("partial append reached installed-before-ack hook")
			}
			frames := collectionCommandWALFrames(t, handle.Dir())
			if len(frames) != len(beforeFrames)+scenario.assigned {
				t.Fatalf("real replayable prefix frames=%d baseline=%d assigned=%d", len(frames), len(beforeFrames), scenario.assigned)
			}
			for i, original := range beforeFrames {
				frame := frames[i]
				if frame.LSN != original.LSN || frame.Kind != original.Kind || frame.PayloadFormat != original.PayloadFormat || !bytes.Equal(frame.Payload, original.Payload) {
					t.Fatalf("failed group changed retained baseline frame %d: %+v", i, frame)
				}
			}
			if calls != scenario.occurrence || len(actual) != scenario.assigned {
				t.Fatalf("actual append cut calls=%d identities=%v", calls, actual)
			}
			for i, frame := range frames[len(beforeFrames):] {
				if frame.LSN != actual[i] || actual[i] != beforeNext+uint64(i) {
					t.Fatalf("replayable frame=%+v actual assigned prefix=%v", frame, actual)
				}
				if i == len(participantIDs) {
					if frame.Kind != commitlog.CommandKindDurablePrefixBarrier || frame.PayloadFormat != commitlog.PayloadFormatDurablePrefixBarrierV1 || len(frame.Payload) != 0 {
						t.Fatalf("assigned barrier endpoint=%+v", frame)
					}
					continue
				}
				if frame.Kind != commitlog.CommandKindCompositeMutation || frame.PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
					t.Fatalf("assigned participant frame=%+v", frame)
				}
				members := 0
				if err := commitlog.ScanCompositeMutationPayload(frame.Payload, func(member commitlog.CompositeMutationMember) error {
					members++ // Exactly one member preserves the original ordinal zero.
					if member.Incarnation == 0 || member.Kind != commitlog.CommandKindCollectionReplaceSourceByID || member.PayloadFormat != commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
						t.Fatalf("assigned participant member=%+v", member)
					}
					payload, err := commitlog.DecodeCollectionTypedSourcePayload(member.Payload)
					if err != nil || payload.Inserted.Collection != "events" || len(payload.DeleteIDs) != 1 || string(payload.DeleteIDs[0]) != participantIDs[i] || len(payload.Inserted.Documents) != 1 || string(payload.Inserted.Documents[0].ID) != participantIDs[i] {
						t.Fatalf("assigned participant %d payload=%+v: %v", i, payload, err)
					}
					return nil
				}); err != nil || members != 1 {
					t.Fatalf("assigned participant ordinal grammar: members=%d err=%v", members, err)
				}
			}
			if now, ok := handle.StateToken(); !ok || now != before || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || handle.CommandWALNextLSN() != beforeNext+uint64(scenario.assigned) {
				t.Fatalf("failed prefix installed logical/physical cut: %+v next=%d", now, handle.CommandWALNextLSN())
			}
			old, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("a"), []byte("b"), []byte("c")}, DocumentFetchOptions{})
			if err != nil || len(old.Results) != 4 || !old.Results[0].Found || old.Results[1].Found || old.Results[2].Found || old.Results[3].Found {
				t.Fatalf("failed prefix changed held cut: %+v %v", old.Results, err)
			}
			if scenario.assigned != 0 {
				if _, _, err := col.InsertTypedBatchWithStats(nativeTypedGroupBatch("retry")); err == nil || handle.CommandWALNextLSN() != beforeNext+uint64(scenario.assigned) {
					t.Fatalf("assigned failed group allowed continuation: %v next=%d", err, handle.CommandWALNextLSN())
				}
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			_ = manager.Close()
			closeErr := database.Close()
			if scenario.assigned == 0 {
				if closeErr != nil || budget.Stats().TotalBytes != 0 {
					t.Fatalf("unassigned failure retained custody: %v %+v", closeErr, budget.Stats())
				}
			} else {
				if closeErr == nil || budget.Stats().TotalBytes == 0 {
					t.Fatalf("assigned uninstalled group lost original custody: %v %+v", closeErr, budget.Stats())
				}
			}
			fmt.Printf("%s case=%s actual=%v no-logical-install\n", marker, scenario.name, actual)
			os.Exit(97)
		})
	}
}
