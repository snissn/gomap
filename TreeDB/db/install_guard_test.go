package db

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestRawBatchInstallGuardMismatchFreesTrackedPagesAndSkipsRetire(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	seed := db.NewBatch()
	for i := 0; i < 256; i++ {
		if err := seed.Set([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte("a"), 128)); err != nil {
			t.Fatalf("seed set: %v", err)
		}
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = seed.Close()

	before := db.Stats()
	beforeFree := installGuardStatUint(t, before, "treedb.freelist.free_pages_total")
	beforeGraveyardPages := installGuardStatUint(t, before, "treedb.graveyard.pages")

	batchIface := db.NewBatch()
	b, ok := batchIface.(*Batch)
	if !ok {
		t.Fatalf("new batch type %T, want *Batch", batchIface)
	}
	defer func() { _ = b.Close() }()
	for i := 0; i < 256; i++ {
		if err := b.Set([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte("b"), 128)); err != nil {
			t.Fatalf("update set: %v", err)
		}
	}
	hookCalls := 0
	db.testInstallGuardHook = func(ev dbInstallGuardHookEvent) error {
		if ev.Kind != dbInstallGuardRawBatch {
			return nil
		}
		hookCalls++
		return ErrInstallGuardMismatch
	}
	t.Cleanup(func() { db.testInstallGuardHook = nil })
	committed, err := b.writeOptimistic(false)
	db.testInstallGuardHook = nil
	if err != nil {
		t.Fatalf("writeOptimistic err=%v", err)
	}
	if committed {
		t.Fatal("writeOptimistic committed despite injected install guard mismatch")
	}
	if hookCalls != 1 {
		t.Fatalf("install guard hook calls=%d want 1", hookCalls)
	}

	after := db.Stats()
	if got := installGuardStatUint(t, after, "treedb.publish.install_guard.failures_total"); got != 1 {
		t.Fatalf("install guard failures=%d want 1", got)
	}
	if got := installGuardStatUint(t, after, "treedb.publish.install_guard.hook_failures_total"); got != 1 {
		t.Fatalf("install guard hook failures=%d want 1", got)
	}
	if got := installGuardStatUint(t, after, "treedb.graveyard.pages"); got != beforeGraveyardPages {
		t.Fatalf("graveyard pages=%d want unchanged %d", got, beforeGraveyardPages)
	}
	if got := installGuardStatUint(t, after, "treedb.freelist.free_pages_total"); got <= beforeFree {
		t.Fatalf("freelist free pages=%d want > %d after abandoning tracked pages", got, beforeFree)
	}
	got, err := db.Get([]byte("k0000"))
	if err != nil {
		t.Fatalf("get k0000: %v", err)
	}
	if bytes.Equal(got, bytes.Repeat([]byte("b"), 128)) {
		t.Fatalf("guard-failed batch became visible")
	}
}

func TestOrderedRootDeltaBatchGroupInstallGuardFailureAbandonsGroup(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	beforeState := db.State()
	if beforeState == nil {
		t.Fatal("state before publish is nil")
	}
	beforeStats := db.Stats()
	beforeFree := installGuardStatUint(t, beforeStats, "treedb.freelist.free_pages_total")

	deltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	hookCalls := 0
	var captured []preparedRootApplyGroup
	db.testInstallGuardHook = func(ev dbInstallGuardHookEvent) error {
		if ev.Kind != dbInstallGuardOrderedRootGroup {
			return nil
		}
		hookCalls++
		return ErrInstallGuardMismatch
	}
	db.testPreparedRootApplyHook = func(group preparedRootApplyGroup) {
		captured = append(captured, group)
	}
	_, _, err = db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot: baseRoot,
		Delta:    delta,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	db.testInstallGuardHook = nil
	db.testPreparedRootApplyHook = nil
	if !errors.Is(err, ErrInstallGuardMismatch) {
		t.Fatalf("publish err=%v want install guard mismatch", err)
	}
	if hookCalls != 1 {
		t.Fatalf("install guard hook calls=%d want 1", hookCalls)
	}

	afterState := db.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("commit seq changed after failed install guard: got %d want %d", afterState.CommitSeq, beforeState.CommitSeq)
	}
	if afterState.RootPageID != beforeState.RootPageID {
		t.Fatalf("user root changed after failed install guard: got %d want %d", afterState.RootPageID, beforeState.RootPageID)
	}
	if afterState.SystemRootPageID != beforeState.SystemRootPageID {
		t.Fatalf("system root changed after failed install guard: got %d want %d", afterState.SystemRootPageID, beforeState.SystemRootPageID)
	}
	afterStats := db.Stats()
	if got := installGuardStatUint(t, afterStats, "treedb.publish.ordered_root_delta_group.install_guard_failures_total"); got != 1 {
		t.Fatalf("ordered install guard failures=%d want 1", got)
	}
	if got := installGuardStatUint(t, afterStats, "treedb.publish.install_guard.hook_failures_total"); got != 1 {
		t.Fatalf("ordered install guard hook failures=%d want 1", got)
	}
	if got := installGuardStatUint(t, afterStats, "treedb.publish.ordered_root_delta_group.roots_total"); got != 0 {
		t.Fatalf("ordered roots total=%d want 0 after failed install guard", got)
	}
	if got := installGuardStatUint(t, afterStats, "treedb.publish.ordered_root_delta_group.finalize_calls_total"); got != 0 {
		t.Fatalf("ordered finalize calls=%d want 0 after failed install guard", got)
	}
	if got := installGuardStatUint(t, afterStats, "treedb.freelist.free_pages_total"); got <= beforeFree {
		t.Fatalf("freelist free pages=%d want > %d after abandoning ordered output", got, beforeFree)
	}
	if len(captured) != 1 {
		t.Fatalf("prepared apply groups=%d want 1", len(captured))
	}
	if captured[0].state != preparedRootApplyStateAbandoned {
		t.Fatalf("prepared group state=%v want abandoned", captured[0].state)
	}
	if got := installGuardStatUint(t, afterStats, "treedb.publish.ordered_root_delta_group.prepared_root.abandoned_total"); got != 2 {
		t.Fatalf("prepared abandoned=%d want 2", got)
	}
	if got := installGuardStatUint(t, afterStats, "treedb.publish.ordered_root_delta_group.prepared_root.installed_total"); got != 0 {
		t.Fatalf("prepared installed=%d want 0", got)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot after failed install guard is nil")
	}
	defer func() { _ = snap.Close() }()
	if _, err := snap.GetEntryAtRoot(afterState.SystemRootPageID, []byte("sys/collections/users/primary")); err == nil {
		t.Fatal("unexpected descriptor installed after failed install guard")
	}
}

func TestInstallGuardMismatchCauseCounters(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.runInstallGuard(rawBatchInstallGuard(999)); !errors.Is(err, ErrInstallGuardMismatch) {
		t.Fatalf("user-root guard err=%v want install guard mismatch", err)
	}
	if _, err := db.runInstallGuard(orderedRootDeltaGroupSystemInstallGuard(999)); !errors.Is(err, ErrInstallGuardMismatch) {
		t.Fatalf("system-root guard err=%v want install guard mismatch", err)
	}

	stats := db.Stats()
	if got := installGuardStatUint(t, stats, "treedb.publish.install_guard.user_root_mismatches_total"); got != 1 {
		t.Fatalf("user-root mismatch counter=%d want 1", got)
	}
	if got := installGuardStatUint(t, stats, "treedb.publish.install_guard.system_root_mismatches_total"); got != 1 {
		t.Fatalf("system-root mismatch counter=%d want 1", got)
	}
	if got := installGuardStatUint(t, stats, "treedb.publish.install_guard.hook_failures_total"); got != 0 {
		t.Fatalf("hook failure counter=%d want 0", got)
	}
}

func TestInstallGuardCountsDualRootMismatchCauses(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	state := db.State()
	if state == nil {
		t.Fatal("state is nil")
	}
	before := db.Stats()
	_, err = db.runInstallGuard(orderedRootDeltaGroupInstallGuard(
		state.RootPageID+1,
		state.SystemRootPageID+1,
	))
	if !errors.Is(err, ErrInstallGuardMismatch) {
		t.Fatalf("install guard err=%v want mismatch", err)
	}
	after := db.Stats()
	statDelta := func(name string) uint64 {
		gotAfter := installGuardStatUint(t, after, name)
		gotBefore := installGuardStatUint(t, before, name)
		if gotAfter < gotBefore {
			t.Fatalf("%s decreased: before=%d after=%d", name, gotBefore, gotAfter)
		}
		return gotAfter - gotBefore
	}
	if got := statDelta("treedb.publish.install_guard.failures_total"); got != 1 {
		t.Fatalf("install guard failures delta=%d want 1", got)
	}
	if got := statDelta("treedb.publish.install_guard.user_root_mismatches_total"); got != 1 {
		t.Fatalf("user-root mismatch delta=%d want 1", got)
	}
	if got := statDelta("treedb.publish.install_guard.system_root_mismatches_total"); got != 1 {
		t.Fatalf("system-root mismatch delta=%d want 1", got)
	}
}

func installGuardStatUint(tb testing.TB, stats map[string]string, key string) uint64 {
	tb.Helper()
	raw, ok := stats[key]
	if !ok {
		tb.Fatalf("missing stat %s", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return got
}
