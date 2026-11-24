package memtable

import (
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWDeltaLifecycleShadowAndFlush(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	// Seed base tree via multi-key batch (flushes directly into tree, delta == nil).
	var seed []COWMutation
	expected := make(map[string]string)
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("k%04d", i*2)
		v := fmt.Sprintf("base-%04d", i)
		seed = append(seed, COWMutation{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline})
		expected[k] = v
	}
	root := cowTestPublish(t, writer, seed)
	if len(root.delta) != 0 || root.l1 != nil || root.deltaAdded != 0 {
		t.Fatalf("multi-key batch left non-empty delta/l1: len=%d l1=%v added=%d", len(root.delta), root.l1 != nil, root.deltaAdded)
	}

	var pinned []*COWRoot
	pinned = append(pinned, root)
	pinnedSnap := map[string]string{}
	for k, v := range expected {
		pinnedSnap[k] = v
	}
	defer func() {
		for _, r := range pinned {
			cowTestRelease(r)
		}
	}()

	// Apply single-op updates across multiple L0->L1 merges: mix of new keys,
	// shadows of existing tree keys, and re-overwrites of delta/L1 keys.
	// Verify every published root's Get, SeekGE, SeekLT, Len, ScanOwned,
	// COWCursor (forward & reverse), Estimate/Prepare exactness, and LogicalSizeDelta.
	baseTree := root.tree
	sawPureL1 := false
	sawL0AndL1 := false
	for step := 0; step < cowDeltaL0MaxEntries*3+10; step++ {
		var k, v string
		switch step % 4 {
		case 0:
			// New key interleaved between base keys.
			k = fmt.Sprintf("k%04d", step*2+1)
			v = fmt.Sprintf("delta-new-%04d", step)
		case 1:
			// Shadow an existing base-tree key.
			k = fmt.Sprintf("k%04d", (step%20)*2)
			v = fmt.Sprintf("delta-shadow-%04d", step)
		case 2:
			// Overwrite a key already in delta/L1 (if step >= 2).
			k = fmt.Sprintf("k%04d", ((step-2)/4)*8+1)
			v = fmt.Sprintf("delta-reover-%04d", step)
		case 3:
			// New high key (sequential tail).
			k = fmt.Sprintf("z%04d", step)
			v = fmt.Sprintf("delta-tail-%04d", step)
		}
		m := []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}}
		est, err := writer.Estimate(m, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("step %d Estimate: %v", step, err)
		}
		if bound, ok := writer.RawPointPredictionUpperBound(m[0]); ok {
			if bound.History() < est.History() {
				t.Fatalf("step %d RawPointPredictionUpperBound history %d < Estimate %d", step, bound.History(), est.History())
			}
		}
		prep, err := writer.Prepare(m, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("step %d Prepare: %v", step, err)
		}
		if prep.Charge().Total() > est.Total() {
			t.Fatalf("step %d Prepare charge %+v exceeded Estimate %+v", step, prep.Charge(), est)
		}
		deltaBytes, valid := prep.LogicalSizeDelta()
		if !valid {
			t.Fatalf("step %d invalid LogicalSizeDelta", step)
		}
		prevLogical := mapLogicalSize(expected)
		expected[k] = v
		nextLogical := mapLogicalSize(expected)
		if deltaBytes != nextLogical-prevLogical {
			t.Fatalf("step %d LogicalSizeDelta=%d want %d", step, deltaBytes, nextLogical-prevLogical)
		}
		nextRoot := prep.Publish()
		if nextRoot.tree != baseTree {
			t.Fatalf("step %d unexpectedly forked base tree while L0+L1 had capacity (l0=%d l1=%d)", step, len(nextRoot.delta), cowDeltaL1Count(nextRoot.l1))
		}
		if prep.Charge() != est {
			t.Fatalf("step %d L0/L1 path should have exact Estimate==Prepare charge: got %+v want %+v", step, prep.Charge(), est)
		}
		if nextRoot.l1 != nil && len(nextRoot.delta) == 0 {
			sawPureL1 = true
		}
		if nextRoot.l1 != nil && len(nextRoot.delta) > 0 {
			sawL0AndL1 = true
		}
		pinned = append(pinned, nextRoot)
		verifyRootMatchesMap(t, nextRoot, expected)
	}
	if !sawPureL1 || !sawL0AndL1 {
		t.Fatalf("expected to observe both pure-L1 (%v) and L0+L1 (%v) states", sawPureL1, sawL0AndL1)
	}

	// Verify the earliest pinned snapshot is still 100% immutable and unchanged.
	verifyRootMatchesMap(t, pinned[0], pinnedSnap)

	// Now test ForkCOWWriter while BOTH L1 and L0 are non-empty.
	latest := pinned[len(pinned)-1]
	if len(latest.delta) == 0 || latest.l1 == nil {
		// Add one single op so latest has both L1 and L0 populated.
		expected["fork-pre"] = "v1"
		latest = cowTestPublish(t, writer, []COWMutation{{Key: []byte("fork-pre"), Value: []byte("v1"), Flags: node.FlagInline}})
		pinned = append(pinned, latest)
	}
	if latest.l1 == nil || len(latest.delta) == 0 {
		t.Fatalf("expected both L1 and L0 populated before fork, got l1=%v l0=%d", latest.l1 != nil, len(latest.delta))
	}
	forked, err := ForkCOWWriter(latest)
	if err != nil {
		t.Fatalf("ForkCOWWriter: %v", err)
	}
	forkExpected := map[string]string{}
	for k, v := range expected {
		forkExpected[k] = v
	}
	// Overwrite an existing base-tree key AND insert a new key on the forked writer.
	forkExpected["k0002"] = "fork-overwrite"
	forkRoot1 := cowTestPublish(t, forked, []COWMutation{{Key: []byte("k0002"), Value: []byte("fork-overwrite"), Flags: node.FlagInline}})
	verifyRootMatchesMap(t, forkRoot1, forkExpected)
	cowTestRelease(forkRoot1)
	forkExpected["fork-only"] = "fv"
	forkRoot := cowTestPublish(t, forked, []COWMutation{{Key: []byte("fork-only"), Value: []byte("fv"), Flags: node.FlagInline}})
	verifyRootMatchesMap(t, forkRoot, forkExpected)
	verifyRootMatchesMap(t, latest, expected)
	cowTestRelease(forkRoot)
	cowTestClose(forked)

	// Physical Remove while L1+L0 are non-empty flushes both levels + removes key.
	removeKey := "k0000"
	delete(expected, removeKey)
	afterRemove := cowTestPublish(t, writer, []COWMutation{{Key: []byte(removeKey), Remove: true}})
	pinned = append(pinned, afterRemove)
	if len(afterRemove.delta) != 0 || afterRemove.l1 != nil {
		t.Fatalf("Remove did not flush L0+L1: l0=%d l1=%v", len(afterRemove.delta), afterRemove.l1 != nil)
	}
	verifyRootMatchesMap(t, afterRemove, expected)
}

func TestCOWDeltaTraversalWorkLimitedAndReversal(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	expected := map[string]string{}
	var seed []COWMutation
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key-%03d", i*2)
		v := fmt.Sprintf("base-%03d", i*2)
		seed = append(seed, COWMutation{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline})
		expected[k] = v
	}
	r := cowTestPublish(t, writer, seed)
	cowTestRelease(r)

	// Layer delta entries that both insert new keys and shadow base keys.
	updates := []struct{ k, v string }{
		{"key-000", "shadow-000"},
		{"key-001", "new-001"},
		{"key-010", "shadow-010"},
		{"key-015", "new-015"},
		{"key-020", "shadow-020"},
		{"key-038", "shadow-038"},
		{"key-039", "new-039"},
	}
	var root *COWRoot
	for _, u := range updates {
		next := cowTestPublish(t, writer, []COWMutation{{Key: []byte(u.k), Value: []byte(u.v), Flags: node.FlagInline}})
		if root != nil {
			cowTestRelease(root)
		}
		root = next
		expected[u.k] = u.v
	}
	defer cowTestRelease(root)
	if len(root.delta) != len(updates) {
		t.Fatalf("expected delta len %d, got %d", len(updates), len(root.delta))
	}

	keys := sortedMapKeys(expected)

	// Test tight-budget resumable traversal with repeated direction reversals
	// on every shadowed and non-shadowed key.
	work := &COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}
	tr, err := root.NewTraversal(work, 0)
	if err != nil {
		t.Fatalf("NewTraversal: %v", err)
	}
	runOp := func(op COWTraversalOperation, key string) (COWRecord, bool) {
		t.Helper()
		for {
			w := &COWTraversalWork{RecordLimit: 4, ByteLimit: 4096}
			if err := tr.Begin(op, key, w); err != nil {
				t.Fatalf("Begin(%d, %q): %v", op, key, err)
			}
			for steps := 0; ; steps++ {
				if steps > 1000 {
					t.Fatalf("Step(%d, %q) exceeded max resumable steps", op, key)
				}
				sw := &COWTraversalWork{RecordLimit: 2, ByteLimit: 512}
				done, err := tr.Step(sw)
				if err != nil {
					t.Fatalf("Step(%d, %q): %v", op, key, err)
				}
				if done {
					break
				}
			}
			rw := &COWTraversalWork{RecordLimit: 4, ByteLimit: 512}
			rec, ok, err := tr.Record(rw)
			if err != nil {
				t.Fatalf("Record(%d, %q): %v", op, key, err)
			}
			return rec, ok
		}
	}

	// Forward walk, ping-ponging Next -> Prev -> Next at each position.
	rec, ok := runOp(COWFirst, "")
	if !ok || rec.Key != keys[0] || rec.Value != expected[keys[0]] {
		t.Fatalf("First got (%q=%q, %v), want %q", rec.Key, rec.Value, ok, keys[0])
	}
	for i := 1; i < len(keys); i++ {
		rec, ok = runOp(COWNext, "")
		if !ok || rec.Key != keys[i] || rec.Value != expected[keys[i]] {
			t.Fatalf("Next[%d] got (%q=%q, %v), want %q=%q", i, rec.Key, rec.Value, ok, keys[i], expected[keys[i]])
		}
		// Reverse one step back to keys[i-1], then forward again to keys[i].
		prevRec, prevOk := runOp(COWPrev, "")
		if !prevOk || prevRec.Key != keys[i-1] || prevRec.Value != expected[keys[i-1]] {
			t.Fatalf("Reverse Prev at %d got (%q=%q, %v), want %q=%q", i, prevRec.Key, prevRec.Value, prevOk, keys[i-1], expected[keys[i-1]])
		}
		fwdRec, fwdOk := runOp(COWNext, "")
		if !fwdOk || fwdRec.Key != keys[i] || fwdRec.Value != expected[keys[i]] {
			t.Fatalf("Re-Forward Next at %d got (%q=%q, %v), want %q=%q", i, fwdRec.Key, fwdRec.Value, fwdOk, keys[i], expected[keys[i]])
		}
	}
	if _, ok := runOp(COWNext, ""); ok {
		t.Fatal("expected EOF after last key")
	}

	// SeekGE/SeekLE to shadowed keys followed by Prev/Next.
	for _, target := range []string{"key-000", "key-001", "key-010", "key-015", "key-020", "key-038", "key-039"} {
		idx := slices.Index(keys, target)
		rec, ok := runOp(COWSeekGE, target)
		if !ok || rec.Key != target || rec.Value != expected[target] {
			t.Fatalf("SeekGE(%q) got (%q=%q, %v)", target, rec.Key, rec.Value, ok)
		}
		if idx > 0 {
			prev, ok := runOp(COWPrev, "")
			if !ok || prev.Key != keys[idx-1] || prev.Value != expected[keys[idx-1]] {
				t.Fatalf("SeekGE(%q)->Prev got (%q=%q, %v), want %q", target, prev.Key, prev.Value, ok, keys[idx-1])
			}
		}
		rec, ok = runOp(COWSeekLE, target)
		if !ok || rec.Key != target || rec.Value != expected[target] {
			t.Fatalf("SeekLE(%q) got (%q=%q, %v)", target, rec.Key, rec.Value, ok)
		}
		if idx+1 < len(keys) {
			next, ok := runOp(COWNext, "")
			if !ok || next.Key != keys[idx+1] || next.Value != expected[keys[idx+1]] {
				t.Fatalf("SeekLE(%q)->Next got (%q=%q, %v), want %q", target, next.Key, next.Value, ok, keys[idx+1])
			}
		}
	}

	// Work-limited close.
	for steps := 0; ; steps++ {
		if steps > 1000 {
			t.Fatal("CloseStep exceeded max resumable steps")
		}
		cw := &COWTraversalWork{RecordLimit: 4, ByteLimit: 4096}
		done, ret, err := tr.CloseStep(cw)
		if err != nil {
			t.Fatalf("CloseStep: %v", err)
		}
		if done {
			ret.Drain()
			break
		}
	}
}

func TestCOWDeltaTraversalPendingCloseAndBoundaryRecovery(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	// 1. Delta-only root: unpositioned COWNext lands on first delta record;
	// Prev before start -> Next recovers; Next past end -> Prev recovers.
	r1 := cowTestPublish(t, writer, []COWMutation{{Key: []byte("b-only"), Value: []byte("v1"), Flags: node.FlagInline}})
	defer cowTestRelease(r1)
	if r1.tree.Len() != 0 || len(r1.delta) != 1 {
		t.Fatalf("expected delta-only root, got tree=%d delta=%d", r1.tree.Len(), len(r1.delta))
	}

	stepToDone := func(tr *COWTraversal, op COWTraversalOperation, key string) (COWRecord, bool) {
		t.Helper()
		w := &COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}
		if err := tr.Begin(op, key, w); err != nil {
			t.Fatalf("Begin(%d, %q): %v", op, key, err)
		}
		done, err := tr.Step(w)
		if err != nil || !done {
			t.Fatalf("Step(%d, %q): done=%v err=%v", op, key, done, err)
		}
		rec, ok, err := tr.Record(w)
		if err != nil {
			t.Fatalf("Record(%d, %q): %v", op, key, err)
		}
		return rec, ok
	}
	closeAll := func(tr *COWTraversal) {
		t.Helper()
		w := &COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}
		done, ret, err := tr.CloseStep(w)
		if err != nil || !done {
			t.Fatalf("CloseStep: done=%v err=%v", done, err)
		}
		ret.Drain()
	}

	tr, err := r1.NewTraversal(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("NewTraversal: %v", err)
	}
	// Unpositioned Prev stays empty, matching StringCursor.
	if rec, ok := stepToDone(tr, COWPrev, ""); ok {
		t.Fatalf("unpositioned Prev returned %+v", rec)
	}
	// Unpositioned Next lands on first delta entry.
	if rec, ok := stepToDone(tr, COWNext, ""); !ok || rec.Key != "b-only" || rec.Value != "v1" {
		t.Fatalf("unpositioned Next got (%+v, %v), want b-only=v1", rec, ok)
	}
	// Next past end -> exhausted, then Prev recovers Last.
	if _, ok := stepToDone(tr, COWNext, ""); ok {
		t.Fatal("expected EOF after single entry")
	}
	if rec, ok := stepToDone(tr, COWPrev, ""); !ok || rec.Key != "b-only" {
		t.Fatalf("Prev after EOF got (%+v, %v), want b-only", rec, ok)
	}
	// Prev before start -> exhausted, then Next recovers First.
	if _, ok := stepToDone(tr, COWPrev, ""); ok {
		t.Fatal("expected BOF before single entry")
	}
	if rec, ok := stepToDone(tr, COWNext, ""); !ok || rec.Key != "b-only" {
		t.Fatalf("Next after BOF got (%+v, %v), want b-only", rec, ok)
	}
	closeAll(tr)

	// 2. Populated base tree + leading delta entry: unpositioned Next sees leading delta.
	r2 := cowTestPublish(t, writer, []COWMutation{
		{Key: []byte("m1"), Value: []byte("vm1"), Flags: node.FlagInline},
		{Key: []byte("m2"), Value: []byte("vm2"), Flags: node.FlagInline},
	})
	defer cowTestRelease(r2)
	longKey := strings.Repeat("a", 4096)
	r3 := cowTestPublish(t, writer, []COWMutation{{Key: []byte(longKey), Value: []byte("va"), Flags: node.FlagInline}})
	defer cowTestRelease(r3)

	tr2, err := r3.NewTraversal(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("NewTraversal r3: %v", err)
	}
	if rec, ok := stepToDone(tr2, COWNext, ""); !ok || rec.Key != longKey || rec.Value != "va" {
		t.Fatalf("unpositioned Next with leading delta got (%q=%q, %v)", rec.Key, rec.Value, ok)
	}
	closeAll(tr2)

	// 3. Pause a delta-backed SeekGE mid-comparison (delta.pending == true),
	// then call CloseStep and confirm clean cancellation with zero leaks.
	tr3, err := r3.NewTraversal(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("NewTraversal r3 pause: %v", err)
	}
	if err := tr3.Begin(COWSeekGE, strings.Repeat("a", 4095)+"b", &COWTraversalWork{RecordLimit: 16, ByteLimit: 4096}); err != nil {
		t.Fatalf("Begin SeekGE: %v", err)
	}
	// Run with a small byte budget so delta.Step pauses mid-key comparison.
	paused := false
	for range 32 {
		done, err := tr3.Step(&COWTraversalWork{RecordLimit: 4, ByteLimit: 512})
		if err != nil {
			t.Fatalf("Step before pause: %v", err)
		}
		if tr3.delta.pending {
			paused = true
			break
		}
		if done {
			break
		}
	}
	if !paused {
		t.Fatal("expected delta traversal to pause mid-SeekGE")
	}
	closeAll(tr3)
}

func TestCOWDeltaRandomizedModel(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	rng := rand.New(rand.NewSource(42))
	expected := map[string]string{}
	var root *COWRoot
	for step := 0; step < 700; step++ {
		op := rng.Intn(20)
		var batch []COWMutation
		if op < 16 {
			// Single put (triggers L0->L1 merges and L1->tree flushes).
			k := fmt.Sprintf("k%03d", rng.Intn(220))
			v := fmt.Sprintf("v-%d", step)
			batch = []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}}
			expected[k] = v
		} else if op < 19 {
			// Multi-key batch (2..4 keys)
			n := 2 + rng.Intn(3)
			for range n {
				k := fmt.Sprintf("k%03d", rng.Intn(220))
				v := fmt.Sprintf("b-%d", step)
				batch = append(batch, COWMutation{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline})
				expected[k] = v
			}
		} else {
			// Physical remove
			k := fmt.Sprintf("k%03d", rng.Intn(220))
			batch = []COWMutation{{Key: []byte(k), Remove: true}}
			delete(expected, k)
		}
		next := cowTestPublish(t, writer, batch)
		if root != nil {
			cowTestRelease(root)
		}
		root = next
		if step%25 == 0 || step == 699 {
			verifyRootMatchesMap(t, root, expected)
		}
	}
	if root != nil {
		cowTestRelease(root)
	}
}

func TestCOWDeltaTwoLevelPureL1AndThreeWayShadowing(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	expected := map[string]string{}
	var seed []COWMutation
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("k%03d", i*3)
		v := fmt.Sprintf("tree-%03d", i*3)
		seed = append(seed, COWMutation{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline})
		expected[k] = v
	}
	root := cowTestPublish(t, writer, seed)

	// Push exactly cowDeltaL0MaxEntries+1 distinct single keys so L0 merges into
	// L1 and leaves a pure-L1 root (len(delta)==0 && l1!=nil) with multiple chunks
	// if we push a second wave.
	for i := 0; i < (cowDeltaL0MaxEntries+1)*2; i++ {
		k := fmt.Sprintf("k%03d", i*2+1) // mix of tree shadows and new keys
		if i%5 == 0 {
			k = fmt.Sprintf("k%03d", (i%12)*3) // explicit tree shadow
		}
		v := fmt.Sprintf("l1-%03d", i)
		next := cowTestPublish(t, writer, []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}})
		cowTestRelease(root)
		root = next
		expected[k] = v
	}
	// Advance until we land right on a pure-L1 boundary.
	for step := 0; len(root.delta) != 0 && step < cowDeltaL0MaxEntries+2; step++ {
		k := fmt.Sprintf("m%03d", step)
		v := fmt.Sprintf("l1-pad-%03d", step)
		next := cowTestPublish(t, writer, []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}})
		cowTestRelease(root)
		root = next
		expected[k] = v
	}
	if len(root.delta) != 0 || root.l1 == nil || cowDeltaL1Count(root.l1) == 0 {
		t.Fatalf("expected pure-L1 root, got l0=%d l1=%d", len(root.delta), cowDeltaL1Count(root.l1))
	}
	verifyRootMatchesMap(t, root, expected)
	verifyTraversalPingPong(t, root, expected)

	// ScanOwned on pure-L1 root must be 0-alloc.
	allocs := testing.AllocsPerRun(50, func() {
		count := 0
		root.ScanOwned(func(COWRecord) bool {
			count++
			return true
		})
		if count != len(expected) {
			t.Fatalf("ScanOwned count=%d want %d", count, len(expected))
		}
	})
	if allocs != 0 {
		t.Fatalf("pure-L1 ScanOwned allocated %.2f times/op, want 0", allocs)
	}

	// Now add L0 keys that shadow L1 keys, shadow tree-only keys, and insert new keys.
	l0Updates := []struct{ k, v string }{
		{"k000", "l0-shadow-l1-and-tree"},
		{"k001", "l0-shadow-l1-only"},
		{"k003", "l0-shadow-tree-only"},
		{"a000", "l0-leading-new"},
		{"z999", "l0-trailing-new"},
	}
	for _, u := range l0Updates {
		next := cowTestPublish(t, writer, []COWMutation{{Key: []byte(u.k), Value: []byte(u.v), Flags: node.FlagInline}})
		cowTestRelease(root)
		root = next
		expected[u.k] = u.v
	}
	if len(root.delta) == 0 || root.l1 == nil || root.tree.Len() == 0 {
		t.Fatalf("expected all 3 layers populated, got l0=%d l1=%d tree=%d", len(root.delta), cowDeltaL1Count(root.l1), root.tree.Len())
	}
	verifyRootMatchesMap(t, root, expected)
	verifyTraversalPingPong(t, root, expected)

	// ScanOwned on 3-layer root must also be 0-alloc.
	allocs3 := testing.AllocsPerRun(50, func() {
		count := 0
		root.ScanOwned(func(COWRecord) bool {
			count++
			return true
		})
		if count != len(expected) {
			t.Fatalf("3-layer ScanOwned count=%d want %d", count, len(expected))
		}
	})
	if allocs3 != 0 {
		t.Fatalf("3-layer ScanOwned allocated %.2f times/op, want 0", allocs3)
	}
	cowTestRelease(root)
}

func TestCOWDeltaTwoLevelTraversalPauseAndSize(t *testing.T) {
	if sz := unsafe.Sizeof(cowDeltaTraversal{}); sz >= 256 {
		t.Fatalf("cowDeltaTraversal size=%dB, want <256B", sz)
	}
	if sz := unsafe.Sizeof(COWTraversal{}); sz >= 1536 {
		t.Fatalf("COWTraversal size=%dB, want <1536B", sz)
	}

	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	// Pure-L1 root with empty tree and a long key inside L1.
	longKey := strings.Repeat("b", 4096)
	var root *COWRoot
	for i := 0; i < cowDeltaL0MaxEntries+1; i++ {
		k := fmt.Sprintf("a%03d", i)
		if i == cowDeltaL0MaxEntries {
			k = longKey
		}
		next := cowTestPublish(t, writer, []COWMutation{{Key: []byte(k), Value: []byte("v"), Flags: node.FlagInline}})
		if root != nil {
			cowTestRelease(root)
		}
		root = next
	}
	defer cowTestRelease(root)
	if root.tree.Len() != 0 || len(root.delta) != 0 || root.l1 == nil {
		t.Fatalf("expected tree-empty pure-L1 root, got tree=%d l0=%d l1=%v", root.tree.Len(), len(root.delta), root.l1 != nil)
	}

	// Unpositioned Next/Prev and BOF/EOF recovery on pure-L1 root.
	tr, err := root.NewTraversal(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("NewTraversal: %v", err)
	}
	stepDone := func(op COWTraversalOperation, key string) (COWRecord, bool) {
		t.Helper()
		w := &COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}
		if err := tr.Begin(op, key, w); err != nil {
			t.Fatalf("Begin(%d): %v", op, err)
		}
		done, err := tr.Step(w)
		if err != nil || !done {
			t.Fatalf("Step(%d): done=%v err=%v", op, done, err)
		}
		rec, ok, err := tr.Record(w)
		if err != nil {
			t.Fatalf("Record(%d): %v", op, err)
		}
		return rec, ok
	}
	if rec, ok := stepDone(COWPrev, ""); ok {
		t.Fatalf("unpositioned Prev on pure-L1 returned %+v", rec)
	}
	if rec, ok := stepDone(COWNext, ""); !ok || rec.Key != "a000" {
		t.Fatalf("unpositioned Next on pure-L1 got (%+v,%v), want a000", rec, ok)
	}
	if rec, ok := stepDone(COWLast, ""); !ok || rec.Key != longKey {
		t.Fatalf("Last on pure-L1 got (%q,%v), want longKey", rec.Key, ok)
	}
	if _, ok := stepDone(COWNext, ""); ok {
		t.Fatal("expected EOF after Last on pure-L1")
	}
	if rec, ok := stepDone(COWPrev, ""); !ok || rec.Key != longKey {
		t.Fatalf("Prev after EOF on pure-L1 got (%q,%v), want longKey", rec.Key, ok)
	}
	if done, ret, err := tr.CloseStep(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}); err != nil || !done {
		t.Fatalf("CloseStep: done=%v err=%v", done, err)
	} else {
		ret.Drain()
	}

	// Pause mid-L1-search comparison and close cleanly.
	tr2, err := root.NewTraversal(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("NewTraversal pause: %v", err)
	}
	if err := tr2.Begin(COWSeekGE, strings.Repeat("b", 4095)+"c", &COWTraversalWork{RecordLimit: 16, ByteLimit: 4096}); err != nil {
		t.Fatalf("Begin SeekGE: %v", err)
	}
	paused := false
	for range 64 {
		done, err := tr2.Step(&COWTraversalWork{RecordLimit: 2, ByteLimit: 512})
		if err != nil {
			t.Fatalf("Step before pause: %v", err)
		}
		if tr2.delta.pending {
			paused = true
			break
		}
		if done {
			break
		}
	}
	if !paused {
		t.Fatal("expected pure-L1 traversal to pause mid-SeekGE")
	}
	if done, ret, err := tr2.CloseStep(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}); err != nil || !done {
		t.Fatalf("CloseStep paused L1: done=%v err=%v", done, err)
	} else {
		ret.Drain()
	}
}

func TestCOWDeltaTightBudgetL1Gating(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxGenerationBytes = 1 << 20
	limits.MaxInFlightBytes = 512 << 10
	budget, writer := cowTestWriter(t, limits)
	var root *COWRoot
	defer func() {
		if root != nil {
			cowTestRelease(root)
		}
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	expected := map[string]string{}
	sawTreeFlush := false
	for i := 0; i < 400; i++ {
		k := fmt.Sprintf("k%06d", (i*73)%400)
		v := fmt.Sprintf("val-%06d", i)
		m := []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}}
		est, err := writer.Estimate(m, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("step %d Estimate: %v", i, err)
		}
		if bound, ok := writer.RawPointPredictionUpperBound(m[0]); ok {
			if bound.History() < est.History() {
				t.Fatalf("step %d RawPointPredictionUpperBound %d < Estimate %d", i, bound.History(), est.History())
			}
		}
		prep, err := writer.Prepare(m, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("step %d Prepare failed under tight 1MiB/512KiB budget: %v", i, err)
		}
		if prep.Charge().Total() > est.Total() {
			t.Fatalf("step %d Prepare charge %+v > Estimate %+v", i, prep.Charge(), est)
		}
		next := prep.Publish()
		if root != nil {
			cowTestRelease(root)
		}
		root = next
		expected[k] = v
		if root.tree.Len() > 0 {
			sawTreeFlush = true
		}
	}
	if !sawTreeFlush {
		t.Fatal("expected tight-budget gating to flush L1 into main tree")
	}
	verifyRootMatchesMap(t, root, expected)
}

func TestCOWDeltaPrivateMaxKeySkipAccuracy(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	expected := map[string]string{}
	var root *COWRoot
	apply := func(batch []COWMutation) {
		t.Helper()
		prep, err := writer.Prepare(batch, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		delta, valid := prep.LogicalSizeDelta()
		if !valid {
			t.Fatal("invalid LogicalSizeDelta")
		}
		prev := mapLogicalSize(expected)
		for _, m := range batch {
			if m.Remove {
				delete(expected, string(m.Key))
			} else {
				expected[string(m.Key)] = string(m.Value)
			}
		}
		if want := mapLogicalSize(expected) - prev; delta != want {
			t.Fatalf("LogicalSizeDelta=%d want %d", delta, want)
		}
		next := prep.Publish()
		if root != nil {
			cowTestRelease(root)
		}
		root = next
		if root.Len() != len(expected) {
			t.Fatalf("Len=%d want %d", root.Len(), len(expected))
		}
	}

	// Seed base tree with keys 000..099 so privateMaxKey == "k0099".
	var seed []COWMutation
	for i := 0; i < 100; i++ {
		seed = append(seed, COWMutation{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: []byte("v1"),
			Flags: node.FlagInline,
		})
	}
	apply(seed)

	// Ascending single keys > privateMaxKey (hits privateMaxKey skip fast path!).
	for i := 100; i < 160; i++ {
		apply([]COWMutation{{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: []byte("v-asc"),
			Flags: node.FlagInline,
		}})
	}
	// Overwrite existing key <= privateMaxKey.
	apply([]COWMutation{{Key: []byte("k0050"), Value: []byte("v-overwrite-longer"), Flags: node.FlagInline}})
	// Remove the current maximum key after flushing to tree.
	apply([]COWMutation{{Key: []byte("k0159"), Remove: true}})
	// Insert a key strictly > new max ("k0158") and overwrite an existing key ("k0158").
	apply([]COWMutation{{Key: []byte("k0158"), Value: []byte("v-158-updated"), Flags: node.FlagInline}})
	apply([]COWMutation{{Key: []byte("k0159"), Value: []byte("v-159-reborn"), Flags: node.FlagInline}})
	verifyRootMatchesMap(t, root, expected)
	cowTestRelease(root)
}

func verifyTraversalPingPong(t *testing.T, root *COWRoot, expected map[string]string) {
	t.Helper()
	keys := sortedMapKeys(expected)
	tr, err := root.NewTraversal(&COWTraversalWork{RecordLimit: 1 << 20, ByteLimit: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("NewTraversal: %v", err)
	}
	runOp := func(op COWTraversalOperation, key string) (COWRecord, bool) {
		t.Helper()
		if err := tr.Begin(op, key, &COWTraversalWork{RecordLimit: 4, ByteLimit: 4096}); err != nil {
			t.Fatalf("Begin(%d, %q): %v", op, key, err)
		}
		for steps := 0; ; steps++ {
			if steps > 2000 {
				t.Fatalf("Step(%d, %q) exceeded max resumable steps", op, key)
			}
			done, err := tr.Step(&COWTraversalWork{RecordLimit: 2, ByteLimit: 512})
			if err != nil {
				t.Fatalf("Step(%d, %q): %v", op, key, err)
			}
			if done {
				break
			}
		}
		rec, ok, err := tr.Record(&COWTraversalWork{RecordLimit: 4, ByteLimit: 512})
		if err != nil {
			t.Fatalf("Record(%d, %q): %v", op, key, err)
		}
		return rec, ok
	}

	rec, ok := runOp(COWFirst, "")
	if !ok || rec.Key != keys[0] || rec.Value != expected[keys[0]] {
		t.Fatalf("First got (%q=%q,%v) want %q", rec.Key, rec.Value, ok, keys[0])
	}
	for i := 1; i < len(keys); i++ {
		rec, ok = runOp(COWNext, "")
		if !ok || rec.Key != keys[i] || rec.Value != expected[keys[i]] {
			t.Fatalf("Next[%d] got (%q=%q,%v) want %q=%q", i, rec.Key, rec.Value, ok, keys[i], expected[keys[i]])
		}
		if i%7 == 0 {
			prevRec, prevOk := runOp(COWPrev, "")
			if !prevOk || prevRec.Key != keys[i-1] || prevRec.Value != expected[keys[i-1]] {
				t.Fatalf("Prev[%d] got (%q=%q,%v) want %q", i, prevRec.Key, prevRec.Value, prevOk, keys[i-1])
			}
			fwdRec, fwdOk := runOp(COWNext, "")
			if !fwdOk || fwdRec.Key != keys[i] || fwdRec.Value != expected[keys[i]] {
				t.Fatalf("ReNext[%d] got (%q=%q,%v) want %q", i, fwdRec.Key, fwdRec.Value, fwdOk, keys[i])
			}
		}
	}
	if _, ok := runOp(COWNext, ""); ok {
		t.Fatal("expected EOF after last key")
	}
	for steps := 0; ; steps++ {
		if steps > 1000 {
			t.Fatal("CloseStep exceeded max resumable steps")
		}
		done, ret, err := tr.CloseStep(&COWTraversalWork{RecordLimit: 4, ByteLimit: 4096})
		if err != nil {
			t.Fatalf("CloseStep: %v", err)
		}
		if done {
			ret.Drain()
			break
		}
	}
}

func cowDeltaL1Count(l1 *cowDeltaL1) int {
	if l1 == nil {
		return 0
	}
	return l1.count
}

func mapLogicalSize(m map[string]string) int64 {
	var total int64
	for k, v := range m {
		total += int64(len(k) + len(v))
	}
	return total
}

func sortedMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func verifyRootMatchesMap(t *testing.T, root *COWRoot, expected map[string]string) {
	t.Helper()
	keys := sortedMapKeys(expected)
	if root.Len() != len(keys) {
		t.Fatalf("Len()=%d want=%d", root.Len(), len(keys))
	}
	// Get
	for _, k := range keys {
		rec, ok := root.Get([]byte(k))
		if !ok || rec.Key != k || rec.Value != expected[k] {
			t.Fatalf("Get(%q)=(%q=%q,%v) want %q", k, rec.Key, rec.Value, ok, expected[k])
		}
	}
	if _, ok := root.Get([]byte("missing-key-zzz")); ok {
		t.Fatal("Get(missing) returned found")
	}
	// ScanOwned (including early stop)
	var scanned []string
	root.ScanOwned(func(rec COWRecord) bool {
		if rec.Value != expected[rec.Key] {
			t.Fatalf("ScanOwned(%q)=%q want %q", rec.Key, rec.Value, expected[rec.Key])
		}
		scanned = append(scanned, rec.Key)
		return true
	})
	if !slices.Equal(scanned, keys) {
		t.Fatalf("ScanOwned keys mismatch: got %d want %d", len(scanned), len(keys))
	}
	if len(keys) > 3 {
		early := 0
		root.ScanOwned(func(COWRecord) bool {
			early++
			return early < 3
		})
		if early != 3 {
			t.Fatalf("ScanOwned early stop visited %d want 3", early)
		}
	}
	// SeekGE / SeekLT
	for i, k := range keys {
		rec, ok := root.SeekGE([]byte(k), nil)
		if !ok || rec.Key != k || rec.Value != expected[k] {
			t.Fatalf("SeekGE(%q)=(%q=%q,%v)", k, rec.Key, rec.Value, ok)
		}
		lt, ok := root.SeekLT(nil, []byte(k))
		if i == 0 {
			if ok {
				t.Fatalf("SeekLT(first %q) returned %q", k, lt.Key)
			}
		} else if !ok || lt.Key != keys[i-1] || lt.Value != expected[keys[i-1]] {
			t.Fatalf("SeekLT(%q)=(%q=%q,%v) want %q", k, lt.Key, lt.Value, ok, keys[i-1])
		}
	}
	// Forward & Reverse COWCursor
	cur, err := root.Cursor(nil, nil)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	var fwd []string
	for {
		rec, ok, err := cur.Record()
		if err != nil {
			t.Fatalf("Cursor.Record: %v", err)
		}
		if !ok {
			break
		}
		if rec.Value != expected[rec.Key] {
			t.Fatalf("Cursor(%q)=%q want %q", rec.Key, rec.Value, expected[rec.Key])
		}
		fwd = append(fwd, rec.Key)
		if err := cur.Next(); err != nil {
			t.Fatalf("Cursor.Next: %v", err)
		}
	}
	cur.Close()
	if !slices.Equal(fwd, keys) {
		t.Fatalf("Forward Cursor mismatch: got %v want %v", strings.Join(fwd, ","), strings.Join(keys, ","))
	}

	rcur, err := root.ReverseCursorWithExtraBytes(nil, nil, 0)
	if err != nil {
		t.Fatalf("ReverseCursor: %v", err)
	}
	var rev []string
	for {
		rec, ok, err := rcur.Record()
		if err != nil {
			t.Fatalf("ReverseCursor.Record: %v", err)
		}
		if !ok {
			break
		}
		if rec.Value != expected[rec.Key] {
			t.Fatalf("ReverseCursor(%q)=%q want %q", rec.Key, rec.Value, expected[rec.Key])
		}
		rev = append(rev, rec.Key)
		if err := rcur.Next(); err != nil {
			t.Fatalf("ReverseCursor.Next: %v", err)
		}
	}
	rcur.Close()
	slices.Reverse(rev)
	if !slices.Equal(rev, keys) {
		t.Fatalf("Reverse Cursor mismatch: got %d want %d", len(rev), len(keys))
	}
}

func TestCOWRawBatchPredictionUpperBoundAndSinglePassSetBytes(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	var root *COWRoot
	defer func() {
		if root != nil {
			cowTestRelease(root)
		}
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	expected := map[string]string{}
	checkBatch := func(label string, batch []COWMutation) {
		t.Helper()
		est, err := writer.Estimate(batch, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("%s Estimate: %v", label, err)
		}
		bound, ok := writer.RawBatchPredictionUpperBound(batch)
		if !ok {
			t.Fatalf("%s RawBatchPredictionUpperBound returned false", label)
		}
		if bound.History() < est.History() || bound.Total() < est.Total() {
			t.Fatalf("%s bound %+v < Estimate %+v", label, bound, est)
		}
		prep, err := writer.Prepare(batch, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("%s Prepare: %v", label, err)
		}
		if prep.Charge().Total() > est.Total() {
			t.Fatalf("%s Prepare charge %+v > Estimate %+v", label, prep.Charge(), est)
		}
		delta, valid := prep.LogicalSizeDelta()
		if !valid {
			t.Fatalf("%s invalid LogicalSizeDelta", label)
		}
		prev := mapLogicalSize(expected)
		for _, m := range batch {
			if m.Flags&node.FlagTombstone != 0 {
				expected[string(m.Key)] = ""
			} else {
				expected[string(m.Key)] = string(m.Value)
			}
		}
		if want := mapLogicalSize(expected) - prev; delta != want {
			t.Fatalf("%s LogicalSizeDelta=%d want %d", label, delta, want)
		}
		next := prep.Publish()
		if root != nil {
			cowTestRelease(root)
		}
		root = next
		if root.Len() != len(expected) {
			t.Fatalf("%s Len=%d want %d", label, root.Len(), len(expected))
		}
	}

	// 1. Empty tree, large multi-key batch (hits skipRawBatchProbes).
	var batch1 []COWMutation
	for i := 0; i < 200; i++ {
		batch1 = append(batch1, COWMutation{
			Key:   []byte(fmt.Sprintf("k%05d", i)),
			Value: []byte(fmt.Sprintf("v1-%05d", i)),
			Flags: node.FlagInline,
		})
	}
	checkBatch("empty-tree-large", batch1)

	// 2. Single-key batch into L0 and across L0->L1.
	for i := 0; i < cowDeltaL0MaxEntries+4; i++ {
		checkBatch(fmt.Sprintf("single-%d", i), []COWMutation{{
			Key:   []byte(fmt.Sprintf("s%05d", i)),
			Value: []byte("sv"),
			Flags: node.FlagInline,
		}})
	}

	// 3. Large replace+insert+tombstone batch while BOTH L1 and L0 are populated.
	var batch2 []COWMutation
	for i := 0; i < 150; i++ {
		if i%5 == 0 {
			batch2 = append(batch2, COWMutation{
				Key:   []byte(fmt.Sprintf("k%05d", i)),
				Flags: node.FlagTombstone,
			})
		} else {
			batch2 = append(batch2, COWMutation{
				Key:   []byte(fmt.Sprintf("k%05d", i*2)),
				Value: []byte(fmt.Sprintf("v2-%05d", i)),
				Flags: node.FlagInline,
			})
		}
	}
	checkBatch("populated-l0-l1-mixed", batch2)

	// 4. Negative checks: empty, Remove, FlagPointer must return ok=false.
	if _, ok := writer.RawBatchPredictionUpperBound(nil); ok {
		t.Fatal("empty batch should return ok=false")
	}
	if _, ok := writer.RawBatchPredictionUpperBound([]COWMutation{{Key: []byte("k00000"), Remove: true}}); ok {
		t.Fatal("Remove should return ok=false")
	}
	if _, ok := writer.RawBatchPredictionUpperBound([]COWMutation{{Key: []byte("k00000"), Flags: node.FlagPointer}}); ok {
		t.Fatal("FlagPointer should return ok=false")
	}
}

func TestCOWNonDeleteNodeBoundExactnessAndTightness(t *testing.T) {
	cowTestIsolateAllocationWitness(t)
	limits := DefaultCOWLimits()
	limits.MaxInFlightBytes = 512 << 20
	limits.MaxTotalBytes = 1 << 30
	limits.MaxRetiredBytes = 1 << 30
	limits.MaxGenerationBytes = 256 << 20

	// 1. Tightness check: 4 shards at oldLen=30,000 + ops=2,000 must stay well
	// below the default 32 MiB available producer in-flight headroom.
	bound, ok := cowNonDeleteNodeBound(30000, 32000, 2000, cowMaximumHeight(32000), 32000, false)
	if !ok {
		t.Fatal("cowNonDeleteNodeBound overflowed at oldLen=30000 ops=2000")
	}
	if 4*bound >= 32<<20 {
		t.Fatalf("4-shard bound %d >= 32 MiB (1-shard=%d)", 4*bound, bound)
	}

	makeEntries := func(count int, startIdx int, mode string, rng *rand.Rand) []COWMutation {
		entries := make([]COWMutation, count)
		for i := range entries {
			var k int
			switch mode {
			case "seq":
				k = startIdx + i
			case "desc":
				k = startIdx + (count - 1 - i)
			case "rand":
				k = rng.Intn(startIdx + count*4 + 1)
			case "replace":
				if startIdx > 0 {
					k = rng.Intn(startIdx)
				}
			}
			flags := byte(node.FlagInline)
			var val []byte
			if i%11 == 7 {
				flags = node.FlagTombstone
			} else {
				val = []byte(fmt.Sprintf("val-%08d", k))
			}
			entries[i] = COWMutation{
				Key:   []byte(fmt.Sprintf("k%010d", k)),
				Value: val,
				Flags: flags,
			}
		}
		return entries
	}

	verifyStep := func(t *testing.T, label string, writer *COWWriter, root **COWRoot, entries []COWMutation) {
		t.Helper()
		est, err := writer.Estimate(entries, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("%s Estimate: %v", label, err)
		}
		var bound COWCharge
		var boundOK bool
		if len(entries) == 1 {
			ptBound, ptOK := writer.RawPointPredictionUpperBound(entries[0])
			if !ptOK || ptBound.History() < est.History() {
				t.Fatalf("%s RawPointPredictionUpperBound=%+v (ok=%t) < Estimate=%+v", label, ptBound, ptOK, est)
			}
		}
		bound, boundOK = writer.RawBatchPredictionUpperBound(entries)
		if !boundOK {
			t.Fatalf("%s prediction upper bound returned false", label)
		}
		if bound.Nodes < est.Nodes || bound.History() < est.History() || bound.Total() < est.Total() {
			t.Fatalf("%s bound %+v < Estimate %+v", label, bound, est)
		}
		runtime.GC()
		var beforeMem, afterMem runtime.MemStats
		runtime.ReadMemStats(&beforeMem)
		prep, err := writer.Prepare(entries, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("%s Prepare: %v", label, err)
		}
		next := prep.Publish()
		runtime.ReadMemStats(&afterMem)
		if prep.Charge().Nodes > est.Nodes || prep.Charge().Total() > est.Total() {
			t.Fatalf("%s Charge %+v > Estimate %+v", label, prep.Charge(), est)
		}
		if alloc := afterMem.TotalAlloc - beforeMem.TotalAlloc; alloc > est.Total() {
			t.Fatalf("%s TotalAlloc=%d > Estimate.Total=%d", label, alloc, est.Total())
		}
		if *root != nil {
			cowTestRelease(*root)
		}
		*root = next
	}

	// 2. Empty, small, boundary (31/32/63/64), L0/L1 mixing, replacement, and
	// large sequential/random trees across oldLen 0..20,000.
	rng := rand.New(rand.NewSource(42))
	for _, tc := range []struct {
		name    string
		seedLen int
		seedOrd string
		steps   []struct {
			n    int
			mode string
		}
	}{
		{
			name:    "empty-and-boundary-batches",
			seedLen: 0,
			steps: []struct {
				n    int
				mode string
			}{{1, "seq"}, {2, "seq"}, {31, "seq"}, {32, "desc"}, {63, "seq"}, {64, "rand"}, {500, "rand"}, {2000, "seq"}, {2000, "replace"}},
		},
		{
			name:    "l0-l1-mixed-into-dense-63-tree",
			seedLen: 441, // 7 * 63
			seedOrd: "seq",
			steps: []struct {
				n    int
				mode string
			}{{1, "seq"}, {1, "rand"}, {1, "replace"}, {31, "rand"}, {32, "seq"}, {63, "desc"}, {64, "rand"}, {500, "replace"}, {2000, "rand"}},
		},
		{
			name:    "large-tree-20k",
			seedLen: 20000,
			seedOrd: "rand",
			steps: []struct {
				n    int
				mode string
			}{{2000, "rand"}, {2000, "seq"}, {2000, "replace"}, {1, "rand"}, {64, "rand"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget, writer := cowTestWriter(t, limits)
			var root *COWRoot
			defer func() {
				if root != nil {
					cowTestRelease(root)
				}
				cowTestClose(writer)
				budget.Close()
			}()
			if tc.seedLen > 0 {
				seed := makeEntries(tc.seedLen, 0, tc.seedOrd, rng)
				verifyStep(t, "seed", writer, &root, seed)
			}
			// Inject L0 + L1 entries via point inserts before mixed batches.
			for j := 0; j < cowDeltaL0MaxEntries+5; j++ {
				pt := makeEntries(1, tc.seedLen+100000+j, "seq", rng)
				verifyStep(t, fmt.Sprintf("point-%d", j), writer, &root, pt)
			}
			offset := tc.seedLen + 200000
			for idx, st := range tc.steps {
				batch := makeEntries(st.n, offset, st.mode, rng)
				offset += st.n
				verifyStep(t, fmt.Sprintf("step-%d-%s-%d", idx, st.mode, st.n), writer, &root, batch)
			}
			// Also verify 4-shard extrapolation at 20k+ keys stays well under 32 MiB.
			if tc.seedLen >= 20000 {
				probe := makeEntries(2000, offset, "rand", rng)
				est, err := writer.Estimate(probe, COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if 4*est.Total() >= 32<<20 {
					t.Fatalf("20k-shard 2000-op 4-shard Total=%d >= 32 MiB", 4*est.Total())
				}
			}
		})
	}
}

func TestCOWDeltaAppendTailImmutabilityAndCancel(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	defer func() {
		cowTestClose(writer)
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Fatalf("leaked budget bytes: %+v", got)
		}
	}()

	expected := make(map[string]string)
	var pinned []*COWRoot
	var pinnedMaps []map[string]string
	defer func() {
		for _, r := range pinned {
			cowTestRelease(r)
		}
	}()

	cloneMap := func(src map[string]string) map[string]string {
		dst := make(map[string]string, len(src))
		for k, v := range src {
			dst[k] = v
		}
		return dst
	}

	// Step 0..cowDeltaL0MaxEntries-1: verify append-tail sharing of the backing
	// array, exact Estimate == Prepare, RawPointPredictionUpperBound >= Estimate,
	// and snapshot immutability for every earlier published COWRoot.
	var firstData *cowDeltaEntry
	for i := 0; i < cowDeltaL0MaxEntries; i++ {
		k := fmt.Sprintf("seq-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		m := []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}}
		est, err := writer.Estimate(m, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("step %d Estimate: %v", i, err)
		}
		if bound, ok := writer.RawPointPredictionUpperBound(m[0]); ok {
			if bound.History() < est.History() {
				t.Fatalf("step %d RawPointPredictionUpperBound history %d < Estimate %d", i, bound.History(), est.History())
			}
		}
		if i > 0 && est.Nodes != 0 {
			t.Fatalf("step %d append-tail Estimate.Nodes=%d, want 0", i, est.Nodes)
		}
		prep, err := writer.Prepare(m, COWPrepareOptions{})
		if err != nil {
			t.Fatalf("step %d Prepare: %v", i, err)
		}
		if prep.Charge() != est {
			t.Fatalf("step %d Prepare charge %+v != Estimate %+v", i, prep.Charge(), est)
		}
		root := prep.Publish()
		if len(root.delta) != i+1 {
			t.Fatalf("step %d len(root.delta)=%d, want %d", i, len(root.delta), i+1)
		}
		if i == 0 {
			firstData = unsafe.SliceData(root.delta)
		} else if unsafe.SliceData(root.delta) != firstData {
			t.Fatalf("step %d did not reuse append-tail backing array", i)
		}
		expected[k] = v
		pinned = append(pinned, root)
		pinnedMaps = append(pinnedMaps, cloneMap(expected))

		// Every earlier snapshot must remain completely unchanged.
		for j, snap := range pinned {
			verifyRootMatchesMap(t, snap, pinnedMaps[j])
		}
	}

	// Fork from an intermediate snapshot (with len < cap in the shared backing
	// array) and publish a write on the fork: it MUST NOT overwrite pinned
	// sibling snapshots from the original writer.
	mid := pinned[3]
	forked, err := ForkCOWWriter(mid)
	if err != nil {
		t.Fatalf("ForkCOWWriter: %v", err)
	}
	forkRoot := cowTestPublish(t, forked, []COWMutation{{
		Key:   []byte("fork-divergent"),
		Value: []byte("fork-val"),
		Flags: node.FlagInline,
	}})
	forkWant := cloneMap(pinnedMaps[3])
	forkWant["fork-divergent"] = "fork-val"
	verifyRootMatchesMap(t, forkRoot, forkWant)
	for j, snap := range pinned {
		verifyRootMatchesMap(t, snap, pinnedMaps[j])
	}
	cowTestRelease(forkRoot)
	cowTestClose(forked)

	// Next write flushes L0 -> L1 and starts a fresh L0 tail on the subsequent write.
	flushKey := "seq-flush-l1"
	flushVal := "val-flush-l1"
	flushed := cowTestPublish(t, writer, []COWMutation{{Key: []byte(flushKey), Value: []byte(flushVal), Flags: node.FlagInline}})
	expected[flushKey] = flushVal
	pinned = append(pinned, flushed)
	pinnedMaps = append(pinnedMaps, cloneMap(expected))
	if len(flushed.delta) != 0 || flushed.l1 == nil {
		t.Fatalf("expected L0->L1 merge, got l0=%d l1=%v", len(flushed.delta), flushed.l1 != nil)
	}

	// Start a new L0 tail, then Prepare + Cancel an append-tail write, and verify
	// the cancelled entry is cleared and cannot corrupt the next Publish.
	r1 := cowTestPublish(t, writer, []COWMutation{{Key: []byte("tail-a"), Value: []byte("va"), Flags: node.FlagInline}})
	expected["tail-a"] = "va"
	pinned = append(pinned, r1)
	pinnedMaps = append(pinnedMaps, cloneMap(expected))

	cancelPrep, err := writer.Prepare([]COWMutation{{Key: []byte("tail-cancelled"), Value: []byte("vc"), Flags: node.FlagInline}}, COWPrepareOptions{})
	if err != nil {
		t.Fatalf("cancel Prepare: %v", err)
	}
	ret := cancelPrep.Cancel()
	ret.Drain()
	verifyRootMatchesMap(t, r1, pinnedMaps[len(pinnedMaps)-1])

	r2 := cowTestPublish(t, writer, []COWMutation{{Key: []byte("tail-b"), Value: []byte("vb"), Flags: node.FlagInline}})
	expected["tail-b"] = "vb"
	pinned = append(pinned, r2)
	pinnedMaps = append(pinnedMaps, cloneMap(expected))
	verifyRootMatchesMap(t, r2, expected)
	if _, ok := r2.Get([]byte("tail-cancelled")); ok {
		t.Fatalf("cancelled key visible after publish")
	}
	for j, snap := range pinned {
		verifyRootMatchesMap(t, snap, pinnedMaps[j])
	}
}
