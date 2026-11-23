package memtable

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

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
	if len(root.delta) != 0 || root.deltaAdded != 0 {
		t.Fatalf("multi-key batch left non-empty delta: len=%d added=%d", len(root.delta), root.deltaAdded)
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

	// Apply single-op updates up to cowDeltaMaxEntries: mix of new keys,
	// shadows of existing tree keys, and re-overwrites of delta keys.
	// Verify every published root's Get, SeekGE, SeekLT, Len, ScanOwned,
	// COWCursor (forward & reverse), COWTraversal (forward, reverse, direction reversal),
	// and LogicalSizeDelta match the reference map.
	baseTree := root.tree
	for step := 0; step < cowDeltaMaxEntries+10; step++ {
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
			// Overwrite a key already in delta (if step >= 2).
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
		if step < cowDeltaMaxEntries/2 && len(nextRoot.delta) > 0 && nextRoot.tree != baseTree {
			t.Fatalf("step %d unexpectedly forked base tree while delta had capacity", step)
		}
		pinned = append(pinned, nextRoot)
		verifyRootMatchesMap(t, nextRoot, expected)
	}

	// Verify the earliest pinned snapshot is still 100% immutable and unchanged.
	verifyRootMatchesMap(t, pinned[0], pinnedSnap)

	// Now test ForkCOWWriter while delta is non-empty.
	latest := pinned[len(pinned)-1]
	if len(latest.delta) == 0 {
		// Add one single op so latest has a non-empty delta.
		expected["fork-pre"] = "v1"
		latest = cowTestPublish(t, writer, []COWMutation{{Key: []byte("fork-pre"), Value: []byte("v1"), Flags: node.FlagInline}})
		pinned = append(pinned, latest)
	}
	forked, err := ForkCOWWriter(latest)
	if err != nil {
		t.Fatalf("ForkCOWWriter: %v", err)
	}
	forkExpected := map[string]string{}
	for k, v := range expected {
		forkExpected[k] = v
	}
	forkExpected["fork-only"] = "fv"
	forkRoot := cowTestPublish(t, forked, []COWMutation{{Key: []byte("fork-only"), Value: []byte("fv"), Flags: node.FlagInline}})
	verifyRootMatchesMap(t, forkRoot, forkExpected)
	verifyRootMatchesMap(t, latest, expected)
	cowTestRelease(forkRoot)
	cowTestClose(forked)

	// Physical Remove while delta is non-empty flushes delta + removes key.
	removeKey := "k0000"
	delete(expected, removeKey)
	afterRemove := cowTestPublish(t, writer, []COWMutation{{Key: []byte(removeKey), Remove: true}})
	pinned = append(pinned, afterRemove)
	if len(afterRemove.delta) != 0 {
		t.Fatalf("Remove did not flush delta: len=%d", len(afterRemove.delta))
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
	for step := 0; step < 250; step++ {
		op := rng.Intn(10)
		var batch []COWMutation
		if op < 7 {
			// Single put
			k := fmt.Sprintf("k%03d", rng.Intn(80))
			v := fmt.Sprintf("v-%d", step)
			batch = []COWMutation{{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline}}
			expected[k] = v
		} else if op < 9 {
			// Multi-key batch (2..4 keys)
			n := 2 + rng.Intn(3)
			for range n {
				k := fmt.Sprintf("k%03d", rng.Intn(80))
				v := fmt.Sprintf("b-%d", step)
				batch = append(batch, COWMutation{Key: []byte(k), Value: []byte(v), Flags: node.FlagInline})
				expected[k] = v
			}
		} else {
			// Physical remove
			k := fmt.Sprintf("k%03d", rng.Intn(80))
			batch = []COWMutation{{Key: []byte(k), Remove: true}}
			delete(expected, k)
		}
		next := cowTestPublish(t, writer, batch)
		if root != nil {
			cowTestRelease(root)
		}
		root = next
		if step%10 == 0 || step == 249 {
			verifyRootMatchesMap(t, root, expected)
		}
	}
	if root != nil {
		cowTestRelease(root)
	}
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
