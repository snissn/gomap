package db

import (
	"bytes"
	"errors"
	"math"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func ownedPointTestLimits() PreparedOwnedPointLimits {
	// Test admission only. Production maxima must derive from the request and
	// captured root; these capacities are not a published product ceiling.
	return PreparedOwnedPointLimits{MaxClosurePages: 512, MaxClosureEntries: 16384, MaxOutputPages: 8192, MaxDepth: 16, MaxKeyBytes: 32}
}

func TestPreparedOwnedPointColdAndRefusalReleaseSnapshot(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snap := database.AcquireSnapshot()
	defer snap.Close()
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("doc/1")}}
	before := snap.readState.Load()
	refusal := errors.New("request credit refused")
	if owner, err := database.CapturePreparedOwnedPointRoot(snap, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), func(uint64) error { return refusal }); owner != nil || !errors.Is(err, refusal) {
		t.Fatalf("refusal owner=%v err=%v", owner, err)
	}
	if snap.readState.Load() != before {
		t.Fatal("failed capture retained a snapshot read pin")
	}
	var charged uint64
	owner, err := database.CapturePreparedOwnedPointRoot(snap, 0, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if owner.BackingBytes() != charged || owner.Ledger().ClosurePages != 0 {
		t.Fatalf("ledger=%+v charged=%d", owner.Ledger(), charged)
	}
	prepared, err := owner.Prepare(ops)
	if err != nil || !prepared.ColdBuild || prepared.PointOps != 1 || cap(prepared.LeafSpans) != 0 {
		t.Fatalf("prepare=%+v err=%v", prepared, err)
	}
	owner.Close()
	owner.Close()
	if snap.readState.Load() != before {
		t.Fatal("owner close retained its snapshot read pin")
	}
	if _, err := owner.Prepare(ops); err == nil {
		t.Fatal("closed owner admitted prepare")
	}
}

func TestPreparedOwnedPointCapturedPagerAndCanonicalKeys(t *testing.T) {
	database := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
	defer database.Close()
	delta := newOrderedRootSpanNativeBatch(t, 1024, "owned-base")
	root := publishOrderedRootSpanNativeBatch(t, database, 0, delta, OrderedRootStoragePagerLeaves)
	delta.Close()
	snap := database.AcquireSnapshot()
	defer snap.Close()
	token, _ := snap.StateToken()
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("key-000030")}, {Type: batch.OpPut, Key: []byte("key-000031")}}
	var charged uint64
	owner, err := database.CapturePreparedOwnedPointRoot(snap, root, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	ledger := owner.Ledger()
	if ledger.Captured != token || ledger.Profile.BaseRoot != root || ledger.Profile.PointOps != 2 || ledger.Profile.TouchedOldLeafPages == 0 || ledger.ClosurePages > ledger.Profile.TouchedOldPages+ledger.Profile.TouchedOldInternalChildren || ledger.ReservedBacking != charged {
		t.Fatalf("ledger=%+v charged=%d", ledger, charged)
	}
	ops[0].Key[0] = 0x7a
	if _, err := owner.Prepare(ops); err == nil {
		t.Fatal("mutated caller keys admitted")
	}
	if string(owner.ops[0].Key) != "key-000030" {
		t.Fatal("caller mutation changed owned canonical key")
	}
	prepared, err := owner.Prepare(owner.ops)
	if err != nil || prepared.TouchedOldPages != ledger.Profile.TouchedOldPages || prepared.TouchedOldInternalChildren != ledger.Profile.TouchedOldInternalChildren || prepared.TouchedOldLeafEntries != ledger.Profile.TouchedOldLeafEntries || cap(prepared.LeafSpans) != 0 {
		t.Fatalf("prepare=%+v ledger=%+v err=%v", prepared, ledger, err)
	}
	wrong := token
	wrong.CommitSeq++
	if err := owner.ValidateCapturedBaseline(wrong); err == nil {
		t.Fatal("different captured state accepted")
	}
	if err := owner.ValidateCapturedBaseline(token); err != nil {
		t.Fatal(err)
	}
	narrow := ownedPointTestLimits()
	narrow.MaxClosurePages = 1
	if other, err := database.CapturePreparedOwnedPointRoot(snap, root, OrderedRootStoragePagerLeaves, owner.ops, narrow, func(uint64) error { return nil }); other != nil || err == nil {
		if other != nil {
			other.Close()
		}
		t.Fatal("closure capacity did not refuse")
	}
}

func TestPreparedOwnedPointFrozenLZ4ImagesAndFreshCorruptionRefusal(t *testing.T) {
	dir := t.TempDir()
	database := openOrderedRootSpanNativeTestDB(t, dir, true, 1)
	defer database.Close()
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockLZ4})
	if err != nil {
		t.Fatal(err)
	}
	database.SetLeafPageLog(leafLog)
	delta := newOrderedRootSpanNativeBatch(t, 1024, "owned-lz4-base")
	root := publishOrderedRootSpanNativeBatch(t, database, 0, delta, OrderedRootStorageValueLogLeaves)
	delta.Close()
	snap := database.AcquireSnapshot()
	defer snap.Close()
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("key-000030")}, {Type: batch.OpPut, Key: []byte("key-000031")}}
	owner, err := database.CapturePreparedOwnedPointRoot(snap, root, OrderedRootStorageValueLogLeaves, ops, ownedPointTestLimits(), func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	var selected *preparedOwnedPointClosureEntry
	for i := range owner.closure {
		e := &owner.closure[i]
		if e.ref.Kind == page.ChildRefLeafLog && e.touched {
			selected = e
			break
		}
	}
	if selected == nil || selected.shape.DictID != 0 || !selected.shape.Compressed || selected.shape.Codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("missing frozen default LZ4 leaf: %v", selected)
	}
	if owner.input != nil || owner.raw != nil || owner.snappy != nil || owner.lz4 != nil {
		t.Fatal("sealed owner retained drained decoder backing")
	}
	ptr := selected.ref.Log.ValuePtr()
	image, err := owner.Workspace().ReadUnsafe(ptr)
	if err != nil || len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) {
		t.Fatalf("frozen image err=%v", err)
	}
	retained := append([]byte(nil), image...)
	mutable, err := os.OpenFile(selected.file.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer mutable.Close()
	oldStat, err := selected.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	mutableStat, err := mutable.Stat()
	if err != nil || !os.SameFile(oldStat, mutableStat) {
		t.Fatal("mutable test descriptor changed physical identity")
	}
	at := int64(ptr.Offset) + valuelog.HeaderSize
	oldByte := make([]byte, 1)
	if _, err := mutable.ReadAt(oldByte, at); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := mutable.WriteAt(oldByte, at); err != nil {
			t.Errorf("restore leaf byte: %v", err)
		}
	}()
	if _, err := mutable.WriteAt([]byte{oldByte[0] ^ 1}, at); err != nil {
		t.Fatal(err)
	}
	still, err := owner.Workspace().ReadUnsafe(ptr)
	if err != nil || !bytes.Equal(still, retained) {
		t.Fatalf("external mutation changed frozen image: %v", err)
	}
	if _, err := owner.Prepare(ops); err != nil {
		t.Fatalf("owned prepare reread mutable leaf record: %v", err)
	}
	if other, err := database.CapturePreparedOwnedPointRoot(snap, root, OrderedRootStorageValueLogLeaves, ops, ownedPointTestLimits(), func(uint64) error { return nil }); other != nil || err == nil {
		if other != nil {
			other.Close()
		}
		t.Fatal("fresh capture accepted corrupted old record")
	}
}

func TestPreparedOwnedPointOutputBoundChecked(t *testing.T) {
	if got, err := ownedPointOutputBound(64, 1471, 16, 112); err != nil || got != 3392 {
		t.Fatalf("Q0=%d err=%v", got, err)
	}
	for _, dims := range [][4]uint64{{math.MaxUint64, 1, 0, 0}, {0, 0, math.MaxUint64, 0}, {0, 0, 0, math.MaxUint64}} {
		if _, err := ownedPointOutputBound(dims[0], dims[1], dims[2], dims[3]); err == nil {
			t.Fatalf("overflow admitted: %v", dims)
		}
	}
}

func TestPreparedOwnedPointLongKeyRefusesBeforeWorkspaceAndReleasesSnapshot(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	limits := ownedPointTestLimits()
	limits.MaxKeyBytes = 3000
	ops := []batch.Entry{{Type: batch.OpPut, Key: bytes.Repeat([]byte{'k'}, 3000)}}
	before := snapshot.readState.Load()
	var charged uint64
	owner, err := database.CapturePreparedOwnedPointRoot(snapshot, 0, OrderedRootStoragePagerLeaves, ops, limits, func(n uint64) error { charged += n; return nil })
	if owner != nil || !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("owner=%v err=%v", owner, err)
	}
	if charged == 0 || snapshot.readState.Load() != before {
		t.Fatal("long-key refusal lost cumulative attempts or retained read pin")
	}
}

// Both fixtures are valid balanced pager trees. Their deliberate unary nodes
// exercise the actual B1 prune and single root-reduction paths, rather than
// merely calling the capture helper on an isolated header.
func TestPreparedOwnedPointUnaryClosureMatchesOrdinaryApply(t *testing.T) {
	for _, reduceRoot := range []bool{false, true} {
		name := "untouched-unary"
		if reduceRoot {
			name = "one-level-root-reduction"
		}
		t.Run(name, func(t *testing.T) {
			database := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
			defer database.Close()
			snapshot := database.AcquireSnapshot()
			defer snapshot.Close()
			build := func(typ page.PageType, keys []string, children []uint64) uint64 {
				id, err := snapshot.idx.allocator.Alloc(0)
				if err != nil {
					t.Fatal(err)
				}
				data, err := snapshot.idx.pager.GetForWrite(id)
				if err != nil {
					t.Fatal(err)
				}
				b := node.NewBuilder(data, typ)
				b.SetPageID(id)
				defer b.ReleaseScratch()
				for i, key := range keys {
					if typ == page.PageTypeLeaf {
						err = b.AddLeafEntry([]byte(key), []byte("old/"+key), node.FlagInline, page.ValuePtr{})
					} else {
						err = b.AddInternalChildRef([]byte(key), page.PageChildRef(children[i]))
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				b.FinishNoNode()
				return id
			}
			a := build(page.PageTypeLeaf, []string{"a"}, nil)
			z := build(page.PageTypeLeaf, []string{"z"}, nil)
			var root, untouched uint64
			ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("z")}}
			delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
			defer delta.Close()
			if err := delta.Delete([]byte("z")); err != nil {
				t.Fatal(err)
			}
			if reduceRoot {
				inner := build(page.PageTypeInternal, []string{"a", "z"}, []uint64{a, z})
				root = build(page.PageTypeInternal, []string{"a"}, []uint64{inner})
			} else {
				untouched = build(page.PageTypeInternal, []string{"a"}, []uint64{a})
				m := build(page.PageTypeLeaf, []string{"m"}, nil)
				right := build(page.PageTypeInternal, []string{"m", "z"}, []uint64{m, z})
				root = build(page.PageTypeInternal, []string{"a", "m"}, []uint64{untouched, right})
				ops = []batch.Entry{{Type: batch.OpPut, Key: []byte("m"), Value: []byte("new/m")}, {Type: batch.OpDelete, Key: []byte("z")}}
				if err := delta.Set([]byte("m"), []byte("new/m")); err != nil {
					t.Fatal(err)
				}
			}
			rootView, err := snapshot.idx.pager.Get(root)
			if err != nil || cap(rootView) <= page.PageSize {
				t.Fatalf("fixture did not exercise larger mmap backing: len=%d cap=%d err=%v", len(rootView), cap(rootView), err)
			}
			before := snapshot.readState.Load()
			owner, err := database.CapturePreparedOwnedPointRoot(snapshot, root, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), func(uint64) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			if untouched != 0 {
				found := false
				for _, e := range owner.closure {
					if e.ref == page.PageChildRef(untouched) && !e.touched {
						found = true
					}
				}
				if !found {
					t.Fatal("untouched unary header was omitted from closure")
				}
			}
			prepared, err := owner.Prepare(ops)
			if err != nil || !prepared.Maintenance {
				t.Fatalf("prepare=%+v err=%v", prepared, err)
			}
			ordinary := snapshot.idx.zipper.CloneWithAllocator(snapshot.idx.allocator)
			ordinaryRoot, _, _, err := ordinary.Apply(root, delta)
			if err != nil {
				t.Fatal(err)
			}
			ownedRoot, _, _, err := owner.zipper.Apply(root, delta)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a", "m", "z"} {
				want, wantErr := snapshot.GetAtRoot(ordinaryRoot, []byte(key))
				got, gotErr := snapshot.GetAtRoot(ownedRoot, []byte(key))
				if (wantErr == nil) != (gotErr == nil) || !bytes.Equal(got, want) {
					t.Fatalf("key=%s owned=%q/%v ordinary=%q/%v", key, got, gotErr, want, wantErr)
				}
			}
			data, err := snapshot.idx.pager.Get(ownedRoot)
			if err != nil {
				t.Fatal(err)
			}
			n := node.NewNodeView(data)
			if reduceRoot {
				if n.Type() != page.PageTypeInternal || n.Count() != 1 {
					t.Fatalf("root recursively collapsed: type=%v count=%d", n.Type(), n.Count())
				}
			} else {
				ref, err := n.GetInternalChildRef(0)
				if err != nil || ref != page.PageChildRef(untouched) {
					t.Fatalf("untouched unary changed: ref=%+v err=%v", ref, err)
				}
			}
			owner.Close()
			if snapshot.readState.Load() != before {
				t.Fatal("terminal owner retained snapshot pin")
			}
		})
	}
}

func TestPreparedOwnedPointCaptureDiagnosticFirstCause(t *testing.T) {
	database := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
	defer database.Close()
	snapshot := database.AcquireSnapshot()
	defer snapshot.Close()
	pager := snapshot.idx.pager
	leaf := func(keys ...string) uint64 {
		id, err := snapshot.idx.allocator.Alloc(0)
		if err != nil {
			t.Fatal(err)
		}
		data, err := pager.GetForWrite(id)
		if err != nil {
			t.Fatal(err)
		}
		builder := node.NewBuilder(data, page.PageTypeLeaf)
		builder.SetPageID(id)
		for _, key := range keys {
			if err := builder.AddLeafEntry([]byte(key), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		builder.FinishNoNode()
		builder.ReleaseScratch()
		return id
	}
	left, right := leaf("a", "b"), leaf("z", "zz")
	root, err := snapshot.idx.allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	data, err := pager.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	builder := node.NewBuilder(data, page.PageTypeInternal)
	builder.SetPageID(root)
	if err := builder.AddInternalChildRef(nil, page.PageChildRef(left)); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddInternalChildRef([]byte("z"), page.PageChildRef(right)); err != nil {
		t.Fatal(err)
	}
	builder.FinishNoNode()
	builder.ReleaseScratch()
	before := snapshot.readState.Load()
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("z")}}
	limits := ownedPointTestLimits()
	limits.MaxClosureEntries = 3
	diagnostic := PreparedOwnedPointCaptureDiagnostic{Observed: 999}
	owner, err := database.CapturePreparedOwnedPointRootWithDiagnostic(snapshot, root, OrderedRootStoragePagerLeaves, ops, limits, func(uint64) error { return nil }, &diagnostic)
	if owner != nil || !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("owner=%v err=%v", owner, err)
	}
	if diagnostic.Phase != PreparedOwnedCaptureInspect || diagnostic.Reason != PreparedOwnedCaptureEntryBudget || diagnostic.RefKind != page.ChildRefPage || diagnostic.PageID != right || diagnostic.Depth != 2 || !diagnostic.Touched || diagnostic.Observed != 4 || diagnostic.Limit != 3 || diagnostic.NodeType != page.PageTypeLeaf || diagnostic.NodeEntries != 2 || diagnostic.PriorClosureEntries != 2 {
		t.Fatalf("first touched-leaf full-entry refusal=%+v", diagnostic)
	}
	if snapshot.readState.Load() != before {
		t.Fatal("diagnostic refusal retained snapshot")
	}
	// Reuse the same caller-owned result. The new first cause replaces the old
	// location; unwinding the recursive capture never overwrites a child cause.
	invalid := []batch.Entry{{Type: batch.OpDelete, Key: nil}}
	owner, err = database.CapturePreparedOwnedPointRootWithDiagnostic(snapshot, root, OrderedRootStoragePagerLeaves, invalid, limits, func(uint64) error { return nil }, &diagnostic)
	if owner != nil || !errors.Is(err, ErrPreparedRootPointProfileLimit) || diagnostic.Phase != PreparedOwnedCaptureInput || diagnostic.Reason != PreparedOwnedCaptureCanonicalOperation || diagnostic.PageID != 0 {
		t.Fatalf("reset diagnostic=%+v owner=%v err=%v", diagnostic, owner, err)
	}
	owner, err = database.CapturePreparedOwnedPointRootWithDiagnostic(snapshot, root, OrderedRootStoragePagerLeaves, ops, ownedPointTestLimits(), func(uint64) error { return nil }, &diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostic != (PreparedOwnedPointCaptureDiagnostic{}) {
		t.Fatalf("successful capture diagnostic=%+v", diagnostic)
	}
	ledger := owner.Ledger()
	if ledger.ClosureEntries != 4 || ledger.HeaderOnlyNodes != 1 || ledger.ClosurePages != 3 {
		t.Fatalf("full-entry/header ledger=%+v", ledger)
	}
	typ, count, fromPager, err := owner.Workspace().ProbePruneHeader(page.PageChildRef(left))
	if err != nil || typ != page.PageTypeLeaf || count != 2 || !fromPager {
		t.Fatalf("untouched sibling header=%v/%d/%v/%v", typ, count, fromPager, err)
	}
	if err := owner.ValidateCapturedBaseline(owner.token); err != nil {
		t.Fatal(err)
	}
	beforePages := pager.PageCount()
	beforeToken, _ := snapshot.StateToken()
	// A precise full-entry budget now admits the same actual two-child root
	// without granting the untouched sibling's keys or full-load authority.
	tight := ownedPointTestLimits()
	tight.MaxClosureEntries = 4
	narrow, err := database.CapturePreparedOwnedPointRoot(snapshot, root, OrderedRootStoragePagerLeaves, ops, tight, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := narrow.Prepare(ops); err != nil {
		t.Fatal(err)
	}
	narrow.Close()
	afterToken, _ := snapshot.StateToken()
	if pager.PageCount() != beforePages || afterToken != beforeToken {
		t.Fatal("capture/Prepare changed pre-WAL state")
	}
	diagnostic.Reason = PreparedOwnedCaptureInvalidInput
	if _, err := owner.Prepare(ops); err != nil {
		t.Fatalf("caller diagnostic mutation changed sealed owner: %v", err)
	}
	owner.Close()
	if snapshot.readState.Load() != before {
		t.Fatal("successful diagnostic owner retained snapshot after Close")
	}
}
