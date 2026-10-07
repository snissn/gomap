package db

import (
	"bytes"
	"errors"
	"math"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
