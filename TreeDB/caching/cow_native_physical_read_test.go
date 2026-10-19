package caching

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
)

// Distinct captures share one exact basis, not a retained current Collection
// snapshot. A later physical handoff cannot replace the old capture's root.
func TestCOWNativeBasisWorkspaceLifetime(t *testing.T) {
	db := cowPointerFixture(t)
	db.valueLogThreshold = 64
	if err := db.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	a, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	basis, budget := a.cowCut.basis, db.cow.budget
	if b.cowCut.basis != basis {
		t.Fatal("fresh captures do not share immutable basis")
	}
	token, ok := basis.snapshot.StateToken()
	if !ok {
		t.Fatal("missing basis")
	}
	// Real same-budget preallocation refusal: no reusable partial is installed.
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = basis.nativeEntryAtRoot(budget, token.RootPageID, []byte("a"), true); !errors.Is(err, memtable.ErrCOWCapacity) || basis.reader != nil {
		t.Fatalf("admission refusal: %v", err)
	}
	pressure.Close()
	check := func() {
		t.Helper()
		entry, err := basis.nativeEntryAtRoot(budget, token.RootPageID, []byte("a"), true)
		if err != nil || string(entry.Value) != "old" || cap(entry.Value) != len(entry.Value) {
			t.Errorf("old physical read: %q %v", entry.Value, err)
			return
		}
		entry.Value[0] = '!'
	}
	check()
	workspace, point, receipt := basis.reader, basis.reader.point, basis.reader.lease
	var wg sync.WaitGroup
	entered := make(chan struct{}, 8)
	resume := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			check()
			entered <- struct{}{}
			<-resume
			for j := 0; j < 16; j++ {
				check()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-entered
	}
	close(resume)
	// Close one capture while distinct retained captures continue shared reads.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if basis.reader != workspace || basis.reader.point != point {
		t.Fatal("fresh read reallocated physical workspace")
	}
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check()
	if db.cow.cut.basis == basis {
		t.Fatal("physical handoff reused old basis")
	}
	// Only b now retains old basis. Clear observer aliases before final release.
	workspace, point, basis = nil, nil, nil
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last old-basis owner did not refund workspace")
	}
	receipt = nil
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 {
		t.Fatal("final original ownership", stats)
	}
}

// The capture and the original conditional transaction must share one real
// dictionary-free ZSTD decoder. Retained output still needs its own original
// receipt, and refusal must leave both opening scopes available for retry.
func TestCOWNativeValueWorkspaceAdmissionAndLifetime(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		TransactionOwner: owner, DisableWAL: true, AllowUnsafe: true,
		MemtableMode: "cow_btree", FlushThreshold: 1 << 30,
		ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionBlock), ValueLogBlockCodec: 2,
	})
	if err != nil {
		if closeErr := backend.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("native-original-pointer"), 256)
	if err := db.Set([]byte("p"), want); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	a, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	}()
	b, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	}()
	tx, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	}()
	basis, budget := a.(*NativeSemanticCapture).cut.basis, db.cow.budget
	if b.(*NativeSemanticCapture).cut.basis != basis || tx.(*nativeConditionalTxn).snap.cowCut.basis != basis {
		t.Fatal("opening scopes do not retain the same immutable basis")
	}
	token, ok := a.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing physical token")
	}
	entry, err := a.Snapshot().GetEntryAtRoot(token.RootPageID, []byte("p"))
	if err != nil || entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("real persistent pointer: %+v %v", entry, err)
	}
	ptr := entry.ValuePtr
	entry = node.LeafEntry{}
	read := func(scope backenddb.NativePhysicalValueReader) {
		t.Helper()
		got, err := scope.NativeValueAtPointer(ptr, nil)
		if err != nil || !bytes.Equal(got, want) || len(got) != cap(got) {
			t.Errorf("captured owned value: length=%d error=%v", len(got), err)
			return
		}
		got[0] ^= 1
	}
	read(a.(backenddb.NativePhysicalValueReader))
	read(tx.(backenddb.NativePhysicalValueReader))
	workspace := basis.reader
	if workspace == nil || len(workspace.codecs) != 1 || workspace.codecs[0] == nil || workspace.codecs[0].codec != valuelog.BlockCodecZSTD || workspace.codecs[0].id != 0 {
		t.Fatal("actual pointer did not activate one dictionary-free ZSTD decoder")
	}
	codec, receipt := workspace.codecs[0], workspace.lease
	output, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	var admitted uint64
	admit := func(n uint64) error {
		admitted = n
		return output.Resize(n)
	}
	if got, err := a.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, admit); !errors.Is(err, memtable.ErrCOWCapacity) || got != nil || admitted != memtable.COWAllocationCharge(uint64(len(want))) {
		t.Fatalf("original output admission refusal: bytes=%d output=%d error=%v", admitted, len(got), err)
	}
	pressure.Close()
	got, err := tx.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, admit)
	if err != nil || !bytes.Equal(got, want) || len(got) != cap(got) {
		t.Fatalf("retained output retry: length=%d error=%v", len(got), err)
	}
	got[0] ^= 1
	got = nil
	output.Close()
	var wg sync.WaitGroup
	entered := make(chan struct{}, 2)
	for _, scope := range []backenddb.NativePhysicalValueReader{b.(backenddb.NativePhysicalValueReader), tx.(backenddb.NativePhysicalValueReader)} {
		wg.Add(1)
		go func(scope backenddb.NativePhysicalValueReader) {
			defer wg.Done()
			read(scope)
			entered <- struct{}{}
			for i := 0; i < 16; i++ {
				read(scope)
			}
		}(scope)
	}
	<-entered
	<-entered
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if basis.reader != workspace || workspace.codecs[0] != codec {
		t.Fatal("distinct opening scopes replaced shared decoder backing")
	}
	if err := db.Set([]byte("p"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if db.cow.cut.basis == basis {
		t.Fatal("checkpoint did not replace the physical basis")
	}
	read(b.(backenddb.NativePhysicalValueReader))
	read(tx.(backenddb.NativePhysicalValueReader))
	basis, workspace, codec = nil, nil, nil
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last original basis edge retained workspace admission")
	}
	receipt = nil
	if _, err := a.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed capture value read: %v", err)
	}
	if _, err := tx.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, nil); !errors.Is(err, backenddb.ErrConditionalTxnClosed) {
		t.Fatalf("closed transaction value read: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.Views != 0 {
		t.Fatal("final original ownership", stats)
	}
}
