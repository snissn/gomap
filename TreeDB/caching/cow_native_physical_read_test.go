package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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
	if _, err = basis.nativeEntryAtRoot(db.cow, nil, token.RootPageID, []byte("a"), true); !errors.Is(err, memtable.ErrCOWCapacity) || basis.reader != nil {
		t.Fatalf("admission refusal: %v", err)
	}
	pressure.Close()
	check := func() {
		t.Helper()
		entry, err := basis.nativeEntryAtRoot(db.cow, nil, token.RootPageID, []byte("a"), true)
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

// Refuse the real provider's snapshot stage after it has admitted a partial
// definition owner. The caller must retire that owner even on preparation error.
type cowNativeDefinitionRefusal struct{ cowDictionaryReadProvider }

var errCOWNativeDefinitionRefused = errors.New("native definition admission refused")

func (p cowNativeDefinitionRefusal) PrepareDictionaryReadDefinition(id uint64, limits valuelog.COWReadLimits, resources int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
	stage := 0
	return p.cowDictionaryReadProvider.PrepareDictionaryReadDefinition(id, limits, resources, func(s dictdb.DictionaryReadAllocationSizes) (func(), error) {
		stage++
		if stage == 2 {
			return nil, errCOWNativeDefinitionRefused
		}
		return admit(s)
	})
}

// The capture and the original conditional transaction must share one real
// dictionary-free ZSTD decoder. Retained output still needs its own original
// receipt, and refusal must leave both opening scopes available for retry.
func TestCOWNativeValueWorkspaceAdmissionAndLifetime(t *testing.T) {
	t.Run("dictionary-free", func(t *testing.T) { testCOWNativeValueWorkspaceAdmissionAndLifetime(t, false, false) })
	t.Run("trained-dictionary", func(t *testing.T) { testCOWNativeValueWorkspaceAdmissionAndLifetime(t, true, false) })
}

func TestCOWNativeValueDictionaryColdAfterClose(t *testing.T) {
	testCOWNativeValueWorkspaceAdmissionAndLifetime(t, true, true)
}

func testCOWNativeValueWorkspaceAdmissionAndLifetime(t *testing.T, trained, coldClose bool) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	compression := uint8(vlogCompressionBlock)
	if trained {
		compression = uint8(vlogCompressionDict)
	}
	db, err := Open(dir, backend, Options{
		TransactionOwner: owner, DisableWAL: true, AllowUnsafe: true,
		MemtableMode: "cow_btree", FlushThreshold: 1 << 30,
		ValueLogPointerThreshold: 1, ValueLogCompression: compression, ValueLogBlockCodec: 2,
		ValueLogAutoPolicy: uint8(vlogAutoSize), ValueLogDictIncompressibleHoldBytes: -1,
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
	var dictID uint64
	expectedCodec := valuelog.BlockCodecZSTD
	if trained {
		samples := make([][]byte, 8)
		for i := range samples {
			samples[i] = bytes.Repeat([]byte(fmt.Sprintf("native-original-pointer-%d|", i)), 256)
		}
		dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{ID: 73, Contents: samples, History: append([]byte(nil), samples[0]...), Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
		if err != nil {
			t.Fatal(err)
		}
		store, err := dictdb.Open(dir+"/dictdb", backenddb.Options{ChunkSize: 64 << 10})
		if err != nil {
			t.Fatal(err)
		}
		dictID, err = store.PutDictBytes(context.Background(), dictionary)
		if err == nil {
			err = store.SetCurrent(context.Background(), dictID)
		}
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		db.SetDictStore(store)
		expectedCodec = valuelog.BlockCodecNone
	}

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
	f, _, err := a.Snapshot().PinnedValueLogFile(ptr.FileID)
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWRecord(f, ptr, db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID != dictID {
		t.Fatalf("actual produced compression shape: %+v %v", shape, err)
	}
	entry = node.LeafEntry{}
	if coldClose {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		before := budget.Stats()
		got, err := a.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, nil)
		if got != nil || !errors.Is(err, memtable.ErrCOWClosed) {
			t.Fatalf("cold closed-owner read must refuse fresh admission: len=%d err=%v", len(got), err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatal("cold refusal leaked reservation", before, after)
		}
		if err := tx.Close(); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for budget.Stats().TotalBytes != 0 {
			if time.Now().After(deadline) {
				t.Fatal("cold-close ownership did not drain", budget.Stats())
			}
			time.Sleep(time.Millisecond)
		}
		return
	}
	if trained {
		// Warm only independently retained workspace/slot metadata so a partial
		// definition refusal must leave the original reservation exactly unchanged.
		db.cow.physicalMu.Lock()
		shared, err := db.cow.physicalWorkspaceLocked(db.cow.readLimits())
		if err == nil {
			_, err = shared.addCodecSlot()
		}
		db.cow.physicalMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		before := budget.Stats()
		capture := a.(*NativeSemanticCapture)
		provider := capture.dictionary
		capture.dictionary = cowNativeDefinitionRefusal{provider}
		got, readErr := capture.NativeValueAtPointer(ptr, nil)
		capture.dictionary = provider
		after := budget.Stats()
		if got != nil || !errors.Is(readErr, errCOWNativeDefinitionRefused) {
			t.Fatalf("partial definition refusal: %d %v", len(got), readErr)
		}
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatal("partial definition owner leaked", before, after)
		}
	}
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
	workspace := db.cow.physicalReader
	if workspace == nil || workspace.snapshot != nil || workspace.basis != nil || len(workspace.codecs) != 1 || workspace.codecs[0] == nil || workspace.codecs[0].codec != expectedCodec || workspace.codecs[0].id != dictID {
		t.Fatal("actual pointer did not activate the expected shared decoder")
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
	if db.cow.physicalReader != workspace || workspace.codecs[0] != codec {
		t.Fatal("distinct opening scopes replaced shared decoder backing")
	}
	currentWant := bytes.Repeat([]byte("native-current-pointer"), 256)
	if err := db.Set([]byte("p"), currentWant); err != nil {
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
	current, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	currentToken, ok := current.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing current physical token")
	}
	currentEntry, err := current.Snapshot().GetEntryAtRoot(currentToken.RootPageID, []byte("p"))
	if err != nil || currentEntry.Flags&node.FlagPointer == 0 {
		t.Fatalf("current persistent pointer: %+v %v", currentEntry, err)
	}
	currentPtr := currentEntry.ValuePtr
	currentEntry = node.LeafEntry{}
	checkCurrent := func() {
		t.Helper()
		got, err := current.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(currentPtr, nil)
		if err != nil || !bytes.Equal(got, currentWant) || len(got) != cap(got) {
			t.Fatalf("current original pointer: length=%d error=%v", len(got), err)
		}
		got[0] ^= 1
	}
	for i := 0; i < 8; i++ {
		checkCurrent()
		read(b.(backenddb.NativePhysicalValueReader))
	}
	if db.cow.physicalReader != workspace || workspace.codecs[0] != codec || codec.lease == nil || workspace.snapshot != nil || workspace.basis != nil {
		t.Fatal("physical handoff duplicated scratch or retained caller authority")
	}
	if basis.reader != nil && len(basis.reader.codecs) != 0 {
		t.Fatal("old physical basis retained dictionary-free decoder")
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed capture value read: %v", err)
	}
	if _, err := tx.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, nil); !errors.Is(err, backenddb.ErrConditionalTxnClosed) {
		t.Fatalf("closed transaction value read: %v", err)
	}
	// DB.Close withdraws producers, not the original held cut/file authority.
	// Both warmed physical bases must retain the SAME admitted decoder until
	// the last capture ends, without attempting fresh admission after Close.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	checkCurrent()
	read(b.(backenddb.NativePhysicalValueReader))
	if db.cow.physicalReader != workspace || workspace.codecs[0] != codec {
		t.Fatal("DB.Close withdrew held decoder backing")
	}
	basis, workspace, codec = nil, nil, nil
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	checkCurrent()
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for budget.Stats().TotalBytes != 0 {
		if time.Now().After(deadline) {
			t.Fatal("original cleanup-only worker did not refund last cache edge", budget.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last original cache edge retained workspace admission")
	}
	receipt = nil
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.Views != 0 {
		t.Fatal("final original ownership", stats)
	}
}

// Real captures and transactions reuse only original physical scratch. The
// returned page belongs to caller dst, including after a different retained
// basis is installed and the producer closes.
func TestCOWNativeLeafWorkspaceAdmissionAndLifetime(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: -1})
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
		IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
		ValueLogCompression: uint8(vlogCompressionBlock), ValueLogBlockCodec: 0,
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
	want := bytes.Repeat([]byte("captured-inline-leaf"), 8)
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
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
	defer a.Close()
	b, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	tx, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	basis, budget := a.(*NativeSemanticCapture).cut.basis, db.cow.budget
	if b.(*NativeSemanticCapture).cut.basis != basis || tx.(*nativeConditionalTxn).snap.cowCut.basis != basis || a.Snapshot() != basis.snapshot {
		t.Fatal("scopes do not share the exact original basis")
	}
	capturePtr := func(snapshot *backenddb.Snapshot) page.LeafLogPtr {
		t.Helper()
		var keyScratch, leafScratch [page.PageSize]byte
		var ptr page.LeafLogPtr
		stop := errors.New("capture actual leaf")
		_, err := snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) { ptr = p; return nil, stop })
		if !errors.Is(err, stop) || ptr.FileID == 0 {
			t.Fatalf("real leaf: %+v %v", ptr, err)
		}
		return ptr
	}
	ptr := capturePtr(a.Snapshot())
	f, _, err := a.Snapshot().PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	limit := budget.Limits().MaxInFlightBytes
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil || !shape.Compressed || shape.Codec != valuelog.BlockCodecSnappy {
		t.Fatalf("actual compressed leaf: %+v %v", shape, err)
	}
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(limit - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	f = nil // no inspected handle alias survives its owning capture
	var dst [page.PageSize]byte
	if _, err := a.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, dst[:]); !errors.Is(err, memtable.ErrCOWCapacity) || basis.reader != nil {
		t.Fatalf("original workspace admission refusal: reader=%p %v", basis.reader, err)
	}
	pressure.Close()
	check := func(snapshot *backenddb.Snapshot, reader backenddb.NativePhysicalLeafReader, expected []byte) {
		t.Helper()
		var keyScratch, leafScratch [page.PageSize]byte
		entry, err := snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], reader.NativeLeafPage)
		if err != nil || !bytes.Equal(entry.Value, expected) {
			t.Fatalf("captured leaf point: %q %v", entry.Value, err)
		}
		entry.Value[0] ^= 1
	}
	check(a.Snapshot(), a.(backenddb.NativePhysicalLeafReader), want)
	check(tx.Snapshot(), tx.(backenddb.NativePhysicalLeafReader), want)
	workspace := db.cow.physicalReader
	if workspace == nil || len(workspace.codecs) != 1 || workspace.codecs[0].codec != valuelog.BlockCodecSnappy || workspace.snapshot != nil || workspace.basis != nil {
		t.Fatal("no original shared block decoder")
	}
	codec, receipt := workspace.codecs[0], workspace.lease
	pageA, err := a.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, dst[:])
	if err != nil || len(pageA) != page.PageSize || &pageA[0] != &dst[0] {
		t.Fatalf("caller page backing: %v", err)
	}
	var other [page.PageSize]byte
	pageB, err := b.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, other[:])
	if err != nil || !bytes.Equal(pageA, pageB) {
		t.Fatalf("distinct page dst: %v", err)
	}
	pageB[0] ^= 1
	if pageA[0] == pageB[0] {
		t.Fatal("leaf page aliases shared/caller backing")
	}
	pageA, pageB = nil, nil
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 8; i++ {
			check(tx.Snapshot(), tx.(backenddb.NativePhysicalLeafReader), want)
		}
	}()
	for i := 0; i < 8; i++ {
		check(b.Snapshot(), b.(backenddb.NativePhysicalLeafReader), want)
	}
	wg.Wait()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("a000"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if db.cow.cut.basis == basis {
		t.Fatal("checkpoint kept original physical basis")
	}
	current, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	check(current.Snapshot(), current.(backenddb.NativePhysicalLeafReader), []byte("new"))
	check(b.Snapshot(), b.(backenddb.NativePhysicalLeafReader), want)
	check(tx.Snapshot(), tx.(backenddb.NativePhysicalLeafReader), want)
	if db.cow.physicalReader != workspace || workspace.codecs[0] != codec {
		t.Fatal("physical handoff replaced original decoder")
	}
	currentPtr := capturePtr(current.Snapshot())
	if _, err := current.(backenddb.NativePhysicalLeafReader).NativeLeafPage(currentPtr, nil); !errors.Is(err, valuelog.ErrCOWReadCapacity) {
		t.Fatalf("unadmitted page destination: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, dst[:]); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed capture: %v", err)
	}
	if _, err := tx.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, dst[:]); !errors.Is(err, backenddb.ErrConditionalTxnClosed) {
		t.Fatalf("closed transaction: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// DB.Close closes the index pager. As with the value-pointer control above,
	// exercise the actual retained leaf/file authority directly, using pointers
	// selected and validated by the captured trees before producer teardown.
	checkHeldLeaf := func(reader backenddb.NativePhysicalLeafReader, ptr page.LeafLogPtr, expected []byte) {
		t.Helper()
		var leaf [page.PageSize]byte
		data, err := reader.NativeLeafPage(ptr, leaf[:])
		if err != nil {
			t.Fatal("held leaf after producer close", err)
		}
		view := node.NewNodeView(data)
		ordinal, found, err := view.SearchLeaf([]byte("a000"))
		if err != nil || !found {
			t.Fatalf("held leaf key: found=%v %v", found, err)
		}
		entry, err := view.GetLeafEntry(ordinal)
		if err != nil || !bytes.Equal(entry.Key, []byte("a000")) || !bytes.Equal(entry.Value, expected) {
			t.Fatalf("held leaf value: %q %v", entry.Value, err)
		}
		entry.Value[0] ^= 1
	}
	checkHeldLeaf(b.(backenddb.NativePhysicalLeafReader), ptr, want)
	checkHeldLeaf(current.(backenddb.NativePhysicalLeafReader), currentPtr, []byte("new"))
	basis, workspace, codec = nil, nil, nil
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	// As in the value-reader control above, the original cleanup-only worker
	// owns the final closed-cache refund after the last retained cut leaves.
	deadline := time.Now().Add(5 * time.Second)
	for budget.Stats().TotalBytes != 0 {
		if time.Now().After(deadline) {
			t.Fatal("original cleanup-only worker did not refund last cache edge", budget.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last held cut did not refund shared decoder workspace")
	}
	receipt = nil
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 {
		t.Fatal("final original ownership", stats)
	}
}

// This fixture exposes the real captured NativePhysicalReader capability, rather
// than bypassing the capture's root and lifetime contract with a private call.
func cowNativePointSlotFixture(t *testing.T) (*DB, *backenddb.DB, *NativeSemanticCapture, uint64) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{TransactionOwner: owner, DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", FlushThreshold: 1 << 30, ValueLogPointerThreshold: 64, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
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
	if err := db.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	capture := captured.(*NativeSemanticCapture)
	t.Cleanup(func() {
		if err := capture.Close(); err != nil {
			t.Error(err)
		}
	})
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing physical token")
	}
	return db, backend, capture, token.RootPageID
}

func TestCOWNativeEntryParallelBasisSlotsProgress(t *testing.T) {
	for free := 0; free < 3; free++ {
		t.Run(fmt.Sprintf("only-slot-%d", free), func(t *testing.T) {
			_, _, capture, root := cowNativePointSlotFixture(t)
			basis := capture.cut.basis
			basis.readMu.Lock()
			for i := range basis.readSlots {
				if i != free {
					basis.readSlots[i].mu.Lock()
				}
			}
			type result struct {
				entry node.LeafEntry
				err   error
			}
			done := make(chan result, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				e, err := backenddb.NativePhysicalReader(capture).NativeEntryAtRoot(root, []byte("a"), true)
				done <- result{e, err}
			}()
			<-started
			var got result
			progressed := false
			select {
			case got = <-done:
				progressed = true
			case <-time.After(2 * time.Second):
			}
			// Always release and join before failing, so RED never strands a goroutine.
			basis.readMu.Unlock()
			for i := range basis.readSlots {
				if i != free {
					basis.readSlots[i].mu.Unlock()
				}
			}
			if !progressed {
				got = <-done
			}
			if got.err != nil || string(got.entry.Value) != "old" || got.entry.Key != nil || cap(got.entry.Value) != len(got.entry.Value) {
				t.Fatalf("exact-root output: %+v %v", got.entry, got.err)
			}
			if !progressed {
				t.Fatal("public native exact-root lookup waited for held primary despite a free basis slot")
			}
			if basis.readSlots[free].reader == nil || basis.readSlots[free].reader.point == nil {
				t.Fatal("public lookup did not activate the sole available slot")
			}
		})
	}
}

// Saturation keeps the legacy primary wait. Releasing primary, while every
// extra owner remains busy, must be sufficient to finish the public lookup.
func TestCOWNativeEntrySaturatedPrimaryFallback(t *testing.T) {
	_, _, capture, root := cowNativePointSlotFixture(t)
	basis := capture.cut.basis
	basis.readMu.Lock()
	for i := range basis.readSlots {
		basis.readSlots[i].mu.Lock()
	}
	done := make(chan error, 1)
	go func() {
		e, err := capture.NativeEntryAtRoot(root, []byte("a"), true)
		if err == nil && string(e.Value) != "old" {
			err = fmt.Errorf("wrong exact-root value: %q", e.Value)
		}
		done <- err
	}()
	var early error
	completed := false
	select {
	case early = <-done:
		completed = true
	case <-time.After(50 * time.Millisecond):
	}
	basis.readMu.Unlock()
	if !completed {
		select {
		case early = <-done:
		case <-time.After(2 * time.Second):
			// Always release all locks and join, including a broken slot fallback.
			for i := range basis.readSlots {
				basis.readSlots[i].mu.Unlock()
			}
			<-done
			t.Fatal("primary release did not unblock saturated lookup")
		}
	}
	for i := range basis.readSlots {
		basis.readSlots[i].mu.Unlock()
	}
	if completed || early != nil {
		t.Fatalf("saturated fallback: early=%t err=%v", completed, early)
	}
	if basis.reader == nil || basis.reader.point == nil {
		t.Fatal("saturated fallback did not use original primary")
	}
	for i := range basis.readSlots {
		if basis.readSlots[i].reader != nil {
			t.Fatal("saturated fallback admitted a busy extra owner")
		}
	}
}

// Refuse a cold slot's workspace or its second point-buffer admission using the
// real original budget. A warm primary must remain usable after that refusal.
func TestCOWNativeEntrySlotAdmissionFallbackAndRefund(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial-point=%t", partial), func(t *testing.T) {
			db, _, capture, root := cowNativePointSlotFixture(t)
			if _, err := capture.NativeEntryAtRoot(root, []byte("a"), true); err != nil {
				t.Fatal(err)
			}
			basis, budget := capture.cut.basis, db.cow.budget
			primary, primaryPoint := basis.reader, basis.reader.point
			before := budget.Stats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			workspaceBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge()
			var room uint64
			if partial {
				room = workspaceBytes + overhead
			}
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead - room)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			pressured := budget.Stats()
			// Verify the pressure's exact discrimination without installing a fake owner.
			probe, err := admitCOWReadWorkspace(budget, db.cow.readLimits())
			if partial {
				if err != nil {
					t.Fatal("pressure failed to allow partial workspace", err)
				}
				if _, err := probe.pointScratch(); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatal("pressure failed to refuse point buffer", err)
				}
				probe.close()
				probe = nil
			} else if !errors.Is(err, memtable.ErrCOWCapacity) {
				if probe != nil {
					probe.close()
				}
				t.Fatal("pressure failed to refuse workspace", err)
			}
			basis.readMu.Lock()
			done := make(chan error, 1)
			go func() {
				e, err := capture.NativeEntryAtRoot(root, []byte("a"), true)
				if err == nil && string(e.Value) != "old" {
					err = fmt.Errorf("fallback output: %q", e.Value)
				}
				done <- err
			}()
			// Admission refusal must release the selected extra mutex before waiting.
			time.Sleep(50 * time.Millisecond)
			slot := &basis.readSlots[0]
			available := slot.mu.TryLock()
			refunded := available && slot.reader == nil
			if available {
				slot.mu.Unlock()
			}
			basis.readMu.Unlock()
			err = <-done
			after := budget.Stats()
			if err != nil || !refunded || basis.reader != primary || primary.point != primaryPoint || after.ReservedBytes != pressured.ReservedBytes || after.ExternalLeases != pressured.ExternalLeases {
				t.Fatalf("pre-traversal fallback/refund: err=%v refunded=%t before=%+v after=%+v", err, refunded, pressured, after)
			}
			pressure.Close()
			// Releasing pressure allows the same real slot to admit and retain scratch.
			basis.readMu.Lock()
			e, readErr := capture.NativeEntryAtRoot(root, []byte("a"), true)
			basis.readMu.Unlock()
			if readErr != nil || string(e.Value) != "old" || slot.reader == nil || slot.reader.point == nil {
				t.Fatalf("slot retry after pressure: %+v %v", e, readErr)
			}
		})
	}
}

func cowNativeEntryUsingExtra(t *testing.T, basis *cowBackendBasis, scope backenddb.NativePhysicalReader, root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	t.Helper()
	basis.readMu.Lock()
	type result struct {
		entry node.LeafEntry
		err   error
	}
	done := make(chan result, 1)
	go func() { e, err := scope.NativeEntryAtRoot(root, key, copyValue); done <- result{e, err} }()
	var got result
	progressed := false
	select {
	case got = <-done:
		progressed = true
	case <-time.After(2 * time.Second):
	}
	basis.readMu.Unlock()
	if !progressed {
		got = <-done
		t.Fatal("native extra-slot read waited for primary")
	}
	return got.entry, got.err
}

func TestCOWNativeEntrySlotMetadataAndLifetime(t *testing.T) {
	db, backend, old, oldRoot := cowNativePointSlotFixture(t)
	keys := [][]byte{{}, {0}, {'a', 0, 'b'}, {255, 0}, []byte("pointer")}
	values := [][]byte{{}, []byte("zero"), []byte("binary-inline"), []byte("high"), bytes.Repeat([]byte("pointer"), 256)}
	batch := db.NewBatch()
	for i, key := range keys {
		if err := batch.SetWithRevision(key, values[i], page.EntryRevision(30+i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	capture := captured.(*NativeSemanticCapture)
	defer capture.Close()
	tx, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing current root")
	}
	basis, budget := capture.cut.basis, db.cow.budget
	if basis == old.cut.basis || tx.(*nativeConditionalTxn).snap.cowCut.basis != basis {
		t.Fatal("scopes lost exact retained basis")
	}
	var owned [][]byte
	for _, scope := range []backenddb.NativePhysicalReader{capture, tx.(backenddb.NativePhysicalReader)} {
		for i, key := range keys {
			input := append([]byte(nil), key...)
			metadata, err := cowNativeEntryUsingExtra(t, basis, scope, token.RootPageID, input, false)
			if err != nil || metadata.Key != nil || metadata.Value != nil || metadata.Revision != page.EntryRevision(30+i) {
				t.Fatalf("metadata key=%x: %+v %v", key, metadata, err)
			}
			entry, err := cowNativeEntryUsingExtra(t, basis, scope, token.RootPageID, input, true)
			if err != nil || entry.Key != nil || entry.Revision != metadata.Revision || entry.ValuePtr != metadata.ValuePtr || entry.Flags != metadata.Flags || !bytes.Equal(input, key) {
				t.Fatalf("owned metadata key=%x: %+v %v", key, entry, err)
			}
			if i == len(keys)-1 {
				if entry.Flags&node.FlagPointer == 0 || entry.Value != nil || entry.ValuePtr.FileID == 0 {
					t.Fatal("pointer retained page bytes", entry)
				}
			} else {
				if !bytes.Equal(entry.Value, values[i]) || len(entry.Value) != cap(entry.Value) || entry.Flags&node.FlagPointer != 0 {
					t.Fatalf("inline copy key=%x: %+v", key, entry)
				}
				owned = append(owned, entry.Value)
				if len(entry.Value) > 0 {
					entry.Value[0] ^= 1
				}
			}
		}
	}
	if _, err := cowNativeEntryUsingExtra(t, basis, capture, token.RootPageID, []byte("missing"), true); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal("exact lookup substituted successor", err)
	}
	oldEntry, err := cowNativeEntryUsingExtra(t, old.cut.basis, old, oldRoot, []byte("a"), true)
	if err != nil || string(oldEntry.Value) != "old" {
		t.Fatal("old retained root changed", oldEntry, err)
	}
	if _, err := cowNativeEntryUsingExtra(t, old.cut.basis, old, oldRoot, keys[2], true); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal("old root saw later publication", err)
	}
	// Close a distinct capture while its transaction continues to use the exact
	// shared basis. Each returned byte slice remains independent of later reuse.
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		e, err := cowNativeEntryUsingExtra(t, basis, tx.(backenddb.NativePhysicalReader), token.RootPageID, keys[2], true)
		if err != nil || !bytes.Equal(e.Value, values[2]) {
			t.Fatal("transaction lost closed peer's basis", e, err)
		}
	}
	if len(owned) < 2 || string(owned[1]) == string(values[1]) {
		t.Fatal("retained owned result was overwritten by reuse")
	}
	receipt := basis.readSlots[0].reader.lease
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if db.cow.cut.basis == basis {
		t.Fatal("checkpoint did not replace physical basis")
	}
	e, err := cowNativeEntryUsingExtra(t, basis, tx.(backenddb.NativePhysicalReader), token.RootPageID, keys[2], true)
	if err != nil || !bytes.Equal(e.Value, values[2]) {
		t.Fatal("checkpoint rebound retained root", e, err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if basis.readSlots[0].reader != nil || !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last retained basis did not drain extra slot")
	}
	basis, receipt = nil, nil
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for budget.Stats().TotalBytes != 0 {
		if time.Now().After(deadline) {
			t.Fatal("original budget did not drain", budget.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}

// A real published native ordered root preserves physical tombstones. It must
// never be replaced by the raw USER root or successor merging during selection.
func TestCOWNativeEntrySlotExactRootTombstone(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	group, err := backend.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	delta := batch.New(nil, 4096)
	defer delta.Close()
	if err := delta.SetWithRevision([]byte("a"), []byte("native"), 37); err != nil {
		t.Fatal(err)
	}
	if err := delta.DeleteWithRevision([]byte("b"), 41); err != nil {
		t.Fatal(err)
	}
	root, err := group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{Delta: delta, IncludeDeletedOnColdBuild: true})
	if err != nil {
		t.Fatal(err)
	}
	meta := batch.New(nil, 4096)
	defer meta.Close()
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], root)
	if err := meta.Set([]byte("collections/root/native-slot"), encoded[:]); err != nil {
		t.Fatal(err)
	}
	if err := group.BuildNativeSystemDelta(meta); err != nil {
		t.Fatal(err)
	}
	final := backend.NewPhysicalBatch().(*backenddb.Batch)
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	if err := final.Write(); err != nil {
		t.Fatal(err)
	}
	if err := final.Close(); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{TransactionOwner: owner, DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", FlushThreshold: 1 << 30})
	if err != nil {
		_ = backend.Close()
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
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	capture := captured.(*NativeSemanticCapture)
	defer capture.Close()
	for _, copyValue := range []bool{false, true} {
		e, err := cowNativeEntryUsingExtra(t, capture.cut.basis, capture, root, []byte("b"), copyValue)
		if err != nil || e.Revision != 41 || e.Flags&node.FlagTombstone == 0 || e.Key != nil || e.Value != nil {
			t.Fatal("native tombstone metadata", e, err)
		}
	}
	e, err := cowNativeEntryUsingExtra(t, capture.cut.basis, capture, root, []byte("a"), true)
	if err != nil || e.Revision != 37 || string(e.Value) != "native" {
		t.Fatal("native ordered root", e, err)
	}
	if _, err := cowNativeEntryUsingExtra(t, capture.cut.basis, capture, root, []byte("aa"), true); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal("native exact lookup substituted successor", err)
	}
}

func TestCOWNativeEntryClosedBudgetWarmPrimaryFallback(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%t", warm), func(t *testing.T) {
			db, _, capture, root := cowNativePointSlotFixture(t)
			basis, budget := capture.cut.basis, db.cow.budget
			if warm {
				if _, err := capture.NativeEntryAtRoot(root, []byte("a"), true); err != nil {
					t.Fatal(err)
				}
			}
			primary := basis.reader
			budget.Close() // existing receipts remain valid; every fresh admission refuses
			before := budget.Stats()
			basis.readMu.Lock()
			type result struct {
				entry node.LeafEntry
				err   error
			}
			done := make(chan result, 1)
			go func() { e, err := capture.NativeEntryAtRoot(root, []byte("a"), true); done <- result{e, err} }()
			time.Sleep(50 * time.Millisecond)
			basis.readMu.Unlock()
			got := <-done
			if warm {
				if got.err != nil || string(got.entry.Value) != "old" {
					t.Fatal("closed-budget warm primary stranded", got.entry, got.err)
				}
			} else if !errors.Is(got.err, memtable.ErrCOWClosed) {
				t.Fatal("cold closed-budget admission", got.err)
			}
			after := budget.Stats()
			if basis.reader != primary || basis.readSlots[0].reader != nil || before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
				t.Fatal("closed-budget fallback changed existing ownership", before, after)
			}
		})
	}
}
