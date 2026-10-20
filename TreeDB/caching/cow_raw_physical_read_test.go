package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Real logged leaves exercise raw ZSTD compatibility as well as shared-basis
// lifetime. Values returned by point/successor reads must survive other cuts.
func TestCOWRawBasisWorkspaceLifetime(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", FlushThreshold: 1 << 30,
		IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
		ValueLogCompression: uint8(vlogCompressionBlock), ValueLogBlockCodec: 2,
	})
	if err != nil {
		if closeErr := backend.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	defer func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	want := bytes.Repeat([]byte("old-inline-value"), 8)
	pointer := bytes.Repeat([]byte("old-pointer-value"), 256)
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set([]byte("p"), pointer); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	a, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	}()
	b, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	}()
	basis, budget := a.cowCut.basis, db.cow.budget
	if b.cowCut.basis != basis {
		t.Fatal("captures do not share exact basis")
	}
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Get([]byte("a000")); !errors.Is(err, memtable.ErrCOWCapacity) || basis.reader != nil {
		pressure.Close()
		t.Fatalf("physical admission refusal: %v", err)
	}
	if _, _, _, err := a.cowSeekGE([]byte("a000"), nil); !errors.Is(err, memtable.ErrCOWCapacity) || basis.reader != nil {
		pressure.Close()
		t.Fatalf("successor admission refusal: %v", err)
	}
	pressure.Close()
	if got, err := a.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("initial physical leaf: %d %v", len(got), err)
	}
	db.cow.physicalMu.Lock()
	compressedLeaf := false
	for _, codec := range db.cow.physicalReader.codecs {
		if codec != nil && codec.codec == valuelog.BlockCodecZSTD && codec.id == 0 {
			compressedLeaf = true
		}
	}
	db.cow.physicalMu.Unlock()
	if !compressedLeaf {
		t.Fatal("fixture did not read a real dictionary-free ZSTD leaf before pointer decoding")
	}
	check := func(s *Snapshot) {
		t.Helper()
		got, err := s.Get([]byte("a000"))
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("physical point: %q %v", got, err)
			return
		}
		got[0] ^= 1
		key, value, found, err := s.cowSeekGE([]byte("p"), nil)
		if err != nil || !found || string(key) != "p" || !bytes.Equal(value, pointer) {
			t.Errorf("pointer successor: %q/%d %t %v", key, len(value), found, err)
			return
		}
		key[0] = '!'
		value[0] ^= 1
	}
	check(a)
	check(b)
	// Both actual leaf and value records require ZSTD. A second generic decoder
	// envelope cannot fit the default producer tranche; both reads must share
	// the original cache codec without changing the caller's file resolver.
	entry, err := a.GetEntryExact([]byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	file, _, err := basis.snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWRecord(file, entry.ValuePtr, db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID != 0 || shape.Codec != valuelog.BlockCodecZSTD {
		t.Fatalf("real pointer record shape: %+v %v", shape, err)
	}
	file = nil
	for _, snapshot := range []*Snapshot{a, b} {
		if len(snapshot.cowReader.codecs) != 0 {
			t.Fatal("caller duplicated the cache-owned dictionary-free decoder")
		}
	}
	workspace, point, receipt := basis.reader, basis.reader.point, basis.reader.lease
	token, ok := basis.snapshot.StateToken()
	if !ok {
		t.Fatal("missing basis token")
	}
	if _, err := basis.nativeEntryAtRoot(db.cow, token.RootPageID, []byte("a000"), true); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
		t.Fatalf("raw initialization changed native codec policy: %v", err)
	}
	var wg sync.WaitGroup
	entered := make(chan struct{}, 4)
	resume := make(chan struct{})
	for i := 0; i < 4; i++ {
		capture, err := db.acquireCOWSnapshotWithError()
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(s *Snapshot) {
			defer wg.Done()
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			check(s)
			entered <- struct{}{}
			<-resume
			for j := 0; j < 8; j++ {
				check(s)
			}
		}(capture)
	}
	for i := 0; i < 4; i++ {
		<-entered
	}
	close(resume)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if basis.reader != workspace || basis.reader.point != point {
		t.Fatal("shared physical backing replaced between fresh cuts")
	}
	if b.cowReader == nil || b.cowReader.point != nil {
		t.Fatal("raw point/successor duplicated physical traversal scratch in caller cut")
	}
	// This pointer is not in the retained disk Snapshot. Reusing its decoder
	// must still resolve the file and visibility through the newer caller cut.
	pendingValue := bytes.Repeat([]byte("pending-pointer-value"), 128)
	if err := db.Set([]byte("q"), pendingValue); err != nil {
		t.Fatal(err)
	}
	pending, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	gotPending, readErr := pending.Get([]byte("q"))
	if readErr != nil || !bytes.Equal(gotPending, pendingValue) {
		t.Errorf("pending pointer original resolver: %d %v", len(gotPending), readErr)
	}
	if _, err := b.Get([]byte("q")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Errorf("old cut observed pending pointer: %v", err)
	}
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
	pending, gotPending = nil, nil
	if err := db.Set([]byte("a000"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	latest, err := db.Get([]byte("a000"))
	if err != nil || string(latest) != "new" {
		t.Fatalf("new cut winner: %q %v", latest, err)
	}
	check(b)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check(b)
	if db.cow.cut.basis == basis {
		t.Fatal("handoff did not replace physical basis")
	}
	// The successor's dictionary-free leaves and pointer must use the original
	// cache scratch too, while old basis files remain independently retained.
	shared := db.cow.physicalReader
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := current.Get([]byte("p")); err != nil || !bytes.Equal(got, pointer) {
		t.Fatalf("new physical basis pointer: %d %v", len(got), err)
	}
	if db.cow.physicalReader != shared || len(current.cowCut.basis.reader.codecs) != 0 || len(basis.reader.codecs) != 0 {
		t.Fatal("physical bases duplicated dictionary-free leaf/value decoding")
	}
	check(b)
	shared = nil
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	workspace, point, basis = nil, nil, nil
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("last old-basis edge did not refund shared workspace")
	}
	receipt = nil
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.ExternalLeases != 0 {
		t.Fatal("final original ownership", stats)
	}
}
