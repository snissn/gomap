package caching

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Actual compressed pointers cross input-capacity boundaries while their raw
// sizes decrease. Replacement must not need two decoder envelopes, and a
// refused replacement must leave the original cut readable after pressure ends.
func TestCOWReadCodecReplacementRefusalRetry(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", FlushThreshold: 1 << 30,
		ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionBlock), ValueLogBlockCodec: 2,
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
	block := make([]byte, 4096)
	if _, err := rand.New(rand.NewSource(1)).Read(block); err != nil {
		t.Fatal(err)
	}
	values := [][]byte{
		bytes.Repeat([]byte("a"), 64<<10),
		bytes.Repeat(block[:2048], 16),
		bytes.Repeat(block, 4),
	}
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for i := range keys {
		if err := db.Set(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	budget := db.cow.budget
	shapes := make([]valuelog.COWRecordShape, len(keys))
	for i := range keys {
		entry, err := s.GetEntryExact(keys[i])
		if err != nil {
			t.Fatal(err)
		}
		// Resolve each original pending pointer through this exact retained cut.
		s.cowReadMu.Lock()
		w, err := s.cowWorkspaceLocked()
		if err != nil {
			s.cowReadMu.Unlock()
			t.Fatal(err)
		}
		f, err := w.file(entry.ValuePtr)
		if err == nil {
			shapes[i], err = valuelog.InspectCOWRecord(f, entry.ValuePtr, db.cow.readLimits())
		}
		f = nil
		s.cowReadMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if !shapes[i].Compressed || shapes[i].DictID != 0 || shapes[i].Codec != valuelog.BlockCodecZSTD {
			t.Fatalf("record %d did not activate actual raw ZSTD: %+v", i, shapes[i])
		}
	}
	if cowReadCapacity(shapes[0].PayloadBytes()) >= cowReadCapacity(shapes[1].PayloadBytes()) ||
		cowReadCapacity(shapes[1].PayloadBytes()) >= cowReadCapacity(shapes[2].PayloadBytes()) || shapes[0].RawBytes <= shapes[1].RawBytes {
		t.Fatalf("records do not exercise cross-dimension replacement: %+v", shapes)
	}
	owned, err := s.Get(keys[0])
	if err != nil || !bytes.Equal(owned, values[0]) {
		t.Fatalf("first read: %d %v", len(owned), err)
	}
	if got, err := s.Get(keys[1]); err != nil || !bytes.Equal(got, values[1]) {
		t.Fatalf("replacement read: %d %v", len(got), err)
	}
	db.cow.physicalMu.Lock()
	if len(db.cow.physicalReader.codecs) != 1 {
		db.cow.physicalMu.Unlock()
		t.Fatal("expected one shared codec")
	}
	codec := db.cow.physicalReader.codecs[0]
	if codec == nil || uint64(cap(codec.raw)) < cowReadCapacity(shapes[0].RawBytes) || uint64(cap(codec.input)) < cowReadCapacity(shapes[1].PayloadBytes()) {
		db.cow.physicalMu.Unlock()
		t.Fatal("successful replacement shrank a previously admitted dimension")
	}
	receipt := codec.lease
	codec = nil
	db.cow.physicalMu.Unlock()
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	if got, err := s.Get(keys[2]); !errors.Is(err, memtable.ErrCOWCapacity) || got != nil {
		t.Fatalf("replacement under full original budget: %d %v", len(got), err)
	}
	db.cow.physicalMu.Lock()
	empty := len(db.cow.physicalReader.codecs) == 1 && db.cow.physicalReader.codecs[0] == nil
	db.cow.physicalMu.Unlock()
	if !empty || !errors.Is(receipt.Resize(1), memtable.ErrCOWClosed) {
		t.Fatal("refusal retained old scratch/receipt")
	}
	receipt = nil
	pressure.Close()
	if got, err := s.Get(keys[2]); err != nil || !bytes.Equal(got, values[2]) {
		t.Fatalf("retry: %d %v", len(got), err)
	}
	if !bytes.Equal(owned, values[0]) {
		t.Fatal("replacement/refusal/retry changed caller-owned output")
	}
	owned[0] ^= 1
	if got, err := s.Get(keys[0]); err != nil || !bytes.Equal(got, values[0]) {
		t.Fatalf("old record after retry: %d %v", len(got), err)
	}
	owned = nil
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("original final ownership: %+v", stats)
	}
}
