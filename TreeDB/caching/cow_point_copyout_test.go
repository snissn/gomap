package caching

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// This is the observed pending-row length, not a pending-key special case.
// Each independent 64KiB block has a random prefix and compressible suffix,
// exercising the same 4MiB input / 16MiB raw Snappy envelope as the refusal.
func cowPointCopyoutValue(t *testing.T) []byte {
	t.Helper()
	value := make([]byte, 13000069)
	rng := rand.New(rand.NewSource(17))
	for at := 0; at < len(value); at += 64 << 10 {
		end := min(at+13<<10, len(value))
		if _, err := rng.Read(value[at:end]); err != nil {
			t.Fatal(err)
		}
	}
	return value
}

func cowPointCopyoutFixture(t *testing.T) (*DB, []byte, page.ValuePtr, valuelog.COWRecordShape) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30,
		ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionBlock), ValueLogBlockCodec: 0})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	value := cowPointCopyoutValue(t)
	batch := db.NewBatch()
	if err := batch.SetWithRevision([]byte("large"), value, 42); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entry, err := s.GetEntryExact([]byte("large"))
	if err != nil {
		t.Fatal(err)
	}
	s.cowReadMu.Lock()
	w, err := s.cowWorkspaceLocked()
	if err != nil {
		s.cowReadMu.Unlock()
		t.Fatal(err)
	}
	f, err := w.file(entry.ValuePtr)
	var shape valuelog.COWRecordShape
	if err == nil {
		shape, err = valuelog.InspectCOWRecord(f, entry.ValuePtr, db.cow.readLimits())
	}
	s.cowReadMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !shape.Compressed || shape.Codec != valuelog.BlockCodecSnappy || shape.DictID != 0 || shape.ValueBytes != 13000069 ||
		cowReadCapacity(shape.PayloadBytes()) != 4<<20 || cowReadCapacity(shape.RawBytes) != 16<<20 {
		t.Fatalf("unexpected actual record shape: %+v", shape)
	}
	return db, value, entry.ValuePtr, shape
}

func cowPointCopyoutPressure(t *testing.T, db *DB, headroom uint64) *memtable.COWExternalLease {
	t.Helper()
	budget := db.cow.budget
	stats := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if stats.ReservedBytes+headroom+overhead > budget.Limits().MaxInFlightBytes {
		t.Fatalf("cannot arrange pressure: %+v", stats)
	}
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - stats.ReservedBytes - headroom - overhead)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pressure.Close)
	if got := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes; got != headroom {
		t.Fatalf("headroom=%d want=%d", got, headroom)
	}
	return pressure
}

func TestCOWPublicPointCopyoutLargeAdmittedDecoder(t *testing.T) {
	db, value, _, shape := cowPointCopyoutFixture(t)
	// Admit only the existing original decoder/input/raw owner. There is no value
	// read or output allocation in setup; those are the operations under test.
	db.cow.physicalMu.Lock()
	w, err := db.cow.physicalWorkspaceLocked(db.cow.readLimits())
	if err == nil {
		_, err = w.codec(shape)
	}
	var outputCap int
	if w != nil {
		outputCap = cap(w.output)
	}
	db.cow.physicalMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if outputCap != 0 {
		t.Fatalf("setup materialized shared output: %d", outputCap)
	}
	pressure := cowPointCopyoutPressure(t, db, 10313520)
	t.Logf("actual shape=%+v pressure=%+v", shape, db.cow.budget.Stats())
	got, revision, err := db.GetVersioned([]byte("large"))
	if err != nil || revision != 42 || !bytes.Equal(got, value) {
		t.Fatalf("owned point read: len=%d revision=%d err=%v", len(got), revision, err)
	}
	got[0] ^= 255
	prefix := []byte("preserve-prefix:")
	dst := make([]byte, len(prefix), len(prefix)+len(value))
	copy(dst, prefix)
	out, err := db.GetAppend([]byte("large"), dst)
	if err != nil || !bytes.Equal(out[:len(prefix)], prefix) || !bytes.Equal(out[len(prefix):], value) {
		t.Fatalf("append copy-out: len=%d err=%v", len(out), err)
	}
	out[len(prefix)] ^= 127
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := db.Get([]byte("large"))
			if e != nil || !bytes.Equal(v, value) {
				t.Errorf("concurrent owned read: len=%d err=%v", len(v), e)
			}
		}()
	}
	wg.Wait()
	db.cow.physicalMu.Lock()
	outputCap = cap(db.cow.physicalReader.output)
	db.cow.physicalMu.Unlock()
	if outputCap != 0 {
		t.Fatalf("owned read retained shared output: %d", outputCap)
	}
	// Borrowed readers still require their own admitted materialization. Do not
	// turn the owned API's copy-out contract into a borrowed admission bypass.
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	if v, e := s.Get([]byte("large")); !errors.Is(e, memtable.ErrCOWCapacity) || v != nil {
		t.Fatalf("borrowed snapshot bypassed output admission: %d %v", len(v), e)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	pressure.Close()
}

func TestCOWPublicPointCopyoutDecoderStillRequiresAdmission(t *testing.T) {
	db, value, _, _ := cowPointCopyoutFixture(t)
	retained, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	pressure := cowPointCopyoutPressure(t, db, 1<<20)
	dst := []byte("unchanged")
	if out, err := db.GetAppend([]byte("large"), dst); !errors.Is(err, memtable.ErrCOWCapacity) || !bytes.Equal(out, dst) {
		t.Fatalf("decoder pressure: out=%q err=%v", out, err)
	}
	pressure.Close()
	if out, err := db.Get([]byte("large")); err != nil || !bytes.Equal(out, value) {
		t.Fatalf("read after pressure release: len=%d err=%v", len(out), err)
	}
}
