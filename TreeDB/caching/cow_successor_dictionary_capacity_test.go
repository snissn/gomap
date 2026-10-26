//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Hide only the optional independent definition capture capability.
type cowCapturedDictionaryOnlyStore struct{ DictStore }

// Two real retained cuts must share synchronous owned dictionary decoding,
// rather than each reserving a full generic decoder from the finite parent.
func TestCOWOwnedDictionarySuccessorTwoCutsDefaultBudget(t *testing.T) {
	db, want, _ := cowSuccessorDictionaryLeafFixture(t)
	first, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	read := func(s *Snapshot) []byte {
		t.Helper()
		key, value, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
		if err != nil || !found || string(key) != "a000" || !bytes.Equal(value, want) {
			t.Fatalf("owned dictionary successor: key=%q found=%t err=%v budget=%+v", key, found, err, db.COWMemoryStats())
		}
		return value
	}
	owned := read(first)
	read(second)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	read(second)
	// An owned cache envelope may not starve the original borrowed iterator.
	it, err := second.Iterator([]byte("a000"), []byte("a001"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.Valid() || !bytes.Equal(it.Value(), want) || it.Error() != nil {
		_ = it.Close()
		t.Fatal("borrowed dictionary iterator changed")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	read(second)
	if !bytes.Equal(owned, want) {
		t.Fatal("later read/close changed owned result")
	}
}

// Actual producer dictionaries alternate in both the stored pointers and the
// reads. Their owned results must survive replacement, refusal and cut closure.
func TestCOWOwnedDictionarySuccessorAlternatingValues(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionDict), ValueLogAutoPolicy: uint8(vlogAutoSize), ValueLogDictIncompressibleHoldBytes: -1})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	})
	store, err := dictdb.Open(dir+"/dictdb", backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	db.SetDictStore(store)
	var ids [2]uint64
	var wants [2][]byte
	for d := range ids {
		samples := make([][]byte, 8)
		for i := range samples {
			samples[i] = bytes.Repeat([]byte(fmt.Sprintf("owned-dictionary-%d-pointer-%d|", d, i)), 256)
		}
		dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{ID: 73, Contents: samples, History: append([]byte(nil), samples[0]...), Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
		if err != nil {
			t.Fatal(err)
		}
		ids[d], err = store.PutDictBytes(context.Background(), dictionary)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.SetCurrent(context.Background(), ids[d]); err != nil {
			t.Fatal(err)
		}
		db.SetDictStore(store) // Refresh the maintained producer's current-ID cache.
		wants[d] = bytes.Repeat([]byte(fmt.Sprintf("owned-dictionary-%d-pointer-0|", d)), 256)
		if err = db.Set([]byte(fmt.Sprintf("p%d", d)), wants[d]); err != nil {
			t.Fatal(err)
		}
		if err = db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("fixture did not produce distinct dictionaries")
	}
	a, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// Prove actual encoded pointer provenance; no codec hook substitutes data.
	for d := range ids {
		entry, err := a.GetEntryExact([]byte(fmt.Sprintf("p%d", d)))
		if err != nil {
			t.Fatal(err)
		}
		a.cowReadMu.Lock()
		w, err := a.cowWorkspaceLocked()
		if err != nil {
			a.cowReadMu.Unlock()
			t.Fatal(err)
		}
		f, err := w.file(entry.ValuePtr)
		var shape valuelog.COWRecordShape
		if err == nil {
			shape, err = valuelog.InspectCOWRecord(f, entry.ValuePtr, w.limits)
		}
		a.cowReadMu.Unlock()
		if err != nil || !shape.Compressed || shape.DictID != ids[d] {
			t.Fatalf("dictionary pointer %d shape=%+v err=%v", d, shape, err)
		}
	}
	read := func(s *Snapshot, d int) ([]byte, error) {
		key := []byte(fmt.Sprintf("p%d", d))
		k, v, found, err := s.SeekGEVersionRange(key, append(append([]byte(nil), key...), 0))
		if err == nil && (!found || !bytes.Equal(k, key) || !bytes.Equal(v, wants[d])) {
			err = fmt.Errorf("wrong owned pointer %d", d)
		}
		return v, err
	}
	owned, err := read(a, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := read(b, i%2); err != nil {
			t.Fatal(err)
		}
		db.cow.physicalMu.Lock()
		shared := db.cow.physicalReader
		var active *cowReadCodec
		for _, c := range shared.codecs {
			if c != nil {
				if active != nil {
					db.cow.physicalMu.Unlock()
					t.Fatal("multiple active generic envelopes")
				}
				active = c
			}
		}
		db.cow.physicalMu.Unlock()
		if active == nil || active.id != ids[i%2] {
			t.Fatal("wrong independently owned producer")
		}
		if _, err := read(b, i%2); err != nil {
			t.Fatal(err)
		}
		db.cow.physicalMu.Lock()
		reused := shared.codecs[0] == active
		db.cow.physicalMu.Unlock()
		if !reused {
			t.Fatal("same producer reconstructed decoder")
		}
	}
	if !bytes.Equal(owned, wants[0]) {
		t.Fatal("dictionary replacement changed owned value")
	}
	// A truly unavailable envelope is still refused, then retryable on this cut.
	db.cow.physicalMu.Lock()
	db.cow.physicalReader.close()
	db.cow.physicalReader = nil
	db.cow.physicalMu.Unlock()
	stats := db.cow.budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - stats.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	_, err = read(a, 0)
	pressure.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("true retained pressure: %v", err)
	}
	if _, err = read(a, 0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, s := range []*Snapshot{a, b} {
		wg.Add(1)
		go func(s *Snapshot) {
			defer wg.Done()
			for i := 0; i < 4; i++ {
				if _, err := read(s, i%2); err != nil {
					errs <- err
					return
				}
			}
		}(s)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// A pending pointer retains its definition in the exact cut. An otherwise
	// supported legacy provider need not expose independent shared capture.
	if err = db.Set([]byte("p2"), wants[1]); err != nil {
		t.Fatal(err)
	}
	legacy, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	db.SetDictStore(cowCapturedDictionaryOnlyStore{store})
	_, legacyValue, found, legacyErr := legacy.SeekGEVersionRange([]byte("p2"), []byte("p3"))
	db.SetDictStore(store)
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if legacyErr != nil || !found || !bytes.Equal(legacyValue, wants[1]) {
		t.Fatalf("captured definition fallback: found=%t err=%v", found, legacyErr)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned, wants[0]) {
		t.Fatal("cut close changed owned value")
	}
	budget := db.cow.budget
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("final ownership: %+v", stats)
	}
}

// A busy physical owner cannot be reclaimed or waited on while a borrowed
// caller holds its original read locks. Capacity remains explicit and bounded.
func TestCOWOwnedDictionaryBusySharedPreservesBorrowedRefusal(t *testing.T) {
	db, want, pointer := cowSuccessorDictionaryLeafFixture(t)
	// Exercise the legacy captured-only admission fallback, not the capable
	// public route (which now serializes decoding on the shared owner).
	f, _, err := db.cow.cut.basis.snapshot.PinnedValueLogFile(pointer.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, pointer.ValuePtr(), db.cow.readLimits())
	if err != nil || shape.DictID == 0 {
		t.Fatalf("fixture dictionary: %+v %v", shape, err)
	}
	store := db.dictStore.(*dictdb.Store)
	if err = store.SetCurrent(context.Background(), shape.DictID); err != nil {
		t.Fatal(err)
	}
	db.SetDictStore(store)
	if err = db.Set([]byte("z-pending-dictionary"), bytes.Repeat(want, 8)); err != nil {
		t.Fatal(err)
	}
	s, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, owned, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if err != nil || !found {
		t.Fatal(err)
	}
	// Prove the exact cut owns a real pending definition before hiding the
	// optional independent-capture capability. No synthetic owner is installed.
	s.cowReadMu.Lock()
	w, err := s.cowWorkspaceLocked()
	var captured []byte
	var owner *dictdb.DictionaryReadDefinition
	if err == nil {
		captured, owner, err = w.definition(shape.DictID)
	}
	s.cowReadMu.Unlock()
	if err != nil || len(captured) == 0 || owner != nil {
		owner.Close()
		t.Fatalf("pending definition was not captured by the real cut: %v", err)
	}
	db.dictStore = cowCapturedDictionaryOnlyStore{store}
	defer func() { db.dictStore = store }()
	db.cow.physicalMu.Lock()
	codec := db.cow.physicalReader.codecs[0]
	done := make(chan error, 1)
	go func() {
		it, err := s.Iterator([]byte("a000"), []byte("a001"))
		if it != nil {
			_ = it.Close()
		}
		done <- err
	}()
	var readErr error
	select {
	case readErr = <-done:
	case <-time.After(2 * time.Second):
		db.cow.physicalMu.Unlock()
		<-done
		t.Fatal("borrowed admission blocked on busy shared owner")
	}
	unchanged := db.cow.physicalReader.codecs[0] == codec && codec.decoder != nil && codec.lease != nil
	db.cow.physicalMu.Unlock()
	if !unchanged || !errors.Is(readErr, memtable.ErrCOWCapacity) {
		t.Fatalf("busy shared ownership: unchanged=%t err=%v", unchanged, readErr)
	}
	if !bytes.Equal(owned, want) {
		t.Fatal("failed borrowed read changed owned result")
	}
	it, err := s.Iterator([]byte("a000"), []byte("a001"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.Valid() || !bytes.Equal(it.Value(), want) || it.Error() != nil {
		_ = it.Close()
		t.Fatal("idle retry changed borrowed result")
	}
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
}
