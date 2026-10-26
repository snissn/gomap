//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
)

// This unchanged-API regression also runs on the predecessor: its second
// public iterator cannot admit a second generic decoder under default limits.
func TestCOWDictionaryTwoLiveIteratorsDefaultBudget(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			db, want, _ := cowSuccessorDictionaryLeafFixture(t)
			budget := db.cow.budget
			if budget.Limits() != memtable.DefaultCOWLimits() || budget.Stats().ReservedBytes < 32<<20 {
				t.Fatal("fixture must retain the original default limits and maintenance reservation")
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
			openA, openB := a.Iterator, b.Iterator
			if reverse {
				openA, openB = a.ReverseIterator, b.ReverseIterator
			}
			first, err := openA([]byte("a000"), []byte("a064"))
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			heldKey := first.Key()
			wantKey := append([]byte(nil), heldKey...)
			borrowed := first.Value()
			if !first.Valid() || !bytes.Equal(borrowed, want) || first.Error() != nil {
				t.Fatalf("first dictionary iterator: %v", first.Error())
			}
			second, err := openB([]byte("a000"), []byte("a064"))
			if err != nil {
				t.Fatalf("second live dictionary iterator open: %v; budget=%+v", err, budget.Stats())
			}
			defer second.Close()
			if !second.Valid() || !bytes.Equal(second.Value(), want) || second.Error() != nil {
				t.Fatalf("second live dictionary iterator read: %v; budget=%+v", second.Error(), budget.Stats())
			}
			for _, it := range []*snapshotBoundIterator{first.(*snapshotBoundIterator), second.(*snapshotBoundIterator)} {
				for _, codec := range it.inner.(*cowMergeIterator).workspace.codecs {
					if codec != nil {
						t.Fatal("capable iterator retained a private generic decoder")
					}
				}
			}
			db.cow.physicalMu.Lock()
			shared := db.cow.physicalReader
			active := shared.codecs[0]
			independent := active != nil && active.id != 0 && active.definition != nil && shared.snapshot == nil && shared.basis == nil
			var activeID uint64
			var inputCapacity, rawCapacity int
			if active != nil {
				activeID, inputCapacity, rawCapacity = active.id, cap(active.input), cap(active.raw)
			}
			db.cow.physicalMu.Unlock()
			if !independent {
				t.Fatal("iterator did not activate independently owned shared dictionary storage")
			}
			second.Next()
			if !second.Valid() || !bytes.Equal(second.Value(), want) || !bytes.Equal(borrowed, want) {
				t.Fatal("another iterator's movement changed borrowed bytes")
			}
			otherKey := []byte("a032")
			if reverse {
				otherKey = []byte("a016")
			}
			second.Seek(otherKey)
			if !second.Valid() || !bytes.Equal(second.Key(), otherKey) || bytes.Equal(second.Key(), wantKey) ||
				!bytes.Equal(first.Key(), wantKey) || !bytes.Equal(heldKey, wantKey) || !bytes.Equal(borrowed, want) || second.Error() != nil {
				t.Fatal("another iterator's cross-page seek changed the unmoved reader's key/value")
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			second.Seek([]byte("a032"))
			if !second.Valid() || string(second.Key()) != "a032" || !bytes.Equal(second.Value(), want) || second.Error() != nil {
				t.Fatalf("closing another cut changed the retained iterator: %v", second.Error())
			}
			db.cow.physicalMu.Lock()
			current := shared.codecs[0]
			// Reverse construction starts on the last, potentially smaller page.
			// A later page may need larger scratch; that one admitted growth is
			// legitimate, but rebuilding without growth or on a repeated seek is not.
			grew := current != nil && current.id == activeID &&
				cap(current.input) >= inputCapacity && cap(current.raw) >= rawCapacity &&
				(cap(current.input) > inputCapacity || cap(current.raw) > rawCapacity)
			reused := current == active
			db.cow.physicalMu.Unlock()
			if !reused && !grew {
				t.Fatal("same producer rebuilt its shared decoder without scratch growth")
			}
			second.Seek([]byte("a032"))
			if !second.Valid() || string(second.Key()) != "a032" || !bytes.Equal(second.Value(), want) || second.Error() != nil {
				t.Fatalf("repeated dictionary seek: %v", second.Error())
			}
			db.cow.physicalMu.Lock()
			reused = shared.codecs[0] == current
			db.cow.physicalMu.Unlock()
			if !reused {
				t.Fatal("repeated same-page seek rebuilt its shared decoder")
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if got := budget.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 {
				t.Fatalf("final iterator/shared ownership did not drain: %+v", got)
			}
		})
	}
}

// Keep the real dictionary leaf producer and add two real pointer producers.
// Both definitions deliberately use inner zstd ID 73, also used by the leaf
// fixture, while their outer content identities and decoded bytes differ.
func cowIteratorDictionaryPointers(t *testing.T) (*DB, [2][]byte, [2]uint64) {
	t.Helper()
	db, _, _ := cowSuccessorDictionaryLeafFixture(t)
	store := db.dictStore.(*dictdb.Store)
	var wants [2][]byte
	var ids [2]uint64
	for d := range ids {
		samples := make([][]byte, 8)
		for i := range samples {
			samples[i] = bytes.Repeat([]byte(fmt.Sprintf("iterator-dictionary-%d-pointer-%d|", d, i)), 256)
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
		db.SetDictStore(store)
		wants[d] = samples[0]
		if err = db.Set([]byte(fmt.Sprintf("p%d", d)), wants[d]); err != nil {
			t.Fatal(err)
		}
		if err = db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("fixture dictionaries must have distinct outer identities")
	}
	return db, wants, ids
}

func TestCOWDictionaryIteratorPointerOwnershipAndTransitions(t *testing.T) {
	db, wants, ids := cowIteratorDictionaryPointers(t)
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
	for d := range ids {
		entry, err := a.GetEntryExact([]byte(fmt.Sprintf("p%d", d)))
		if err != nil || entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("actual pointer %d: %+v %v", d, entry, err)
		}
		f, _, err := a.cowCut.basis.snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
		if err != nil {
			t.Fatal(err)
		}
		shape, err := valuelog.InspectCOWRecord(f, entry.ValuePtr, db.cow.readLimits())
		if err != nil || !shape.Compressed || shape.DictID != ids[d] {
			t.Fatalf("actual pointer producer %d: %+v %v", d, shape, err)
		}
	}
	first, err := a.Iterator([]byte("p0"), []byte("p2"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	borrowed := first.Value()
	if !bytes.Equal(borrowed, wants[0]) || first.Error() != nil {
		t.Fatalf("first pointer: %v", first.Error())
	}
	second, err := b.Iterator([]byte("p0"), []byte("p2"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for i := 0; i < 6; i++ {
		d := i % 2
		second.Seek([]byte(fmt.Sprintf("p%d", d)))
		if !second.Valid() || !bytes.Equal(second.Value(), wants[d]) || second.Error() != nil || !bytes.Equal(borrowed, wants[0]) {
			t.Fatalf("borrowed pointer transition %d: %v", d, second.Error())
		}
		db.cow.physicalMu.Lock()
		shared := db.cow.physicalReader
		active := 0
		for _, codec := range shared.codecs {
			if codec != nil {
				active++
				if codec.id != ids[d] || codec.definition == nil {
					db.cow.physicalMu.Unlock()
					t.Fatal("wrong shared producer/definition after pointer read")
				}
			}
		}
		db.cow.physicalMu.Unlock()
		if active != 1 {
			t.Fatalf("active generic decoder envelopes=%d want 1", active)
		}
	}
	// Each iterator is moved by one goroutine; both share only serialized
	// decoding, never their cursor or borrowed output backing.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, it := range []*snapshotBoundIterator{first.(*snapshotBoundIterator), second.(*snapshotBoundIterator)} {
		wg.Add(1)
		go func(it *snapshotBoundIterator) {
			defer wg.Done()
			defer it.Close()
			for i := 0; i < 6; i++ {
				d := i % 2
				it.Seek([]byte(fmt.Sprintf("p%d", d)))
				if !it.Valid() || !bytes.Equal(it.Value(), wants[d]) || it.Error() != nil {
					errs <- fmt.Errorf("concurrent iterator %d: %v", d, it.Error())
					return
				}
			}
		}(it)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	budget := db.cow.budget
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("pointer/cursor closure retained ownership: %+v", got)
	}
}

func TestCOWDictionaryIteratorCRCAndAdmissionRefusal(t *testing.T) {
	db, wants, _ := cowIteratorDictionaryPointers(t)
	s, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entry, err := s.GetEntryExact([]byte("p0"))
	if err != nil {
		t.Fatal(err)
	}
	f, _, err := s.cowCut.basis.snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
	if err != nil {
		t.Fatal(err)
	}
	rw, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()
	var original [1]byte
	offset := int64(entry.ValuePtr.Offset - 4) // exact record CRC, not metadata
	if _, err = rw.ReadAt(original[:], offset); err != nil {
		t.Fatal(err)
	}
	if _, err = rw.WriteAt([]byte{original[0] ^ 1}, offset); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = rw.WriteAt(original[:], offset) }()
	it, err := s.Iterator([]byte("p0"), []byte("p1"))
	if err != nil {
		t.Fatal(err)
	}
	if value := it.Value(); len(value) != 0 || !errors.Is(it.Error(), valuelog.ErrCorrupt) {
		_ = it.Close()
		t.Fatalf("shared dictionary pointer skipped full CRC: len=%d err=%v", len(value), it.Error())
	}
	if err = it.Close(); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("corrupt iterator close lost original error: %v", err)
	}
	if _, err = rw.WriteAt(original[:], offset); err != nil {
		t.Fatal(err)
	}
	// Genuine constructor pressure remains a refusal with complete rollback.
	budget := db.cow.budget
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	pressured := budget.Stats()
	refused, readErr := s.Iterator([]byte("p0"), []byte("p1"))
	if refused != nil {
		_ = refused.Close()
	}
	after := budget.Stats()
	pressure.Close()
	if !errors.Is(readErr, memtable.ErrCOWCapacity) || after != pressured {
		t.Fatalf("true pressure changed refusal/ownership: err=%v before=%+v after=%+v", readErr, pressured, after)
	}
	retry, err := s.Iterator([]byte("p0"), []byte("p1"))
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Close()
	if !retry.Valid() || !bytes.Equal(retry.Value(), wants[0]) || retry.Error() != nil {
		t.Fatalf("restored exact cut after refusal/corruption: %v", retry.Error())
	}
}
