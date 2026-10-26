//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// This whole file uses APIs present on 6792, including the real trained leaf
// producer. It can be overlaid unchanged for the causal predecessor control.
func TestCOWDictionaryLeafPointCompletesFirstTraversal(t *testing.T) {
	db, want, ptr := cowSuccessorDictionaryLeafFixture(t)
	basis := db.cow.cut.basis
	file, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(file, ptr.ValuePtr(), db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID == 0 {
		t.Fatalf("fixture must actually read a dictionary leaf: %+v %v", shape, err)
	}
	// A retraversal through a transient Snapshot would refuse at this frontier.
	// The original retained cut and independently owned definition suffice for
	// inline copy-out, without borrowing another view or changing its budget.
	release := cowPinViewFrontier(t, db.cow)
	defer release()
	views := db.COWMemoryStats().Views
	got, err := db.GetAppend([]byte("a000"), []byte("prefix:"))
	if err != nil || !bytes.Equal(got, append([]byte("prefix:"), want...)) {
		t.Fatalf("dictionary inline point retraversed through Snapshot admission: %q %v", got, err)
	}
	if db.COWMemoryStats().Views != views {
		t.Fatal("inline dictionary point changed view ownership")
	}
	// The same maintained first-traversal method used by GetAppend must report
	// completion, rather than requesting another root search by its caller.
	var fresh cowFreshReadOwner
	got, _, handled, err := basis.appendInline(db, []byte("a000"), nil, &fresh)
	if err != nil || !handled || !bytes.Equal(got, want) || fresh.reader != nil {
		t.Fatalf("dictionary first traversal incomplete: handled=%t err=%v", handled, err)
	}
}

func TestCOWDictionaryLeafColdCacheAdmissionAndPolicy(t *testing.T) {
	for _, iterator := range []bool{false, true} {
		t.Run(map[bool]string{false: "point", true: "iterator"}[iterator], func(t *testing.T) {
			original, want, ptr := cowSuccessorDictionaryLeafFixture(t)
			dir := filepath.Dir(original.dir) // the cache owns dir/wal
			if err := original.Close(); err != nil {
				t.Fatal(err)
			}
			// Reopen removes every write-origin cache page and decoder. No helper
			// injects a decoded page or fabricates a record-verification credit.
			store, err := dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
			if err != nil {
				t.Fatal(err)
			}
			backend, err := backenddb.Open(backenddb.Options{
				Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true, LeafPageReadCacheEntries: 128,
				ValueLog: backenddb.ValueLogOptions{DictLookup: func(id uint64) ([]byte, error) {
					return store.GetDictBytes(context.Background(), id)
				}},
			})
			if err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			db, err := Open(dir, backend, Options{
				DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30,
				IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
			})
			if err != nil {
				_ = backend.Close()
				_ = store.Close()
				t.Fatal(err)
			}
			db.SetDictStore(store)
			t.Cleanup(func() { _ = db.Close() })
			if cowLeafCacheCounter(t, backend, "stores") != 0 {
				t.Fatal("reopened cache was not cold")
			}
			basis := db.cow.cut.basis
			file, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
			if err != nil {
				t.Fatal(err)
			}
			shape, err := valuelog.InspectCOWLeafRecord(file, ptr.ValuePtr(), db.cow.readLimits())
			if err != nil || !shape.Compressed || shape.DictID == 0 {
				t.Fatalf("reopened read must still encounter a dictionary: %+v %v", shape, err)
			}
			read := func() ([]byte, error) {
				if !iterator {
					return db.GetAppend([]byte("a000"), nil)
				}
				s, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					return nil, err
				}
				defer s.Close()
				it, err := s.Iterator([]byte("a000"), []byte("a001"))
				if err != nil {
					return nil, err
				}
				defer it.Close()
				if !it.Valid() || string(it.Key()) != "a000" {
					if err := it.Error(); err != nil {
						return nil, err
					}
					return nil, errors.New("dictionary iterator lost its exact key")
				}
				return append([]byte(nil), it.Value()...), it.Error()
			}
			writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			var crc [1]byte
			crcOffset := int64(ptr.Offset - 4)
			if _, err := writer.ReadAt(crc[:], crcOffset); err != nil {
				t.Fatal(err)
			}
			defer writer.WriteAt(crc[:], crcOffset)
			if _, err := writer.WriteAt([]byte{crc[0] ^ 1}, crcOffset); err != nil {
				t.Fatal(err)
			}
			if _, err := read(); !errors.Is(err, valuelog.ErrCorrupt) {
				t.Fatalf("cold dictionary record CRC was bypassed: %v", err)
			}
			if cowLeafCacheCounter(t, backend, "stores") != 0 {
				t.Fatal("failed dictionary CRC admitted a decoded page")
			}
			if _, err := writer.WriteAt(crc[:], crcOffset); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 8; i++ {
				got, err := read()
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("dictionary read %d: %q %v", i, got, err)
				}
				got[0] ^= 1 // a cache page/decoder must never alias owned output
			}
			if cowLeafCacheCounter(t, backend, "read_miss_admission_stores") == 0 || cowLeafCacheCounter(t, backend, "hits") == 0 {
				t.Fatal("actual dictionary reads bypassed cold admission/warm verified cache")
			}
			// Verified immutable pages do not depend on the continued residency
			// of the producer decoder. A now-refusing provider cannot revoke
			// existing verified bytes under the same fixed cache policy.
			db.cow.closePhysicalReader()
			refusal := errors.New("cold dictionary definition refused")
			db.dictStore = cowDictionaryLeafAdmissionStore{DictStore: store, prepare: func(uint64, valuelog.COWReadLimits, int, func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
				return nil, refusal
			}}
			defer func() { db.dictStore = store }()
			if got, err := read(); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("verified dictionary page required a retired decoder: %q %v", got, err)
			}
			// A concrete different-limit private caller cannot borrow that
			// fixed-policy credit. With no decoder it must consult the provider
			// and propagate its refusal, without admitting a new page.
			s, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			s.cowReadMu.Lock()
			w, err := s.cowWorkspaceLocked()
			if err == nil {
				w.limits.MaxRecordBytes--
				var scratch [page.PageSize]byte
				_, err = basis.readPhysicalLeafWithWorkspace(db.cow, w, ptr, scratch[:], false, true)
			}
			s.cowReadMu.Unlock()
			_ = s.Close()
			if !errors.Is(err, refusal) {
				t.Fatalf("alternate limits bypassed dictionary definition policy: %v", err)
			}
			db.dictStore = store
			// The verified cache remains subordinate to inspection, even after
			// successful full-record CRC and normal tree page validation.
			var version [1]byte
			if _, err := writer.ReadAt(version[:], int64(ptr.Offset)); err != nil {
				t.Fatal(err)
			}
			defer writer.WriteAt(version[:], int64(ptr.Offset))
			if _, err := writer.WriteAt([]byte{version[0] ^ 1}, int64(ptr.Offset)); err != nil {
				t.Fatal(err)
			}
			hits := cowLeafCacheCounter(t, backend, "hits")
			if _, err := read(); !errors.Is(err, valuelog.ErrCorrupt) {
				t.Fatalf("warm dictionary cache bypassed inspection: %v", err)
			}
			if cowLeafCacheCounter(t, backend, "hits") != hits {
				t.Fatal("invalid dictionary record reached verified cache lookup")
			}
		})
	}
}

// Preserve the real dictionary implementation; only its supported admission
// boundary is faulted. No fabricated definition, decoder or decoded page is used.
type cowDictionaryLeafAdmissionStore struct {
	DictStore
	prepare func(uint64, valuelog.COWReadLimits, int, func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error)
}

func (s cowDictionaryLeafAdmissionStore) PrepareDictionaryReadDefinition(id uint64, limits valuelog.COWReadLimits, maxResources int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
	return s.prepare(id, limits, maxResources, admit)
}

func TestCOWDictionaryLeafCapableAdmissionRefundsFreshBasis(t *testing.T) {
	db, want, _ := cowSuccessorDictionaryLeafFixture(t)
	basis, cut := db.cow.cut.basis, db.cow.cut
	if basis.reader != nil {
		t.Fatal("fixture must begin with fresh basis traversal")
	}
	store := db.dictStore
	defer func() { db.dictStore = store }()
	provider := store.(cowDictionaryReadProvider)
	refusals, captures, unlockedRetry := 0, 0, false
	db.dictStore = cowDictionaryLeafAdmissionStore{DictStore: store, prepare: func(id uint64, limits valuelog.COWReadLimits, maxResources int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
		// Initial decode legitimately holds basis.readMu. A subsequent local
		// materializer also refuses while that fresh owner survives. Only its
		// existing refund permits this real provider's final capture to proceed.
		locked := basis.readMu.TryLock()
		freshPresent := true
		if locked {
			freshPresent = basis.reader != nil
			basis.readMu.Unlock()
		}
		if freshPresent {
			refusals++
			release, err := admit(dictdb.DictionaryReadAllocationSizes{Definition: db.cow.budget.Limits().MaxInFlightBytes})
			if release != nil {
				release()
			}
			if err == nil {
				return nil, errors.New("real admission accepted injected over-capacity envelope")
			}
			return nil, err
		}
		unlockedRetry = locked
		captures++
		return provider.PrepareDictionaryReadDefinition(id, limits, maxResources, admit)
	}}
	refs, views := cut.refs, db.COWMemoryStats().Views
	got, err := db.GetAppend([]byte("a000"), []byte("prefix:"))
	if err != nil || !bytes.Equal(got, append([]byte("prefix:"), want...)) {
		t.Fatalf("capable dictionary same-cut capacity handoff: %q %v", got, err)
	}
	if refusals < 2 || captures != 1 || !unlockedRetry || basis.reader != nil || db.cow.cut != cut || cut.refs != refs || db.COWMemoryStats().Views != views {
		t.Fatalf("fresh owner/refund/cut lifecycle: refusals=%d captures=%d unlocked=%t basis=%p refs=%d->%d", refusals, captures, unlockedRetry, basis.reader, refs, cut.refs)
	}
}
