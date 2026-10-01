package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// This source reuses its payload on movement and poisons it at Close. The
// successor adapter instead hands ownership of a fresh record to its caller.
type ownershipIterator struct {
	treedb.Iterator
	key, record []byte
	valid       bool
	reads       int
	err         error
}

func (it *ownershipIterator) Valid() bool   { return it.valid }
func (it *ownershipIterator) Key() []byte   { return it.key }
func (it *ownershipIterator) Value() []byte { it.reads++; return it.record }
func (it *ownershipIterator) Error() error  { return it.err }
func (it *ownershipIterator) Next()         { copy(it.record[1:], "next"); it.valid = false }
func (it *ownershipIterator) Seek([]byte)   { copy(it.record[1:], "seek"); it.valid = true }
func (it *ownershipIterator) Close() error  { clear(it.record); it.valid = false; return nil }

type ownershipDB struct{ raw *ownershipIterator }

func (db *ownershipDB) Iterator([]byte, []byte) (treedb.Iterator, error) { return db.raw, nil }
func (*ownershipDB) NewBatchWithSize(int) treedb.Batch                   { return nil }

type ownershipSuccessorDB struct {
	ownershipDB
	lastRecord []byte
}

func (db *ownershipSuccessorDB) SeekGE([]byte, []byte) ([]byte, []byte, bool, error) {
	db.lastRecord = bytes.Clone(db.raw.record)
	return bytes.Clone(db.raw.key), db.lastRecord, true, nil
}

type ownershipQualifiedDB struct{ ownershipSuccessorDB }

func (db *ownershipQualifiedDB) SeekGEVersionRange(start, end []byte) ([]byte, []byte, bool, error) {
	return db.SeekGE(start, end)
}

func ownershipSource(t testing.TB) *ownershipIterator {
	t.Helper()
	key, err := mvcckey.Encode([]byte("key"), 7)
	if err != nil {
		t.Fatal(err)
	}
	return &ownershipIterator{key: key, record: []byte{recordValueV1, 'o', 'l', 'd', '!'}, valid: true}
}

var inspectionSink Version

func TestVersionIteratorEntryViewBorrowedInspection(t *testing.T) {
	raw := ownershipSource(t)
	it := &VersionIterator{raw: raw}
	it.advance()
	// Assert the public seam dynamically so the unchanged baseline produces a
	// behavioral red, rather than failing to compile the rest of the package.
	view, ok := any(it).(interface{ EntryView() Version })
	if !ok {
		t.Fatal("VersionIterator lacks borrowed EntryView inspection")
	}
	first, second := view.EntryView(), view.EntryView()
	if &first.Key[0] != &second.Key[0] || &first.Value[0] != &raw.record[1] {
		t.Fatal("inspection copied borrowed bytes")
	}
	if got := testing.AllocsPerRun(100, func() { inspectionSink = view.EntryView() }); got != 0 {
		t.Fatalf("inspection allocations=%g, want 0", got)
	}
	if raw.reads != 1 {
		t.Fatalf("Value reads=%d, want 1 at advance", raw.reads)
	}
	owned := it.Entry()
	it.Next()
	if string(first.Value) != "next" || string(owned.Value) != "old!" || string(owned.Key) != "key" {
		t.Fatalf("movement view=%q owned=%+v", first.Value, owned)
	}
	if got := view.EntryView(); got.Key != nil || got.Value != nil {
		t.Fatalf("invalid view=%+v", got)
	}
	raw.key, _ = mvcckey.Encode([]byte("new"), 7)
	it.Seek([]byte("new"), 7)
	if string(view.EntryView().Value) != "seek" {
		t.Fatal("Seek did not refresh view")
	}
	if string(first.Key) != "new" || string(owned.Key) != "key" {
		t.Fatal("Seek did not reuse the borrowed key buffer independently of Entry")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if got := view.EntryView(); got.Key != nil || got.Value != nil {
		t.Fatalf("closed view=%+v", got)
	}
	if string(owned.Value) != "old!" {
		t.Fatal("owned entry changed at Close")
	}
}

func TestGetAtSuccessorTransfersOwnedPayload(t *testing.T) {
	for _, qualified := range []bool{false, true} {
		t.Run(map[bool]string{false: "generic_successor", true: "qualified_successor"}[qualified], func(t *testing.T) {
			base := ownershipSuccessorDB{ownershipDB: ownershipDB{raw: ownershipSource(t)}}
			var db treeDB = &base
			last := &base.lastRecord
			if qualified {
				q := &ownershipQualifiedDB{ownershipSuccessorDB: base}
				db = q
				last = &q.lastRecord
			}
			result, err := (&Store{db: db, floorLoaded: true}).GetAt([]byte("key"), 9)
			if err != nil {
				t.Fatal(err)
			}
			if result.State != Present || result.Timestamp != 7 || string(result.Value) != "old!" {
				t.Fatalf("result=%+v", result)
			}
			if &result.Value[0] != &(*last)[1] {
				t.Fatal("owned successor payload was copied again")
			}
		})
	}
}

func TestGetAtIteratorCopiesBorrowedPayloadBeforeClose(t *testing.T) {
	raw := ownershipSource(t)
	result, err := (&Store{db: &ownershipDB{raw: raw}, floorLoaded: true}).GetAt([]byte("key"), 9)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Value) != "old!" || !bytes.Equal(raw.record, make([]byte, len(raw.record))) {
		t.Fatalf("result=%+v closed record=%x", result, raw.record)
	}
}

func TestVersionViewPreservesEagerReadErrors(t *testing.T) {
	raw := ownershipSource(t)
	raw.err = errors.New("payload read failure")
	it := &VersionIterator{raw: raw}
	it.advance()
	if it.Valid() || !errors.Is(it.Error(), raw.err) || !errors.Is(it.Error(), ErrStorage) {
		t.Fatalf("valid=%t err=%v", it.Valid(), it.Error())
	}
}

func TestMVCCOwnedOutputsSurviveMutationFlushAndClose(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		for _, width := range []int{8, 16, 4096} {
			t.Run(fmt.Sprintf("pointers=%t/width=%d", pointers, width), func(t *testing.T) {
				opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
				opts.DisableSideStores = true
				opts.BackgroundCheckpointInterval = -1
				opts.ValueLog.ForcePointers = pointers
				if pointers {
					opts.ValueLog.PointerThreshold = 1
				}
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { db.Close() })
				store := New(db)
				want := bytes.Repeat([]byte{'v'}, width)
				commitHistory(t, store, "key", MutationAt{7, want, false})
				point, err := store.GetAt([]byte("key"), 9)
				if err != nil {
					t.Fatal(err)
				}
				it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: []byte("key")})
				if err != nil {
					t.Fatal(err)
				}
				owned, view := it.Entry(), it.EntryView()
				owned.Key[0] = 'X'
				owned.Value[0] = 'X'
				if string(view.Key) != "key" || !bytes.Equal(view.Value, want) {
					t.Fatal("owned Entry aliases borrowed view")
				}
				owned.Key[0], owned.Value[0] = 'k', 'v'
				point.Value[0] = 'X'
				requireResult(t, store, []byte("key"), 9, Present, 7, want)
				point.Value[0] = 'v'
				it.Next()
				it.Seek([]byte("key"), 9)
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				commitHistory(t, store, "key", MutationAt{7, bytes.Repeat([]byte{'n'}, width), false})
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				afterFlush, err := store.GetAt([]byte("key"), 9)
				if err != nil || !bytes.Equal(afterFlush.Value, bytes.Repeat([]byte{'n'}, width)) {
					t.Fatalf("published result=%+v err=%v", afterFlush, err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(point.Value, want) || !bytes.Equal(owned.Value, want) || string(owned.Key) != "key" {
					t.Fatal("owned outputs changed after replacement, flush or DB Close")
				}
				if len(afterFlush.Value) != width || afterFlush.Value[0] != 'n' {
					t.Fatal("published owned result changed after DB Close")
				}
			})
		}
	}
}

func TestVersionEntryViewEnvelopeStatesAndErrors(t *testing.T) {
	for _, record := range [][]byte{{recordValueV1}, {recordTombstoneV1}, {}, {0xff}, {recordTombstoneV1, 1}} {
		t.Run(fmt.Sprintf("record=%x", record), func(t *testing.T) {
			raw := ownershipSource(t)
			raw.record = record
			it := &VersionIterator{raw: raw}
			it.advance()
			if len(record) == 1 && (record[0] == recordValueV1 || record[0] == recordTombstoneV1) {
				view := it.EntryView()
				if !it.Valid() || len(view.Value) != 0 || view.Timestamp != 7 || view.State != ReadState(record[0]) {
					t.Fatalf("view=%+v err=%v", view, it.Error())
				}
			} else if it.Valid() || !errors.Is(it.Error(), ErrMalformedRecord) || it.EntryView().Key != nil {
				t.Fatalf("valid=%t err=%v", it.Valid(), it.Error())
			}
		})
	}
}
