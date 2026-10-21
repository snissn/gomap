package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

type nextKeySource struct {
	treedb.Iterator
	keys, values                              [][]byte
	index                                     int
	nexts, seeks, reads                       int
	err, nextErr, seekErr, valueErr, closeErr error
	valueErrAt                                int
}

func (s *nextKeySource) Valid() bool { return s.err == nil && s.index >= 0 && s.index < len(s.keys) }
func (s *nextKeySource) Key() []byte { return s.keys[s.index] }
func (s *nextKeySource) Value() []byte {
	s.reads++
	if s.valueErr != nil && s.index == s.valueErrAt {
		s.err = s.valueErr
	}
	return s.values[s.index]
}
func (s *nextKeySource) Error() error { return s.err }
func (s *nextKeySource) Close() error { return s.closeErr }
func (s *nextKeySource) Next() {
	s.nexts++
	if s.nextErr != nil {
		s.err = s.nextErr
		return
	}
	s.index++
}
func (s *nextKeySource) Seek(key []byte) {
	s.seeks++
	if s.seekErr != nil {
		s.err = s.seekErr
		return
	}
	s.index = sort.Search(len(s.keys), func(i int) bool { return bytes.Compare(s.keys[i], key) >= 0 })
}
func (s *nextKeySource) add(t testing.TB, key []byte, ts uint64, value []byte) {
	t.Helper()
	physical, err := mvcckey.Encode(key, ts)
	if err != nil {
		t.Fatal(err)
	}
	s.keys = append(s.keys, physical)
	s.values = append(s.values, value)
}

func TestVersionIteratorNextKeyBoundedAndValueLazy(t *testing.T) {
	for _, depth := range []int{1, 4, nextKeyStepLimit, nextKeyStepLimit + 1, 40} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			raw := new(nextKeySource)
			for ts := depth; ts > 0; ts-- {
				raw.add(t, []byte("a"), uint64(ts), []byte{recordValueV1, byte(ts)})
			}
			raw.add(t, []byte("b"), 1, []byte{recordValueV1, 'b'})
			it := &VersionIterator{raw: raw}
			it.advance()
			owned := it.Entry()
			it.NextKey()
			if got := it.EntryView(); string(got.Key) != "b" || got.Timestamp != 1 || string(got.Value) != "b" || it.Error() != nil {
				t.Fatalf("next key=%+v err=%v", got, it.Error())
			}
			wantSteps := min(depth, nextKeyStepLimit)
			wantSeeks := 0
			if depth > nextKeyStepLimit {
				wantSeeks = 1
			}
			if raw.nexts != wantSteps || raw.seeks != wantSeeks || raw.reads != 2 {
				t.Fatalf("steps=%d seeks=%d reads=%d want %d/%d/2", raw.nexts, raw.seeks, raw.reads, wantSteps, wantSeeks)
			}
			duplicates := min(depth-1, nextKeyStepLimit)
			if got := it.Stats(); got != (VersionIteratorStats{Visited: uint64(duplicates + 2), Skipped: uint64(duplicates), Retained: 2}) {
				t.Fatalf("stats=%+v", got)
			}
			it.NextKey()
			if it.Valid() || it.Error() != nil {
				t.Fatalf("end valid=%t err=%v", it.Valid(), it.Error())
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			if string(owned.Key) != "a" || owned.Timestamp != uint64(depth) || len(owned.Value) != 1 || owned.Value[0] != byte(depth) {
				t.Fatalf("owned result changed: %+v", owned)
			}
		})
	}
}

func TestVersionIteratorNextKeyBinaryPrefixAndMaximumKey(t *testing.T) {
	// A maximum canonical key cannot be extended with a synthetic NUL. The
	// deep history forces the exact physical upper-bound fallback for that key.
	maximum := bytes.Repeat([]byte{'z'}, mvcckey.MaxEncodedKeySize-19)
	keys := [][]byte{{}, {0}, {'a'}, {'a', 0}, {'a', 0, 'b'}, maximum, {255}}
	raw := new(nextKeySource)
	for _, key := range keys {
		for ts := nextKeyStepLimit + 2; ts > 0; ts-- {
			raw.add(t, key, uint64(ts), []byte{recordValueV1, 'v'})
		}
	}
	it := &VersionIterator{raw: raw}
	it.advance()
	for _, want := range keys {
		if got := it.EntryView(); !it.Valid() || !bytes.Equal(got.Key, want) || got.Timestamp != nextKeyStepLimit+2 {
			t.Fatalf("key len=%d want len=%d timestamp=%d err=%v", len(got.Key), len(want), got.Timestamp, it.Error())
		}
		it.NextKey()
	}
	if it.Valid() || it.Error() != nil || raw.seeks != len(keys) {
		t.Fatalf("end valid=%t seeks=%d err=%v", it.Valid(), raw.seeks, it.Error())
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVersionIteratorNextKeyOptionsAndTombstone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options VersionIteratorOptions
		want    []string
	}{
		{"timestamp", VersionIteratorOptions{ReadTimestamp: 8}, []string{"a@7:2:", "a\x00@2:1:child", "b@4:1:four"}},
		{"prefix", VersionIteratorOptions{Prefix: []byte("a"), ReadTimestamp: 8}, []string{"a@7:2:", "a\x00@2:1:child"}},
		{"exact", VersionIteratorOptions{ExactKey: []byte("a"), ReadTimestamp: 8}, []string{"a@7:2:"}},
		{"bounds", VersionIteratorOptions{LowerBound: []byte{'a', 0}, UpperBound: []byte("b"), ReadTimestamp: 8}, []string{"a\x00@2:1:child"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := new(nextKeySource)
			for _, r := range []struct {
				key   string
				ts    uint64
				value []byte
			}{
				{"a", 12, []byte{recordValueV1, 'n'}}, {"a", 7, []byte{recordTombstoneV1}}, {"a", 3, []byte{recordValueV1, 'o'}},
				{"a\x00", 9, []byte{recordValueV1, 'n'}}, {"a\x00", 2, append([]byte{recordValueV1}, []byte("child")...)},
				{"b", 10, []byte{recordValueV1, 'n'}}, {"b", 4, append([]byte{recordValueV1}, []byte("four")...)},
			} {
				raw.add(t, []byte(r.key), r.ts, r.value)
			}
			it := &VersionIterator{raw: raw, options: tc.options}
			it.advance()
			var got []Version
			for it.Valid() {
				got = append(got, it.Entry())
				it.NextKey()
			}
			requireVersionLabels(t, got, tc.want...)
			if it.Error() != nil {
				t.Fatal(it.Error())
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVersionIteratorNextKeyFuturePrefixChild(t *testing.T) {
	newSource := func() *nextKeySource {
		raw := new(nextKeySource)
		raw.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
		for ts := uint64(65); ts > 0; ts-- {
			raw.add(t, []byte{'a', 0}, ts, []byte{recordValueV1, byte(ts)})
		}
		return raw
	}
	raw := newSource()
	it := &VersionIterator{raw: raw, options: VersionIteratorOptions{ReadTimestamp: 1}}
	it.advance()
	it.NextKey()
	got := it.Entry()
	if !bytes.Equal(got.Key, []byte{'a', 0}) || got.Timestamp != 1 || !bytes.Equal(got.Value, []byte{1}) || it.Error() != nil {
		t.Fatalf("prefix child=%+v err=%v", got, it.Error())
	}
	// Only the first future key is inspected; a timestamp seek bypasses the
	// remaining future history without reading any rejected payload.
	if raw.nexts != 1 || raw.seeks != 1 || raw.reads != 2 || it.Stats() != (VersionIteratorStats{Visited: 3, Skipped: 1, Retained: 2}) {
		t.Fatalf("nexts=%d seeks=%d reads=%d stats=%+v", raw.nexts, raw.seeks, raw.reads, it.Stats())
	}
	// The legacy range adapter's synthetic successor is this exact prefix-child
	// key, so its timestamp seek also bypasses those future versions. The new
	// method preserves its selected result without an unbounded future walk.
	legacyRaw := newSource()
	legacy := &VersionIterator{raw: legacyRaw, options: VersionIteratorOptions{ReadTimestamp: 1}}
	legacy.advance()
	legacy.Seek([]byte{'a', 0}, 1)
	if old := legacy.Entry(); !bytes.Equal(old.Key, got.Key) || old.Timestamp != got.Timestamp || old.State != got.State || !bytes.Equal(old.Value, got.Value) || legacy.Error() != nil {
		t.Fatalf("legacy=%+v next=%+v err=%v", old, got, legacy.Error())
	}
	if legacyRaw.nexts != 0 || legacyRaw.seeks != 1 || legacyRaw.reads != 2 || legacy.Stats() != (VersionIteratorStats{Visited: 2, Retained: 2}) {
		t.Fatalf("legacy nexts=%d seeks=%d reads=%d stats=%+v", legacyRaw.nexts, legacyRaw.seeks, legacyRaw.reads, legacy.Stats())
	}
	if err := errors.Join(it.Close(), legacy.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestVersionIteratorNextKeyFutureGroupsWithoutAdmittedVersion(t *testing.T) {
	raw := new(nextKeySource)
	raw.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
	for _, key := range [][]byte{[]byte{'a', 0}, []byte("b")} {
		for ts := uint64(65); ts > 1; ts-- {
			raw.add(t, key, ts, []byte{0xff})
		}
	}
	raw.add(t, []byte("c"), 1, []byte{recordValueV1, 'c'})
	it := &VersionIterator{raw: raw, options: VersionIteratorOptions{ReadTimestamp: 1}}
	it.advance()
	it.NextKey()
	if got := it.EntryView(); string(got.Key) != "c" || got.Timestamp != 1 || it.Error() != nil {
		t.Fatalf("next=%+v err=%v", got, it.Error())
	}
	if raw.nexts != 1 || raw.seeks != 2 || raw.reads != 2 || it.Stats() != (VersionIteratorStats{Visited: 4, Skipped: 2, Retained: 2}) {
		t.Fatalf("nexts=%d seeks=%d reads=%d stats=%+v", raw.nexts, raw.seeks, raw.reads, it.Stats())
	}
	_ = it.Close()
}

func TestVersionIteratorNextKeyMaximumFutureTimestampSeek(t *testing.T) {
	raw := new(nextKeySource)
	maximum := bytes.Repeat([]byte{'z'}, mvcckey.MaxEncodedKeySize-19)
	raw.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
	for ts := uint64(65); ts > 0; ts-- {
		raw.add(t, maximum, ts, []byte{recordValueV1, byte(ts)})
	}
	raw.add(t, []byte{255}, 65, []byte{0xff})
	it := &VersionIterator{raw: raw, options: VersionIteratorOptions{ReadTimestamp: 1}}
	it.advance()
	it.NextKey()
	if got := it.EntryView(); !bytes.Equal(got.Key, maximum) || got.Timestamp != 1 || !bytes.Equal(got.Value, []byte{1}) || it.Error() != nil {
		t.Fatalf("max length=%d timestamp=%d err=%v", len(got.Key), got.Timestamp, it.Error())
	}
	it.NextKey()
	if it.Valid() || it.Error() != nil || raw.nexts != 2 || raw.seeks != 2 || raw.reads != 2 || it.Stats() != (VersionIteratorStats{Visited: 4, Skipped: 2, Retained: 2}) {
		t.Fatalf("end valid=%t nexts=%d seeks=%d reads=%d stats=%+v err=%v", it.Valid(), raw.nexts, raw.seeks, raw.reads, it.Stats(), it.Error())
	}
	_ = it.Close()
}

type nextKeyCoalescedSource struct {
	*nextKeySource
	calls              int
	upper              []byte
	limit              int
	malformed, storage error
}

func (s *nextKeyCoalescedSource) AdvanceMVCCKey(upper []byte, limit int) (visited, skipped uint64, malformed, storage error) {
	s.calls++
	s.upper = append(s.upper[:0], upper...)
	s.limit = limit
	if s.malformed != nil || s.storage != nil {
		return 1, 0, s.malformed, s.storage
	}
	// The fixture starts at a@2, with a@1 then b@1. Simulate the optional
	// source operation's final position and accounting, rather than its loop.
	s.index = 2
	return 1, 1, nil, nil
}

func TestVersionIteratorNextKeyUsesCoalescedSource(t *testing.T) {
	raw := &nextKeyCoalescedSource{nextKeySource: new(nextKeySource)}
	raw.add(t, []byte("a"), 2, []byte{recordValueV1, 'a'})
	raw.add(t, []byte("a"), 1, []byte{0xff})
	raw.add(t, []byte("b"), 1, []byte{recordValueV1, 'b'})
	it := &VersionIterator{raw: raw}
	it.advance()
	it.NextKey()
	wantUpper, err := mvcckey.AppendKeyVersionsUpper(nil, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := it.EntryView(); string(got.Key) != "b" || it.Error() != nil || raw.calls != 1 || raw.limit != nextKeyStepLimit || !bytes.Equal(raw.upper, wantUpper) || raw.nexts != 0 || raw.seeks != 0 || raw.reads != 2 || it.Stats() != (VersionIteratorStats{Visited: 3, Skipped: 1, Retained: 2}) {
		t.Fatalf("next=%+v calls=%d limit=%d nexts=%d seeks=%d reads=%d stats=%+v err=%v", got, raw.calls, raw.limit, raw.nexts, raw.seeks, raw.reads, it.Stats(), it.Error())
	}
	_ = it.Close()
}

func TestVersionIteratorNextKeyCoalescedErrorClasses(t *testing.T) {
	for _, kind := range []string{"malformed", "storage"} {
		t.Run(kind, func(t *testing.T) {
			raw := &nextKeyCoalescedSource{nextKeySource: new(nextKeySource)}
			raw.add(t, []byte("a"), 2, []byte{recordValueV1, 'a'})
			injected := errors.New(kind)
			class := ErrMalformedRecord
			if kind == "malformed" {
				raw.malformed = injected
			} else {
				raw.storage = injected
				class = ErrStorage
			}
			it := &VersionIterator{raw: raw}
			it.advance()
			it.NextKey()
			if !errors.Is(it.Error(), class) || !errors.Is(it.Error(), injected) || it.Valid() || raw.calls != 1 || raw.reads != 1 || it.Stats() != (VersionIteratorStats{Visited: 2, Retained: 1}) {
				t.Fatalf("err=%v calls=%d reads=%d stats=%+v", it.Error(), raw.calls, raw.reads, it.Stats())
			}
			_ = it.Close()
		})
	}
}

type nextKeySnapshot struct {
	treedb.Snapshot
	err    error
	closes int
}

func (s *nextKeySnapshot) Close() error { s.closes++; return s.err }

func TestVersionIteratorNextKeyErrorsAndReverse(t *testing.T) {
	t.Run("malformed-key", func(t *testing.T) {
		raw := new(nextKeySource)
		raw.add(t, []byte("a"), math.MaxUint64, []byte{recordValueV1, 'a'})
		prefix, err := mvcckey.AppendKeyVersionsLower(nil, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		raw.keys = append(raw.keys, append(prefix, 0x80))
		raw.values = append(raw.values, []byte{0xff})
		raw.add(t, []byte("b"), 1, []byte{recordValueV1, 'b'})
		it := &VersionIterator{raw: raw}
		it.advance()
		it.NextKey()
		if !errors.Is(it.Error(), ErrMalformedRecord) || !errors.Is(it.Error(), mvcckey.ErrMalformedKey) || raw.reads != 1 || it.Stats().Visited != 2 || it.Stats().Skipped != 0 {
			t.Fatalf("err=%v reads=%d stats=%+v", it.Error(), raw.reads, it.Stats())
		}
	})
	t.Run("discarded-value", func(t *testing.T) {
		raw := new(nextKeySource)
		raw.add(t, []byte("a"), 2, []byte{recordValueV1, 'a'})
		raw.add(t, []byte("a"), 1, []byte{0xff})
		raw.add(t, []byte("b"), 1, []byte{recordValueV1, 'b'})
		it := &VersionIterator{raw: raw}
		it.advance()
		it.NextKey()
		if it.Error() != nil || string(it.EntryView().Key) != "b" || raw.reads != 2 {
			t.Fatalf("err=%v reads=%d", it.Error(), raw.reads)
		}
	})
	for _, operation := range []string{"next", "seek", "future-seek", "value"} {
		t.Run(operation, func(t *testing.T) {
			raw := new(nextKeySource)
			depth := 1
			if operation == "seek" {
				depth = nextKeyStepLimit + 2
			}
			for ts := depth; ts > 0; ts-- {
				raw.add(t, []byte("a"), uint64(ts), []byte{recordValueV1, 'a'})
			}
			if operation == "future-seek" {
				raw.add(t, []byte("b"), 2, []byte{recordValueV1, 'f'})
			}
			raw.add(t, []byte("b"), 1, []byte{recordValueV1, 'b'})
			injected := errors.New(operation + " failure")
			switch operation {
			case "next":
				raw.nextErr = injected
			case "seek", "future-seek":
				raw.seekErr = injected
			case "value":
				raw.valueErr = injected
				raw.valueErrAt = depth
			}
			it := &VersionIterator{raw: raw}
			if operation == "future-seek" {
				it.options.ReadTimestamp = 1
			}
			it.advance()
			it.NextKey()
			if !errors.Is(it.Error(), ErrStorage) || !errors.Is(it.Error(), injected) || it.Valid() {
				t.Fatalf("valid=%t err=%v", it.Valid(), it.Error())
			}
			raw.closeErr = errors.New("raw close")
			snapshot := &nextKeySnapshot{err: errors.New("snapshot close")}
			it.snapshot = snapshot
			if err := it.Close(); !errors.Is(err, raw.closeErr) || !errors.Is(err, snapshot.err) || snapshot.closes != 1 {
				t.Fatalf("close=%v count=%d", err, snapshot.closes)
			}
			it.NextKey()
			_ = it.Close()
			if snapshot.closes != 1 {
				t.Fatal("double snapshot close")
			}
		})
	}
	t.Run("reverse", func(t *testing.T) {
		raw := new(nextKeySource)
		raw.add(t, []byte("a"), 1, []byte{recordValueV1, 'a'})
		it := &VersionIterator{raw: raw, options: VersionIteratorOptions{Reverse: true}}
		it.advance()
		it.NextKey()
		if !errors.Is(it.Error(), ErrNextKeyReverse) || it.Valid() || raw.nexts != 0 || raw.seeks != 0 || raw.reads != 1 {
			t.Fatalf("err=%v source=%+v", it.Error(), raw)
		}
	})
}

func TestVersionIteratorNextKeyOrderedPinnedCutAndFloor(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.SupportsLogicalMVCC() || !db.SupportsMVCCReadCut() {
		t.Fatal("ordered original-cut route inactive")
	}
	store := New(db)
	commitHistory(t, store, "a", MutationAt{12, []byte("future-a"), false}, MutationAt{3, []byte("old-a"), false})
	commitHistory(t, store, "a\x00", MutationAt{9, []byte("future-child"), false}, MutationAt{2, []byte("old-child"), false})
	commitHistory(t, store, "b", MutationAt{10, []byte("future-b"), false}, MutationAt{4, []byte("old-b"), false})
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitGroupAt([]CommitGroup{{Timestamp: 7, Mutations: []Mutation{{Key: []byte("a"), Delete: true}}}, {Timestamp: 6, Mutations: []Mutation{{Key: []byte("b"), Value: []byte("tail-b")}}}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	old, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, ok := old.raw.(mvccKeyAdvancer); !ok {
		t.Fatal("original ordered snapshot lacks the coalesced group advance capability")
	}
	first := old.Entry()
	if err = store.CommitGroupAt([]CommitGroup{{Timestamp: 8, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("new-a")}, {Key: []byte("a\x00"), Value: []byte("new-child")}, {Key: []byte("b"), Delete: true}}}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err = store.AdvanceDiscardFloor(8, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	var got []Version
	for old.Valid() {
		got = append(got, old.Entry())
		old.NextKey()
	}
	if old.Error() != nil {
		t.Fatal(old.Error())
	}
	requireVersionLabels(t, got, "a@7:2:", "a\x00@2:1:old-child", "b@6:1:tail-b")
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if first.State != Tombstone || string(first.Key) != "a" || first.Timestamp != 7 {
		t.Fatalf("owned first=%+v", first)
	}
	if _, err = store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 8}); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("new floor admission=%v", err)
	}
}
