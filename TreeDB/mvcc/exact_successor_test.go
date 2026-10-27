package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

type exactProbeDB struct {
	*treedb.DB
	probes, opens, captures, closes int
	failProbe                       int
	probeErr, openErr, closeErr     error
}

func (d *exactProbeDB) AcquireMVCCReadCut() (treedb.MVCCReadSnapshot, error) {
	d.captures++
	c, e := d.DB.AcquireMVCCReadCut()
	if e != nil {
		return nil, e
	}
	return &exactProbeCut{MVCCReadSnapshot: c, d: d}, nil
}

type exactProbeCut struct {
	treedb.MVCCReadSnapshot
	d *exactProbeDB
}

func (c *exactProbeCut) SeekGEVersionRange(a, b []byte) ([]byte, []byte, bool, error) {
	c.d.probes++
	if c.d.probes == c.d.failProbe {
		return nil, nil, false, c.d.probeErr
	}
	return c.MVCCReadSnapshot.SeekGEVersionRange(a, b)
}
func (c *exactProbeCut) Iterator(a, b []byte) (treedb.Iterator, error) {
	c.d.opens++
	if c.d.openErr != nil {
		return nil, c.d.openErr
	}
	return c.MVCCReadSnapshot.Iterator(a, b)
}
func (c *exactProbeCut) Close() error {
	c.d.closes++
	return errors.Join(c.MVCCReadSnapshot.Close(), c.d.closeErr)
}

func TestExactSuccessorLifecycle(t *testing.T) {
	for _, depth := range []int{0, 1, 2, 32} {
		for _, ts := range []uint64{0, math.MaxUint64, 100} {
			t.Run(fmt.Sprintf("%d/%d", depth, ts), func(t *testing.T) {
				db := &exactProbeDB{DB: c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false)}
				s := newStore(db)
				key := []byte{'a', 0, 255}
				for n := 1; n <= depth; n++ {
					if err := s.CommitAt(uint64(n), []Mutation{{Key: key, Value: []byte{byte(n)}, Delete: n%3 == 0}}, CommitRelaxed); err != nil {
						t.Fatal(err)
					}
				}
				it, err := s.IterateVersions(VersionIteratorOptions{ExactKey: key, ReadTimestamp: ts})
				if err != nil {
					t.Fatal(err)
				}
				seen := 0
				for it.Valid() {
					if it.EntryView().Timestamp != uint64(depth-seen) {
						t.Fatal(it.EntryView())
					}
					seen++
					it.Next()
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				if seen != depth {
					t.Fatalf("seen%d depth%d", seen, depth)
				}
				if got := it.Stats(); got != (VersionIteratorStats{Visited: uint64(depth), Retained: uint64(depth)}) {
					t.Fatal(got)
				}
				probes, opens := 1, 0
				if depth > 0 {
					probes = 2
				}
				if depth > 1 {
					opens = 1
				}
				if ts == 100 {
					probes, opens = 0, 1
				}
				if db.probes != probes || db.opens != opens || db.captures != 1 {
					t.Fatalf("probes/opens/captures=%d/%d/%d", db.probes, db.opens, db.captures)
				}
				it.Seek(key, math.MaxUint64)
				it.Seek(key, math.MaxUint64)
				if depth > 0 {
					it.NextKey()
					if it.Valid() {
						t.Fatal("NextKey escaped exact domain")
					}
					it.Seek(key, math.MaxUint64)
					if !it.Valid() {
						t.Fatal(it.Error())
					}
				}
				if db.opens != 1 {
					t.Fatalf("raw opens %d", db.opens)
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				if db.closes != 1 {
					t.Fatal(db.closes)
				}
				if got := db.Stats()["treedb.cache.cow.views"]; got != "0" {
					t.Fatalf("views leaked: %s", got)
				}
			})
		}
	}
}

func TestExactSuccessorErrorsDoNotRetry(t *testing.T) {
	injected := errors.New("injected storage/integrity failure")
	cleanup := errors.New("close failure")
	for _, fail := range []int{1, 2} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			db := &exactProbeDB{DB: c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false), failProbe: fail, probeErr: injected, closeErr: cleanup}
			s := newStore(db)
			commitHistory(t, s, "a", MutationAt{1, []byte("one"), false})
			it, err := s.IterateVersions(VersionIteratorOptions{ExactKey: []byte("a")})
			if fail == 1 {
				if it != nil || !errors.Is(err, ErrStorage) || !errors.Is(err, injected) || !errors.Is(err, cleanup) {
					t.Fatalf("it%v err%v", it, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				it.Next()
				it.Next()
				if !errors.Is(it.Error(), ErrStorage) || !errors.Is(it.Error(), injected) {
					t.Fatal(it.Error())
				}
				if !errors.Is(it.Close(), cleanup) {
					t.Fatal("lost close error")
				}
			}
			if db.probes != fail || db.opens != 0 || db.closes != 1 {
				t.Fatalf("probe/open/close %d/%d/%d", db.probes, db.opens, db.closes)
			}
		})
	}
	t.Run("transition", func(t *testing.T) {
		db := &exactProbeDB{DB: c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false), openErr: injected}
		s := newStore(db)
		commitHistory(t, s, "a", MutationAt{1, []byte("one"), false}, MutationAt{2, []byte("two"), false})
		it, err := s.IterateVersions(VersionIteratorOptions{ExactKey: []byte("a")})
		if err != nil {
			t.Fatal(err)
		}
		it.Next()
		it.Next()
		if !errors.Is(it.Error(), injected) || db.opens != 1 || db.probes != 2 {
			t.Fatalf("err%v opens%d probes%d", it.Error(), db.opens, db.probes)
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if db.closes != 1 {
			t.Fatal(db.closes)
		}
	})
}

func TestExactSuccessorRawOracle(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		t.Run(fmt.Sprint(pointers), func(t *testing.T) {
			db := c3ReadDB(t, treedb.ProfileCommandWALRelaxed, pointers)
			s := newStore(db)
			keys := [][]byte{{}, {'a', 0, 255}}
			for _, key := range keys {
				for n := 1; n <= 8; n++ {
					if err := s.CommitAt(uint64(n), []Mutation{{Key: key, Value: bytes.Repeat([]byte{byte(n)}, 256), Delete: n%3 == 0}}, CommitRelaxed); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, bytes.Repeat([]byte{'z'}, mvcckey.MaxEncodedKeySize-19))
			for _, key := range keys {
				for _, ts := range []uint64{0, math.MaxUint64, 5} {
					opts := VersionIteratorOptions{ExactKey: key, ReadTimestamp: ts}
					want := exactGenericOracle(t, s, opts, false, nil, 0)
					it, err := s.IterateVersions(opts)
					if err != nil {
						t.Fatal(err)
					}
					got := collectVersions(t, it)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("key%x ts%d got%v want%v", key, ts, got, want)
					}
				}
			}
		})
	}
}

func TestExactSuccessorMalformedExtensionAndRecord(t *testing.T) {
	for _, malformedKey := range []bool{false, true} {
		t.Run(fmt.Sprint(malformedKey), func(t *testing.T) {
			db := c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false)
			s := newStore(db)
			key := []byte("a")
			commitHistory(t, s, "a", MutationAt{1, []byte("one"), false}, MutationAt{3, []byte("three"), false})
			physical, err := mvcckey.Append(nil, key, 2)
			if err != nil {
				t.Fatal(err)
			}
			record := []byte{0xff}
			if malformedKey {
				physical, err = mvcckey.Append(nil, key, 3)
				if err != nil {
					t.Fatal(err)
				}
				physical = append(physical, 0)
				record = []byte{recordValueV1, 'x'}
			}
			if err := db.Set(physical, record); err != nil {
				t.Fatal(err)
			}
			it, err := s.IterateVersions(VersionIteratorOptions{ExactKey: key})
			if err != nil {
				t.Fatal(err)
			}
			it.Next()
			if !errors.Is(it.Error(), ErrMalformedRecord) {
				t.Fatal(it.Error())
			}
			if it.Stats().Visited != 2 || it.Stats().Retained != 1 {
				t.Fatal(it.Stats())
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExactSuccessorOldCutPruneAndPhysicalMask(t *testing.T) {
	db := c3ReadDB(t, treedb.ProfileCommandWALRelaxed, true)
	s := newStore(db)
	key := []byte("a")
	commitHistory(t, s, "a", MutationAt{1, []byte("old"), false}, MutationAt{2, []byte("new"), false})
	it, err := s.IterateVersions(VersionIteratorOptions{ExactKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AdvanceDiscardFloor(3, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitRelaxed}); err != nil {
		t.Fatal(err)
	}
	requireVersionLabels(t, collectVersions(t, it), "a@2:1:new", "a@1:1:old")
	// A physical mask shadows the persisted root; it is not a logical tombstone.
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	physical, err := mvcckey.Append(nil, key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(physical); err != nil {
		t.Fatal(err)
	}
	it, err = s.IterateVersions(VersionIteratorOptions{ExactKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if got := collectVersions(t, it); len(got) != 0 {
		t.Fatal(got)
	}
}

// The physical codec accepts keys larger than a persisted leaf page. Exercise
// the maximum encoded key through an immutable cut without asking the backend
// to persist an unsupported oversized leaf entry.
type exactSyntheticCut struct {
	treedb.MVCCReadSnapshot
	source    *nextKeySource
	lastStart []byte
	readErr   error
	reads     int
}

func (c *exactSyntheticCut) SeekGEVersionRange(a, b []byte) ([]byte, []byte, bool, error) {
	c.lastStart = append(c.lastStart[:0], a...)
	for i, key := range c.source.keys {
		if bytes.Compare(key, a) >= 0 && bytes.Compare(key, b) < 0 {
			c.reads++
			if c.readErr != nil {
				return nil, nil, false, c.readErr
			}
			return append([]byte(nil), key...), append([]byte(nil), c.source.values[i]...), true, nil
		}
	}
	return nil, nil, false, nil
}
func (c *exactSyntheticCut) Iterator(a, b []byte) (treedb.Iterator, error) {
	c.source.index = 0
	return c.source, nil
}
func (c *exactSyntheticCut) Close() error { return nil }
func TestExactSuccessorMaximumPhysicalKey(t *testing.T) {
	key := bytes.Repeat([]byte{'z'}, mvcckey.MaxEncodedKeySize-19)
	for _, depth := range []int{1, 2} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			source := new(nextKeySource)
			for ts := depth; ts > 0; ts-- {
				source.add(t, key, uint64(ts), []byte{recordValueV1, 'x'})
			}
			cut := &exactSyntheticCut{source: source}
			_, upper, err := versionPhysicalBounds(VersionIteratorOptions{ExactKey: key})
			if err != nil {
				t.Fatal(err)
			}
			it := &VersionIterator{snapshot: cut, options: VersionIteratorOptions{ExactKey: key}, exactSuccessor: true, seekBuf: upper}
			it.selectExactSuccessor(append([]byte(nil), source.keys[0]...), append([]byte(nil), source.values[0]...))
			first := append([]byte(nil), source.keys[0]...)
			it.Next()
			if !bytes.Equal(cut.lastStart, append(first, 0)) {
				t.Fatal("second lookup skipped strict physical successor")
			}
			for it.Valid() {
				it.Next()
			}
			if err := it.Error(); err != nil {
				t.Fatal(err)
			}
			if it.Stats().Retained != uint64(depth) {
				t.Fatal(it.Stats())
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExactSuccessorDoubleCorruptionPrecedence(t *testing.T) {
	// Owned successor materialization can fail before returning a malformed key.
	// Preserve the real storage error rather than retrying through the raw path.
	source := new(nextKeySource)
	source.add(t, []byte("a"), 1, []byte{recordValueV1, 'x'})
	source.keys[0] = append(source.keys[0], 0)
	integrity := errors.New("value CRC failure")
	cut := &exactSyntheticCut{source: source, readErr: integrity}
	lower, upper, err := versionPhysicalBounds(VersionIteratorOptions{ExactKey: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = cut.SeekGEVersionRange(lower, upper)
	if !errors.Is(err, integrity) || cut.reads != 1 {
		t.Fatal(err)
	}
	// Ordinary raw decoding rejects the malformed physical key without Value.
	raw := &VersionIterator{raw: source}
	raw.advance()
	if !errors.Is(raw.Error(), ErrMalformedRecord) || source.reads != 0 {
		t.Fatalf("err%v reads%d", raw.Error(), source.reads)
	}
}

func TestExactSuccessorBoundsAndOptionOwnership(t *testing.T) {
	db := c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false)
	s := New(db)
	for _, key := range [][]byte{{}, {'a', 0}, {'a', 0, 'b'}, []byte("z")} {
		if err := s.CommitAt(1, []Mutation{{Key: key, Value: []byte("value")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	for _, opts := range []VersionIteratorOptions{
		{ExactKey: []byte{}},
		{ExactKey: []byte{'a', 0}, Prefix: []byte("a"), LowerBound: []byte("a"), UpperBound: []byte("b")},
		{ExactKey: []byte{'a', 0}, Prefix: []byte("z")},
		{ExactKey: []byte{'a', 0}, LowerBound: []byte("z")},
		{ExactKey: []byte{'a', 0}, UpperBound: []byte{'a', 0}},
		{ExactKey: []byte{'a', 0}, LowerBound: []byte("z"), UpperBound: []byte("a")},
	} {
		want := exactGenericOracle(t, s, opts, false, nil, 0)
		it, err := s.IterateVersions(opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range [][]byte{opts.ExactKey, opts.Prefix, opts.LowerBound, opts.UpperBound} {
			if len(v) > 0 {
				v[0] ^= 255
			}
		}
		got := collectVersions(t, it)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got%v want%v", got, want)
		}
	}
}
