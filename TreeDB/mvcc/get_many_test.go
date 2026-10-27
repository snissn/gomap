package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func readBatchFixture(t testing.TB, pointers bool) (*treedb.DB, *Store, [][]byte) {
	t.Helper()
	db := c3ReadDB(t, treedb.ProfileCommandWALRelaxed, pointers)
	st := New(db)
	muts := make([]Mutation, 2048)
	for i := range muts {
		muts[i] = Mutation{Key: []byte(fmt.Sprintf("batch-%06d", i)), Value: []byte(fmt.Sprintf("value-%06d", i))}
	}
	if err := st.CommitAt(10, muts, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	keys := make([][]byte, MaxGetManyAtKeys)
	for i := range keys {
		keys[i] = append([]byte(nil), muts[256+i].Key...)
	}
	return db, st, keys
}

func batchCacheReads(t testing.TB, db *treedb.DB) uint64 {
	t.Helper()
	stats := db.Stats()
	var n uint64
	for _, key := range []string{"hits", "misses"} {
		v, e := strconv.ParseUint(stats["treedb.process.read_path.outer_leaf.cache."+key], 10, 64)
		if e != nil {
			t.Fatalf("missing cache counter %s: %v", key, e)
		}
		n += v
	}
	return n
}

func TestGetManyAtPublicLeafReuse(t *testing.T) {
	db, s, keys := readBatchFixture(t, false)
	// Public reads establish real producer/cache state, never inject page credit.
	for _, key := range keys {
		if _, e := s.GetAt(key, 15); e != nil {
			t.Fatal(e)
		}
	}
	before := batchCacheReads(t, db)
	for _, key := range keys {
		if _, e := s.GetAt(key, 15); e != nil {
			t.Fatal(e)
		}
	}
	scalar := batchCacheReads(t, db) - before
	before = batchCacheReads(t, db)
	got, e := s.GetManyAt(keys, 15)
	if e != nil {
		t.Fatal(e)
	}
	batch := batchCacheReads(t, db) - before
	for i, r := range got {
		if r.Err != nil || r.Result.Timestamp != 10 || string(r.Result.Value) != fmt.Sprintf("value-%06d", 256+i) {
			t.Fatalf("%d: %+v", i, r)
		}
	}
	t.Logf("public actual leaf cache callbacks: scalar=%d batch=%d", scalar, batch)
	if scalar < 32 || batch >= scalar/2 {
		t.Fatalf("leaf reuse inactive: scalar=%d batch=%d", scalar, batch)
	}
}

func TestGetManyAtScalarOracleAndOwnership(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		t.Run(fmt.Sprint(pointers), func(t *testing.T) {
			db, s, keys := readBatchFixture(t, pointers)
			muts := make([]Mutation, 0, len(keys))
			for i, key := range keys {
				muts = append(muts, Mutation{Key: key, Value: bytes.Repeat([]byte{byte(i)}, 257), Delete: i%7 == 0})
			}
			if e := s.CommitAt(20, muts, CommitRelaxed); e != nil {
				t.Fatal(e)
			}
			// Mixed physical mask, duplicate, absent, binary and unordered probes.
			physical, _ := mvcckey.Encode(keys[3], 20)
			if e := db.Delete(physical); e != nil {
				t.Fatal(e)
			}
			keys[5] = keys[4]
			keys[6] = []byte("missing")
			keys[7] = []byte{0, 255}
			for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
				keys[i], keys[j] = keys[j], keys[i]
			}
			for _, ts := range []uint64{5, 15, 25} {
				got, e := s.GetManyAt(keys, ts)
				if e != nil {
					t.Fatal(e)
				}
				for i, key := range keys {
					want, e := s.GetAt(key, ts)
					if e != nil || got[i].Err != nil || !reflect.DeepEqual(got[i].Result, want) {
						t.Fatalf("ts%d key%x got%+v want%+v err%v", ts, key, got[i], want, e)
					}
				}
			}
			got, e := s.GetManyAt(keys, 25)
			if e != nil {
				t.Fatal(e)
			}
			saved := make([][]byte, len(got))
			for i, r := range got {
				saved[i] = append([]byte(nil), r.Result.Value...)
			}
			if e := db.Checkpoint(); e != nil {
				t.Fatal(e)
			}
			if _, e := s.GetManyAt(keys, 15); e != nil {
				t.Fatal(e)
			}
			for i, r := range got {
				if !bytes.Equal(r.Result.Value, saved[i]) {
					t.Fatalf("value aliases scratch %d", i)
				}
			}
		})
	}
}

func TestGetManyAtErrorsAndValidation(t *testing.T) {
	var invalid *Store
	if _, e := invalid.GetManyAt(nil, 0); !errors.Is(e, ErrZeroTimestamp) {
		t.Fatal(e)
	}
	if _, e := invalid.GetManyAt(nil, 1); !errors.Is(e, treedb.ErrClosed) {
		t.Fatal(e)
	}
	db := c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false)
	s := New(db)
	if r, e := s.GetManyAt(nil, 1); e != nil || len(r) != 0 {
		t.Fatal(r, e)
	}
	if _, e := s.GetManyAt(make([][]byte, 33), 1); !errors.Is(e, ErrBatchTooLarge) {
		t.Fatal(e)
	}
	if e := s.CommitAt(10, []Mutation{{Key: []byte("good"), Value: []byte("v")}}, CommitRelaxed); e != nil {
		t.Fatal(e)
	}
	bad := bytes.Repeat([]byte{'x'}, mvcckey.MaxEncodedKeySize+1)
	got, e := s.GetManyAt([][]byte{[]byte("good"), bad, []byte("absent")}, 15)
	if e != nil || got[0].Err != nil || !errors.Is(got[1].Err, ErrInvalidKey) || got[2].Result.State != Absent {
		t.Fatal(got, e)
	}
	physical, _ := mvcckey.Encode([]byte("bad-record"), 10)
	if e := db.Set(physical, []byte{255}); e != nil {
		t.Fatal(e)
	}
	got, e = s.GetManyAt([][]byte{[]byte("good"), []byte("bad-record")}, 15)
	if e != nil || got[0].Err != nil || !errors.Is(got[1].Err, ErrMalformedRecord) {
		t.Fatal(got, e)
	}
	if e := s.AdvanceDiscardFloor(15, CommitRelaxed); e != nil {
		t.Fatal(e)
	}
	if _, e = s.GetManyAt([][]byte{[]byte("good"), []byte("absent")}, 15); !errors.Is(e, ErrReadBeforeDiscardFloor) {
		t.Fatal(e)
	}
}

func TestGetManyAtLegacyCutFallbackAndCloseError(t *testing.T) {
	d := &exactProbeDB{DB: c3ReadDB(t, treedb.ProfileCommandWALRelaxed, false), closeErr: errors.New("batch close")}
	s := newStore(d)
	if e := s.CommitAt(1, []Mutation{{Key: []byte("a"), Value: []byte("a")}, {Key: []byte("b"), Value: []byte("b")}}, CommitRelaxed); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetManyAt([][]byte{[]byte("b"), []byte("a")}, 2)
	if !errors.Is(e, d.closeErr) || len(got) != 2 || string(got[0].Result.Value) != "b" || d.captures != 1 || d.closes != 1 || d.probes != 2 {
		t.Fatalf("%+v err%v captures%d closes%d probes%d", got, e, d.captures, d.closes, d.probes)
	}
	d.closeErr = nil
	d.failProbe = d.probes + 1
	d.probeErr = errors.New("batch read")
	got, e = s.GetManyAt([][]byte{[]byte("a"), []byte("b")}, 2)
	if e != nil || !errors.Is(got[0].Err, d.probeErr) || got[1].Err != nil || d.probes != 4 {
		t.Fatal(got, e, d.probes)
	}
}

func TestGetManyAtConcurrentReuse(t *testing.T) {
	_, s, keys := readBatchFixture(t, true)
	var wg sync.WaitGroup
	for n := 0; n < 6; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 12; n++ {
				got, e := s.GetManyAt(keys, 15)
				if e != nil {
					t.Error(e)
					return
				}
				for _, r := range got {
					if r.Err != nil || r.Result.Timestamp != 10 {
						t.Errorf("bad result %+v", r)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

type readBatchGateDB struct {
	*treedb.DB
	before               func()
	captureErr, closeErr error
}

func (d *readBatchGateDB) AcquireMVCCReadCut() (treedb.MVCCReadSnapshot, error) {
	if d.captureErr != nil {
		return nil, d.captureErr
	}
	cut, e := d.DB.AcquireMVCCReadCut()
	if e != nil {
		return nil, e
	}
	return &readBatchGateCut{MVCCReadSnapshot: cut, owner: d}, nil
}

type readBatchGateCut struct {
	treedb.MVCCReadSnapshot
	owner *readBatchGateDB
}

func (c *readBatchGateCut) SeekGEVersionRanges(a, b [][]byte, r []treedb.MVCCVersionRangeResult) error {
	if c.owner.before != nil {
		f := c.owner.before
		c.owner.before = nil
		f()
	}
	return c.MVCCReadSnapshot.(treedb.MVCCReadBatchSnapshot).SeekGEVersionRanges(a, b, r)
}
func (c *readBatchGateCut) Close() error {
	return errors.Join(c.MVCCReadSnapshot.Close(), c.owner.closeErr)
}

func TestGetManyAtCapturedCutSurvivesFloorPrune(t *testing.T) {
	db, _, keys := readBatchFixture(t, true)
	d := &readBatchGateDB{DB: db}
	s := newStore(d)
	d.before = func() {
		if e := s.CommitAt(20, []Mutation{{Key: keys[0], Value: []byte("new")}}, CommitRelaxed); e != nil {
			t.Fatal(e)
		}
		if e := s.AdvanceDiscardFloor(18, CommitRelaxed); e != nil {
			t.Fatal(e)
		}
		if _, e := s.PruneVersions(PruneOptions{BatchSize: 64, Mode: CommitRelaxed}); e != nil {
			t.Fatal(e)
		}
	}
	got, e := s.GetManyAt(keys, 15)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range got {
		if r.Err != nil || r.Result.Timestamp != 10 {
			t.Fatal(r)
		}
	}
	if _, e := s.GetManyAt(keys, 15); !errors.Is(e, ErrReadBeforeDiscardFloor) {
		t.Fatal(e)
	}
	d.captureErr = errors.New("capture refused")
	if _, e := s.GetManyAt(keys, 25); !errors.Is(e, d.captureErr) {
		t.Fatal(e)
	}
	d.captureErr = nil
	d.closeErr = errors.New("actual batch close")
	if r, e := s.GetManyAt(keys, 25); !errors.Is(e, d.closeErr) || len(r) != len(keys) {
		t.Fatal(e, len(r))
	}
}

func TestGetManyAtMalformedSuffix(t *testing.T) {
	db, s, _ := readBatchFixture(t, false)
	key := []byte("batch-malformed")
	physical, _ := mvcckey.Encode(key, 12)
	physical = append(physical, 0)
	if e := db.Set(physical, []byte{recordValueV1, 'x'}); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetManyAt([][]byte{key, []byte("absent")}, 15)
	if e != nil || !errors.Is(got[0].Err, ErrMalformedRecord) || got[1].Err != nil {
		t.Fatal(got, e)
	}
}

func BenchmarkGetManyAt(b *testing.B) {
	_, s, dense := readBatchFixture(b, false)
	for _, shape := range []string{"dense", "random", "singleton"} {
		keys := make([][]byte, len(dense))
		copy(keys, dense)
		if shape == "random" {
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("batch-%06d", (i*61)%2048))
			}
		}
		if shape == "singleton" {
			keys = keys[:1]
		}
		for _, batch := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/batch=%t", shape, batch), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if batch {
						r, e := s.GetManyAt(keys, 15)
						if e != nil {
							b.Fatal(e)
						}
						for _, v := range r {
							if v.Err != nil {
								b.Fatal(v.Err)
							}
						}
					} else {
						for _, k := range keys {
							if _, e := s.GetAt(k, 15); e != nil {
								b.Fatal(e)
							}
						}
					}
				}
			})
		}
	}
}
