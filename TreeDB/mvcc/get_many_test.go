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
	if len(got) != len(keys) {
		t.Fatalf("results=%d keys=%d", len(got), len(keys))
	}
	for i, r := range got {
		if r.Err != nil || r.Result.State != Present || r.Result.Timestamp != 10 || string(r.Result.Value) != fmt.Sprintf("value-%06d", 256+i) {
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
		counts := []int{2, 4, 8, 16, 32}
		if shape == "singleton" {
			counts = []int{1}
		}
		for _, count := range counts {
			keys := keys[:count]
			for _, batch := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/keys=%d/batch=%t", shape, count, batch), func(b *testing.B) {
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
}

// These adapters retain scalar operation boundaries, including legacy iterator
// cleanup. They deliberately expose no batch implementation.
type getManyErrorDB struct {
	treeDB
	key, record                []byte
	found                      bool
	readErr, closeErr, openErr error
	closes, reads              int
}

func (d *getManyErrorDB) Iterator(_, _ []byte) (treedb.Iterator, error) {
	if d.openErr != nil {
		return nil, d.openErr
	}
	return &getManyErrorIterator{d: d}, nil
}

type getManyErrorIterator struct {
	treedb.Iterator
	d *getManyErrorDB
}

func (i *getManyErrorIterator) Valid() bool   { return i.d.found }
func (i *getManyErrorIterator) Key() []byte   { return i.d.key }
func (i *getManyErrorIterator) Value() []byte { return i.d.record }
func (i *getManyErrorIterator) Error() error  { i.d.reads++; return i.d.readErr }
func (i *getManyErrorIterator) Close() error  { i.d.closes++; return i.d.closeErr }

type getManyErrorCutDB struct {
	*getManyErrorDB
	captureErr error
	nilCut     bool
}

func (*getManyErrorCutDB) SupportsMVCCReadCut() bool { return true }
func (d *getManyErrorCutDB) AcquireMVCCReadCut() (treedb.MVCCReadSnapshot, error) {
	if d.captureErr != nil {
		return nil, d.captureErr
	}
	if d.nilCut {
		return nil, nil
	}
	return &getManyErrorCut{d: d.getManyErrorDB}, nil
}

type getManyErrorCut struct {
	treedb.MVCCReadSnapshot
	d *getManyErrorDB
}

func (c *getManyErrorCut) SeekGEVersionRange(_, _ []byte) ([]byte, []byte, bool, error) {
	c.d.reads++
	return bytes.Clone(c.d.key), bytes.Clone(c.d.record), c.d.found, c.d.readErr
}
func (c *getManyErrorCut) Close() error { c.d.closes++; return c.d.closeErr }

func TestGetManyAtScalarErrorProvenance(t *testing.T) {
	physical, e := mvcckey.Encode([]byte("key"), 7)
	if e != nil {
		t.Fatal(e)
	}
	readErr, closeErr := errors.New("scalar read failure"), errors.New("scalar cleanup failure")
	for _, qualified := range []bool{false, true} {
		for _, n := range []int{1, 2} {
			for _, cleanup := range []bool{false, true} {
				for _, tc := range []struct {
					name             string
					found            bool
					record           []byte
					readErr, itemErr error
					want             Result
				}{
					{name: "present", found: true, record: []byte{recordValueV1, 'v'}, want: Result{State: Present, Timestamp: 7, Value: []byte("v")}},
					{name: "absent", want: Result{State: Absent}},
					{name: "tombstone", found: true, record: []byte{recordTombstoneV1}, want: Result{State: Tombstone, Timestamp: 7}},
					{name: "seek", readErr: readErr, itemErr: readErr},
					{name: "decode", found: true, record: []byte{255}, itemErr: ErrMalformedRecord},
				} {
					t.Run(fmt.Sprintf("qualified%t/n%d/close%t/%s", qualified, n, cleanup, tc.name), func(t *testing.T) {
						d := &getManyErrorDB{key: physical, record: tc.record, found: tc.found, readErr: tc.readErr}
						var db treeDB = d
						if qualified {
							db = &getManyErrorCutDB{getManyErrorDB: d}
						}
						if cleanup {
							d.closeErr = closeErr
						}
						s := newStore(db)
						s.floorLoaded = true
						input := make([][]byte, n)
						for i := range input {
							input[i] = []byte("key")
						}
						got, callErr := s.GetManyAt(input, 9)
						if (callErr != nil) != cleanup || cleanup && !errors.Is(callErr, closeErr) {
							t.Fatalf("call error %v", callErr)
						}
						if callErr != nil && errors.Is(callErr, readErr) {
							t.Fatalf("read error escaped per-item channel: %v", callErr)
						}
						if len(got) != n {
							t.Fatalf("results=%d want%d", len(got), n)
						}
						for i, r := range got {
							if !reflect.DeepEqual(r.Result, tc.want) || !errors.Is(r.Err, tc.itemErr) || cleanup && errors.Is(r.Err, closeErr) {
								t.Fatalf("item%d=%+v want%+v itemErr%v", i, r, tc.want, tc.itemErr)
							}
						}
						wantCloses := n
						if qualified && n > 1 {
							wantCloses = 1
						}
						if d.closes != wantCloses || d.reads != n {
							t.Fatalf("closes/reads=%d/%d want%d/%d", d.closes, d.reads, wantCloses, n)
						}
						_, scalarErr := s.GetAt([]byte("key"), 9)
						if !errors.Is(scalarErr, tc.itemErr) && tc.itemErr != nil || cleanup && !errors.Is(scalarErr, closeErr) {
							t.Fatalf("GetAt lost cause: %v", scalarErr)
						}
						var wantRead, wantClose error
						if tc.name == "seek" {
							op := "iterate"
							if qualified {
								op = "seek version"
							}
							wantRead = storageError(op, readErr)
						} else if tc.name == "decode" {
							wantRead = fmt.Errorf("%w: unknown envelope tag 0xff", ErrMalformedRecord)
						}
						if cleanup {
							op := "close iterator"
							if qualified {
								op = "close read cut"
							}
							wantClose = storageError(op, closeErr)
						}
						wantError := errors.Join(wantRead, wantClose)
						if (scalarErr == nil) != (wantError == nil) || scalarErr != nil && scalarErr.Error() != wantError.Error() {
							t.Fatalf("GetAt text=%v want%v", scalarErr, wantError)
						}
					})
				}
			}
		}
	}
}

func TestGetManyAtScalarCallFailures(t *testing.T) {
	failure := errors.New("scalar admission failure")
	for _, n := range []int{1, 2} {
		for _, tc := range []string{"capture", "nil-cut", "open", "floor", "floor-record"} {
			t.Run(fmt.Sprintf("n%d/%s", n, tc), func(t *testing.T) {
				d := &getManyErrorDB{}
				var db treeDB = d
				want := failure
				switch tc {
				case "capture":
					db = &getManyErrorCutDB{getManyErrorDB: d, captureErr: failure}
				case "nil-cut":
					db = &getManyErrorCutDB{getManyErrorDB: d, nilCut: true}
					want = treedb.ErrClosed
				case "open":
					d.openErr = failure
				case "floor":
					want = ErrReadBeforeDiscardFloor
				case "floor-record":
					d.key = discardFloorKey
					d.record = []byte{255}
					d.found = true
					want = ErrMalformedRecord
				}
				s := newStore(db)
				s.floorLoaded = true
				if tc == "floor" {
					s.discardFloor = 9
				}
				if tc == "floor-record" {
					s.floorLoaded = false
				}
				keys := make([][]byte, n)
				for i := range keys {
					keys[i] = []byte("key")
				}
				got, err := s.GetManyAt(keys, 9)
				if !errors.Is(err, want) {
					t.Fatalf("got%+v callErr%v want%v", got, err, want)
				}
				if n == 1 && got != nil {
					t.Fatalf("pre-read admission failure produced singleton results: %+v", got)
				}
				for _, r := range got {
					if r.Err != nil {
						t.Fatalf("call failure became item error: %v", r.Err)
					}
				}
			})
		}
	}
}

type getManyTypedFailure struct{ message string }

func (e *getManyTypedFailure) Error() string { return e.message }

func TestGetManyAtNestedReadErrorStaysPerItem(t *testing.T) {
	failure := &getManyTypedFailure{message: "nested source open failure"}
	inner := newStore(&getManyErrorDB{openErr: failure})
	inner.floorLoaded = true
	_, nested := inner.GetAt([]byte("key"), 9)
	var typed *getManyTypedFailure
	if !errors.As(nested, &typed) || typed != failure || nested.Error() != storageError("open iterator", failure).Error() {
		t.Fatalf("GetAt changed text or cause: %v", nested)
	}
	for _, qualified := range []bool{false, true} {
		for _, n := range []int{1, 2} {
			t.Run(fmt.Sprintf("qualified%t/n%d", qualified, n), func(t *testing.T) {
				d := &getManyErrorDB{readErr: nested}
				var db treeDB = d
				if qualified {
					db = &getManyErrorCutDB{getManyErrorDB: d}
				}
				s := newStore(db)
				s.floorLoaded = true
				keys := make([][]byte, n)
				for i := range keys {
					keys[i] = []byte("key")
				}
				got, err := s.GetManyAt(keys, 9)
				if err != nil || len(got) != n {
					t.Fatalf("nested cause escaped key error: %+v %v", got, err)
				}
				for _, r := range got {
					var typed *getManyTypedFailure
					if !errors.Is(r.Err, nested) || !errors.As(r.Err, &typed) || typed != failure {
						t.Fatalf("nested cause lost: %v", r.Err)
					}
				}
			})
		}
	}
}
