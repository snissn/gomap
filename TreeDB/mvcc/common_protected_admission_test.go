package mvcc

import (
	"bytes"
	"errors"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"testing"
)

func TestCommonProtectedAdmissionOrdinaryRoutes(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.BackgroundCheckpointInterval = -1
			opts.ValueLog.PointerThreshold = 1
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := New(db)
			if store.admission == nil {
				t.Fatal("missing actual producer capability")
			}
			request, err := store.admission.Capture()
			if err != nil {
				t.Fatal(err)
			}
			value := bytes.Repeat([]byte("persistent"), 256)
			if err := store.CommitAt(10, []Mutation{{Key: []byte("a"), Value: value}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err := request.Validate(); err != nil {
				t.Fatalf("qualified point invalidated request: %v", err)
			}
			if err := store.CommitAt(20, []Mutation{{Key: []byte("b"), Value: value}, {Key: []byte("c"), Value: value}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err := request.Validate(); err != nil {
				t.Fatalf("qualified batch invalidated request: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := request.Validate(); err != nil {
				t.Fatalf("physical materialization invalidated request: %v", err)
			}
			if err := db.Set([]byte("raw"), []byte("same")); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(request.Validate(), iterator.ErrOrdinalScanStale) {
				t.Fatal("raw point did not make request sticky stale")
			}
			if err := store.CommitAt(30, []Mutation{{Key: []byte("d"), Value: value}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(request.Validate(), iterator.ErrOrdinalScanStale) {
				t.Fatal("later qualified point restored stale request")
			}
			fresh, err := store.admission.Capture()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Delete([]byte("raw")); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(fresh.Validate(), iterator.ErrOrdinalScanStale) {
				t.Fatal("raw delete did not invalidate request")
			}
		})
	}
}

func TestCommonProtectedFloorActualDrainAndMaterialization(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	value := bytes.Repeat([]byte("old-resource"), 400)
	if err := store.CommitAt(10, []Mutation{{Key: []byte("key"), Value: value}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(20, CommitDurable); err != nil {
		t.Fatal(err)
	}
	protection, err := store.prepareProtectedFloor()
	if err != nil {
		t.Fatal(err)
	}
	for n := uint64(21); n < 29; n++ {
		// The large logical key forces real current DATA materialization, while the
		// protected key/value semantics stay at the same timestamp and revision.
		key := bytes.Repeat([]byte{byte('a' + n - 21)}, 1700)
		if err := store.CommitAt(n, []Mutation{{Key: key, Value: value}}, CommitDurable); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if ready, err := protection.ValidateWithWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
			t.Fatalf("materialization invalidated semantic preparation: %v %v", ready, err)
		}
	}
	if err := protection.Request().Validate(); err != nil {
		t.Fatal(err)
	}
	physical, err := mvcckey.Encode([]byte("key"), 10)
	if err != nil {
		t.Fatal(err)
	}
	same, err := db.Get(physical)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync(physical, same); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(protection.Request().Validate(), iterator.ErrOrdinalScanStale) {
		t.Fatal("raw identical postimage survived")
	}
	if err := store.CommitAt(40, []Mutation{{Key: []byte("future"), Value: value}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(protection.Request().Validate(), iterator.ErrOrdinalScanStale) {
		t.Fatal("legal write erased sticky foreign authority")
	}
}

// The injection is after the actual completed drain, before the Store issuer
// regains control. It must not be adopted as a freshly captured foreign epoch.
type protectedFloorGapDB struct {
	*treedb.DB
	afterDrain func() error
}

func (d *protectedFloorGapDB) Checkpoint() error {
	if err := d.DB.Checkpoint(); err != nil {
		return err
	}
	if d.afterDrain != nil {
		return d.afterDrain()
	}
	return nil
}
func TestCommonProtectedFloorRejectsForeignAfterDrain(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	wrapped := &protectedFloorGapDB{DB: db}
	store := newStore(wrapped)
	if err := store.CommitAt(10, []Mutation{{Key: []byte("protected"), Value: []byte("value")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(20, CommitDurable); err != nil {
		t.Fatal(err)
	}
	physical, err := mvcckey.Encode([]byte("gap"), 10)
	if err != nil {
		t.Fatal(err)
	}
	wrapped.afterDrain = func() error { return db.SetSync(physical, []byte{1, 'x'}) }
	if protection, err := store.prepareProtectedFloor(); protection != nil || !errors.Is(err, iterator.ErrOrdinalScanStale) {
		t.Fatalf("post-drain foreign input certified: protection=%v err=%v", protection, err)
	}
}
func TestCommonProtectedHigherFloorPreservesOriginalRequest(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.BackgroundCheckpointInterval = -1
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := New(db)
			if err := store.AdvanceDiscardFloor(20, CommitDurable); err != nil {
				t.Fatal(err)
			}
			request, err := store.admission.Capture()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.AdvanceDiscardFloor(30, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err := request.Validate(); err != nil {
				t.Fatalf("own higher durable floor invalidated Request: %v", err)
			}
			if err := store.AdvanceDiscardFloor(40, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err := request.Validate(); err != nil {
				t.Fatalf("own relaxed floor invalidated Request: %v", err)
			}
		})
	}
}
