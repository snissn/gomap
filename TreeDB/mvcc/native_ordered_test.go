package mvcc

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

type logicalWriterProbe struct {
	enabled       bool
	probes        int
	calls         int
	physicalSets  int
	mutations     []mvccadmission.LogicalMutation
	input         mvccadmission.LogicalInput
	sync          bool
	err           error
	physicalKey   []byte
	physicalValue []byte
}

func (p *logicalWriterProbe) SupportsLogicalMVCC() bool { p.probes++; return p.enabled }
func (p *logicalWriterProbe) CommitLogicalMVCC(ms []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput, sync bool) error {
	p.calls++
	p.mutations, p.input, p.sync = ms, input, sync
	return p.err
}
func (*logicalWriterProbe) Iterator([]byte, []byte) (treedb.Iterator, error) {
	panic("unexpected floor/storage access")
}
func (*logicalWriterProbe) NewBatchWithSize(int) treedb.Batch {
	panic("unexpected physical batch")
}
func (p *logicalWriterProbe) Set(key, value []byte) error {
	p.physicalSets++
	p.physicalKey, p.physicalValue = bytes.Clone(key), bytes.Clone(value)
	return p.err
}

func logicalProbeStore(p *logicalWriterProbe, floor uint64) (*Store, *mvccadmission.Authority) {
	authority := new(mvccadmission.Authority)
	return &Store{db: p, admission: authority.Issue(), floorLoaded: true, discardFloor: floor}, authority
}

func TestNativeOrderedStoreTypedGroupAndAuthority(t *testing.T) {
	for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
		probe := &logicalWriterProbe{enabled: true}
		store, authority := logicalProbeStore(probe, 4)
		key := []byte{'k', 0, 0xff}
		groups := []CommitGroup{
			{Timestamp: 9, Mutations: []Mutation{{Key: key, Value: []byte("nine")}}},
			{Timestamp: 5, Mutations: []Mutation{{Key: key, Delete: true}, {Key: nil, Value: nil}}},
			{Timestamp: math.MaxUint64, Mutations: []Mutation{{Key: key, Value: []byte{}}}},
		}
		if err := store.CommitGroupAt(groups, mode); err != nil {
			t.Fatal(err)
		}
		if probe.calls != 1 || probe.physicalSets != 0 || probe.sync != (mode == CommitDurable) || len(probe.mutations) != 4 {
			t.Fatalf("typed route inactive or split: %+v", probe)
		}
		if !probe.input.Matches(probe.mutations) || probe.input.Floor() != 4 {
			t.Fatal("typed operands lost exact admitted floor proof")
		}
		cut := authority.Begin()
		if !cut.ObserveLogical(probe.mutations, probe.input) {
			t.Fatal("typed route detached from original Authority")
		}
		cut.Commit()
		for n, want := range []struct {
			ts    uint64
			del   bool
			value []byte
		}{
			{9, false, []byte("nine")}, {5, true, nil}, {5, false, nil}, {math.MaxUint64, false, []byte{}},
		} {
			got := probe.mutations[n]
			if got.Timestamp != want.ts || got.Delete != want.del || !bytes.Equal(got.Value, want.value) {
				t.Fatalf("operand %d = %+v, want %+v", n, got, want)
			}
		}
	}
}

func TestNativeOrderedStoreValidationPrecedesFloorAccess(t *testing.T) {
	oversize := bytes.Repeat([]byte{0}, mvcckey.MaxEncodedKeySize/2)
	for _, tc := range []struct {
		name     string
		groups   []CommitGroup
		want     error
		position string
	}{
		{"invalid-key", []CommitGroup{{Timestamp: 1, Mutations: []Mutation{{Key: oversize}}}}, ErrInvalidKey, "group index 0 key index 0"},
		{"duplicate-nil-empty", []CommitGroup{{Timestamp: 1, Mutations: []Mutation{{Key: nil}}}, {Timestamp: 1, Mutations: []Mutation{{Key: []byte{}}}}}, ErrDuplicateKey, "group index 1 key index 0"},
		{"zero-empty-group", []CommitGroup{{Timestamp: 0}}, ErrZeroTimestamp, "group index 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &logicalWriterProbe{enabled: true}
			store, _ := logicalProbeStore(probe, 100)
			store.floorLoaded = false // any premature load panics in Iterator.
			err := store.CommitGroupAt(tc.groups, CommitRelaxed)
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.position) || probe.calls != 0 {
				t.Fatalf("got %v, expected %v at %s, calls %d", err, tc.want, tc.position, probe.calls)
			}
		})
	}
	probe := &logicalWriterProbe{enabled: true}
	store, _ := logicalProbeStore(probe, 0)
	store.floorLoaded = false
	if err := store.CommitGroupAt([]CommitGroup{{Timestamp: 1}}, CommitRelaxed); err != nil || probe.probes != 0 {
		t.Fatalf("empty group probed storage/capability: err=%v probes=%d", err, probe.probes)
	}
}

func TestNativeOrderedStoreFloorAndStorageErrors(t *testing.T) {
	probe := &logicalWriterProbe{enabled: true}
	store, _ := logicalProbeStore(probe, 7)
	err := store.CommitGroupAt([]CommitGroup{
		{Timestamp: 9, Mutations: []Mutation{{Key: []byte("a")}}},
		{Timestamp: 7, Mutations: []Mutation{{Key: []byte("b")}}},
	}, CommitRelaxed)
	if !errors.Is(err, ErrVersionBelowDiscardFloor) || !strings.Contains(err.Error(), "group index 1") || probe.calls != 0 {
		t.Fatalf("floor did not reject complete typed group: %v calls=%d", err, probe.calls)
	}
	injected := errors.New("typed producer failure")
	probe.err = injected
	err = store.CommitAt(8, []Mutation{{Key: []byte("k")}}, CommitRelaxed)
	if !errors.Is(err, ErrStorage) || !errors.Is(err, injected) || probe.calls != 1 || probe.physicalSets != 0 {
		t.Fatalf("typed failure fell back or lost storage error: %v %+v", err, probe)
	}
}

func TestNativeOrderedStoreUnsupportedAndDecoratorFallback(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		probe := &logicalWriterProbe{enabled: !admitted}
		store, _ := logicalProbeStore(probe, 0)
		if !admitted {
			// newStore's decorators intentionally have no concrete DB capability.
			store = newStore(probe)
			store.floorLoaded = true
		}
		if err := store.CommitAt(3, []Mutation{{Key: []byte("k"), Value: []byte("v")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
		if probe.calls != 0 || probe.physicalSets != 1 || !bytes.Equal(probe.physicalValue, []byte{recordValueV1, 'v'}) {
			t.Fatalf("unsupported/decorated Store bypassed ordinary Set: %+v", probe)
		}
		logical, timestamp, err := mvcckey.Decode(probe.physicalKey)
		if err != nil || !bytes.Equal(logical, []byte("k")) || timestamp != 3 {
			t.Fatalf("fallback codec changed: %x %d %v", logical, timestamp, err)
		}
	}
}

// Existing COW writers still receive Store-qualified physical operands even
// when they do not select logical history. Qualification must not make those
// operands disappear from the ordinary physical tree.
func TestNativeOrderedUnsupportedCOWStoreRetainsQualifiedPhysicalWrites(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			open := func() *treedb.DB {
				t.Helper()
				opts := treedb.OptionsFor(profile, dir)
				opts.MemtableMode = "cow_btree"
				opts.MemtableShards = 4
				opts.FlushThreshold = 1 << 30
				opts.DisableSideStores = true
				opts.DisableBackgroundPrune = true
				opts.BackgroundCheckpointInterval = -1
				database, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				if database.SupportsLogicalMVCC() {
					_ = database.Close()
					t.Fatal("cow_btree unexpectedly selected the typed logical route")
				}
				return database
			}
			database := open()
			defer func() {
				if database != nil {
					_ = database.Close()
				}
			}()
			store := New(database)
			if err := store.AdvanceDiscardFloor(1, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			work := iterator.OrdinalScanWork{RecordLimit: math.MaxUint64, ByteLimit: math.MaxUint64}
			request, ok, err := store.admission.CaptureRequest(1, &work)
			if err != nil || !ok {
				t.Fatalf("original Store owner unavailable: available=%t error=%v", ok, err)
			}
			key := []byte{'a', 0, 0xff}
			if err := store.CommitAt(10, []Mutation{{Key: key, Value: []byte("point")}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			requireResult(t, store, key, 10, Present, 10, []byte("point"))
			if err := store.CommitGroupAt([]CommitGroup{
				{Timestamp: 20, Mutations: []Mutation{{Key: key, Value: []byte("twenty")}, {Key: []byte("other"), Value: nil}}},
				{Timestamp: 15, Mutations: []Mutation{{Key: key, Value: []byte("backfill")}}},
				{Timestamp: 25, Mutations: []Mutation{{Key: key, Delete: true}}},
			}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			work = iterator.OrdinalScanWork{RecordLimit: math.MaxUint64, ByteLimit: math.MaxUint64}
			if ok, err := request.Validate(&work); err != nil || !ok {
				t.Fatalf("physical fallback detached original Store input: available=%t error=%v", ok, err)
			}
			check := func() {
				t.Helper()
				requireResult(t, store, key, 10, Present, 10, []byte("point"))
				requireResult(t, store, key, 19, Present, 15, []byte("backfill"))
				requireResult(t, store, key, 20, Present, 20, []byte("twenty"))
				requireResult(t, store, key, 25, Tombstone, 25, nil)
				requireResult(t, store, []byte("other"), 20, Present, 20, nil)
			}
			check()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			database = open()
			store = New(database)
			check()
		})
	}
}
