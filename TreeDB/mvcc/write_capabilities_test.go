package mvcc

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

func TestWriteCapabilitiesFollowCurrentProducerAndSupport(t *testing.T) {
	first := &logicalWriterProbe{}
	store, _ := logicalProbeStore(first, 0)
	mutation := []Mutation{{Key: []byte("key"), Value: []byte("value")}}
	if err := store.CommitAt(1, mutation, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	first.enabled = true
	injected := errors.New("logical producer failure")
	first.err = injected
	if err := store.CommitAt(2, mutation, CommitDurable); !errors.Is(err, injected) || !errors.Is(err, ErrStorage) {
		t.Fatalf("logical error=%v", err)
	}
	if first.physicalSets != 1 || first.calls != 1 || !first.sync {
		t.Fatalf("logical failure fell back or lost sync: %+v", first)
	}
	first.enabled, first.err = false, nil
	if err := store.CommitAt(3, mutation, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if first.probes != 3 || first.physicalSets != 2 || first.calls != 1 {
		t.Fatalf("support state was cached: %+v", first)
	}

	// Internal adapters can replace the producer after its previous call joins.
	second := &logicalWriterProbe{enabled: true}
	store.db = second
	if err := store.CommitAt(4, mutation, CommitDurable); err != nil {
		t.Fatal(err)
	}
	if second.probes != 1 || second.calls != 1 || !second.sync || first.probes != 3 {
		t.Fatalf("producer was cached: first=%+v second=%+v", first, second)
	}
	store.admission = nil
	if err := store.CommitAt(5, mutation, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if second.probes != 1 || second.physicalSets != 1 {
		t.Fatalf("unadmitted producer bypassed ordinary Set: %+v", second)
	}
}

type capabilityPointProbe struct {
	logicalWriterProbe
	input      mvccadmission.Input
	typedCalls int
	syncCalls  int
	typedSync  bool
}

func (p *capabilityPointProbe) SetPointWithMVCCInput(key, value []byte, sync bool, input mvccadmission.Input) error {
	p.typedCalls++
	p.physicalKey, p.physicalValue = bytes.Clone(key), bytes.Clone(value)
	p.typedSync, p.input = sync, input
	return p.err
}

func (p *capabilityPointProbe) SetSync(key, value []byte) error {
	p.syncCalls++
	return p.Set(key, value)
}

func TestWriteCapabilitiesPreserveAdmittedPointAndDecoratorErrors(t *testing.T) {
	for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
		probe := &capabilityPointProbe{}
		authority := new(mvccadmission.Authority)
		store := &Store{db: probe, admission: authority.Issue(), floorLoaded: true}
		injected := errors.New("typed point failure")
		probe.err = injected
		if err := store.CommitAt(7, []Mutation{{Key: []byte("key"), Value: []byte("value")}}, mode); !errors.Is(err, injected) || !errors.Is(err, ErrStorage) {
			t.Fatalf("mode=%d typed point error=%v", mode, err)
		}
		if probe.typedCalls != 1 || probe.physicalSets != 0 || probe.typedSync != (mode == CommitDurable) {
			t.Fatalf("typed point fell back or changed mode: %+v", probe)
		}
		if ts, ok := probe.input.LogicalTimestamp(probe.physicalKey); !ok || ts != 7 {
			t.Fatalf("point lost exact key/timestamp admission: ts=%d ok=%t", ts, ok)
		}
		// No admission means inherited typed-input methods must not bypass the
		// ordinary decorator's Set/SetSync override or its injected error.
		decorated := newStore(probe)
		decorated.floorLoaded = true
		if err := decorated.CommitAt(8, []Mutation{{Key: []byte("key")}}, mode); !errors.Is(err, injected) {
			t.Fatalf("decorator error=%v", err)
		}
		if probe.typedCalls != 1 || probe.physicalSets != 1 || probe.syncCalls != int(mode) {
			t.Fatalf("decorator route changed: %+v", probe)
		}
	}
}

func TestWriteCapabilitiesPublicCloseAndDurableReopen(t *testing.T) {
	for _, memtable := range []string{"cow_btree", "cow_ordered"} {
		t.Run(memtable, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = memtable
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			store := New(db)
			if err := store.CommitAt(1, []Mutation{{Key: []byte("key"), Value: []byte("durable")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitAt(0, nil, CommitMode(99)); !errors.Is(err, ErrZeroTimestamp) {
				t.Fatalf("timestamp precedence=%v", err)
			}
			if err := store.CommitGroupAt(nil, CommitMode(99)); !errors.Is(err, ErrInvalidCommitMode) {
				t.Fatalf("mode precedence=%v", err)
			}
			if err := store.CommitGroupAt([]CommitGroup{{Timestamp: 1}}, CommitDurable); err != nil {
				t.Fatalf("empty closed-store group=%v", err)
			}
			if err := store.CommitAt(2, []Mutation{{Key: []byte("key"), Value: []byte("closed")}}, CommitDurable); !errors.Is(err, ErrStorage) || !errors.Is(err, treedb.ErrClosed) {
				t.Fatalf("closed-store commit=%v", err)
			}
			reopened, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			requireResult(t, New(reopened), []byte("key"), 2, Present, 1, []byte("durable"))
		})
	}
}

func TestWriteCapabilitiesConcreteSelectionAllocations(t *testing.T) {
	producer := new(treedb.DB)
	store := &Store{db: producer}
	var logical logicalMVCCWriter
	var point pointMVCCInputWriter
	allocs := testing.AllocsPerRun(1000, func() {
		logical = logicalMVCCWriterFor(store.db)
		point = pointMVCCInputWriterFor(store.db)
	})
	if logical != logicalMVCCWriter(producer) || point != pointMVCCInputWriter(producer) {
		t.Fatal("selection changed the concrete receiver")
	}
	t.Logf("concrete logical+point selection: %g allocations/pair (non-timed control)", allocs)
	if allocs != 0 {
		t.Fatalf("concrete selection allocations=%g, want zero", allocs)
	}
}
