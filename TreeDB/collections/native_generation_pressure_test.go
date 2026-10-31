package collections

import (
	"bytes"
	"errors"
	"os"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeGenerationPressurePolicy(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		history, pending, limit uint64
		want                    bool
	}{
		{"below", 74, 0, 100, false},
		{"threshold", 75, 0, 100, true},
		{"pending-below", 50, 24, 100, false},
		{"pending-threshold", 50, 25, 100, true},
		{"zero-limit", 0, 0, 0, true},
		{"over-limit", 101, 0, 100, true},
		{"overflow", ^uint64(0) - 1, 10, ^uint64(0), true},
		{"pending-overflow", 1, ^uint64(0), ^uint64(0), true},
		{"round-up-below", 3, 0, 5, false},
		{"round-up-threshold", 3, 1, 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nativeGenerationUnderPressure(memtable.COWGenerationUsage{HistoryBytes: tc.history, PendingBytes: tc.pending, MaxGenerationBytes: tc.limit})
			if got != tc.want {
				t.Fatalf("history=%d pending=%d limit=%d pressure=%t want=%t", tc.history, tc.pending, tc.limit, got, tc.want)
			}
		})
	}
}

func openNativeGenerationFixture(t *testing.T) (*treedb.DB, *backenddb.DB, *Collection, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "treedb-native-generation-")
	if err != nil {
		t.Fatal(err)
	}
	// A timed-out cohort must leave its owners and files intact until the test
	// process exits; never close/remove storage underneath unjoined producers.
	mayClose := true
	t.Cleanup(func() {
		if mayClose {
			if err := os.RemoveAll(dir); err != nil {
				t.Error(err)
			}
		}
	})
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxGenerationBytes = 2 << 20
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if mayClose {
			if err := owner.Close(); err != nil {
				mayClose = false
				t.Error(err)
			}
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if mayClose {
			if err := manager.Close(); err != nil {
				mayClose = false
				t.Error(err)
			}
		}
	})
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("not native command WAL")
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	return owner, backend, c, func() { mayClose = false }
}

// Private reservations exercise the observer without publishing fictitious
// documents or depending on wall-clock scheduling. Final public admission is
// covered separately; an observation must neither cancel nor adopt this work.
func TestNativePendingGenerationObservationDomains(t *testing.T) {
	owner, backend, c, _ := openNativeGenerationFixture(t)
	doc := []byte(`{"tenant":"one","value":1}`)
	if _, err := c.Insert([]byte("row"), doc); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	})
	e := read.capture.State().(*nativeEngineVersion)
	if e.catalog == nil || e.catalog.pending == nil {
		t.Fatal("fixture lacks active catalog root")
	}
	domains := map[string]*nativeDomainState{"catalog": e.catalog}
	for _, name := range []string{"primary", "index/tenant", "index-state"} {
		d, ok := read.collection.pending.Get(name)
		if !ok || d.pending == nil {
			t.Fatalf("fixture lacks active %s", name)
		}
		domains[name] = d
	}
	basis, valid := read.capture.Snapshot().StateToken()
	if !valid {
		t.Fatal("missing original basis")
	}
	prepare := func(t *testing.T, d *nativeDomainState) {
		t.Helper()
		fork, err := memtable.ForkCOWWriter(d.pending)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { retirement := fork.Close(); retirement.Drain() })
		prepared, err := fork.Prepare([]memtable.COWMutation{{Key: []byte("private-pressure"), Value: bytes.Repeat([]byte("p"), 7<<18)}}, memtable.COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { retirement := prepared.Cancel(); retirement.Drain() })
	}
	for _, name := range []string{"catalog", "primary", "index/tenant", "index-state"} {
		t.Run(name, func(t *testing.T) {
			before, err := backend.ObserveNativeProducer()
			if err != nil || before.UnderPressure || before.Semantic.ConstructionBytes != 0 {
				t.Fatalf("unexpected initial pressure=%+v err=%v", before, err)
			}
			next := backend.CommandWALNextLSN()
			prepare(t, domains[name])
			observed, err := backend.ObserveNativeProducer()
			if err != nil || !observed.UnderPressure || !observed.Semantic.GenerationUnderPressure || observed.Semantic.ConstructionBytes != 0 || observed.Basis != basis || observed.LogicalSequence != before.LogicalSequence || backend.CommandWALNextLSN() != next {
				t.Fatalf("active %s pressure=%+v err=%v original=%+v", name, observed, err, before)
			}
		})
		if after, err := backend.ObserveNativeProducer(); err != nil || after.UnderPressure {
			t.Fatalf("canceled %s reservation retained pressure=%+v err=%v", name, after, err)
		}
	}
	t.Run("old-generation-is-not-active", func(t *testing.T) {
		prepare(t, domains["primary"])
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		usage, err := domains["primary"].pending.GenerationUsage()
		if err != nil || !nativeGenerationUnderPressure(usage) {
			t.Fatalf("old owned generation is not still pressured: %+v err=%v", usage, err)
		}
		if after, err := backend.ObserveNativeProducer(); err != nil || after.Semantic.GenerationUnderPressure {
			t.Fatalf("inactive retained generation scheduled maintenance: %+v err=%v", after, err)
		}
		if got, err := read.Get([]byte("row")); err != nil || !bytes.Equal(got, doc) {
			t.Fatalf("old read lost acknowledged bytes: %q err=%v", got, err)
		}
	})
}

func TestNativeOrdinaryGenerationOversizedRefusal(t *testing.T) {
	_, backend, c, _ := openNativeGenerationFixture(t)
	before, err := backend.ObserveNativeProducer()
	if err != nil || before.UnderPressure {
		t.Fatalf("fresh state=%+v err=%v", before, err)
	}
	next := backend.CommandWALNextLSN()
	doc := append(append([]byte(`{"tenant":"one","payload":"`), bytes.Repeat([]byte("a"), 3<<20)...), []byte(`"}`)...)
	id, err := c.Insert([]byte("oversized"), doc)
	if !errors.Is(err, memtable.ErrCOWCapacity) || errors.Is(err, ErrCommitAmbiguous) || backenddb.CommitPublicationAccepted(err) || len(id) != 0 {
		t.Fatalf("oversized result id=%q err=%v", id, err)
	}
	after, err := backend.ObserveNativeProducer()
	if err != nil || after.LogicalSequence != before.LogicalSequence || backend.CommandWALNextLSN() != next {
		t.Fatalf("refusal changed logical/WAL state: before=%+v after=%+v err=%v", before, after, err)
	}
	if got, err := c.Get([]byte("oversized")); err != nil || got != nil {
		t.Fatalf("oversized refusal visible: bytes=%d err=%v", len(got), err)
	}
}
