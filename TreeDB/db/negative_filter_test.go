package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func openNegativeTestDB(t *testing.T, opts Options) *DB {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	opts.DisableBackgroundPrune = true
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

func TestNegativeFilterPublicRoutesNeverDescend(t *testing.T) {
	d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 1024})
	if d.snapshotViewRO.Load().negativeFilter == nil {
		t.Fatal("no empty-root coverage")
	}
	// A missing pager makes descent fail. Registry/admission/lifetime remain real.
	idx := d.idx.Load()
	p := idx.pager
	idx.pager = nil
	defer func() { idx.pager = p }()
	k := []byte("absent")
	for _, read := range []func() error{
		func() error {
			v, e := d.Get(k)
			if v != nil {
				t.Fatal(v)
			}
			return e
		},
		func() error { _, e := d.GetUnsafe(k); return e },
		func() error { _, _, e := d.GetVersioned(k); return e },
		func() error {
			_, e := d.GetAppend(k, nil)
			if errors.Is(e, tree.ErrKeyNotFound) {
				return nil
			}
			return e
		},
		func() error {
			_, _, e := d.GetVersionedAppend(k, nil)
			if errors.Is(e, tree.ErrKeyNotFound) {
				return nil
			}
			return e
		},
		func() error {
			found, e := d.Has(k)
			if found {
				t.Fatal("found")
			}
			return e
		},
		func() error {
			v, e := d.GetMany([][]byte{k})
			if len(v) != 1 || v[0] != nil {
				t.Fatal(v)
			}
			return e
		},
		func() error {
			return d.GetManyView([][]byte{k}, func(_ int, _, v []byte, found bool) error {
				if found || v != nil {
					t.Fatal(found, v)
				}
				return nil
			})
		},
	} {
		if e := read(); e != nil {
			t.Fatal(e)
		}
	}
	snap := d.AcquireSnapshot()
	defer snap.Close()
	if _, e := snap.GetEntry(k); !errors.Is(e, tree.ErrKeyNotFound) {
		t.Fatal(e)
	}
	if _, e := snap.Get(k); !errors.Is(e, tree.ErrKeyNotFound) {
		t.Fatal(e)
	}
	// Only the captured main root carries coverage. An arbitrary root still
	// reaches the missing pager rather than reusing the main-root rejection.
	if _, e := snap.GetAtRoot(snap.state.RootPageID+1, k); e == nil || errors.Is(e, tree.ErrKeyNotFound) {
		t.Fatal("arbitrary root skipped exact lookup", e)
	}
	if e := snap.GetManyView([][]byte{k}, func(_ int, _, v []byte, found bool) error {
		if found || v != nil {
			t.Fatal(found, v)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := snap.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := snap.Get(k); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	d.publicationPoisoned.Store(true)
	if _, e := d.Get(k); !errors.Is(e, ErrRecoveryRequired) {
		t.Fatal(e)
	}
	d.publicationPoisoned.Store(false)
}

func TestNegativeFilterDifferentialReopenAndOldSnapshots(t *testing.T) {
	for _, command := range []bool{false, true} {
		t.Run(fmt.Sprintf("command=%v", command), func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), NegativeLookupFilterBytes: 4096, CommandWAL: command}
			d := openNegativeTestDB(t, opts)
			old := d.AcquireSnapshot()
			defer old.Close()
			keys := [][]byte{nil, {}, []byte("empty"), {0, 255, 0}, []byte("deleted"), bytes.Repeat([]byte("k"), tree.NegativeFilterMaxKeyBytes+1)}
			for i, k := range keys {
				v := []byte{byte(i)}
				if i < 3 {
					v = []byte{}
				}
				if e := d.SetSync(k, v); e != nil {
					t.Fatal(e)
				}
			}
			tombBatch := d.NewBatch().(*Batch)
			if e := tombBatch.DeleteWithRevision([]byte("deleted"), 101); e != nil {
				t.Fatal(e)
			}
			if e := tombBatch.WriteSync(); e != nil {
				t.Fatal(e)
			}
			tombBatch.Close()
			f := d.snapshotViewRO.Load().negativeFilter
			if f == nil {
				t.Fatal("writes lost coverage")
			}
			for _, k := range keys {
				if len(k) <= tree.NegativeFilterMaxKeyBytes && f.DefinitelyAbsent(normalizeRawKVPointKey(k)) {
					t.Fatalf("mutation key rejected %q", k)
				}
			}
			_, rev, e := d.GetVersioned([]byte("deleted"))
			if e != nil || rev != 0 {
				t.Fatal(rev, e)
			}
			if _, e := old.Get([]byte("empty")); !errors.Is(e, tree.ErrKeyNotFound) {
				t.Fatal(e)
			}
			exact := d.AcquireSnapshot()
			defer exact.Close()
			exact.tree.SetNegativeFilter(nil)
			for _, k := range append(keys, []byte("missing")) {
				a, ar, ae := d.GetVersioned(k)
				b, br, be := exact.GetVersioned(k)
				if errors.Is(be, tree.ErrKeyNotFound) {
					be = nil
					b = nil
				}
				if !bytes.Equal(a, b) || (a == nil) != (b == nil) || ar != br || !errors.Is(ae, be) {
					t.Fatalf("differential %q: %q %d %v vs %q %d %v", k, a, ar, ae, b, br, be)
				}
			}
			exact.Close()
			old.Close()
			if e := d.Close(); e != nil {
				t.Fatal(e)
			}
			d = openNegativeTestDB(t, opts)
			if d.snapshotViewRO.Load().negativeFilter == nil {
				t.Fatal("reopen bootstrap unavailable")
			}
			_, rr, e := d.GetVersioned([]byte("deleted"))
			if rr != rev || e != nil {
				t.Fatal(rr, rev, e)
			}
		})
	}
}

// Ordinary zipper deletes remove entries. Build an explicit physical tombstone
// fixture to exercise bootstrap's revision-preserving coverage requirement.
func TestNegativeFilterBootstrapIncludesTombstones(t *testing.T) {
	opts := Options{Dir: t.TempDir(), NegativeLookupFilterBytes: 4096}
	d := openNegativeTestDB(t, Options{Dir: opts.Dir})
	idx := d.idx.Load()
	root := d.State().RootPageID
	image := make([]byte, page.PageSize)
	builder := node.NewBuilder(image, page.PageTypeLeaf)
	builder.SetPageID(root)
	if err := builder.AddLeafEntryWithRevision([]byte("tombstone"), nil, node.FlagTombstone, page.ValuePtr{}, 101); err != nil {
		t.Fatal(err)
	}
	builder.Finish()
	if err := idx.pager.Write(root, image); err != nil {
		t.Fatal(err)
	}
	if err := idx.pager.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openNegativeTestDB(t, opts)
	f := d.snapshotViewRO.Load().negativeFilter
	if f == nil || f.DefinitelyAbsent([]byte("tombstone")) {
		t.Fatal("bootstrap omitted tombstone")
	}
	value, revision, err := d.GetVersioned([]byte("tombstone"))
	if err != nil || value != nil || revision != 101 {
		t.Fatal(value, revision, err)
	}
	dst := []byte("prefix")
	out, revision, err := d.GetVersionedAppend([]byte("tombstone"), dst)
	if !errors.Is(err, tree.ErrKeyNotFound) || !bytes.Equal(out, dst) || revision != 101 {
		t.Fatal(out, revision, err)
	}
	snap := d.AcquireSnapshot()
	defer snap.Close()
	entry, err := snap.GetEntry([]byte("tombstone"))
	if err != nil || entry.Flags&node.FlagTombstone == 0 || entry.Revision != 101 {
		t.Fatal(entry, err)
	}
}

// Both operations explicitly bind the current user root under the durable gate.
// Advancing metadata must carry existing coverage without reviving an uncovered base.
func TestNegativeFilterCurrentRootMetadataCoverage(t *testing.T) {
	for _, uncovered := range []bool{false, true} {
		t.Run(fmt.Sprintf("uncovered=%v", uncovered), func(t *testing.T) {
			d := openNegativeTestDB(t, Options{CommandWAL: true, NegativeLookupFilterBytes: 1024})
			if err := d.SetSync([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			old := d.AcquireSnapshot()
			defer old.Close()
			original := d.snapshotViewRO.Load()
			if original.negativeFilter == nil {
				t.Fatal("fixture lacks initial coverage")
			}
			if uncovered {
				// Model a prior unknown-lineage publication with the same visible state.
				d.clearSnapshotView()
				d.publishSnapshotView(original.idx, original.state, original.vlogManager)
			}
			for _, publish := range []func() error{
				d.RefreshCommandWALCheckpointFallback,
				func() error { return d.PublishCommandWALAppliedLSN(d.State().AppliedCommandLSN, nil, true) },
			} {
				before := d.snapshotViewRO.Load()
				if err := publish(); err != nil {
					t.Fatal(err)
				}
				after := d.snapshotViewRO.Load()
				if after.idx != before.idx || after.state.RootPageID != before.state.RootPageID || after.state.SystemRootPageID != before.state.SystemRootPageID || after.state.CommitSeq <= before.state.CommitSeq {
					t.Fatal("fixture did not perform current-root metadata publication", before.state, after.state)
				}
				if after.negativeFilter != before.negativeFilter {
					t.Fatal("current-root metadata lost coverage or revived an uncovered base")
				}
			}
			// Supplied candidates do not gain the current-roots capability merely
			// because their numeric root IDs happen to equal the visible roots.
			state := d.State()
			if err := d.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, state.AppliedCommandLSN, nil, true); err != nil {
				t.Fatal(err)
			}
			if d.snapshotViewRO.Load().negativeFilter != nil {
				t.Fatal("supplied-root publication retained coverage")
			}
			if err := d.PublishCommandWALAppliedLSN(state.AppliedCommandLSN, nil, true); err != nil {
				t.Fatal(err)
			}
			if d.snapshotViewRO.Load().negativeFilter != nil {
				t.Fatal("metadata revived unknown-lineage coverage")
			}
			if got, err := old.Get([]byte("key")); err != nil || string(got) != "value" {
				t.Fatal(got, err)
			}
		})
	}
}

func TestNegativeFilterUnknownPublicationAndBudgetStayExact(t *testing.T) {
	d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 128})
	if e := d.SetSync([]byte("a"), []byte("A")); e != nil {
		t.Fatal(e)
	}
	old := d.AcquireSnapshot()
	defer old.Close()
	before := d.snapshotViewRO.Load()
	state := cloneDBState(before.state)
	state.CommitSeq++
	d.publishSnapshotView(before.idx, state, before.vlogManager)
	if d.snapshotViewRO.Load().negativeFilter != nil {
		t.Fatal("unknown publication retained coverage")
	}
	if c := d.prepareNegativeCoverage(before.idx, state.CommitSeq, state.RootPageID, state.RootPageID, nil); c != nil {
		t.Fatal("deltas resurrected uncovered base")
	}
	state2 := cloneDBState(state)
	state2.CommitSeq++
	d.publishSnapshotView(before.idx, state2, before.vlogManager)
	if d.snapshotViewRO.Load().negativeFilter != nil {
		t.Fatal("descendant recovered coverage")
	}
	if v, e := old.Get([]byte("a")); e != nil || string(v) != "A" {
		t.Fatal(v, e)
	}
	for i := 0; i < 110; i++ {
		if e := d.SetSync([]byte(fmt.Sprintf("k%04d", i)), []byte("v")); e != nil {
			t.Fatal(e)
		}
	}
	d.bootstrapNegativeFilter(8)
	if d.snapshotViewRO.Load().negativeFilter != nil {
		t.Fatal("partial bootstrap admitted")
	}
	for _, budget := range []int{-1, 1, 7, 64<<20 + 1} {
		if _, e := Open(Options{Dir: t.TempDir(), NegativeLookupFilterBytes: budget}); e == nil {
			t.Fatal(budget)
		}
	}
}

func TestNegativeFilterBootstrapReadFailureStaysExact(t *testing.T) {
	d := openNegativeTestDB(t, Options{})
	idx := d.idx.Load()
	p := idx.pager
	idx.pager = nil // Bootstrap cannot finish the root scan.
	defer func() { idx.pager = p }()
	d.bootstrapNegativeFilter(1024)
	if d.snapshotViewRO.Load().negativeFilter != nil {
		t.Fatal("failed scan authorized coverage")
	}
}

func TestNegativeFilterGroupedIntermediateKeys(t *testing.T) {
	d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 1024})
	g, e := d.BeginRootPublicationBuildGroup()
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close()
	for i := 0; i < 3; i++ {
		b := d.NewPhysicalBatch().(*Batch)
		if e := b.SetRootPublicationBuildGroup(g, i == 2); e != nil {
			t.Fatal(e)
		}
		if e := b.Set([]byte(fmt.Sprintf("key%d", i)), []byte("v")); e != nil {
			t.Fatal(e)
		}
		if e := b.Write(); e != nil {
			t.Fatal(e)
		}
		b.Close()
	}
	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		if d.snapshotViewRO.Load().negativeFilter.DefinitelyAbsent(k) {
			t.Fatal("intermediate rejected", i)
		}
		if v, e := d.Get(k); e != nil || string(v) != "v" {
			t.Fatal(v, e)
		}
	}
}

func TestNegativeFilterMembershipPrecedesOptimisticPublication(t *testing.T) {
	d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 1024})
	old := d.AcquireSnapshot()
	defer old.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	k := []byte("published-after-membership")
	d.testAfterOptimisticPublishPrepareHook = func() { close(entered); <-release }
	done := make(chan error, 1)
	go func() { done <- d.SetSync(k, []byte("value")) }()
	<-entered
	if d.snapshotViewRO.Load().negativeFilter.DefinitelyAbsent(k) {
		t.Fatal("candidate not hashed before publication")
	}
	if v, e := d.Get(k); e != nil || v != nil {
		t.Fatal("candidate visible early", v, e)
	}
	if _, e := old.Get(k); !errors.Is(e, tree.ErrKeyNotFound) {
		t.Fatal(e)
	}
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	d.testAfterOptimisticPublishPrepareHook = nil
	if v, e := d.Get(k); e != nil || string(v) != "value" {
		t.Fatal(v, e)
	}
	if _, e := old.Get(k); !errors.Is(e, tree.ErrKeyNotFound) {
		t.Fatal(e)
	}
}

func TestNegativeFilterCoverageCoordinates(t *testing.T) {
	d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 1024})
	old := d.snapshotViewRO.Load()
	c := d.prepareNegativeCoverage(old.idx, old.state.CommitSeq, old.state.RootPageID, 123, nil)
	next := cloneDBState(old.state)
	next.CommitSeq++
	next.RootPageID = 123
	if c == nil || !c.matches(old, old.idx, next) {
		t.Fatal("matching candidate not covered")
	}
	for _, edit := range []func(*negativeRootCoverage){func(c *negativeRootCoverage) { c.baseSeq++ }, func(c *negativeRootCoverage) { c.baseRoot++ }, func(c *negativeRootCoverage) { c.nextSeq++ }, func(c *negativeRootCoverage) { c.nextRoot++ }, func(c *negativeRootCoverage) { c.idx = &indexGen{} }} {
		bad := *c
		edit(&bad)
		if bad.matches(old, old.idx, next) {
			t.Fatal("mismatched coordinates covered")
		}
	}
	uncovered := *old
	uncovered.negativeFilter = nil
	if c.matches(&uncovered, old.idx, next) {
		t.Fatal("delta recovered uncovered base")
	}
	changed := *old
	changed.idx = &indexGen{}
	if c.matches(&changed, old.idx, next) {
		t.Fatal("index identity mismatch covered")
	}
}

func TestNegativeFilterQueuedAcceptedErrorRetainsMembership(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("grouped=%v", grouped), func(t *testing.T) {
			d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 1024, CommandWAL: true})
			b := d.NewBatch().(*Batch)
			var g *RootPublicationBuildGroup
			if grouped {
				b.Close()
				var e error
				g, e = d.BeginRootPublicationBuildGroup()
				if e != nil {
					t.Fatal(e)
				}
				defer g.Close()
				b = d.NewPhysicalBatch().(*Batch)
				if e := b.SetRootPublicationBuildGroup(g, true); e != nil {
					t.Fatal(e)
				}
				intent := mustRawKVCommandWALIntent(t, d, "accepted", "value")
				lsn, e := d.AppendCommandWALIntent(intent, false)
				if e != nil {
					t.Fatal(e)
				}
				if e := b.SetCommandWALPublish(lsn, []CommandWALLSNRange{{First: lsn, Last: lsn}}); e != nil {
					t.Fatal(e)
				}
			}
			defer b.Close()
			if e := b.Set([]byte("accepted"), []byte("value")); e != nil {
				t.Fatal(e)
			}
			d.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
			d.testFailWriteMeta.Store(true)
			e := b.WriteSync()
			d.testFailWriteMeta.Store(false)
			if !CommitPublicationAccepted(e) || !errors.Is(e, errTestWriteMetaFailpoint) {
				t.Fatal("want accepted wait failure", e)
			}
			f := d.snapshotViewRO.Load().negativeFilter
			if f == nil || f.DefinitelyAbsent([]byte("accepted")) {
				t.Fatal("accepted candidate lost coverage")
			}
			if v, e := d.Get([]byte("accepted")); e != nil || string(v) != "value" {
				t.Fatal(v, e)
			}
			if e := d.SetSync([]byte("after"), []byte("v")); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestNegativeFilterCommandRecoveryBootstrapsAfterReplay(t *testing.T) {
	opts := Options{Dir: t.TempDir(), CommandWAL: true, NegativeLookupFilterBytes: 4096}
	d := openNegativeTestDB(t, opts)
	if err := d.SetSync([]byte("before"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	d.testFailFinalizeCommit.Store(true)
	batch := d.NewBatch()
	if err := batch.Set([]byte("replayed"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatal(err)
	}
	batch.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openNegativeTestDB(t, opts)
	f := d.snapshotViewRO.Load().negativeFilter
	if f == nil || f.DefinitelyAbsent([]byte("replayed")) {
		t.Fatal("recovered key not covered")
	}
	assertDBValue(t, d, "replayed", "durable")
	if value, err := d.Get([]byte("before")); err != nil || value != nil {
		t.Fatal(value, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnly = true
	ro := openNegativeTestDB(t, opts)
	if ro.snapshotViewRO.Load().negativeFilter == nil {
		t.Fatal("read-only bootstrap missing")
	}
	assertDBValue(t, ro, "replayed", "durable")
}

func TestNegativeFilterIndexReplacementOldSnapshotAndGuardRelease(t *testing.T) {
	opts := Options{Dir: t.TempDir(), NegativeLookupFilterBytes: 4096}
	d := openNegativeTestDB(t, opts)
	if err := d.SetSync([]byte("present"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	defer old.Close()
	f := d.snapshotViewRO.Load().negativeFilter
	guard, err := d.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	// The filter pointer is private to tree. Compare its address through reflection
	// only to assert that release resets the same reference as pager/reader refs.
	field := reflect.ValueOf(&guard.snapshot.tree).Elem().FieldByName("negativeFilter")
	if field.IsNil() {
		t.Fatal("guard omitted captured coverage")
	}
	if err := guard.close(); err != nil {
		t.Fatal(err)
	}
	if !field.IsNil() {
		t.Fatal("pooled guard retained filter")
	}
	if err := d.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.snapshotViewRO.Load().negativeFilter != nil {
		t.Fatal("replacement reused old index coverage")
	}
	if value, err := old.Get([]byte("present")); err != nil || string(value) != "value" {
		t.Fatal(value, err)
	}
	if err := d.SetSync([]byte("after"), []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if d.snapshotViewRO.Load().negativeFilter != nil {
		t.Fatal("delta resurrected uncovered replacement")
	}
	if f.DefinitelyAbsent([]byte("present")) {
		t.Fatal("old coverage mutated destructively")
	}
	assertDBValue(t, d, "after", "replacement")
	old.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openNegativeTestDB(t, opts)
	if d.snapshotViewRO.Load().negativeFilter == nil {
		t.Fatal("reopen did not restore complete coverage")
	}
	assertDBValue(t, d, "after", "replacement")
}

func TestNegativeFilterValidMaximumLeafKeyFallsBack(t *testing.T) {
	// Derive the current maximum key from the existing leaf builder, rather than
	// adding a filter-specific key-size policy to accepted storage keys.
	image := make([]byte, page.PageSize)
	maximum := 0
	for n := page.PageSize; n > tree.NegativeFilterMaxKeyBytes; n-- {
		builder := node.NewBuilder(image, page.PageTypeLeaf)
		if err := builder.AddLeafEntryWithRevision(bytes.Repeat([]byte("k"), n), nil, node.FlagInline, page.ValuePtr{}, 1); err == nil {
			maximum = n
			break
		}
	}
	if maximum <= tree.NegativeFilterMaxKeyBytes {
		t.Fatal("missing maximum-key fixture")
	}
	opts := Options{Dir: t.TempDir(), NegativeLookupFilterBytes: 4096}
	d := openNegativeTestDB(t, opts)
	key := bytes.Repeat([]byte("k"), maximum)
	if err := d.SetSync(key, []byte{}); err != nil {
		t.Fatal("valid maximum key rejected", maximum, err)
	}
	if d.snapshotViewRO.Load().negativeFilter.DefinitelyAbsent(key) {
		t.Fatal("oversized key eligible for negative")
	}
	if value, err := d.Get(key); err != nil || value == nil {
		t.Fatal(value, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openNegativeTestDB(t, opts)
	if value, err := d.Get(key); err != nil || value == nil {
		t.Fatal(value, err)
	}
}

func TestNegativeFilterPublicAllocationsAndSharedSnapshotRetention(t *testing.T) {
	d := openNegativeTestDB(t, Options{NegativeLookupFilterBytes: 4096})
	key, missing := []byte("key"), []byte("missing")
	if err := d.SetSync(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 64)
	for _, k := range [][]byte{key, missing} {
		allocations := testing.AllocsPerRun(100, func() {
			_, err := d.GetAppend(k, dst[:0])
			if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatal(err)
			}
		})
		if allocations != 0 {
			t.Fatal("filter/capture allocated beyond caller buffer", allocations)
		}
	}
	f := d.snapshotViewRO.Load().negativeFilter
	var snapshots []*Snapshot
	for i := 0; i < 20; i++ {
		snap := d.AcquireSnapshot()
		snapshots = append(snapshots, snap)
		if ptr := reflect.ValueOf(&snap.tree).Elem().FieldByName("negativeFilter").Pointer(); ptr != reflect.ValueOf(f).Pointer() {
			t.Fatal("snapshot allocated independent bit storage")
		}
		if err := d.SetSync([]byte(fmt.Sprintf("later/%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if d.snapshotViewRO.Load().negativeFilter != f || f.Bytes() != 4096 {
		t.Fatal("publication replaced fixed bit storage")
	}
	for _, snap := range snapshots {
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
