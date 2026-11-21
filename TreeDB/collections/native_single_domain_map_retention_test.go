package collections_test

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeSingleDomainOwnedMapRetentionAcrossCheckpoint(t *testing.T) {
	const (
		rows        = 8
		name        = "single_domain_retention"
		acceptedLSN = "treedb.command_wal.live_accepted_max_lsn"
		durableLSN  = "treedb.command_wal.durable_wal_lsn"
		appliedLSN  = "treedb.command_wal.applied_lsn"
	)
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	opts.IndexInternalBaseDelta = false
	opts.BackgroundIndexVacuumInterval = -1
	t.Setenv("TREEDB_COLLECTION_DOCUMENT_FORMAT", "bson")

	var owner *treedb.DB
	var backend *backenddb.DB
	var manager *collections.CollectionManager
	var collection *collections.Collection
	var budget interface{ Stats() memtable.COWStats }
	var held []*collections.CollectionReadView
	closeOwner := func() {
		t.Helper()
		for _, view := range held {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		}
		held = nil
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if owner == nil {
			return
		}
		if err := owner.Close(); err != nil {
			t.Errorf("checked Close: %v", err)
		}
		owner = nil
		if budget != nil {
			stats := budget.Stats()
			if stats.TotalBytes != 0 || stats.ExternalBytes != 0 || stats.ReservedBytes != 0 ||
				stats.ExternalLeases != 0 || stats.Views != 0 || stats.Sources != 0 || stats.Generations != 0 {
				t.Errorf("budget still live after Close: %+v", stats)
			}
		}
		if backend != nil {
			pins := backend.ValueLogIdentityPinRegistry()
			if stats := pins.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 ||
				stats.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
				t.Errorf("value-log custody still live after Close: %+v, %v", stats, pins.CleanupError())
			}
		}
		budget, backend = nil, nil
	}
	t.Cleanup(closeOwner)

	open := func(create bool) {
		t.Helper()
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		limits := capture.NativeAllocator().Limits()
		budget = capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		if limits != opts.COWMemtableLimits {
			t.Fatalf("live limits=%+v want %+v", limits, opts.COWMemtableLimits)
		}
		manager = collections.NewCollectionManager(backend)
		if !manager.UsesNativeCommandWALApply() || owner.Stats()["treedb.command_wal.writer.container"] != "fixed" {
			t.Fatal("fixture requires the fixed native command WAL owner")
		}
		if create {
			_, err = manager.CreateCollection(&collections.CollectionMeta{
				Name:    name,
				Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		collection, err = manager.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
	}
	stat := func(stats map[string]string, key string) uint64 {
		t.Helper()
		value, err := strconv.ParseUint(stats[key], 10, 64)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		return value
	}
	settledBudget := func() memtable.COWStats {
		t.Helper()
		if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
			t.Fatal(err)
		}
		return budget.Stats()
	}
	open(true)
	var ids, docs [rows][]byte
	for i := range rows {
		ids[i], docs[i] = benchmarkDocumentID(i), benchmarkBSONDocument(t, i, true)
	}
	for i := range rows {
		before, err := backend.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		next := backend.CommandWALNextLSN()
		got, err := collection.InsertBatchValidatedBSON(ids[i:i+1], docs[i:i+1])
		if err != nil || len(got) != 1 || !bytes.Equal(got[0], ids[i]) {
			t.Fatalf("insert %d: ID=%q err=%v", i, got, err)
		}
		after, err := backend.ObserveNativeProducer()
		stats := owner.Stats()
		if err != nil || after.LogicalSequence != before.LogicalSequence+1 ||
			backend.CommandWALNextLSN() != next+1 || stat(stats, acceptedLSN) != next ||
			stat(stats, durableLSN) < next {
			t.Fatalf("ACK %d: logical %d to %d, LSN wanted %d, next %d, accepted %s, durable %s, err=%v",
				i, before.LogicalSequence, after.LogicalSequence, next, backend.CommandWALNextLSN(),
				stats[acceptedLSN], stats[durableLSN], err)
		}
		view, err := collection.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, view)
	}
	accepted := stat(owner.Stats(), acceptedLSN)
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpoint := owner.Stats()
	if stat(checkpoint, appliedLSN) != accepted {
		t.Fatalf("checkpoint applied %s, want %d", checkpoint[appliedLSN], accepted)
	}
	for i, view := range held {
		got, err := view.FetchDocumentsByID(ids[:], collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != rows {
			t.Fatalf("held view %d: results=%d err=%v", i, len(got.Results), err)
		}
		for j, row := range got.Results {
			if row.Found != (j <= i) || (row.Found && !bytes.Equal(row.Document, docs[j])) {
				t.Fatalf("held view %d changed BSON at row %d", i, j)
			}
		}
	}

	before := settledBudget()
	for _, view := range held {
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	after := settledBudget()
	if before.ExternalBytes <= after.ExternalBytes || before.ReservedBytes <= after.ReservedBytes ||
		before.ExternalLeases <= after.ExternalLeases || after.Views != 0 {
		t.Fatalf("held view charges did not refund: before=%+v after=%+v", before, after)
	}
	for _, view := range held {
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if repeated := settledBudget(); repeated != after {
		t.Fatalf("repeated releases changed the settled budget: %+v to %+v", after, repeated)
	}
	afterRelease := owner.Stats()
	for _, key := range []string{acceptedLSN, durableLSN, appliedLSN, "treedb.cache.cow.txn.logical_seq", "treedb.command_wal.next_lsn"} {
		if afterRelease[key] != checkpoint[key] {
			t.Fatalf("view release changed %s: %q to %q", key, checkpoint[key], afterRelease[key])
		}
	}
	t.Logf("held owner refund: external=%d reserved=%d leases=%d", before.ExternalBytes-after.ExternalBytes,
		before.ReservedBytes-after.ReservedBytes, before.ExternalLeases-after.ExternalLeases)

	census := func() {
		t.Helper()
		seen := 0
		truncated, err := collection.ScanDocumentsFunc(rows+1, func(record collections.DocumentRecord) (bool, error) {
			if seen >= rows || !bytes.Equal(record.ID, ids[seen]) || !bytes.Equal(record.Document, docs[seen]) {
				return false, fmt.Errorf("unexpected BSON at row %d", seen)
			}
			seen++
			return true, nil
		})
		if err != nil || truncated || seen != rows {
			t.Fatalf("typed census=%d want=%d truncated=%t err=%v", seen, rows, truncated, err)
		}
	}
	census()
	closeOwner()
	open(false)
	census()
	closeOwner()
}
