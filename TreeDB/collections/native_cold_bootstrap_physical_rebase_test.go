//go:build !windows

package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A cold private import must cross public physical maintenance without changing
// its original logical opening. The controls distinguish published state and a
// compatible raw-only checkpoint. Windows rejects public online vacuum.
func TestNativeColdBootstrapPreservesPhysicalRebase(t *testing.T) {
	for _, mode := range []string{"cold_move", "cold_no_move", "published_move", "cold_raw_checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			const name = "bootstrap_docs"
			var owner *treedb.DB
			var backend *backenddb.DB
			var manager *CollectionManager
			var collection *Collection
			var held, middle, current *NativeJSONRead
			var txn *NativeJSONTxn
			var observer *backenddb.NativeInstallObserverForTest
			var budget interface{ Stats() memtable.COWStats }
			closeAll := func() error {
				var outcome error
				if observer != nil {
					observer.Close()
					observer = nil
				}
				if txn != nil {
					closing := txn
					txn = nil
					outcome = errors.Join(outcome, closing.Close())
				}
				for _, slot := range []**NativeJSONRead{&current, &middle, &held} {
					if *slot != nil {
						closing := *slot
						*slot = nil
						outcome = errors.Join(outcome, closing.Close())
					}
				}
				if manager != nil {
					closing := manager
					manager, collection = nil, nil
					outcome = errors.Join(outcome, closing.Close())
				}
				if owner != nil {
					closing := owner
					owner, backend = nil, nil
					outcome = errors.Join(outcome, closing.Close())
				}
				if budget != nil {
					if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
						outcome = errors.Join(outcome, fmt.Errorf("full close retained admission: %+v", stats))
					}
					budget = nil
				}
				return outcome
			}
			t.Cleanup(func() {
				if err := closeAll(); err != nil {
					t.Errorf("original cleanup: %v", err)
				}
			})
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
				manager = NewCollectionManager(backend)
				if !manager.UsesNativeCommandWALApply() {
					t.Fatal("INVALID_WITNESS: not the native public owner")
				}
				if create {
					if _, err := manager.CreateCollection(&CollectionMeta{Name: name}); err != nil {
						t.Fatal(err)
					}
				}
				collection, err = manager.OpenCollection(name)
				if err != nil || collection == nil || collection.nativeJSON == nil {
					t.Fatalf("open native collection: %v", err)
				}
			}
			document := []byte(`{"tenant":"seed","payload":"` + strings.Repeat("x", 256) + `"}`)
			seedID := func(row int) []byte { return []byte(fmt.Sprintf("d%03d", row)) }
			open(true)
			// Two fixed public batches and checkpoints leave normal obsolete pager
			// pages. No forced roots, allocator/selector override or setup loop.
			for batch := 0; batch < 2; batch++ {
				ids, docs := make([][]byte, 128), make([][]byte, 128)
				for i := range ids {
					ids[i], docs[i] = seedID(batch*128+i), document
				}
				if _, err := collection.InsertBatch(ids, docs); err != nil {
					t.Fatal(err)
				}
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if err := closeAll(); err != nil {
				t.Fatalf("original seed Close: %v", err)
			}
			open(false) // Real cold owner, not a reconstructed semantic state.
			if mode == "published_move" {
				if _, err := collection.Insert([]byte("published"), []byte(`{"tenant":"published"}`)); err != nil {
					t.Fatal(err)
				}
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			held, err = collection.nativeJSON.Capture(name)
			if err != nil {
				t.Fatal(err)
			}
			txn, err = collection.nativeJSON.Begin(name)
			if err != nil {
				t.Fatal(err)
			}
			budget = txn.core.NativeAllocator().(interface{ Stats() memtable.COWStats })
			published := mode == "published_move"
			if (txn.core.State() != nil) != published || (txn.imported == nil) != published || (held.capture.State() != nil) != published {
				t.Fatal("INVALID_WITNESS: cold/private-bootstrap versus published-state arm not activated")
			}
			opening := txn.core.Snapshot()
			beforeToken, valid := opening.StateToken()
			beforePager := opening.Pager()
			beforeLogical, expectedLSN := txn.core.LogicalSequence(), backend.CommandWALNextLSN()
			beforeDomain, found := txn.bindings[0].probe.collection.domains.Get("primary")
			if !valid || beforePager == nil || !found || beforeDomain.catalogRoot == 0 || beforeDomain.baseRoot != beforeDomain.catalogRoot {
				t.Fatal("INVALID_WITNESS: missing original persisted primary basis")
			}
			beforeRoot := beforeDomain.catalogRoot
			heldToken, heldValid := held.capture.Snapshot().StateToken()
			heldPager := held.capture.Snapshot().Pager()
			if !heldValid {
				t.Fatal("INVALID_WITNESS: missing original held physical cut")
			}
			targetID, target := []byte("target"), []byte(`{"tenant":"once"}`)
			if err := txn.InsertBatch([][]byte{targetID}, [][]byte{target}); err != nil {
				t.Fatal(err)
			}
			if backend.CommandWALNextLSN() != expectedLSN {
				t.Fatal("INVALID_WITNESS: staging assigned a command")
			}
			if mode == "cold_raw_checkpoint" {
				if err := owner.Set([]byte("unrelated_raw"), []byte("raw")); err != nil {
					t.Fatal(err)
				}
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				expectedLSN++
				beforeLogical++
			} else if mode != "cold_no_move" {
				attempt := owner.VacuumOnlineStats().AttemptID
				vacuumErr := owner.VacuumIndexOnline(t.Context()) // Exactly one original public operation.
				work := owner.VacuumOnlineStats()
				t.Logf("COLD_BOOTSTRAP_MAINTENANCE mode=%s attempt=%d completed=%t canceled=%t pages=%d error=%v", mode, work.AttemptID, work.WorkCompleted, work.Canceled, work.ReplacementPagerPages, vacuumErr)
				if vacuumErr != nil || work.AttemptID <= attempt || !work.WorkCompleted || work.Canceled || work.ReplacementPagerPages == 0 {
					t.Fatalf("INVALID_WITNESS: original public maintenance: %v", vacuumErr)
				}
			}
			middle, err = collection.nativeJSON.Capture(name)
			if err != nil {
				t.Fatalf("INVALID_WITNESS: post-maintenance capture: %v", err)
			}
			afterToken, valid := middle.capture.Snapshot().StateToken()
			afterPager := middle.capture.Snapshot().Pager()
			catalog, err := loadCollectionCatalog(middle.capture.Snapshot(), name)
			if err != nil || catalog == nil {
				t.Fatalf("INVALID_WITNESS: actual maintained catalog: %v", err)
			}
			afterRoot := catalog.rootID(collectionPrimaryRootName(name))
			wantApplied := beforeToken.AppliedCommandLSN
			if mode == "cold_raw_checkpoint" {
				wantApplied++
			}
			if !valid || afterRoot == 0 || middle.domain.domain == nil || middle.domain.domain.catalogRoot != afterRoot ||
				middle.capture.LogicalSequence() != beforeLogical || (middle.capture.State() != nil) != published ||
				afterToken.AppliedCommandLSN != wantApplied || backend.CommandWALNextLSN() != expectedLSN {
				t.Fatal("INVALID_WITNESS: maintenance changed logical identity or lacks an authentic catalog")
			}
			if mode == "cold_no_move" {
				if afterPager != beforePager || afterRoot != beforeRoot || afterToken != beforeToken {
					t.Fatal("INVALID_WITNESS: no-move control moved its physical basis")
				}
			} else if mode == "cold_raw_checkpoint" {
				if afterRoot != beforeRoot || afterPager != beforePager || afterToken.SystemRootPageID != beforeToken.SystemRootPageID || afterToken == beforeToken {
					t.Fatal("INVALID_WITNESS: raw-only checkpoint did not preserve collection roots and advance physical coverage")
				}
			} else if afterPager == beforePager || afterRoot == beforeRoot {
				t.Fatalf("INVALID_WITNESS: fixed public vacuum did not move both pager and primary root: before=%d after=%d", beforeRoot, afterRoot)
			}
			if mode != "cold_raw_checkpoint" && afterToken.MaxEntryRevision != beforeToken.MaxEntryRevision {
				t.Fatal("INVALID_WITNESS: physical maintenance changed entry revisions")
			}
			if got, err := middle.Get(seedID(0)); err != nil || !bytes.Equal(got, document) {
				t.Fatalf("INVALID_WITNESS: maintained original row: %v", err)
			}
			t.Logf("COLD_BOOTSTRAP_ACTIVATED mode=%s published=%t root=%d->%d logical=%d assigned_next=%d", mode, published, beforeRoot, afterRoot, beforeLogical, expectedLSN)
			closingRead := middle
			middle = nil
			if err := closingRead.Close(); err != nil {
				t.Fatalf("original diagnostic read Close: %v", err)
			}
			observer, err = backenddb.NewNativeInstallObserverForTest(backend, commitlog.CommandKindCollectionInsertBatchByID, expectedLSN)
			if err != nil {
				t.Fatal(err)
			}
			commitErr := txn.Commit() // The original transaction is committed exactly ONCE.
			closingTxn := txn
			txn = nil
			txnCloseErr := closingTxn.Close()
			if commitErr != nil || txnCloseErr != nil {
				t.Errorf("original once-only Commit/Close: commit=%v close=%v", commitErr, txnCloseErr)
			}
			select {
			case event := <-observer.Events():
				t.Logf("COLD_BOOTSTRAP_ACK mode=%s receipt=%+v", mode, event)
				if observer.Dropped() || !event.Consumed || !event.Assigned || !event.LogicalInstalled || !event.WALCompleted || event.AssignedLSN != expectedLSN || event.LogicalSequence != beforeLogical+1 {
					t.Errorf("original acknowledgement identity/completion: %+v", event)
				}
			default:
				t.Error("original commit produced no exact install acknowledgement")
			}
			select {
			case extra := <-observer.Events():
				t.Errorf("duplicate original install acknowledgement: %+v", extra)
			default:
			}
			observer.Close()
			observer = nil
			if backend.CommandWALNextLSN() != expectedLSN+1 {
				t.Error("original commit did not assign exactly one command identity")
			}
			frames := 0
			for _, frame := range collectionCommandWALFrames(t, backend.Dir()) {
				if frame.LSN == expectedLSN {
					frames++
					if frame.Kind != commitlog.CommandKindCollectionInsertBatchByID {
						t.Error("original command kind changed")
					}
				}
			}
			if frames != 1 {
				t.Errorf("original command frame count=%d want1", frames)
			}
			current, err = collection.nativeJSON.Capture(name)
			if err != nil {
				t.Errorf("current capture after original commit: %v", err)
			} else {
				physicalCatalog, catalogErr := loadCollectionCatalog(current.capture.Snapshot(), name)
				if catalogErr != nil || physicalCatalog == nil || current.domain.domain == nil {
					t.Errorf("current persisted catalog pairing unavailable: %v", catalogErr)
				} else {
					physicalRoot := physicalCatalog.rootID(collectionPrimaryRootName(name))
					domain := current.domain.domain
					t.Logf("COLD_BOOTSTRAP_PAIR mode=%s physical=%d semantic_catalog=%d semantic_base=%d", mode, physicalRoot, domain.catalogRoot, domain.baseRoot)
					if physicalRoot != afterRoot || domain.catalogRoot != physicalRoot || domain.baseRoot != physicalRoot || current.capture.Snapshot().Pager() != afterPager {
						t.Error("accepted logical state retained stale imported physical provenance")
					}
				}
				refreshed, refreshErr := prepareNativeEngineBasisRefresh(current.engine, current.capture.Snapshot(), current.capture.Snapshot())
				var refundErr error
				if refreshed != nil {
					refundErr = finishPrivateNative(refreshed.Release())
					refreshed = nil
				}
				t.Logf("COLD_BOOTSTRAP_SUBSEQUENT_REFRESH mode=%s error=%v refund=%v", mode, refreshErr, refundErr)
				if refreshErr != nil || refundErr != nil {
					t.Errorf("original physical refresh/cleanup after accepted commit: refresh=%v cleanup=%v", refreshErr, refundErr)
				}
				for _, row := range []struct{ id, want []byte }{{seedID(0), document}, {targetID, target}} {
					if got, err := current.Get(row.id); err != nil || !bytes.Equal(got, row.want) {
						t.Errorf("current acknowledged value %q: %v", row.id, err)
					}
				}
				for row := 1; row < 256; row++ {
					if got, err := current.Get(seedID(row)); err != nil || !bytes.Equal(got, document) {
						t.Errorf("current acknowledged seed %d: %v", row, err)
					}
				}
			}
			if got, err := held.Get(seedID(0)); err != nil || !bytes.Equal(got, document) {
				t.Errorf("original held row changed: %v", err)
			}
			if _, err := held.Get(targetID); !errors.Is(err, tree.ErrKeyNotFound) {
				t.Errorf("original held view saw later mutation: %v", err)
			}
			if token, valid := held.capture.Snapshot().StateToken(); !valid || token != heldToken || held.capture.Snapshot().Pager() != heldPager {
				t.Error("original held physical cut mutated")
			}
			if err := closeAll(); err != nil {
				t.Errorf("original checked owner cleanup: %v", err)
				return // Do not reopen while physical teardown is uncertain.
			}
			open(false)
			for _, row := range []struct{ id, want []byte }{{seedID(0), document}, {targetID, target}} {
				if got, err := collection.Get(row.id); err != nil || !bytes.Equal(got, row.want) {
					t.Errorf("fresh reopen acknowledged value %q: %v", row.id, err)
				}
			}
			for row := 1; row < 256; row++ {
				if got, err := collection.Get(seedID(row)); err != nil || !bytes.Equal(got, document) {
					t.Errorf("fresh reopen acknowledged seed %d: %v", row, err)
				}
			}
			if backend.CommandWALNextLSN() != expectedLSN+1 {
				t.Error("fresh reopen duplicated the original command")
			}
			if mode == "cold_raw_checkpoint" {
				if got, err := owner.Get([]byte("unrelated_raw")); err != nil || !bytes.Equal(got, []byte("raw")) {
					t.Errorf("fresh reopen unrelated raw value: %v", err)
				}
			}
			if err := closeAll(); err != nil {
				t.Errorf("original reopened owner Close: %v", err)
			}
		})
	}
}
