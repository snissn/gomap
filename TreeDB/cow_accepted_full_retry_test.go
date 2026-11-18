package treedb

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCOWPublicAcceptedFullCheckpointRetriesOriginalWrite(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, name := range []string{"success", "rotation-error"} {
		t.Run(name, func(t *testing.T) {
			opts := indexedSliceOptions(ProfileCommandWALRelaxed, t.TempDir())
			opts.MemtableShards = 2
			opts.COWMemtableLimits = DefaultCOWMemtableLimits()
			opts.COWMemtableLimits.MaxSources = 4
			opts.COWMemtableLimits.MaxGenerations = 8
			opts.DisableBackgroundPrune = true
			opts.BackgroundIndexVacuumInterval = -1
			open := func() (*DB, *backenddb.DB, *collections.CollectionManager) {
				t.Helper()
				db, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				backend, err := db.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager := collections.NewCollectionManager(backend)
				t.Cleanup(func() { _ = manager.Close() })
				return db, backend, manager
			}
			closeDB := func(db *DB, manager *collections.CollectionManager) {
				t.Helper()
				cache := db.cached
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				requirePublicCOWFinalZero(t, cache)
			}
			openRows := func(manager *collections.CollectionManager) *collections.Collection {
				t.Helper()
				rows, err := manager.OpenCollection("rows")
				if err != nil {
					t.Fatal(err)
				}
				return rows
			}
			db, backend, manager := open()
			if _, err := manager.CreateCollection(&collections.CollectionMeta{
				Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
			}); err != nil {
				t.Fatal(err)
			}
			rows := openRows(manager)
			id := []byte("row")
			doc := nativeBSONDocument(t, bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(0)}})
			if _, err := rows.InsertBatchValidatedBSON([][]byte{id}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			held, err := rows.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = held.Close() })
			assertHeld := func() {
				t.Helper()
				got, err := held.FetchDocumentsByID([][]byte{id}, collections.DocumentFetchOptions{})
				if err != nil || len(got.Results) != 1 || !got.Results[0].Found {
					t.Fatalf("held pre-checkpoint document unavailable: %+v, %v", got, err)
				}
				if value, ok := bson.Raw(got.Results[0].Document).Lookup("n").Int64OK(); !ok || value != 0 {
					t.Fatalf("held pre-checkpoint document changed: %v", bson.Raw(got.Results[0].Document))
				}
			}
			assertLatest := func(rows *collections.Collection, want int64) {
				t.Helper()
				got, err := rows.Get(id)
				value, ok := bson.Raw(got).Lookup("n").Int64OK()
				if err != nil || !ok || value != want {
					t.Fatalf("current document = %v, %v, want n=%d", bson.Raw(got), err, want)
				}
			}
			update := func(n int64) (bool, bool, error) {
				return rows.UpdateBSONSet(id, []collections.BSONSetField{{Key: "n", Value: nativeBSONQuery(t, n)}})
			}

			prefix, _, err := backend.PrivateCOWAcceptedPrefix(db.txnOwner)
			if err != nil {
				t.Fatal(err)
			}
			state := func() backenddb.StateToken {
				t.Helper()
				token, ok := backend.StateToken()
				if !ok {
					t.Fatal("missing physical state token")
				}
				return token
			}
			initial, physical := prefix.LogicalFrontiers(), state()
			// These small native BSON writes retain one original accepted entry
			// each and leave the producer's byte limits at their defaults.
			capacity := int64(opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations)
			for n := int64(1); n <= capacity; n++ {
				if matched, changed, err := update(n); err != nil || !matched || !changed {
					t.Fatalf("filling ordinary slot %d: matched=%t changed=%t err=%v", n, matched, changed, err)
				}
			}
			full := prefix.LogicalFrontiers()
			next := backend.CommandWALNextLSN()
			if full.V != initial.V+uint64(capacity) || full.C != initial.C+uint64(capacity) ||
				full.S != initial.S || state() != physical || next != full.C+1 {
				t.Fatalf("ordinary writes did not fill one unsealed prefix: initial=%+v full=%+v next=%d", initial, full, next)
			}
			probe, err := prefix.ReserveAccepted(false)
			if err == nil {
				if err := probe.CancelAccepted(); err != nil {
					t.Fatal(err)
				}
			}
			if !errors.Is(err, rootpublication.ErrAcceptedCapacity) {
				t.Fatalf("expected actual ordinary-slot saturation, got %v", err)
			}
			pressure, err := backend.ObserveNativeProducer()
			if err != nil || pressure.UnderPressure {
				t.Fatalf("fixture reached another producer pressure boundary: %+v, %v", pressure, err)
			}

			rotationErr := errors.New("test: full-prefix checkpoint rotation refused")
			var appends, refusals atomic.Int32
			var failRotation atomic.Bool
			failRotation.Store(name == "rotation-error")
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				switch event.Point {
				case durabilitycut.BeforeDependencyAppend:
					appends.Add(1)
				case durabilitycut.BeforeDependencyFileSync:
					if failRotation.Load() {
						refusals.Add(1)
						return rotationErr
					}
				}
				return nil
			})
			defer restore()
			if failRotation.Load() {
				_, _, err := update(capacity + 1)
				if !errors.Is(err, rotationErr) || refusals.Load() != 1 || appends.Load() != 0 {
					t.Fatalf("full write did not return its original checkpoint error before append: err=%v refusals=%d appends=%d", err, refusals.Load(), appends.Load())
				}
				if got := prefix.LogicalFrontiers(); got != full || state() != physical || backend.CommandWALNextLSN() != next {
					t.Fatalf("failed checkpoint advanced the accepted cut or assigned the write: before=%+v after=%+v", full, got)
				}
				assertLatest(rows, capacity)
				assertHeld()
				failRotation.Store(false)
			}

			matched, changed, err := update(capacity + 1)
			after, disk := prefix.LogicalFrontiers(), state()
			if err != nil || !matched || !changed {
				t.Fatalf("original full-queue write did not complete: matched=%t changed=%t err=%v before=%+v after=%+v", matched, changed, err, full, after)
			}
			sealed := rootpublication.SealedPrefix{LogicalSequence: full.V, CanonicalLSN: full.C}
			// S advances under the accepted-owner latch when the entry is
			// retired and its slot recycled. The retry occupies the new suffix.
			if after.S != sealed || disk.AppliedCommandLSN != full.C || disk.CommitSeq != physical.CommitSeq+1 ||
				after.V != full.V+1 || after.C != next || backend.CommandWALNextLSN() != next+1 || appends.Load() != 1 {
				t.Fatalf("need exact physical seal, retirement and a single appended retry: before=%+v after=%+v applied=%d commits=%d appends=%d", full, after, disk.AppliedCommandLSN, disk.CommitSeq-physical.CommitSeq, appends.Load())
			}
			t.Logf("ordinary slots=%d retired=%+v retry_lsn=%d appended=%d preappend_refusals=%d", capacity, sealed, next, appends.Load(), refusals.Load())
			assertLatest(rows, capacity+1)
			assertHeld()

			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			closeDB(db, manager)
			reopened, reopenedBackend, reopenedManager := open()
			assertLatest(openRows(reopenedManager), capacity+1)
			if got := reopenedBackend.State().AppliedCommandLSN; got != next {
				t.Fatalf("reopened applied LSN=%d, want original retried LSN %d", got, next)
			}
			closeDB(reopened, reopenedManager)
		})
	}
}
