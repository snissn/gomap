package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Isolate the buffered conflict boundary from the combiner's later successful
// member. Even an owner with a physically appended frame must get the original
// conflict, without any permission to split or replay its assigned mutation.
func TestBufferedInsertConflictOutcomeBoundary(t *testing.T) {
	for _, assigned := range []bool{false, true} {
		for _, unique := range []bool{false, true} {
			t.Run(fmt.Sprintf("assigned_%t/unique_%t", assigned, unique), func(t *testing.T) {
				d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := cleanup(); err != nil {
						t.Errorf("close: %v", err)
					}
				})
				mgr := NewCollectionManager(d)
				if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON, DisableBufferedIndexedAsyncFlush: true}, Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}}}); err != nil {
					t.Fatal(err)
				}
				col, err := mgr.OpenCollection("users")
				if err != nil {
					t.Fatal(err)
				}
				seed := mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "seed@example.com"}})
				if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil {
					t.Fatal(err)
				}
				ids := [][]byte{[]byte("seed"), []byte("valid")}
				docs := [][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "replacement@example.com"}}), mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "valid@example.com"}})}
				want := ErrDocumentExists
				if unique {
					ids[0] = []byte("conflict")
					docs[0] = seed
					want = ErrUniqueIndexConflict
				}
				intent, err := col.newCollectionInsertCommandWALIntent([]commitlog.CollectionDocument{{ID: ids[0], Document: docs[0]}, {ID: ids[1], Document: docs[1]}}, nil, nil)
				if err != nil || intent == nil {
					t.Fatalf("intent=%v err=%v", intent, err)
				}
				if assigned {
					// Retain the existing buffered seed while modeling an external
					// owner that has already appended this conflicting frame.
					unlock := d.LockCommandWALStaging()
					_, err := d.AppendStagedCommandWALIntent(intent, true)
					unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
				beforeLSN := intent.AssignedLSN()
				if (beforeLSN != 0) != assigned {
					t.Fatalf("assigned=%t lsn=%d", assigned, beforeLSN)
				}
				snap := d.AcquireSnapshot()
				if snap == nil {
					t.Fatal("missing snapshot")
				}
				defer func() { _ = snap.Close() }()
				catalog, err := col.catalogForSnapshot(snap)
				if err != nil {
					t.Fatal(err)
				}
				options, err := collectionPlannerOptions(catalog.meta)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := (insertBatchPlanner{collection: "users", primaryRoot: collectionPrimaryRootName("users"), indexes: plannerIndexes(catalog.meta.Indexes), options: options}).planInsertBatch(ids, docs)
				if err != nil {
					t.Fatal(err)
				}
				defer resetCollectionRunTables(plan.runs)
				before, _ := d.StateToken()
				next := d.CommandWALNextLSN()
				frames := collectionCommandWALFrames(t, d.Dir())
				domain := col.writeDomain
				domain.mu.RLock()
				generation, count, first, last := domain.writeGeneration, domain.count, domain.pendingCommandWALFirst, domain.pendingCommandWALLast
				domain.mu.RUnlock()
				var eventMu sync.Mutex
				var events []bufferedConflictEvent
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					record := bufferedConflictEvent{Event: event, Retirement: bufferedConflictRetirementEvent(event)}
					record.Paths = append([]string(nil), event.Paths...)
					if event.StablePath != nil {
						record.StablePathName = event.StablePath.Name()
					}
					if event.StableParent != nil {
						record.StableParentName = event.StableParent.Name()
					}
					// Stable handles belong only to the synchronous callback.
					record.StablePath, record.StableParent = nil, nil
					eventMu.Lock()
					events = append(events, record)
					eventMu.Unlock()
					return nil
				})
				var restored bool
				defer func() {
					if !restored {
						restore()
					}
				}()
				unlock := col.lockMutation()
				_, err = col.bufferIndexedInsertPlanLocked(snap, catalog, snapshotCommitSeq(snap), snapshotSystemRoot(snap), plan, nil, nil, nil, 0, intent, false, nil)
				unlock.Unlock()
				restore()
				restored = true
				eventMu.Lock()
				observed := append([]bufferedConflictEvent(nil), events...)
				eventMu.Unlock()
				for _, event := range observed {
					t.Logf("durability event: %+v", event)
					if !event.Retirement {
						t.Errorf("rejection reached a publication or unknown durability event: %+v", event)
					}
				}
				if !errors.Is(err, want) || errors.Is(err, ErrMutationNotAccepted) == assigned || errors.Is(err, ErrCommitAmbiguous) || backenddb.CommitPublicationAccepted(err) {
					t.Fatalf("conflict=%v assigned=%t want=%v", err, assigned, want)
				}
				now, _ := d.StateToken()
				if now != before {
					t.Errorf("rejection changed state: got=%+v want=%+v", now, before)
				}
				if got := intent.AssignedLSN(); got != beforeLSN {
					t.Errorf("rejection changed assigned LSN: got=%d want=%d", got, beforeLSN)
				}
				if got := d.CommandWALNextLSN(); got != next {
					t.Errorf("rejection changed next LSN: got=%d want=%d", got, next)
				}
				if got := collectionCommandWALFrames(t, d.Dir()); !reflect.DeepEqual(frames, got) {
					t.Errorf("rejection changed WAL frames: got=%+v want=%+v", got, frames)
				}
				domain.mu.RLock()
				unchanged := domain.writeGeneration == generation && domain.count == count && domain.pendingCommandWALFirst == first && domain.pendingCommandWALLast == last
				domain.mu.RUnlock()
				if !unchanged {
					t.Fatal("rejection changed buffered publication state")
				}
				if got, err := col.Get([]byte("seed")); err != nil || !bytes.Equal(got, seed) {
					t.Fatalf("seed changed: %x err=%v", got, err)
				}
				if got, err := col.Get([]byte("valid")); err != nil || got != nil {
					t.Fatalf("valid member became visible before fallback: %x err=%v", got, err)
				}
			})
		}
	}
}
