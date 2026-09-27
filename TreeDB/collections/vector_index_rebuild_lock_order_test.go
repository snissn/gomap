package collections

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A raw barrier may retain a selected write domain even after its pending writes
// have drained. It must be able to take mutation ownership before either rebuild
// capture or final publication acquires that ownership.
// TryLock turns the otherwise unbounded lock cycle into a deterministic failure.
func TestVectorIndexRebuildDrainsRawBarriersBeforeMutation(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalized_%t/empty_%t", normalized, empty), func(t *testing.T) {
				meta := typedMinimaCollectionMeta()
				if normalized {
					meta = cosineNormalizedF32V1TestMeta()
				}
				_, db, col := openTypedMinimaCollectionMeta(t, meta)
				defer db.Close()
				if !empty {
					if _, _, err := col.InsertTypedBatchWithStats(
						[][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)},
						cosineNormalizedF32V1TestColumns([]float32{1, 0, 0, 0, 0, 0, 0, 0}),
					); err != nil {
						t.Fatal(err)
					}
				}
				var calls atomic.Int32
				unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
					calls.Add(1)
					if !col.writeDomain.mutationMu.TryLock() {
						return errors.New("rebuild entered raw barrier while owning collection mutation")
					}
					col.writeDomain.mutationMu.Unlock()
					return nil
				})
				defer unregister()
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatal(err)
				}
				if got := calls.Load(); got != 2 {
					t.Fatalf("raw barrier calls=%d; want capture and final handoff drains, no nested drain", got)
				}
			})
		}
	}
}

func TestVectorIndexRebuildCheckpointPublicationHandoff(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		for _, changedSource := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalized_%t/changed_source_%t", normalized, changedSource), func(t *testing.T) {
				meta := typedMinimaCollectionMeta()
				if normalized {
					meta = cosineNormalizedF32V1TestMeta()
				}
				dir, db, col := openTypedMinimaCollectionMeta(t, meta)
				defer db.Close()
				put := func(id string) {
					t.Helper()
					if _, _, err := col.InsertTypedBatchWithStats(
						[][]byte{[]byte(id)}, [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, id))},
						cosineNormalizedF32V1TestColumns([]float32{1, 0, 0, 0, 0, 0, 0, 0}),
					); err != nil {
						t.Fatal(err)
					}
				}
				put("a")
				constructed, resumeConstruction := make(chan struct{}), make(chan struct{})
				resumeBuild := sync.OnceFunc(func() { close(resumeConstruction) })
				defer resumeBuild()
				restoreBuild := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
					close(constructed)
					<-resumeConstruction
				})
				defer restoreBuild()
				handedOff, resumePublication := make(chan struct{}), make(chan struct{})
				resumePublish := sync.OnceFunc(func() { close(resumePublication) })
				defer resumePublish()
				restoreHandoff := setColumnVectorGraphRebuildAfterMutationReleaseTestHook(func(current *Collection) {
					if current == col {
						close(handedOff)
						<-resumePublication
					}
				})
				defer restoreHandoff()
				wait := func(done <-chan struct{}, stage string) {
					t.Helper()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatalf("timeout waiting for %s", stage)
					}
				}
				rebuilt := make(chan error, 1)
				go func() { _, err := col.RebuildVectorIndex("embedding_graph"); rebuilt <- err }()
				wait(constructed, "paused construction")
				// A sibling/raw command must not be serialized behind graph construction.
				published := make(chan error, 1)
				go func() { published <- db.Set([]byte("unrelated"), []byte("retained")) }()
				select {
				case err := <-published:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("graph construction held global publication ownership")
				}
				selected, tryMutation := make(chan struct{}), make(chan struct{})
				allowMutation := sync.OnceFunc(func() { close(tryMutation) })
				var barrierCalls atomic.Int32
				unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
					if barrierCalls.Add(1) == 1 {
						close(selected)
						<-tryMutation
					}
					if !col.writeDomain.mutationMu.TryLock() {
						return errors.New("checkpoint barrier could not acquire released rebuild mutation")
					}
					col.writeDomain.mutationMu.Unlock()
					return nil
				})
				defer func() { allowMutation(); unregister() }()
				checkpointed := make(chan error, 1)
				go func() { checkpointed <- db.Checkpoint() }()
				wait(selected, "checkpoint raw-barrier ownership")
				resumeBuild()
				wait(handedOff, "rebuild mutation release")
				allowMutation()
				select {
				case err := <-checkpointed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("checkpoint did not complete through rebuild handoff")
				}
				if changedSource {
					put("b")
					// Finish the accepted source command before measuring rejection;
					// the final raw drain may legitimately publish lower commands.
					if err := col.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				beforeCommit, beforeSystem := dbCommitSeqAndSystemRoot(db)
				beforeFrames := countCollectionCommandWALFrames(t, dir)
				resumePublish()
				select {
				case err := <-rebuilt:
					if changedSource && (err == nil || (!errors.Is(err, ErrConcurrentMutation) && !strings.Contains(err.Error(), `[stage=root_descriptor_system_delta diff=manifest_progress]`))) {
						t.Fatalf("changed source rebuild=%v; want concurrent-mutation rejection", err)
					}
					if !changedSource && err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("rebuild did not finish after checkpoint")
				}
				if changedSource {
					commit, system := dbCommitSeqAndSystemRoot(db)
					if commit != beforeCommit || system != beforeSystem || countCollectionCommandWALFrames(t, dir) != beforeFrames {
						t.Fatal("source rejection changed a root or appended a rebuild frame")
					}
				}
				restoreBuild()
				restoreHandoff()
				unregister()
				if changedSource {
					if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
						t.Fatalf("fresh rebuild after source rejection: %v", err)
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				reopened := openTypedMinimaDB(t, dir)
				defer reopened.Close()
				current, err := NewCollectionManager(reopened).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
				if normalized {
					// Lock handoff coverage above runs on every host; normalized
					// search additionally requires production holder authority.
					requireTypedGraphPublicServingTest(t)
					if err := current.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
						t.Fatal(err)
					}
				}
				query := VectorIndexSearchOptions{
					IndexName: "embedding_graph", Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 2, EfSearch: 8,
				}
				var result VectorIndexSearchResponse
				if normalized {
					query.StatsMode = VectorIndexSearchStatsModeProduction
					query.QueryMode = VectorIndexQueryModeExact
					var buffer VectorIndexSearchBuffer
					var view *CollectionReadView
					result, view, err = current.SearchVectorIndexWithBufferReadView(query, &buffer)
					if view != nil {
						defer view.Close()
					}
				} else {
					result, err = current.SearchVectorIndex(query)
				}
				want := 1
				if changedSource {
					want = 2
				}
				if err != nil || len(result.Results) != want {
					t.Fatalf("reopened graph results=%+v err=%v; want%d documents", result.Results, err, want)
				}
				if got, err := reopened.Get([]byte("unrelated")); err != nil || string(got) != "retained" {
					t.Fatalf("reopened raw prefix=%q err=%v", got, err)
				}
			})
		}
	}
}

func TestVectorIndexRebuildReleasesRawWhileMutationIsBusy(t *testing.T) {
	for _, busyAt := range []int32{1, 2} {
		t.Run(fmt.Sprintf("barrier_%d", busyAt), func(t *testing.T) {
			_, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			contended := make(chan struct{})
			var calls atomic.Int32
			var locked atomic.Bool
			release := func() {
				if locked.CompareAndSwap(true, false) {
					col.writeDomain.mutationMu.Unlock()
				}
			}
			unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
				if calls.Add(1) == busyAt {
					// Another mutation owner survives the completed raw drain.
					// Rebuild must release raw before waiting on that owner.
					col.writeDomain.mutationMu.Lock()
					locked.Store(true)
					close(contended)
				}
				return nil
			})
			defer func() { release(); unregister() }()
			rebuilt := make(chan error, 1)
			go func() { _, err := col.RebuildVectorIndex("embedding_graph"); rebuilt <- err }()
			select {
			case <-contended:
			case <-time.After(3 * time.Second):
				t.Fatal("rebuild did not reach mutation contention")
			}
			published := make(chan error, 1)
			go func() { published <- db.Set([]byte("unrelated"), []byte("retained")) }()
			select {
			case err := <-published:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("rebuild waited on mutation while retaining raw")
			}
			release()
			select {
			case err := <-rebuilt:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("rebuild did not finish after mutation release")
			}
		})
	}
}
