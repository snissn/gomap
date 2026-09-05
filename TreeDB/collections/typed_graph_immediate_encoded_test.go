package collections

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestTypedGraphEncodedImmediateFailureRetry(t *testing.T) {
	for _, operation := range []string{"insert", "update", "replace", "delete", "delete-batch", "source-delete", "source-replace", "typed-source"} {
		for _, reject := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/reject=%v", operation, reject), func(t *testing.T) {
				meta := typedMinimaCollectionMeta()
				if operation == "insert" {
					meta.TextIndexes = nil
				}
				dir, db, col := openTypedMinimaCollectionMeta(t, meta)
				defer db.Close()
				columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha beta"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}}}
				id := []byte("a")
				if operation != "insert" {
					if _, _, err := col.InsertTypedBatchWithStats([][]byte{id}, [][]byte{[]byte(`{"id":"a"}`)}, columns); err != nil {
						t.Fatal(err)
					}
					if err := col.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatal(err)
				}
				limits := typedGraphPublicationLimits{Rows: 32, Tombstones: 16, ValueSlots: 128, OwnedBytes: 1 << 20, EncodedOutputBytes: 8 << 20}
				cold := typedGraphColdLimits{ManifestRecords: 128, ManifestBytes: 128 << 10, AssetBytes: 4 << 20, DecodedTermBytes: 4 << 20}
				if reject {
					limits.EncodedOutputBytes = 1
				}
				if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
					t.Fatal(err)
				}
				retained := []byte(fmt.Sprintf(`{"id":"a","remainder":%q}`, strings.Repeat("x", 64<<10)))
				mutate := func() error {
					switch operation {
					case "insert":
						_, _, err := col.InsertTypedBatchWithStats([][]byte{id}, [][]byte{retained}, columns)
						return err
					case "update":
						_, err := col.ReplaceTypedBatch([][]byte{id}, [][]byte{retained}, columns)
						return err
					case "replace":
						doc := []byte(fmt.Sprintf(`{"id":"a","embedding":[1,0,0,0,0,0,0,0],"content":"changed alpha","meta":{"user_id":"u","fpath":"p"},"remainder":%q}`, strings.Repeat("x", 64<<10)))
						_, err := col.Replace(id, doc)
						return err
					case "delete":
						return col.Delete(id)
					case "delete-batch":
						_, err := col.DeleteBatch([][]byte{id})
						return err
					case "source-replace":
						// Existing legacy source API; this is not a zero-JSON typed
						// source producer. Admission reuses its one-time extraction.
						doc := []byte(fmt.Sprintf(`{"id":"a","embedding":[1,0,0,0,0,0,0,0],"content":"changed alpha","meta":{"user_id":"u","fpath":"p"},"remainder":%q}`, strings.Repeat("x", 64<<10)))
						_, err := col.replaceSourceDocumentsWithCommandWALIntent([][]byte{id}, [][]byte{id}, [][]byte{doc}, nil, nil)
						return err
					case "typed-source":
						_, err := col.ReplaceTypedSourceByID([][]byte{id}, [][]byte{id}, [][]byte{retained}, columns)
						return err
					default:
						_, err := col.replaceSourceDocumentsWithCommandWALIntent([][]byte{id}, nil, nil, nil, nil)
						return err
					}
				}
				frames := len(collectionCommandWALFrames(t, dir))
				if reject {
					var earlyAppends atomic.Int64
					restoreEarly := durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Point == durabilitycut.BeforeDependencyAppend {
							earlyAppends.Add(1)
						}
						return nil
					})
					earlyErr := mutate()
					restoreEarly()
					if !errors.Is(earlyErr, errTypedGraphOverlayFoldNeeded) || earlyAppends.Load() != 0 || len(collectionCommandWALFrames(t, dir)) != frames {
						t.Fatalf("pre-effect cap err=%v appends=%d", earlyErr, earlyAppends.Load())
					}
					return
				}
				before := col.typedGraphPublicationSnapshot()
				injected := errors.New("immediate after append")
				var fired atomic.Bool
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					cut := event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.AfterDependencyAppend
					if operation == "delete" || operation == "delete-batch" || operation == "source-delete" {
						// Small delete tables stay inline: inject actual pre-WAL failure,
						// not a nonexistent value-log append, for those siblings.
						cut = event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyAppend
					}
					if cut && fired.CompareAndSwap(false, true) {
						return injected
					}
					return nil
				})
				err := mutate()
				restore()
				coord := col.collectionSchemaCoordinator()
				coord.typedPublicationDebtMu.Lock()
				spent, debt, pending := coord.typedPublicationEncodedBytes, coord.typedPublicationDebt, coord.typedPublicationPending
				coord.typedPublicationDebtMu.Unlock()
				if !errors.Is(err, injected) || !fired.Load() || spent <= 0 || debt != (typedGraphPublicationCost{}) || pending != (typedGraphPublicationCost{}) || col.typedGraphPublicationSnapshot() != before || len(collectionCommandWALFrames(t, dir)) != frames {
					t.Fatalf("failure err=%v fired=%v spent=%d debt=%+v pending=%+v", err, fired.Load(), spent, debt, pending)
				}
				if err := mutate(); err != nil {
					t.Fatal(err)
				}
				coord.typedPublicationDebtMu.Lock()
				after, pending := coord.typedPublicationEncodedBytes, coord.typedPublicationPending
				coord.typedPublicationDebtMu.Unlock()
				wantRows := 1
				if operation == "source-replace" || operation == "typed-source" {
					wantRows = 2
				}
				if after <= spent || pending != (typedGraphPublicationCost{}) || col.typedGraphPublicationSnapshot().physicalRows != wantRows {
					t.Fatalf("retry after=%d spent=%d pending=%+v state=%+v", after, spent, pending, col.typedGraphPublicationSnapshot())
				}
				if operation == "update" || operation == "delete" || operation == "delete-batch" || operation == "source-delete" {
					frames = len(collectionCommandWALFrames(t, dir))
					if err := mutate(); err != nil {
						t.Fatal(err)
					}
					// Existing command-WAL APIs preserve a no-op command frame;
					// it must not reserve another encoded-output attempt.
					if coord.typedPublicationEncodedBytes != after || len(collectionCommandWALFrames(t, dir)) != frames+1 {
						t.Fatalf("no-op encoded=%d->%d WAL=%d->%d", after, coord.typedPublicationEncodedBytes, frames, len(collectionCommandWALFrames(t, dir)))
					}
				}
			})
		}
	}
}

func TestTypedGraphEncodedDetachedPredecessorIDs(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	docs := []columnWriteDocument{{ID: []byte("next"), declaredValuesReady: true, declaredValues: []columnDeclaredValue{
		{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{Type: ColumnStoreValueString, Present: true, String: ""}, {Type: ColumnStoreValueString, Present: true, String: "u"}, {Type: ColumnStoreValueString, Present: true, String: "p"},
	}}}
	prepared := []preparedTextIndexInsert{{indexName: catalog.meta.TextIndexes[0].Name, states: []textDocumentStateValue{{}}}}
	bound := func(domain *collectionWriteDomain) int64 {
		t.Helper()
		cost, err := col.bufferedTypedGraphEncodedBound(snap, catalog, domain, &insertBatchPlan{}, docs, prepared)
		if err != nil {
			t.Fatal(err)
		}
		return cost.flush
	}
	base := bound(&collectionWriteDomain{})
	receipt := func(n int) *typedGraphPublicationReceipt {
		return &typedGraphPublicationReceipt{documents: []columnWriteDocument{{ID: []byte(strings.Repeat("x", n))}}}
	}
	// Exercise the actual ownership containers read under domain.mu: active,
	// queued, and detached publishing predecessors all affect future ordinals.
	domain := &collectionWriteDomain{
		typedReceipts:          []*typedGraphPublicationReceipt{receipt(17)},
		indexedFlushUnits:      []indexedFlushUnit{{typedReceipts: []*typedGraphPublicationReceipt{receipt(512)}}},
		indexedPublishingUnits: []indexedFlushUnit{{typedReceipts: []*typedGraphPublicationReceipt{receipt(255)}}},
	}
	if got := bound(domain) - base; got != 17+512+255 {
		t.Fatalf("predecessor ID allowance=%d", got)
	}
}
