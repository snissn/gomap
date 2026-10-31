package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Stage through the original census/admission/recipe owners, then advance the
// source before the original commit. No recipe, root, LSN or cap is forged.
// These controls exercise real refusal and a later fresh public preflight; the
// wrapper's bounded second attempt remains a separate source-reviewed policy.
func TestNativeOrdinaryGraphPreassignmentSourceChangesHaveNoEffect(t *testing.T) {
	for _, mode := range []string{"physical-handoff", "logical-row"} {
		t.Run(mode, func(t *testing.T) {
			clearNativeOrdinaryPendingGraphCodecEnv(t)
			owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if owner != nil {
					if err := owner.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			defer func() {
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			meta := nativeOrdinaryPendingGraphMeta()
			if _, err := manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			insert := func(start, end int) {
				t.Helper()
				ids, docs := make([][]byte, end-start), make([][]byte, end-start)
				for i := range ids {
					ids[i] = []byte(fmt.Sprintf("doc-%04d", start+i))
					docs[i] = []byte(fmt.Sprintf(`{"embedding":[%d,1]}`, start+i+1))
				}
				if _, err := col.InsertBatch(ids, docs); err != nil {
					t.Fatal(err)
				}
			}
			insert(0, 128)
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			insert(128, 256)

			tx, err := col.nativeJSON.Begin(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if tx != nil {
					if err := tx.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			g := &tx.bindings[0]
			primary, pok := g.probe.collection.domains.Get("primary")
			manifest, mok := g.probe.collection.domains.Get("column/manifest")
			def, found := findVectorIndex(g.probe.meta.VectorIndexes, "vec")
			if !pok || primary.typed == nil || !mok || manifest.baseRoot == 0 || !found {
				t.Fatal("missing original physical prefix/pending graph source")
			}
			root, generation := manifest.baseRoot, g.probe.collection.documentGeneration
			capacity, rows, workers, codec, err := nativeOrdinaryVectorGraphCapacity(tx, g, primary, root, def)
			if err != nil || rows != 256 || codec == 0 {
				t.Fatalf("original graph census capacity=%d rows=%d codec=%d: %v", capacity, rows, codec, err)
			}
			owned := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeVectorGraphRecipe{}))) +
				memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTypedRows{}))) +
				memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
			lease, err := tx.core.NativeAllocator().AdmitNativeBytes(owned)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.adoptNativeTyped(g, nil, nil, nil, ColumnPublishOperationInsert, capacity, lease); err != nil {
				lease.Close()
				t.Fatal(err)
			}
			schema := g.probe.collection.schema
			schema.retain()
			recipe := &nativeVectorGraphRecipe{schema: schema, def: def, rows: rows, ordinary: true, sourceRoot: root, sourceDocumentGeneration: generation, constructionWorkers: workers, pendingSourceCodec: codec}
			if identity := g.probe.meta.Options.ColumnStore.ActiveManifest; identity != nil {
				recipe.identity = *identity
			}
			g.typed.data.graph, g.typed.graphSourceRoot = recipe, root
			if err := tx.admitStage(g, memtable.COWAllocationCharge(uint64(10+len(g.name)+len(def.Name)))); err != nil {
				t.Fatal(err)
			}
			payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload(g.name, def.Name)
			if err != nil {
				t.Fatal(err)
			}
			apply := nativeCommandWALApplyCommit{frame: commandwalapply.LoweredFrame{Kind: commitlog.CommandKindCollectionRebuildVectorIndex, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionRebuildVectorIndexV1, Payload: payload}, sync: true}
			// This extra original edge lets the test inspect the admitted immutable
			// recipe after commit's normal transaction teardown. It is released
			// before the fresh public operation, never used as another publisher.
			staged := g.typed
			staged.retain()
			defer func() {
				if staged != nil {
					staged.release().Complete()
				}
			}()
			budget := tx.core.NativeAllocator().(nativeHNSWAdmissionBudget)

			wantRows := 256
			if mode == "physical-handoff" {
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			} else {
				insert(256, 257)
				wantRows++
			}
			before, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if before != nil {
					if err := before.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			beforeRoot, ok := before.collection.domains.Get("column/manifest")
			if !ok {
				t.Fatal("source manifest disappeared")
			}
			currentRoot, currentGeneration, currentSequence := beforeRoot.baseRoot, before.collection.documentGeneration, before.capture.LogicalSequence()
			if err := before.Close(); err != nil {
				t.Fatal(err)
			}
			before = nil
			if mode == "physical-handoff" && (currentRoot == root || currentGeneration != generation) {
				t.Fatalf("not a physical-only source handoff: root=%d/%d generation=%d/%d", root, currentRoot, generation, currentGeneration)
			}
			if mode == "logical-row" && (currentRoot != root || currentGeneration == generation) {
				t.Fatalf("not an isolated newer logical row: root=%d/%d generation=%d/%d", root, currentRoot, generation, currentGeneration)
			}
			nextLSN := backend.CommandWALNextLSN()
			err = tx.commitNativeApply(true, &apply)
			if mode == "physical-handoff" {
				if !nativeGraphPreparationOnlyStale(err) {
					t.Fatalf("expected exclusive physical preassignment refusal: %v", err)
				}
			} else if !errors.Is(err, backenddb.ErrConcurrentModification) || nativeGraphPreparationOnlyStale(err) {
				t.Fatalf("newer logical row is not a physical-only refresh: %v", err)
			}
			if apply.result.Assigned || apply.result.AssignedLSN != 0 || apply.result.Uncertain || apply.result.LogicalInstalled || backenddb.CommitPublicationAccepted(err) || backend.CommandWALNextLSN() != nextLSN {
				t.Fatalf("source refusal assigned or installed work: result=%+v nextLSN=%d want=%d err=%v", apply.result, backend.CommandWALNextLSN(), nextLSN, err)
			}
			if staged.data.graph != recipe || recipe.schema != schema || recipe.sourceRoot != root || recipe.sourceDocumentGeneration != generation ||
				staged.graphSourceRoot != root || staged.appliedLSN != 0 || staged.memberOrdinal != 0 || staged.buildCapacity != capacity ||
				recipe.rows != rows || recipe.constructionWorkers != workers || recipe.pendingSourceCodec != codec {
				t.Fatal("source refusal changed original admitted recipe or command identity")
			}
			if err := tx.Close(); err != nil {
				t.Fatalf("original transaction Close: %v", err)
			}
			tx = nil
			staged.release().Complete()
			staged = nil
			after, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatalf("source refusal poisoned current read: %v", err)
			}
			defer func() {
				if after != nil {
					if err := after.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			afterRoot, ok := after.collection.domains.Get("column/manifest")
			if !ok || afterRoot.baseRoot != currentRoot || after.collection.documentGeneration != currentGeneration || after.capture.LogicalSequence() != currentSequence {
				t.Fatal("rejected graph changed current source or publication sequence")
			}
			for n := 0; n < wantRows; n++ {
				id := []byte(fmt.Sprintf("doc-%04d", n))
				want := []byte(fmt.Sprintf(`{"embedding":[%d,1]}`, n+1))
				if got, err := after.Get(id); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("source refusal changed acknowledged row %q: %s %v", id, got, err)
				}
			}
			if err := after.Close(); err != nil {
				t.Fatal(err)
			}
			after = nil
			status, err := col.RebuildVectorIndex("vec")
			if err != nil || !status.Loaded || status.RebuildNeeded || status.Stats.LiveDocs != wantRows {
				t.Fatalf("fresh public graph did not progress after no-effect refusal: %+v %v", status, err)
			}
			for n := 0; n < wantRows; n++ {
				id := []byte(fmt.Sprintf("doc-%04d", n))
				want := []byte(fmt.Sprintf(`{"embedding":[%d,1]}`, n+1))
				if got, err := col.Get(id); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("fresh graph changed acknowledged row %q: %s %v", id, got, err)
				}
			}
			closeErr := manager.Close()
			manager = nil
			if closeErr != nil {
				t.Fatalf("original manager Close: %v", closeErr)
			}
			closeErr = owner.Close()
			owner = nil
			if closeErr != nil {
				t.Fatalf("original owner Close: %v", closeErr)
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("source refusal retained original custody: %+v", stats)
			}
		})
	}
}
