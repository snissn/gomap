package treedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestOrdinaryLeafPublicationAfterUnreachableGenerationGC(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("pack publication requires retained-parent namespace support")
	}
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	ctx := context.Background()
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.DisableBackgroundPrune = true
	opts.FlushApplyConcurrency = 1
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 8 << 10

	const name = "db.gc_rollover"
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	closeOwners := func() error {
		if manager == nil {
			return nil
		}
		err := errors.Join(manager.Close(), database.Close())
		manager = nil
		return err
	}
	t.Cleanup(func() {
		if err := closeOwners(); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	const count = 256
	ids, documents := make([][]byte, count), make([][]byte, count)
	update := func(version int, converge bool) {
		t.Helper()
		items := make([]collections.BSONSetUpdateBatchItem, count)
		for i := range ids {
			id := fmt.Sprintf("row-%06d", i)
			ids[i] = []byte(id)
			payload := make([]byte, 0, 192)
			for j := 0; j < 6; j++ {
				part := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%d", id, version, j)))
				payload = append(payload, part[:]...)
			}
			var err error
			documents[i], err = bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: payload}})
			if err != nil {
				t.Fatal(err)
			}
			items[i] = collections.BSONSetUpdateBatchItem{DocumentID: ids[i], Fields: []collections.BSONSetField{
				{Key: "payload", Value: bson.Raw(documents[i]).Lookup("payload")},
			}}
		}
		if version == 0 {
			if inserted, err := collection.InsertBatchValidatedBSON(ids, documents); err != nil || len(inserted) != count {
				t.Fatalf("insert: inserted=%d, err=%v", len(inserted), err)
			}
		} else {
			results, complete, err := collection.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(items)
			if err != nil || !complete || len(results) != count {
				t.Fatalf("update %d: complete=%t results=%d err=%v", version, complete, len(results), err)
			}
			for i, result := range results {
				if !result.Matched || !result.Modified {
					t.Fatalf("update %d row %d was not matched and modified: %+v", version, i, result)
				}
			}
		}
		if err := backend.SyncNativeSemanticPrefix(); err != nil {
			t.Fatalf("sync %d: %v", version, err)
		}
		if converge {
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("checkpoint %d: %v", version, err)
			}
		}
	}
	update(0, true)
	if err := closeOwners(); err != nil {
		t.Fatalf("close before pack: %v", err)
	}
	maintenance, closeMaintenance, err := OpenBackend(OptionsFor(ProfileCommandWALRelaxed, opts.Dir))
	if err != nil {
		t.Fatal(err)
	}
	packed, err := func() (_ uint32, err error) {
		defer func() { err = errors.Join(err, closeMaintenance()) }()
		plan, err := maintenance.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1})
		if err != nil || len(plan.CandidateGenerationIDs) == 0 {
			t.Fatalf("plan real pack: %+v, %v", plan, err)
		}
		result, err := maintenance.LeafGenerationPack(ctx, LeafGenerationPackOptions{GenerationIDs: plan.CandidateGenerationIDs[:1], Force: true, Sync: true})
		if err != nil || len(result.CreatedFileIDs) != 1 {
			t.Fatalf("publish real pack: %+v, %v", result, err)
		}
		return result.CreatedFileIDs[0], maintenance.Checkpoint()
	}()
	if err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	collection, err = manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	initial := newerOrdinaryLeafSlot(inspectOrdinaryLeafSlots(t, backend, opts.Dir, name, ids[0], ids[0]))
	if !slices.Contains(initial.packedFiles, packed) || initial.generation[packed] == 0 || initial.pointer.FileID != packed {
		t.Fatalf("seed row is not rooted in the exact packed dependency %d: %+v", packed, initial)
	}
	held := backend.AcquireSnapshot()
	if held == nil {
		t.Fatal("capture packed predecessor snapshot")
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	descriptor, err := held.GetAtRoot(initial.record.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
	if err != nil || len(descriptor) != 8 {
		t.Fatalf("held collection descriptor: %x, %v", descriptor, err)
	}
	heldRoot := binary.BigEndian.Uint64(descriptor)
	heldDocument := slices.Clone(documents[0])
	for i := 1; i < 5; i++ {
		update(i, true)
	}
	leafDir := filepath.Join(opts.Dir, "maindb", "leaf_vlog")
	operationalPackState := func() string {
		t.Helper()
		path := filepath.Join(leafDir, "manifest.json")
		if durable, err := filepath.Glob(filepath.Join(leafDir, "manifest.durable.*.json")); err != nil {
			t.Fatal(err)
		} else if len(durable) > 0 {
			path = durable[len(durable)-1]
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var manifest struct {
			Generations []struct {
				State string   `json:"state"`
				Files []uint32 `json:"file_ids"`
			} `json:"generations"`
		}
		if err := json.Unmarshal(data, &manifest); err != nil {
			t.Fatal(err)
		}
		for _, generation := range manifest.Generations {
			if slices.Contains(generation.Files, packed) {
				return generation.State
			}
		}
		t.Fatalf("operational manifest no longer records packed file %d", packed)
		return ""
	}
	if _, err := database.LeafGenerationGC(ctx, LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("collect while old pack remains pinned: %v", err)
	}
	if state := operationalPackState(); state != "retiring" {
		t.Fatalf("held pack %d state=%q, want retiring", packed, state)
	}
	if got, err := held.GetAtRoot(heldRoot, ids[0]); err != nil || !bytes.Equal(got, heldDocument) {
		t.Fatalf("read held packed row after GC: bytes=%d, err=%v", len(got), err)
	}
	heldErr := held.Close()
	held = nil
	if heldErr != nil {
		t.Fatal(heldErr)
	}
	if _, err := database.LeafGenerationGC(ctx, LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("collect unreachable pack: %v", err)
	}
	if state := operationalPackState(); state != "deleted" {
		t.Fatalf("obsolete pack %d state=%q, want deleted", packed, state)
	}
	if _, err := os.Stat(valuelog.SegmentPath(leafDir, page.ValueLogFileID(packed))); err != nil {
		t.Fatalf("recoverable predecessor no longer retains packed file %d: %v", packed, err)
	}
	before := inspectOrdinaryLeafSlots(t, backend, opts.Dir, name, ids[count-1], ids[count-1])
	for i, slot := range before {
		if !slices.Contains(slot.packedFiles, packed) || slot.generation[packed] == 0 || slot.pointer.FileID == packed {
			t.Fatalf("slot %d must still retain the exact pack %d while referencing a newer raw leaf: %+v", i, packed, slot)
		}
	}
	prior := newerOrdinaryLeafSlot(before)
	update(5, false)
	after := inspectOrdinaryLeafSlots(t, backend, opts.Dir, name, ids[count-1], ids[count-1])
	successor := newerOrdinaryLeafSlot(after)
	newFile := successor.pointer.FileID
	if successor.record.AppliedCommandLSN <= prior.record.AppliedCommandLSN ||
		successor.manifest.Generation <= prior.manifest.Generation || slices.Contains(successor.packedFiles, packed) ||
		prior.generation[newFile] != 0 || successor.generation[newFile] == 0 {
		t.Fatalf("successor did not bind the rotation and shed obsolete pack %d: prior=%+v successor=%+v", packed, prior, successor)
	}
	alternateIndex := slices.IndexFunc(after[:], func(slot ordinaryLeafSlot) bool {
		return slot.record.DurableSeq == prior.record.DurableSeq
	})
	if alternateIndex < 0 {
		t.Fatalf("alternate durable slot missing prior sequence %d", prior.record.DurableSeq)
	}
	alternate := after[alternateIndex]
	if alternate.manifest.Generation != prior.manifest.Generation || alternate.manifest.Digest != prior.manifest.Digest ||
		alternate.manifest.Identity != prior.manifest.Identity || !slices.Contains(alternate.packedFiles, packed) {
		t.Fatalf("alternate slot lost original packed authority: prior=%+v alternate=%+v", prior, alternate)
	}
	if err := closeOwners(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	collection, err = manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		got, err := collection.Get(id)
		if err != nil || !bytes.Equal(got, documents[i]) {
			t.Fatalf("reopened row %q: bytes=%d want=%d, err=%v", id, len(got), len(documents[i]), err)
		}
	}
	if err := closeOwners(); err != nil {
		t.Fatal(err)
	}
}
