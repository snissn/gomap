package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func persistLifecycleCheckpointBuildV1(t *testing.T, store *VectorPartitionStoreV1, generation uint64) (VectorPartitionManifestV1, []byte) {
	t.Helper()
	raw, manifest := lifecycleManifestPayloadV1(t, "building")
	manifest.Generation = generation
	manifest.Canonicalize()
	var err error
	raw, err = EncodeVectorPartitionManifestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, generation, raw)
	if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Skipf("checkpoint publication unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return manifest, raw
}

func lifecycleCheckpointReadyForBuildingV1(t *testing.T, building VectorPartitionManifestV1) (VectorPartitionManifestV1, []byte) {
	t.Helper()
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	ready.Generation = building.Generation
	ready.RouterGeneration = building.Generation
	ready.RouterAsset.Ref.Generation = building.Generation
	ready.Canonicalize()
	payload := lifecycleReadyPromotionPayloadV1(t, building, ready)
	return ready, payload
}

func TestVectorPartitionLifecycleCheckpointStoreV1BuildTailAndReopen(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	building, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	_, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, building)
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, building.Generation, readyPayload); err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleLocalActivateV1, building.Generation, nil); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.checkpoint.Epoch != 1 || len(loaded.deltas) != 2 ||
		loaded.state.ActiveGeneration != building.Generation ||
		loaded.state.Generations[building.Generation].Manifest.State != "ready" {
		t.Fatalf("loaded checkpoint state=%+v epoch=%d deltas=%d", loaded.state, loaded.checkpoint.Epoch, len(loaded.deltas))
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleLocalActivateV1, building.Generation, []byte("invalid")); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("idempotent activate accepted invalid payload: %v", err)
	}
	reopened, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if again.state.LastDigest != loaded.state.LastDigest || again.state.LastSequence != loaded.state.LastSequence {
		t.Fatalf("reopen state=%+v want=%+v", again.state, loaded.state)
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1CompactsOversizedTailIntoCheckpoint(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	building, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	ready, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, building)
	for _, operation := range []vectorPartitionLifecycleOperationV1{
		vectorPartitionLifecycleReadyV1,
		vectorPartitionLifecycleLocalActivateV1,
		vectorPartitionLifecycleDeactivateV1,
	} {
		payload := []byte(nil)
		if operation == vectorPartitionLifecycleReadyV1 {
			payload = readyPayload
		}
		if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", operation, building.Generation, payload); err != nil {
			t.Fatal(err)
		}
	}
	reclaim, err := newVectorPartitionReclaimStateV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	prepareRaw, err := encodeVectorPartitionReclaimRecordV1(reclaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleDeletePrepareV1, building.Generation, prepareRaw); err != nil {
		t.Fatal(err)
	}
	template := reclaim.OriginalRefs[0]
	reclaim.SupersededRefs = make([]ColumnAssetRef, 100000)
	for i := range reclaim.SupersededRefs {
		ref := template
		ref.FileID = uint32(1000000 + i)
		ref.Offset = int64(i) * ref.Length
		ref.Checksum += uint32(i + 1)
		reclaim.SupersededRefs[i] = ref
	}
	progressRaw, err := encodeVectorPartitionReclaimRecordV1(reclaim)
	if err != nil {
		t.Fatal(err)
	}
	if len(progressRaw) <= vectorPartitionLifecycleCheckpointTailMaxBytesV1 || len(progressRaw) >= vectorPartitionLifecycleCheckpointMaxBytesV1 {
		t.Fatalf("test reclaim bytes=%d", len(progressRaw))
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReclaimProgressV1, building.Generation, progressRaw); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	generation := loaded.state.Generations[building.Generation]
	if loaded.checkpoint.Epoch != 2 || len(loaded.deltas) != 0 || generation.Reclaim == nil ||
		len(generation.Reclaim.SupersededRefs) != len(reclaim.SupersededRefs) {
		t.Fatalf("compacted checkpoint epoch=%d deltas=%d generation=%+v", loaded.checkpoint.Epoch, len(loaded.deltas), generation)
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1PostInstallRetries(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	t.Run("checkpoint", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		buildRaw, build := lifecycleManifestPayloadV1(t, "building")
		forced := errors.New("after checkpoint install")
		restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
			if boundary == "after_checkpoint_install" {
				return forced
			}
			return nil
		})
		err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
		restore()
		if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Skipf("checkpoint publication unsupported: %v", err)
		}
		if !errors.Is(err, forced) {
			t.Fatalf("post-install checkpoint err=%v", err)
		}
		if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
		if err != nil {
			t.Fatal(err)
		}
		if loaded.checkpoint.Epoch != 1 || loaded.state.GenerationHighWater != build.Generation {
			t.Fatalf("retry checkpoint state=%+v epoch=%d", loaded.state, loaded.checkpoint.Epoch)
		}
	})

	t.Run("delta", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		_, base := lifecycleManifestPayloadV1(t, "building")
		building, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
		_, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, building)
		forced := errors.New("after delta install")
		restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
			if boundary == "after_delta_install" {
				return forced
			}
			return nil
		})
		err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, building.Generation, readyPayload)
		restore()
		if !errors.Is(err, forced) {
			t.Fatalf("post-install delta err=%v", err)
		}
		if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, building.Generation, readyPayload); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded.deltas) != 1 || loaded.state.Generations[building.Generation].Manifest.State != "ready" {
			t.Fatalf("retry delta state=%+v deltas=%d", loaded.state, len(loaded.deltas))
		}
	})
}

func TestVectorPartitionLifecycleCheckpointStoreV1PreInstallRetries(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	t.Run("checkpoint", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		buildRaw, build := lifecycleManifestPayloadV1(t, "building")
		forced := errors.New("before checkpoint install")
		restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
			if boundary == "before_checkpoint_install" {
				return forced
			}
			return nil
		})
		err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
		restore()
		if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Skipf("checkpoint publication unsupported: %v", err)
		}
		if !errors.Is(err, forced) {
			t.Fatalf("pre-install checkpoint err=%v", err)
		}
		empty, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
		if err != nil || empty.checkpoint.Epoch != 0 || empty.state.GenerationHighWater != 0 {
			t.Fatalf("pre-install checkpoint became authority: state=%+v epoch=%d err=%v", empty.state, empty.checkpoint.Epoch, err)
		}
		if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
		if err != nil {
			t.Fatal(err)
		}
		if loaded.checkpoint.Epoch != 1 || loaded.state.GenerationHighWater != build.Generation {
			t.Fatalf("retry checkpoint state=%+v epoch=%d", loaded.state, loaded.checkpoint.Epoch)
		}
	})

	t.Run("delta", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		_, base := lifecycleManifestPayloadV1(t, "building")
		building, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
		_, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, building)
		forced := errors.New("before delta install")
		restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
			if boundary == "before_delta_install" {
				return forced
			}
			return nil
		})
		err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, building.Generation, readyPayload)
		restore()
		if !errors.Is(err, forced) {
			t.Fatalf("pre-install delta err=%v", err)
		}
		loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded.deltas) != 0 || loaded.state.Generations[building.Generation].Manifest.State != "building" {
			t.Fatalf("pre-install delta changed authority state=%+v deltas=%d", loaded.state, len(loaded.deltas))
		}
		if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, building.Generation, readyPayload); err != nil {
			t.Fatal(err)
		}
		loaded, err = store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded.deltas) != 1 || loaded.state.Generations[building.Generation].Manifest.State != "ready" {
			t.Fatalf("retry delta state=%+v deltas=%d", loaded.state, len(loaded.deltas))
		}
	})
}

func TestVectorPartitionLifecycleCheckpointStoreV1HighestCheckpointIsSoleAuthority(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	second, _ := persistLifecycleCheckpointBuildV1(t, store, first.Generation+1)
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.checkpoint.Epoch != 2 || len(loaded.state.Generations) != 2 {
		t.Fatalf("checkpoint state=%+v epoch=%d", loaded.state, loaded.checkpoint.Epoch)
	}
	firstName, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 1)
	firstInfo, err := os.Stat(filepath.Join(store.dir, firstName))
	if err != nil {
		t.Fatal(err)
	}
	if firstInfo.Size() != 0 {
		t.Fatalf("superseded checkpoint bytes=%d, want audit stub", firstInfo.Size())
	}
	highestName, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 2)
	if err := os.WriteFile(filepath.Join(store.dir, highestName), []byte("corrupt-highest"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("corrupt highest checkpoint load err=%v", err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatal("test generation setup")
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1AuditRetryKeepsNewAuthority(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	buildRaw, second := lifecycleManifestPayloadV1(t, "building")
	second.Generation = first.Generation + 1
	second.Canonicalize()
	buildRaw, err = EncodeVectorPartitionManifestV1(second)
	if err != nil {
		t.Fatal(err)
	}
	forced := errors.New("before audit")
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "before_audit_truncate" {
			return forced
		}
		return nil
	})
	err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, second.Generation, buildRaw)
	restore()
	if !errors.Is(err, forced) {
		t.Fatalf("audit fault err=%v", err)
	}
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.checkpoint.Epoch != 2 || loaded.state.GenerationHighWater != second.Generation {
		t.Fatalf("new authority lost after audit fault: epoch=%d state=%+v", loaded.checkpoint.Epoch, loaded.state)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, second.Generation, buildRaw); err != nil {
		t.Fatal(err)
	}
	firstName, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 1)
	info, err := os.Stat(filepath.Join(store.dir, firstName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("audit retry retained %d bytes", info.Size())
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1RejectsAuditHardLinkAlias(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	buildRaw, second := lifecycleManifestPayloadV1(t, "building")
	second.Generation = first.Generation + 1
	second.Canonicalize()
	buildRaw, err = EncodeVectorPartitionManifestV1(second)
	if err != nil {
		t.Fatal(err)
	}
	firstName, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 1)
	secondName, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 2)
	replaced := false
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary != "before_audit_truncate" || replaced {
			return nil
		}
		replaced = true
		if err := os.Remove(filepath.Join(store.dir, firstName)); err != nil {
			return err
		}
		return os.Link(filepath.Join(store.dir, secondName), filepath.Join(store.dir, firstName))
	})
	err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleBuildV1, second.Generation, buildRaw)
	restore()
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("hard-link audit alias err=%v", err)
	}
	if !replaced {
		t.Fatal("hard-link replacement hook did not run")
	}
	info, statErr := os.Stat(filepath.Join(store.dir, secondName))
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Size() == 0 {
		t.Fatal("audit alias truncated current checkpoint authority")
	}
	if _, loadErr := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding"); !errors.Is(loadErr, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("hard-link alias namespace load err=%v", loadErr)
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1RejectsDeletePrepareWithProgress(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	building, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	ready, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, building)
	for _, operation := range []vectorPartitionLifecycleOperationV1{
		vectorPartitionLifecycleReadyV1,
		vectorPartitionLifecycleLocalActivateV1,
		vectorPartitionLifecycleDeactivateV1,
	} {
		payload := []byte(nil)
		if operation == vectorPartitionLifecycleReadyV1 {
			payload = readyPayload
		}
		if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", operation, building.Generation, payload); err != nil {
			t.Fatal(err)
		}
	}
	reclaim, err := newVectorPartitionReclaimStateV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	prepareRaw, err := encodeVectorPartitionReclaimRecordV1(reclaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleDeletePrepareV1, building.Generation, prepareRaw); err != nil {
		t.Fatal(err)
	}
	superseded := reclaim.OriginalRefs[0]
	superseded.FileID++
	superseded.Offset += superseded.Length
	superseded.Checksum++
	reclaim.SupersededRefs = []ColumnAssetRef{superseded}
	progressRaw, err := encodeVectorPartitionReclaimRecordV1(reclaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReclaimProgressV1, building.Generation, progressRaw); err != nil {
		t.Fatal(err)
	}
	before, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleDeletePrepareV1, building.Generation, progressRaw)
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("DELETE_PREPARE accepted reclaim progress: %v", err)
	}
	after, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if after.state.LastSequence != before.state.LastSequence || after.state.LastDigest != before.state.LastDigest {
		t.Fatalf("rejected DELETE_PREPARE changed authority: before=%+v after=%+v", before.state, after.state)
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1RejectsGapHigherEpochAndMalformedName(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, test := range []struct {
		name  string
		entry func(t *testing.T, store *VectorPartitionStoreV1, loaded vectorPartitionLifecycleCheckpointStoreStateV1) string
	}{
		{
			name: "gap",
			entry: func(t *testing.T, store *VectorPartitionStoreV1, loaded vectorPartitionLifecycleCheckpointStoreStateV1) string {
				record := lifecycleRecordV1(t, loaded.state.LastSequence+2, loaded.state.LastDigest, vectorPartitionLifecycleLocalActivateV1, loaded.state.GenerationHighWater, nil)
				raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(record)
				if err != nil {
					t.Fatal(err)
				}
				name, _ := vectorPartitionLifecycleDeltaNameV1("docs", "embedding", loaded.checkpoint.Epoch, record.Sequence)
				dir, err := store.openDir()
				if err != nil {
					t.Fatal(err)
				}
				defer dir.Close()
				writeLifecycleSlotV1(t, dir, name, raw)
				return name
			},
		},
		{
			name: "higher-epoch-delta",
			entry: func(t *testing.T, store *VectorPartitionStoreV1, loaded vectorPartitionLifecycleCheckpointStoreStateV1) string {
				record := lifecycleRecordV1(t, loaded.state.LastSequence+1, loaded.state.LastDigest, vectorPartitionLifecycleLocalActivateV1, loaded.state.GenerationHighWater, nil)
				raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(record)
				if err != nil {
					t.Fatal(err)
				}
				name, _ := vectorPartitionLifecycleDeltaNameV1("docs", "embedding", loaded.checkpoint.Epoch+1, record.Sequence)
				dir, err := store.openDir()
				if err != nil {
					t.Fatal(err)
				}
				defer dir.Close()
				writeLifecycleSlotV1(t, dir, name, raw)
				return name
			},
		},
		{
			name: "malformed-name",
			entry: func(t *testing.T, store *VectorPartitionStoreV1, _ vectorPartitionLifecycleCheckpointStoreStateV1) string {
				name := vectorPartitionLifecycleNamePrefixV1("docs", "embedding") + "checkpoint.bad.vlc"
				if err := os.WriteFile(filepath.Join(store.dir, name), []byte("bad"), 0o600); err != nil {
					t.Fatal(err)
				}
				return name
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			_, base := lifecycleManifestPayloadV1(t, "building")
			persistLifecycleCheckpointBuildV1(t, store, base.Generation)
			loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
			if err != nil {
				t.Fatal(err)
			}
			_ = test.entry(t, store, loaded)
			if _, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("invalid checkpoint namespace load err=%v", err)
			}
		})
	}
}

func TestVectorPartitionLifecycleCheckpointStoreV1PreflightsPhysicalCapBeforeInstall(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	second, _ := persistLifecycleCheckpointBuildV1(t, store, first.Generation+1)
	loaded, err := store.loadVectorPartitionLifecycleCheckpointStateV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint2Name, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 2)
	checkpoint2Info, err := os.Stat(filepath.Join(store.dir, checkpoint2Name))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint1Name, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 1)
	if err := os.Truncate(filepath.Join(store.dir, checkpoint1Name), int64(vectorPartitionStoreMaxBytesV1)-checkpoint2Info.Size()); err != nil {
		t.Fatal(err)
	}
	if loaded.physicalBytes >= vectorPartitionStoreMaxBytesV1 {
		t.Fatal("test expected audit stub before sparse expansion")
	}
	_, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, second)
	err = store.persistVectorPartitionLifecycleOperationV1("docs", "embedding", vectorPartitionLifecycleReadyV1, second.Generation, readyPayload)
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("physical-cap append err=%v", err)
	}
	checkpoint3Name, _ := vectorPartitionLifecycleCheckpointNameV1("docs", "embedding", 3)
	if _, statErr := os.Stat(filepath.Join(store.dir, checkpoint3Name)); !os.IsNotExist(statErr) {
		t.Fatalf("over-cap operation installed checkpoint 3: %v", statErr)
	}
}
