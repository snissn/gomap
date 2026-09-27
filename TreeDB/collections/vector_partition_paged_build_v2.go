package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
)

// BuildAndStageVectorPartitionSourceProjectionV2 builds a source-only local
// projection from completed immutable imports. The production adapter derives
// input and complete local scope from the applied BUILD record. Nodes hosting
// ANN groups require the combined graph producer and cannot stage a partial
// source-only projection through this method. No distributed activation occurs.
func (c *Collection) BuildAndStageVectorPartitionSourceProjectionV2(ctx context.Context, input VectorPartitionPreparedInputV2, ownership sourcepartition.ResolvedSourceShardMapV2, walk func(context.Context, func(string, VectorPartitionSourceSnapshotV2) error) error) (VectorPartitionManifestV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.db == nil {
		return VectorPartitionManifestV1{}, errCollectionDBNil
	}
	if len(input.LocalSourceOwners) == 0 || len(input.LocalANNOwners) != 0 || walk == nil || input.Collection != c.name {
		return VectorPartitionManifestV1{}, ErrVectorPartitionPagedRuntimeUnsupportedV2
	}
	if err := validateVectorPartitionLocalOwnersV2(input.LocalSourceOwners); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	var result VectorPartitionManifestV1
	err := WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		unlock := c.lockMutation()
		defer unlock.Unlock()
		cfg := c.meta.Options.ColumnStore
		if cfg == nil || cfg.AssetManager == nil {
			return errors.New("collections: paged projection requires typed asset storage")
		}
		def, err := c.vectorPartitionRouterDefinitionV1(input.IndexName)
		if err != nil {
			return err
		}
		if VectorIndexDefinitionDigestV1(def) != input.IndexDefinitionDigest {
			return fmt.Errorf("%w: prepared index identity", ErrVectorPartitionManifestInvalid)
		}
		lease, err := c.db.AcquireStableResourceCaptureLease()
		if err != nil {
			return err
		}
		defer lease.Release()
		store, err := OpenVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		snapshot := c.db.AcquireSnapshot()
		if snapshot == nil {
			return errCollectionDBNil
		}
		defer snapshot.Close()
		verify := func(m VectorPartitionManifestV1) error {
			session := VectorPartitionPagedSourceSessionV2{collection: c, manifest: m, ownership: ownership, snapshot: snapshot}
			if err := session.verifyPreparedSourcesV2(ctx, input); err != nil {
				return err
			}
			return walkVectorPartitionManifestAssetsV2(ctx, c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, m, nil)
		}
		// A retry reuses the immutable installed roots. Reappending equivalent
		// records would create a different physical local projection.
		existing, err := store.OpenWithContext(ctx, c.name, input.IndexName, input.Generation)
		if err == nil {
			if err := verify(existing); err != nil {
				return err
			}
			result = existing
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
		root := &VectorPartitionPagedRootV2{SourceMapEpoch: input.SourceMapEpoch, SourceMapDigest: input.SourceMapDigest, SourceSnapshotSetDigest: input.SnapshotSetDigest, GraphProfileDigest: input.GraphProfileDigest, PlacementDigest: input.PlacementDigest, SourceOwners: slices.Clone(input.LocalSourceOwners)}
		var pages uint64
		emit := func(p VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error) {
			if err := ctx.Err(); err != nil {
				return VectorPartitionAssetV1{}, err
			}
			raw, err := EncodeVectorPartitionDirectoryPageV2(p)
			if err != nil {
				return VectorPartitionAssetV1{}, err
			}
			pages++
			// This existing physical kind also carries opaque router envelopes.
			// Payload here is VDP2, validated by its own decoder, exact length,
			// checksum, namespace and generation before durable Stage.
			refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(c.db.ColumnAssetRootDir(), *cfg, columnAssetM12ASegmentFileID, []StableColumnPhysicalAssetAppend{{Payload: raw, Kind: ColumnAssetKindTCS1HNSWSearchPack, Generation: input.Generation, PartID: pages}}, c.db.StableResourceIdentityPinRegistry(), lease)
			if err != nil {
				return VectorPartitionAssetV1{}, err
			}
			if resources == nil {
				return VectorPartitionAssetV1{}, errors.New("collections: missing directory producer authority")
			}
			defer resources.Release()
			if len(refs) != 1 {
				return VectorPartitionAssetV1{}, errors.New("collections: directory append count")
			}
			if err := resources.SyncThrough(); err != nil {
				return VectorPartitionAssetV1{}, err
			}
			sum := sha256.Sum256(raw)
			return VectorPartitionAssetV1{ID: fmt.Sprintf("directory-page-%d", pages), Ref: refs[0], Bytes: uint64(len(raw)), Checksum: hex.EncodeToString(sum[:])}, nil
		}
		root.SourceShardDirectory, err = writeVectorPartitionDirectoryV2(ctx, "source", input.Generation, func(visit func(VectorPartitionDirectoryRecordV2) error) error {
			return walk(ctx, func(owner string, source VectorPartitionSourceSnapshotV2) error {
				if !slices.Contains(input.LocalSourceOwners, owner) || source.RowCount > ^uint64(0)-root.LocalSourceRowCount || root.LocalSourceShardCount == ^uint64(0) {
					return fmt.Errorf("%w: source projection coverage/count", ErrVectorPartitionManifestInvalid)
				}
				if err := visit(VectorPartitionDirectoryRecordV2{Owner: owner, Snapshot: &source}); err != nil {
					return err
				}
				root.LocalSourceShardCount++
				root.LocalSourceRowCount += source.RowCount
				return nil
			})
		}, emit)
		if err != nil {
			return err
		}
		root.SourceShardDirectory.ID = "vector_partition_source_root_v2"
		result = VectorPartitionManifestV1{Format: VectorPartitionManifestFormatV2, State: "building", Collection: c.name, IndexName: input.IndexName, IndexDefinitionDigest: input.IndexDefinitionDigest, Generation: input.Generation, PagedRootV2: root}
		result.Canonicalize()
		if err := verify(result); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return store.persistVerifiedVectorPartitionManifestLifecycleModeV1(result, false)
	})
	if err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return result, nil
}
