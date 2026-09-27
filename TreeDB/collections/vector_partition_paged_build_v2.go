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
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// BuildAndStageVectorPartitionSourceProjectionV2 builds a source-only local
// projection from completed immutable imports. The production adapter derives
// input and complete local scope from the applied BUILD record. Nodes hosting
// ANN groups require the combined graph producer and cannot stage a partial
// source-only projection through this method. No distributed activation occurs.
func (c *Collection) BuildAndStageVectorPartitionSourceProjectionV2(ctx context.Context, input VectorPartitionPreparedInputV2, ownership sourcepartition.ResolvedSourceShardMapV2, walk func(context.Context, func(string, VectorPartitionSourceSnapshotV2) error) error) (VectorPartitionManifestV1, error) {
	if len(input.LocalANNOwners) != 0 {
		return VectorPartitionManifestV1{}, ErrVectorPartitionPagedRuntimeUnsupportedV2
	}
	return c.buildAndStageVectorPartitionProjectionV2(ctx, input, ownership, walk, nil)
}

// BuildAndStageVectorPartitionProjectionV2 constructs one atomic local source
// and ANN projection. Both producer streams are consumed once into immutable
// private pages before source availability and committed intent are verified.
func (c *Collection) BuildAndStageVectorPartitionProjectionV2(ctx context.Context, input VectorPartitionPreparedInputV2, ownership sourcepartition.ResolvedSourceShardMapV2, walk func(context.Context, func(string, VectorPartitionSourceSnapshotV2) error) error, ann func(context.Context, func(string, source.ANNRecordV2) error) error) (VectorPartitionManifestV1, error) {
	if len(input.LocalANNOwners) == 0 || ann == nil {
		return VectorPartitionManifestV1{}, ErrVectorPartitionPagedRuntimeUnsupportedV2
	}
	return c.buildAndStageVectorPartitionProjectionV2(ctx, input, ownership, walk, ann)
}

func (c *Collection) buildAndStageVectorPartitionProjectionV2(ctx context.Context, input VectorPartitionPreparedInputV2, ownership sourcepartition.ResolvedSourceShardMapV2, walk func(context.Context, func(string, VectorPartitionSourceSnapshotV2) error) error, ann func(context.Context, func(string, source.ANNRecordV2) error) error) (VectorPartitionManifestV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.db == nil {
		return VectorPartitionManifestV1{}, errCollectionDBNil
	}
	if len(input.LocalSourceOwners) == 0 || walk == nil || input.Collection != c.name {
		return VectorPartitionManifestV1{}, ErrVectorPartitionPagedRuntimeUnsupportedV2
	}
	if err := validateVectorPartitionLocalOwnersV2(input.LocalSourceOwners); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := validateVectorPartitionLocalOwnersV2(input.LocalANNOwners); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	var result VectorPartitionManifestV1
	err := WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() (buildErr error) {
		unlock := c.lockMutation()
		defer unlock.Unlock()
		cfg := c.meta.Options.ColumnStore
		if cfg == nil || cfg.AssetManager == nil {
			return errors.New("collections: paged projection requires typed asset storage")
		}
		// Completed source imports establish the manifest used by ordinary
		// orphan GC after a process crash. Refuse before creating any output
		// when that recovery authority has not been materialized.
		if cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 || cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
			return errors.New("collections: paged projection requires active source directory manifest")
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
			if err := session.verifyANNIntentV2(ctx, input, m.PagedRootV2.MetadataDirectory); err != nil {
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
			// Retry must complete the idempotent lifecycle namespace sync too.
			if err := store.persistVerifiedVectorPartitionManifestLifecycleModeV1(existing, false); err != nil {
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
		var output, intentSegment vectorPartitionPrivateSegmentV2
		var stageAttempted, staged bool
		registry := c.db.StableResourceIdentityPinRegistry()
		storageRoot := c.db.Dir()
		cleanup := func() error {
			intentErr := intentSegment.remove(registry)
			if staged {
				return intentErr
			}
			if stageAttempted {
				bound, boundErr := store.openDir()
				if boundErr != nil {
					return errors.Join(intentErr, boundErr)
				}
				installed, openErr := store.OpenWithContext(context.Background(), input.Collection, input.IndexName, input.Generation)
				boundErr = errors.Join(store.verifyBoundDirV1(bound), bound.Close())
				if boundErr != nil {
					return errors.Join(intentErr, boundErr)
				}
				if openErr == nil {
					if !vectorPartitionManifestCanonicalEqualV1(installed, result) {
						return errors.Join(intentErr, fmt.Errorf("%w: ambiguous projection authority", ErrVectorPartitionManifestInvalid))
					}
					return intentErr // Installed roots retain output, even before namespace sync.
				}
				if !os.IsNotExist(openErr) {
					return errors.Join(intentErr, openErr)
				}
			}
			return errors.Join(intentErr, output.remove(registry))
		}
		defer func() {
			if cleanupErr := cleanup(); cleanupErr != nil {
				// Unresolved cleanup blocks another build in this DB lifetime.
				// Teardown uses captured filesystem authority only, and never waits
				// for a builder holding the barrier while seeking DB admission.
				retainErr := lease.RetainStableResourceCaptureRecovery(func() error {
					unlock, ok := tryVectorPartitionStorageBarrier(storageRoot)
					if !ok {
						return ErrRecoveryRequired
					}
					defer unlock()
					return cleanup()
				})
				buildErr = errors.Join(buildErr, cleanupErr, retainErr, ErrRecoveryRequired)
			}
		}()
		appendPrivate := func(segment *vectorPartitionPrivateSegmentV2, raw []byte, part uint64) (VectorPartitionAssetV1, error) {
			if err := ctx.Err(); err != nil {
				return VectorPartitionAssetV1{}, err
			}
			ref, err := segment.append(c, input.Generation, part, raw, lease)
			if err != nil {
				return VectorPartitionAssetV1{}, err
			}
			sum := sha256.Sum256(raw)
			return VectorPartitionAssetV1{Ref: ref, Bytes: uint64(len(raw)), Checksum: hex.EncodeToString(sum[:])}, nil
		}
		appendAsset := func(raw []byte, part uint64) (VectorPartitionAssetV1, error) {
			return appendPrivate(&output, raw, part)
		}
		emitTo := func(segment *vectorPartitionPrivateSegmentV2, p VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error) {
			raw, err := EncodeVectorPartitionDirectoryPageV2(p)
			if err != nil {
				return VectorPartitionAssetV1{}, err
			}
			pages++
			// The existing opaque physical kind also carries router payloads. VDP2
			// decoding, exact length, checksum, namespace and generation govern pages.
			a, err := appendPrivate(segment, raw, pages)
			a.ID = fmt.Sprintf("directory-page-%d", pages)
			return a, err
		}
		emit := func(p VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error) {
			return emitTo(&output, p)
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
		root = result.PagedRootV2
		// Source pages are private and complete before any ANN input can be used.
		sourceInput := input
		sourceInput.LocalANNOwners = nil
		session := VectorPartitionPagedSourceSessionV2{collection: c, manifest: result, ownership: ownership, snapshot: snapshot}
		if err := session.verifyPreparedSourcesV2(ctx, sourceInput); err != nil {
			return err
		}
		if ann != nil {
			var owner string
			var domain uint32
			intent, err := writeVectorPartitionDirectoryV2(ctx, "metadata", input.Generation, func(visit func(VectorPartitionDirectoryRecordV2) error) error {
				return ann(ctx, func(group string, record source.ANNRecordV2) error {
					if !slices.Contains(input.LocalANNOwners, group) || (record.Domain == nil) == (record.Member == nil) {
						return fmt.Errorf("%w: local ANN intent", ErrVectorPartitionManifestInvalid)
					}
					if record.Domain != nil {
						owner = group
						domain = record.Domain.DomainID
						return visit(VectorPartitionDirectoryRecordV2{Owner: group, DomainID: uint64(domain), Domain: record.Domain})
					}
					if group != owner {
						return fmt.Errorf("%w: undeclared owner domain", ErrVectorPartitionManifestInvalid)
					}
					return visit(VectorPartitionDirectoryRecordV2{Owner: group, DomainID: uint64(domain), Member: &record.Member.Source, MembershipKind: record.Member.Kind})
				})
			}, func(p VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error) {
				return emitTo(&intentSegment, p)
			})
			if err != nil {
				return err
			}
			// All owner commitments and all source references are checked before the
			// first graph. Subsequent graph work reads only these immutable pages.
			if err := session.verifyANNIntentV2(ctx, input, intent); err != nil {
				return err
			}
			root.MetadataDirectory, root.LocalDomainCount, root.LocalMembershipCount, root.LocalPhysicalPackCount, err = session.buildVectorPartitionANNDirectoryV2(ctx, input, intent, def, emit, func(raw []byte, domain uint32) (VectorPartitionAssetV1, error) {
				return appendAsset(raw, uint64(domain)+1)
			})
			if err != nil {
				return err
			}
			root.MetadataDirectory.ID = "vector_partition_metadata_root_v2"
			root.ANNOwners = slices.Clone(input.LocalANNOwners)
			result.Canonicalize()
		}
		if err := walkVectorPartitionManifestAssetsV2(ctx, c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, result, nil); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Intent readers are closed once the final immutable directory is built.
		// Remove their private segment before any lifecycle authority can install.
		if err := intentSegment.remove(registry); err != nil {
			return err
		}
		stageAttempted = true
		err = store.persistVerifiedVectorPartitionManifestLifecycleModeV1(result, false)
		staged = err == nil
		return err
	})
	if err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return result, nil
}
