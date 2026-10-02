package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// VectorPartitionPrepareCompletionV1 is persisted in the same native root as
// the first live binding. Command positions come only from committed FSM apply;
// ManifestDigest is owner-local while AssetSetDigest excludes physical refs.
type VectorPartitionPrepareCompletionV1 struct {
	Command                                        commitlog.VectorPrepareV1
	AssetSetDigest, ManifestDigest, ReadySetDigest string
}

func vectorPrepareSourceV1(v commitlog.VectorPrepareV1) VectorPartitionSourceIdentityV1 {
	return VectorPartitionSourceIdentityV1{Generation: v.SourceGeneration, Checksum: v.SourceChecksum, SchemaHash: v.SourceSchemaHash, RowCount: v.SourceRowCount}
}

// WithPreparedCommandWALVectorPrepareV1 owns the existing schema, native
// admission, coverage, mutation, and partition storage barriers through Append,
// owner execution and Finalize/Abort. It does not synthesize admission.
func (c *Collection) WithPreparedCommandWALVectorPrepareV1(ctx context.Context, v commitlog.VectorPrepareV1, apply func(*CommandWALAdmittedCollection) error) error {
	if c == nil || c.db == nil {
		return errCollectionDBNil
	}
	capture, err := AcquireVectorPrepareStableCaptureV1(c.db)
	if err != nil {
		return err
	}
	defer capture.Close()
	return capture.WithStorageBarrierV1(ctx, func(storage *VectorPrepareStorageOwnerV1) error {
		return c.WithPreparedCommandWALVectorPrepareOwnedV1(ctx, v, storage, apply)
	})
}

// WithPreparedCommandWALVectorPrepareOwnedV1 borrows a factory-minted outer
// stable capture and storage barrier; it never reacquires either under FSM or
// raw staging locks. The outer callback must span Append through Finalize/Abort.
func (c *Collection) WithPreparedCommandWALVectorPrepareOwnedV1(ctx context.Context, v commitlog.VectorPrepareV1, storage *VectorPrepareStorageOwnerV1, apply func(*CommandWALAdmittedCollection) error) error {
	if c == nil || c.db == nil {
		return errCollectionDBNil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if apply == nil {
		return errors.New("collections: vector prepare apply callback unavailable")
	}
	return storage.withDBV1(c.db, func(sourcePin *backenddb.Snapshot) error {
		acquire := func() func() {
			admission := c.lockNativeVectorAdmissionWrite()
			coverage := c.lockVectorIndexCoveragePersistence()
			return func() { coverage(); admission() }
		}
		return c.withPreparedCommandWALMutation(acquire, true, func(owner *CommandWALAdmittedCollection) error {
			owner.partitionStorageHeld = true
			owner.partitionSourcePin = sourcePin
			defer func() { owner.partitionStorageHeld = false; owner.partitionSourcePin = nil }()
			if err := owner.preflightVectorPrepareV1(v, false); err != nil {
				return err
			}
			return apply(owner)
		})
	})
}

func (c *Collection) PreflightVectorPrepareV1(ctx context.Context, v commitlog.VectorPrepareV1) error {
	return c.WithPreparedCommandWALVectorPrepareV1(ctx, v, func(*CommandWALAdmittedCollection) error { return nil })
}

func (owner *CommandWALAdmittedCollection) preflightVectorPrepareV1(v commitlog.VectorPrepareV1, replay bool) error {
	if err := owner.validate(); err != nil {
		return err
	}
	if err := v.ValidateV1(); err != nil {
		return err
	}
	c := owner.collection
	def, ok := findVectorIndex(c.meta.VectorIndexes, v.Index)
	if !ok || c.name != v.Collection || VectorIndexDefinitionDigestV1(def) != v.IndexDefinitionDigest ||
		def.Strategy != VectorIndexStrategyColumnGraph || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 ||
		def.Representation != "" || def.SchemaGeneration != 0 || def.Dimensions > 4096 || def.M > 64 || def.EfConstruction > 4096 || def.EfSearch > 4096 || len(def.QuantizedIndexes) != 0 {
		return errors.New("collections: bounded vector prepare definition mismatch")
	}
	if err := owner.vectorPreparePhysicalSourcePrerequisitesV1(def); err != nil {
		return err
	}
	if !VectorPartitionLiveDocumentProofSupportedV1(c.meta) {
		return errors.New("collections: vector prepare requires the supported physical typed source")
	}
	// Count the authoritative primary rows before any build allocation. Admission
	// and mutation are held; the prepare source must include every primary row.
	count, err := owner.vectorPrepareDocumentCountV1(v.MaxSourceRows)
	if err != nil {
		return err
	}
	if count == 0 || count > v.MaxSourceRows {
		return errors.New("collections: vector prepare source exceeds bounded nonempty fixture")
	}
	if v.Operation == "prepare" {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return backenddb.ErrClosed
		}
		source, err := c.vectorPartitionSourceIdentityAtSnapshotV1(v.Index, snap)
		closeErr := snap.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
		if source != vectorPrepareSourceV1(v) || source.RowCount != count {
			return errors.New("collections: vector prepare frozen source changed")
		}
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	loaded, present, err := store.loadVectorPartitionLifecycleAuthorityV1(c.name, v.Index)
	if err != nil {
		return err
	}
	// A first apply cannot adopt an offline generation, active pointer, or a
	// completed/deleted generation. Only this pending command's local WAL replay
	// may reuse BUILD/READY, under the same frozen tuple checked below.
	if !replay && present {
		return errors.New("collections: vector prepare requires an unprepared index namespace")
	}
	if replay && present {
		if loaded.state.ActiveGeneration != 0 || loaded.state.GenerationHighWater != v.Generation || len(loaded.state.Generations) != 1 {
			return errors.New("collections: replayed vector prepare namespace differs")
		}
		entry, ok := loaded.state.Generations[v.Generation]
		if !ok || entry.Manifest == nil || entry.Scope != nil || entry.Deleting {
			return errors.New("collections: replayed vector prepare generation differs")
		}
		return validateVectorPrepareManifestV1(v, *entry.Manifest)
	}
	return nil
}

func validateVectorPrepareManifestV1(v commitlog.VectorPrepareV1, m VectorPartitionManifestV1) error {
	if m.PrepareOrigin == nil || *m.PrepareOrigin != (VectorPartitionPrepareOriginV1{Term: v.Term, Index: v.IndexPosition, CommandDigest: v.CommandDigest}) {
		return errors.New("collections: vector prepare stage has no matching committed command origin")
	}
	if m.Collection != v.Collection || m.IndexName != v.Index || m.IndexDefinitionDigest != v.IndexDefinitionDigest ||
		m.Generation != v.Generation || m.PartitionCount != 1 || m.DomainCount != 1 || len(m.DomainPacks) != 1 || m.DomainPacks[0] != (VectorPartitionDomainPackV1{DomainID: 0, PackID: 0}) || m.BalancePolicy != "disjoint_v1" ||
		m.SourceGeneration != v.SourceGeneration || m.SourceChecksum != v.SourceChecksum || m.SourceSchemaHash != v.SourceSchemaHash || m.SourceRowCount != v.SourceRowCount ||
		len(m.Placements) != 1 || m.Placements[0] != (VectorPartitionPlacementV1{PartitionID: 0, GroupID: v.Group}) ||
		len(m.Memberships) != int(v.SourceRowCount) || len(m.OverlapMemberships) != 0 || len(m.Assets) != 1 ||
		(m.State != "building" && m.State != "ready") {
		return errors.New("collections: vector prepare staged identity differs from its command")
	}
	for i, member := range m.Memberships {
		if member.VectorOrdinal != uint64(i) || member.PartitionID != 0 {
			return errors.New("collections: vector prepare staged membership differs")
		}
	}
	if m.Assets[0].ID != vectorPartitionLocalAssetIDV1(0) || m.Assets[0].PartitionID != 0 || m.Assets[0].GraphVariant != string(vectorPartitionLocalDefaultGraphVariantV1) {
		return errors.New("collections: vector prepare staged pack identity differs")
	}
	return m.Validate(DefaultVectorPartitionManifestLimits())
}

// ApplyVectorPrepareWithCommandWALIntentV1 borrows only the actual owner. The
// first operation publishes the source graph; the second stages immutable
// artifacts and publishes live binding plus completion once at its own LSN.
func (owner *CommandWALAdmittedCollection) ApplyVectorPrepareWithCommandWALIntentV1(ctx context.Context, v commitlog.VectorPrepareV1, intent *backenddb.CommandWALIntent, replay bool) error {
	if err := owner.validate(); err != nil {
		return err
	}
	if intent == nil || v.Term == 0 || v.IndexPosition == 0 || v.CommandDigest == "" || !owner.partitionStorageHeld {
		return errors.New("collections: vector prepare lacks committed WAL/owner authority")
	}
	if replay {
		if owner.replayOperation != intent {
			return errors.New("collections: foreign prepare replay operation")
		}
		if err := owner.collection.db.ValidateCommandWALReplayOperationV1(intent); err != nil {
			return err
		}
	} else if err := owner.partitionCapture.ValidateCommandWALStagingCaptureV1(owner.collection.db, intent); err != nil {
		return err
	}
	if err := owner.preflightVectorPrepareV1(v, replay); err != nil {
		return err
	}
	c := owner.collection
	if v.Operation == "rebuild" {
		status, err := c.rebuildVectorIndexWithCommandWALIntentAndOwner(v.Index, intent, owner)
		if err != nil {
			return err
		}
		if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded {
			return errors.New("collections: vector prepare rebuild did not produce a loaded source")
		}
		return nil
	}
	source, rows, err := c.ReadVectorPartitionRouterSourceRowsV1(v.Index)
	if err != nil {
		return err
	}
	if source != vectorPrepareSourceV1(v) || uint64(len(rows)) != v.SourceRowCount {
		return errors.New("collections: vector prepare source reader differs")
	}
	building := VectorPartitionManifestV1{
		State: "building", Collection: v.Collection, IndexName: v.Index, IndexDefinitionDigest: v.IndexDefinitionDigest,
		SourceGeneration: v.SourceGeneration, SourceChecksum: v.SourceChecksum, SourceSchemaHash: v.SourceSchemaHash, SourceRowCount: v.SourceRowCount,
		Generation: v.Generation, PartitionCount: 1, DomainCount: 1, BalancePolicy: "disjoint_v1",
		DomainPacks:   []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
		PrepareOrigin: &VectorPartitionPrepareOriginV1{Term: v.Term, Index: v.IndexPosition, CommandDigest: v.CommandDigest},
		Placements:    []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: v.Group}},
	}
	input := VectorPartitionSearchAssetV1{Source: source, Generation: v.Generation, PartitionID: 0, Dimensions: len(rows[0].Values)}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for i, row := range rows {
		if row.VectorOrdinal != uint64(i) {
			return errors.New("collections: vector prepare source ordinal differs")
		}
		building.Memberships = append(building.Memberships, VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		input.IDs = append(input.IDs, string(row.DocumentID))
		input.Vectors = append(input.Vectors, row.Values)
		input.Kinds = append(input.Kinds, VectorPartitionMembershipHomeV1)
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: row.Values, MembershipKind: string(VectorPartitionMembershipHomeV1)})
	}
	building.Canonicalize()
	input.ManifestChecksum = building.IntegrityDigest
	var staged VectorPartitionManifestV1
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err == nil {
		staged, err = store.Open(c.name, v.Index, v.Generation)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err != nil {
		// The materializer derives/validates authoritative rows. The ID is a
		// logical artifact name; the manager selects a fresh physical file.
		assets, resources, err := c.materializeVectorPartitionLocalSearchAssetsVariantV1(v.Index, building, 0, []VectorPartitionSearchAssetV1{input}, vectorPartitionSearchAssetMaxBytesV1, vectorPartitionLocalDefaultGraphVariantV1, nil, false, nil, vectorPartitionMaterializeOwnerV1{fresh: true, owner: owner})
		if err != nil {
			return err
		}
		if len(assets) != 1 {
			if resources != nil {
				resources.Release()
			}
			return errors.New("collections: incomplete vector prepare pack")
		}
		building.Assets = assets
		building.Canonicalize()
		// BUILD does not transfer ready resources. Keep producer pins through its
		// durable stage, then the root storage barrier excludes reclamation.
		err = c.publishVectorPartitionManifestModeV1(building, nil, false, vectorPartitionLocalDefaultGraphVariantV1, owner)
		if resources != nil {
			resources.Release()
		}
		if err != nil {
			return err
		}
		staged = building
	}
	if err := validateVectorPrepareManifestV1(v, staged); err != nil {
		return err
	}
	if err := c.validateVectorPartitionAssetMembershipBindingsForGraphVariantV1(staged, vectorPartitionLocalDefaultGraphVariantV1); err != nil {
		return err
	}
	if staged.State == "building" {
		cfg := internalrouter.DefaultRouterConfigV1()
		cfg.MaxVectors = int(v.MaxSourceRows)
		cfg.MaxDimensions = 4096
		if _, err := c.buildAndPublishVectorPartitionRouterForGraphVariantV1(ctx, staged, []internalrouter.RouterPartitionV1{partition}, VectorPartitionRouterBuildOptionsV1{Config: cfg}, vectorPartitionLocalDefaultGraphVariantV1, owner); err != nil {
			return err
		}
		store, err = OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		staged, err = store.Open(c.name, v.Index, v.Generation)
		if err != nil {
			return err
		}
	}
	if err := validateVectorPrepareManifestV1(v, staged); err != nil {
		return err
	}
	// READY replay verifies both complete ranges through exact stable handles,
	// not names or a guessed frontier. Capture pins last through the root commit.
	assets := append(append([]VectorPartitionAssetV1(nil), staged.Assets...), staged.RouterAsset)
	_, releaseCapture, err := owner.vectorPrepareCaptureLeaseV1()
	if err != nil {
		return err
	}
	// Existing resource pins survive capture admission; startup replay must
	// release its ordinary teardown lease before ordinary native publication.
	defer releaseCapture()
	resources, err := captureVectorPartitionRouterExistingAssetsV1(c.db.ColumnAssetRootDir(), assets, c.db.StableResourceIdentityPinRegistry())
	if err != nil {
		return err
	}
	defer resources.Release()
	if err := verifyVectorPartitionAssetsV1(c.db.ColumnAssetRootDir(), c.meta.Options.ColumnStore.AssetManager.Namespace, assets); err != nil {
		return err
	}
	releaseCapture()
	owner.preparation = &VectorPartitionPrepareCompletionV1{Command: v, AssetSetDigest: VectorPartitionLogicalAssetSetDigestV1(v.Group, staged), ManifestDigest: staged.IntegrityDigest, ReadySetDigest: staged.ReadySetDigest}
	defer func() { owner.preparation = nil }()
	return c.ensureVectorPartitionLiveBindingV1(ctx, staged, intent, owner)
}

// VectorPartitionLogicalAssetSetDigestV1 is the existing M8 group digest. It
// excludes local physical file IDs, offsets, CRC refs and local READY digests.
func VectorPartitionLogicalAssetSetDigestV1(group string, m VectorPartitionManifestV1) string {
	var fields []string
	for _, asset := range m.Assets {
		for _, placement := range m.Placements {
			if asset.PartitionID == placement.PartitionID && placement.GroupID == group {
				fields = append(fields, fmt.Sprintf("%d/%s/%d/%s", asset.PartitionID, asset.ID, asset.Bytes, asset.Checksum))
			}
		}
	}
	fields = append(fields, fmt.Sprintf("router/%s/%d/%s/%d/%d", m.RouterAsset.ID, m.RouterAsset.Bytes, m.RouterAsset.Checksum, m.RouterGeneration, m.Generation))
	sort.Strings(fields)
	sum := sha256.Sum256([]byte(group + "\n" + strings.Join(fields, "\n")))
	return hex.EncodeToString(sum[:])
}

// VectorPartitionPrepareCompletionV1 reads durable native-root metadata; it
// does not construct assets, load a new binding, or advance command coverage.
func (c *Collection) VectorPartitionPrepareCompletionV1(index string) (VectorPartitionPrepareCompletionV1, bool, error) {
	var out VectorPartitionPrepareCompletionV1
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return out, false, backenddb.ErrClosed
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, c.name)
	if err != nil {
		return out, false, err
	}
	if catalog == nil {
		return out, false, errCollectionNotFound
	}
	root := collectionVectorIndexRootName(c.name, index)
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, root, []byte(vectorIndexNativeKeyMeta), nil)
	if err != nil || !ok {
		return out, false, err
	}
	var meta vectorIndexPersistMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return out, false, err
	}
	if meta.Preparation == nil {
		return out, false, nil
	}
	out = *meta.Preparation
	if err := out.Command.ValidateV1(); err != nil {
		return out, false, err
	}
	if out.Command.Operation != "prepare" || out.Command.Collection != c.name || out.Command.Index != index || out.Command.Term == 0 || out.Command.IndexPosition == 0 || meta.PartitionLive == nil {
		return out, false, errors.New("collections: invalid persisted vector preparation")
	}
	return out, true, nil
}

func replayVectorPrepareCommandWALV1(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	v, err := commitlog.DecodeVectorPreparePayloadV1(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	if err := db.ValidateCommandWALReplayOperationV1(intent); err != nil {
		return err
	}
	sourcePin := db.AcquireStableSnapshot()
	if sourcePin == nil {
		return backenddb.ErrClosed
	}
	defer sourcePin.Close()
	c, err := newCommandWALReplayCollectionManager(db).openCollectionWithCommandWALIntent(v.Collection, intent)
	if err != nil {
		return err
	}
	return WithVectorPartitionStorageBarrierV1(db.Dir(), func() error {
		acquire := func() func() {
			admission := c.lockNativeVectorAdmissionWrite()
			coverage := c.lockVectorIndexCoveragePersistence()
			return func() { coverage(); admission() }
		}
		return c.withPreparedCommandWALMutationAndReplayIntent(acquire, true, intent, func(owner *CommandWALAdmittedCollection) error {
			owner.partitionStorageHeld = true
			owner.partitionSourcePin = sourcePin
			defer func() { owner.partitionStorageHeld = false }()
			return owner.ApplyVectorPrepareWithCommandWALIntentV1(context.Background(), v, intent, true)
		})
	})
}

// validateVectorPrepareSourceManifestV1 validates the existing TVIS/physical
// typed source against a coherent snapshot while the actual mutation is owned.
func (owner *CommandWALAdmittedCollection) validateVectorPrepareSourceManifestV1(m VectorPartitionManifestV1) error {
	if err := owner.validate(); err != nil {
		return err
	}
	c := owner.collection
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	source, err := c.vectorPartitionSourceIdentityAtSnapshotV1(m.IndexName, snap)
	closeErr := snap.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	if source != (VectorPartitionSourceIdentityV1{Generation: m.SourceGeneration, Checksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash, RowCount: m.SourceRowCount}) {
		return errors.New("collections: vector prepare stage source identity mismatch")
	}
	return nil
}

func (owner *CommandWALAdmittedCollection) vectorPrepareDocumentCountV1(max uint64) (uint64, error) {
	c := owner.collection
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, backenddb.ErrClosed
	}
	defer snap.Close()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return 0, err
	}
	if catalog == nil {
		return 0, errCollectionNotFound
	}
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(c.name), nil, nil, false)
	if err != nil {
		return 0, err
	}
	if it == nil {
		return 0, nil
	}
	defer it.Close()
	count := uint64(0)
	for it.Valid() {
		if !it.IsDeleted() {
			count++
			if count > max {
				return count, nil
			}
		}
		it.Next()
	}
	return count, it.Error()
}

// SetVectorPrepareCaptureLeaseV1 borrows only a DB-validated live staging
// capture. The caller retains ownership through Apply and Finalize/Abort.
func (owner *CommandWALAdmittedCollection) SetVectorPrepareCaptureLeaseV1(lease *backenddb.StableResourceCaptureLease) error {
	if err := owner.validate(); err != nil {
		return err
	}
	if lease == nil {
		return backenddb.ErrClosed
	}
	if err := lease.ValidateCommandWALStagingCaptureDBV1(owner.collection.db); err != nil {
		return err
	}
	owner.partitionCapture = lease
	return nil
}
func (owner *CommandWALAdmittedCollection) vectorPrepareCaptureLeaseV1() (*backenddb.StableResourceCaptureLease, func(), error) {
	if err := owner.validate(); err != nil {
		return nil, nil, err
	}
	if owner.partitionCapture != nil {
		if err := owner.partitionCapture.ValidateDBV1(owner.collection.db); err != nil {
			return nil, nil, err
		}
		return owner.partitionCapture, func() {}, nil
	}
	if owner.replayOperation == nil {
		return nil, nil, errors.New("collections: prepare capture requires actual staging borrower")
	}
	if err := owner.collection.db.ValidateCommandWALReplayOperationV1(owner.replayOperation); err != nil {
		return nil, nil, err
	}
	lease, err := owner.collection.db.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, nil, err
	}
	return lease, lease.Release, nil
}

// Validate persisted physical prerequisites without building/loading a graph.
// Use the current catalog snapshot; retained Collection metadata is not root authority.
func (owner *CommandWALAdmittedCollection) vectorPreparePhysicalSourcePrerequisitesV1(def VectorIndexDefinition) error {
	c := owner.collection
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, c.name)
	if err != nil {
		return err
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		return errors.New("collections: vector prepare requires physical column asset support")
	}
	if err := validateColumnStoreConfig(c.name, *cfg); err != nil {
		return err
	}
	if _, _, supported, err := columnVectorGraphTypedColumnVectorField(*cfg, def.Field, def.Dimensions); err != nil {
		return err
	} else if !supported {
		return errors.New("collections: vector prepare requires the matching typed-column float32 vector")
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return errors.New("collections: vector prepare requires active recovery-authoritative physical manifest")
	}
	root := catalog.rootID(collectionColumnManifestRootName(c.name))
	if root == 0 {
		return errors.New("collections: vector prepare requires physical manifest root")
	}
	return validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest)
}
