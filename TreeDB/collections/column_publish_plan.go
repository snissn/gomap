package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	columnManifestRootSuffix = "/column/manifest"
)

// ErrColumnPublishPlanRequiresEnabledColumnStore is returned when publish-plan
// construction or publication sees non-empty column-store metadata that is not
// enabled for column writes.
var ErrColumnPublishPlanRequiresEnabledColumnStore = errors.New("collections: column publish plan requires enabled=true column_store")

var (
	errColumnPublishPlanRequiresEnabledColumnStore = ErrColumnPublishPlanRequiresEnabledColumnStore
)

type ColumnPublishOperation string

const (
	// ColumnPublishOperationInsert publishes column assets for inserted rows.
	ColumnPublishOperationInsert ColumnPublishOperation = "insert"
	// ColumnPublishOperationUpdate publishes column assets for updated rows.
	ColumnPublishOperationUpdate ColumnPublishOperation = "update"
	// ColumnPublishOperationDelete publishes column tombstone/delete metadata.
	ColumnPublishOperationDelete ColumnPublishOperation = "delete"
)

type ColumnManifestPartRole string

const (
	// ColumnManifestPartRoleBase identifies a complete base part in the typed-storage lineage.
	ColumnManifestPartRoleBase ColumnManifestPartRole = "base"
	// ColumnManifestPartRoleDelta identifies a mutation/append delta part layered over older bases/deltas.
	ColumnManifestPartRoleDelta ColumnManifestPartRole = "delta"
	// ColumnManifestPartRoleTombstone identifies a typed-row tombstone part with no typed-column payload.
	ColumnManifestPartRoleTombstone ColumnManifestPartRole = "tombstone"
)

// ColumnAssetKind identifies the storage format behind a column asset ref.
type ColumnAssetKind string

const (
	// ColumnAssetKindTCS1PartImage references an immutable typed-row TCS1/TCPA
	// part image retained for compatibility.
	ColumnAssetKindTCS1PartImage ColumnAssetKind = "tcs1_part_image"
	// ColumnAssetKindTCS1TypedColumnPart references an immutable sectioned
	// typed-column TCS1 part image.
	ColumnAssetKindTCS1TypedColumnPart ColumnAssetKind = "tcs1_typed_column_part"
	// ColumnAssetKindTCS1AggregateMetadata references grouped aggregate metadata
	// stored as a typed column asset beside physical part images.
	ColumnAssetKindTCS1AggregateMetadata ColumnAssetKind = "tcs1_aggregate_metadata"
	// ColumnAssetKindTCS1DictionaryCodes references low-cardinality dictionary
	// codes derived from one declared dictionary string column in a TCS1 part.
	ColumnAssetKindTCS1DictionaryCodes ColumnAssetKind = "tcs1_dictionary_codes"
	// ColumnAssetKindTCS1Int64Values references dense int64 values derived from
	// one non-null declared int64 column in a TCS1 part.
	ColumnAssetKindTCS1Int64Values ColumnAssetKind = "tcs1_int64_values"
	// ColumnAssetKindTCS1HNSWSearchPack references one durable
	// hnsw_search_pack_v1 serving artifact owned by vector-index state.
	ColumnAssetKindTCS1HNSWSearchPack ColumnAssetKind = "tcs1_hnsw_search_pack"
	// ColumnAssetKindQueryReadyBase references one rebuildable,
	// non-authoritative QRBG image held in the existing prepared-asset registry.
	ColumnAssetKindQueryReadyBase ColumnAssetKind = "query_ready_base_v1"
	// ColumnAssetKindQueryReadyDelta references one rebuildable,
	// non-authoritative QRDG delta image held before publication handoff.
	ColumnAssetKindQueryReadyDelta ColumnAssetKind = "query_ready_delta_v1"
	// ColumnAssetKindQueryReadyConsolidatedBase references one rebuildable QRDG
	// consolidated-base image. It does not itself publish or select recovery state.
	ColumnAssetKindQueryReadyConsolidatedBase ColumnAssetKind = "query_ready_consolidated_base_v1"
)

// ColumnAssetRef is the durable typed address of a column-asset-manager-owned
// object. It is value-log-shaped, but it must not imply ordinary value_vlog
// ownership.
type ColumnAssetRef struct {
	Kind       ColumnAssetKind
	Namespace  string
	Generation uint64
	PartID     uint64
	FileID     uint32
	Offset     int64
	Length     int64
	Checksum   uint32
}

// ColumnPreparedAsset describes an immutable asset staged for manifest publish.
type ColumnPreparedAsset struct {
	Ref          ColumnAssetRef
	Rows         int
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
	Reason       string
	PartRole     ColumnManifestPartRole
	SortKey      string
}

// ColumnPublishPlanInput contains the normalized collection state and stage
// hooks required to build an atomic column manifest publish plan.
type ColumnPublishPlanInput struct {
	Collection               string
	ColumnStore              *ColumnStoreConfig
	ColumnStoreNormalized    bool
	ActiveVectorIndexes      []VectorIndexDefinition
	ActiveVectorIndexesKnown bool
	Operation                ColumnPublishOperation
	CurrentManifest          *ColumnManifestIdentity
	CurrentManifestRecords   []columnManifestRecord
	AppliedCommandLSN        uint64
	BaseManifestRootID       uint64
	Hooks                    ColumnPublishPlanHooks
}

// ColumnPublishPlanHooks provide the engine-specific stages for a publish plan.
type ColumnPublishPlanHooks struct {
	ExtractDocuments      func() error
	EncodeDeclaredColumns func(ColumnPublishDeclaredColumnEncodeInput) error
	PrepareAssets         func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error)
	EncodeManifest        func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error)
	ValidateClosure       func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error)
	BuildRootDelta        func(ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error)
	BuildSystemDelta      func(ColumnPublishSystemDeltaInput) error
}

// ColumnPublishDeclaredColumnEncodeInput is passed to the declared-column
// encoding stage.
type ColumnPublishDeclaredColumnEncodeInput struct {
	Collection  string
	ColumnStore ColumnStoreConfig
	Operation   ColumnPublishOperation
}

// ColumnPublishAssetPrepareInput is passed to the asset preparation stage.
type ColumnPublishAssetPrepareInput struct {
	Collection        string
	ColumnStore       ColumnStoreConfig
	Operation         ColumnPublishOperation
	AppliedCommandLSN uint64
	CurrentManifest   *ColumnManifestIdentity
}

// ColumnPublishPreparedAssets is the row/byte/accounting summary for staged
// column assets before they are referenced by a manifest generation.
type ColumnPublishPreparedAssets struct {
	Assets                  []ColumnPreparedAsset
	RowCount                int
	CommandBytes            int64
	RowRemainderBytes       int64
	ColumnPayloadBytes      int64
	AssetMetrics            ColumnPublishAssetPreparationMetrics
	stableResources         *rootpublication.StableResourceSet
	stableResourcesRequired bool
}

// ColumnPublishManifestEncodeInput is passed to the manifest encoding stage.
type ColumnPublishManifestEncodeInput struct {
	Collection               string
	ColumnStore              ColumnStoreConfig
	ActiveVectorIndexes      []VectorIndexDefinition
	ActiveVectorIndexesKnown bool
	Operation                ColumnPublishOperation
	AppliedCommandLSN        uint64
	CurrentManifest          *ColumnManifestIdentity
	CurrentManifestRecords   []columnManifestRecord
	Prepared                 ColumnPublishPreparedAssets
}

// ColumnPublishManifestEncodeResult identifies the encoded manifest generation.
type ColumnPublishManifestEncodeResult struct {
	Identity      ColumnManifestIdentity
	ManifestBytes int64
	Records       []columnManifestRecord
}

// ColumnPublishClosureValidationInput is passed to the durability-closure
// validation stage.
type ColumnPublishClosureValidationInput struct {
	Collection        string
	ColumnStore       ColumnStoreConfig
	Operation         ColumnPublishOperation
	AppliedCommandLSN uint64
	Prepared          ColumnPublishPreparedAssets
	Manifest          ColumnPublishManifestEncodeResult
}

// ColumnPublishDurabilityClosure records the assets that must be flushed and
// synced before the manifest root can become authoritative.
type ColumnPublishDurabilityClosure struct {
	// PreparedAssets is owned by the closure/plan boundary. BuildColumnPublishPlan
	// compares it as an order-independent multiset against the manifest snapshot
	// before publishing.
	PreparedAssets []ColumnPreparedAsset
	RequiredAssets int
	RequiredBytes  int64
	FlushRequired  bool
	SyncRequired   bool
}

// ColumnPublishRootDeltaInput is passed to the manifest-root delta stage.
type ColumnPublishRootDeltaInput struct {
	Collection         string
	ColumnStore        ColumnStoreConfig
	BaseManifestRootID uint64
	Manifest           ColumnPublishManifestEncodeResult
	Closure            ColumnPublishDurabilityClosure
}

// ColumnPublishSystemDeltaInput is passed to the metadata/system-root stage.
type ColumnPublishSystemDeltaInput struct {
	Plan ColumnPublishPlan
}

// ColumnManifestPublishSystemDeltaInput contains the collection metadata base
// and publish plan used to build the atomic system-root update.
type ColumnManifestPublishSystemDeltaInput struct {
	BaseMeta           CollectionMeta
	BaseCommitSeq      uint64
	BaseSystemRoot     uint64
	BaseManifestRootID uint64
	Plan               ColumnPublishPlan
}

// ColumnPublishPlan is the complete, validated plan for publishing a column
// manifest generation and making it recovery-authoritative.
type ColumnPublishPlan struct {
	Enabled                                bool
	Collection                             string
	Operation                              ColumnPublishOperation
	AppliedCommandLSN                      uint64
	ManifestRootName                       string
	ManifestRootBaseID                     uint64
	UpdatedActiveManifest                  ColumnManifestIdentity
	RecoveryAuthoritativeManifest          ColumnManifestIdentity
	RecoveryAuthoritativeAppliedCommandLSN uint64
	RootDelta                              ColumnManifestRootDelta
	PreparedAssets                         []ColumnPreparedAsset
	RequiredAssetCount                     int
	RequiredAssetBytes                     int64
	RequiredAssetFlush                     bool
	RequiredAssetSync                      bool
	Rows                                   int
	CommandBytes                           int64
	RowRemainderBytes                      int64
	ColumnPayloadBytes                     int64
	ManifestBytes                          int64
	ManifestMutationRecords                int
	ManifestMutationBytes                  int64
	Lifecycle                              ColumnPublishLifecycleSummary
	StageMetrics                           ColumnPublishStageMetrics
	stableResources                        *rootpublication.StableResourceSet
	durableResourceRequirements            rootpublication.StableLogicalObligationRequirements
	durableResourceMutation                rootpublication.StableLogicalObligationMutation
	durableResourceRequirementsFallback    func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error)
	durableResourceRequirementWork         rootpublication.StableResourceClosureWork
}

// ColumnManifestRootDelta is the ordered-root publish descriptor for the
// collection column manifest root.
type ColumnManifestRootDelta struct {
	RootName       string
	BaseRootID     uint64
	StoragePolicy  RootStoragePolicy
	Identity       ColumnManifestIdentity
	IdentityRecord [columnManifestIdentityRecordSize]byte
	// Records is the complete logical post-state manifest used for checksum,
	// durability-closure, and correctness validation.
	Records []columnManifestRecord
	// Mutations is the minimal root-local change set from the base manifest to
	// Records. MutationDelta distinguishes an intentionally empty mutation set
	// from legacy/custom full-record root deltas.
	Mutations     []columnManifestMutation
	MutationDelta bool
}

// ColumnPublishLifecycleSummary summarizes lifecycle/GC-relevant asset effects
// of the publish.
type ColumnPublishLifecycleSummary struct {
	PublishedRefs         int
	PublishedBytes        int64
	PreparedRefs          int
	PreparedBytes         int64
	SupersededRefs        int
	SupersededBytes       int64
	CleanupSafeGeneration uint64
	CleanupSafeRefs       int
	CleanupSafeBytes      int64
	RewriteDebtBytes      int64
}

// ColumnPublishStageMetrics records stage timings and optional allocation
// counters for publish-plan construction.
type ColumnPublishStageMetrics struct {
	// DocumentExtraction also includes the production command-WAL JSON-to-
	// declared-row conversion when that path prepares declared rows before
	// BuildColumnPublishPlan. Typed-column asset encoding remains charged to
	// AssetPreparation unless an explicit EncodeDeclaredColumns hook runs.
	DocumentExtraction      time.Duration
	DeclaredColumnEncoding  time.Duration
	AssetPreparation        time.Duration
	AssetMetrics            ColumnPublishAssetPreparationMetrics
	AssetClosureValidation  time.Duration
	ManifestEncode          time.Duration
	RootDeltaConstruction   time.Duration
	SystemDeltaConstruction time.Duration
}

// ColumnPublishAssetPreparationMetrics breaks the production asset-preparation
// stage down by the asset families that make up column-store insert/load cost.
// Subphase durations are producer-attributed and may overlap; AssetPreparation
// remains the enclosing wall-clock duration for the full prepare stage.
// Fused row-sidecar build time is byte-attributed to dictionary/int64/aggregate
// families by the producer; SharedAppendDuration remains separately reported so
// callers can choose exact upper-bound or byte-share accounting.
type ColumnPublishAssetPreparationMetrics struct {
	RowAssetDuration                      time.Duration
	RowAssetBytes                         int64
	RowAssetCount                         int
	TypedColumnPartDuration               time.Duration
	TypedColumnDictionaryBuild            time.Duration
	TypedColumnRowMaterialization         time.Duration
	TypedColumnPartBuild                  time.Duration
	TypedColumnImageBuild                 time.Duration
	TypedColumnPartBytes                  int64
	TypedColumnPartCount                  int
	DictionarySidecarDuration             time.Duration
	DictionarySidecarBytes                int64
	DictionarySidecarCount                int
	Int64SidecarDuration                  time.Duration
	Int64SidecarBytes                     int64
	Int64SidecarCount                     int
	AggregateMetadataDuration             time.Duration
	AggregateMetadataBytes                int64
	AggregateMetadataCount                int
	RowSidecarSharedBuildDuration         time.Duration
	SharedAppendOpenDuration              time.Duration
	SharedAppendWriteDuration             time.Duration
	SharedAppendCloseDuration             time.Duration
	SharedAppendFileSyncDuration          time.Duration
	SharedAppendFileCloseDuration         time.Duration
	SharedAppendDirSyncDuration           time.Duration
	SharedAppendCleanupDuration           time.Duration
	SharedAppendCloseCount                int
	SharedAppendFileSyncCount             int
	SharedAppendSyncEpochCount            int
	SharedAppendDuration                  time.Duration
	SharedAppendBytes                     int64
	SharedAppendCount                     int
	SharedSegmentAppendBytes              int64
	SharedSegmentAppendCount              int
	SharedSegmentAppendCloseCount         int
	SharedSegmentAppendFileSyncCount      int
	SharedSegmentAppendSyncEpochCount     int
	DirectViewSegmentAppendBytes          int64
	DirectViewSegmentAppendCount          int
	DirectViewSegmentAppendCloseCount     int
	DirectViewSegmentAppendFileSyncCount  int
	DirectViewSegmentAppendSyncEpochCount int
}

// BuildColumnPublishPlan validates and stages an atomic column manifest publish
// plan. A nil or completely empty disabled column_store is a zero-work fast path.
func BuildColumnPublishPlan(input ColumnPublishPlanInput) (ColumnPublishPlan, error) {
	if input.ColumnStore == nil {
		return ColumnPublishPlan{}, nil
	}
	if !input.ColumnStore.Enabled {
		if columnStoreConfigEmpty(*input.ColumnStore) {
			return ColumnPublishPlan{}, nil
		}
		return ColumnPublishPlan{}, errColumnPublishPlanRequiresEnabledColumnStore
	}
	if err := validateColumnPublishOperation(input.Operation); err != nil {
		return ColumnPublishPlan{}, err
	}
	if input.AppliedCommandLSN == 0 {
		return ColumnPublishPlan{}, errors.New("collections: column publish plan requires AppliedCommandLSN")
	}
	cfg := input.ColumnStore
	if !input.ColumnStoreNormalized {
		normalized, err := normalizeColumnStoreConfig(input.Collection, input.ColumnStore)
		if err != nil {
			return ColumnPublishPlan{}, err
		}
		cfg = normalized
	}
	if err := validateColumnPublishPlanConfig(input.Collection, cfg); err != nil {
		return ColumnPublishPlan{}, err
	}

	var metrics ColumnPublishStageMetrics
	if input.Hooks.ExtractDocuments != nil {
		start := time.Now()
		if err := input.Hooks.ExtractDocuments(); err != nil {
			return ColumnPublishPlan{}, fmt.Errorf("collections: column publish document extraction failed: %w", err)
		}
		metrics.DocumentExtraction = time.Since(start)
	}
	if input.Hooks.EncodeDeclaredColumns != nil {
		start := time.Now()
		if err := input.Hooks.EncodeDeclaredColumns(ColumnPublishDeclaredColumnEncodeInput{
			Collection:  input.Collection,
			ColumnStore: columnPublishHookConfig(*cfg),
			Operation:   input.Operation,
		}); err != nil {
			return ColumnPublishPlan{}, fmt.Errorf("collections: column publish declared-column encode failed: %w", err)
		}
		metrics.DeclaredColumnEncoding = time.Since(start)
	}

	start := time.Now()
	prepared, err := prepareColumnPublishAssets(input, *cfg)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish asset preparation failed: %w", err)
	}
	metrics.AssetPreparation = time.Since(start)
	metrics.AssetMetrics = prepared.AssetMetrics
	stableResources := prepared.stableResources
	prepared.stableResources = nil
	defer func() {
		if stableResources != nil {
			stableResources.Release()
		}
	}()
	if err := validateColumnPublishPreparedAssets(prepared); err != nil {
		return ColumnPublishPlan{}, err
	}
	if prepared.stableResourcesRequired || stableResources != nil {
		if err := validateStableColumnResourcesMatchPrepared(prepared.Assets, stableResources); err != nil {
			return ColumnPublishPlan{}, err
		}
	}
	manifestPrepared := prepared

	start = time.Now()
	manifest, err := encodeColumnPublishManifest(input, *cfg, manifestPrepared)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish manifest encode failed: %w", err)
	}
	metrics.ManifestEncode = time.Since(start)
	normalizeColumnManifestIdentityDefaults(&manifest.Identity)
	if err := validateColumnManifestIdentity(manifest.Identity); err != nil {
		return ColumnPublishPlan{}, err
	}
	if manifest.ManifestBytes < 0 {
		return ColumnPublishPlan{}, errors.New("collections: column publish manifest byte count cannot be negative")
	}

	start = time.Now()
	closurePrepared := manifestPrepared
	if input.Hooks.ValidateClosure != nil {
		closurePrepared = cloneColumnPublishPreparedAssets(manifestPrepared)
	}
	closure, err := validateColumnPublishClosure(input, *cfg, closurePrepared, manifest)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish asset-closure validation failed: %w", err)
	}
	metrics.AssetClosureValidation = time.Since(start)
	if err := validateColumnPublishDurabilityClosure(closure, *cfg); err != nil {
		return ColumnPublishPlan{}, err
	}
	if err := validateColumnPublishClosureMatchesPrepared(manifestPrepared, closure); err != nil {
		return ColumnPublishPlan{}, err
	}

	start = time.Now()
	rootDelta, err := buildColumnPublishRootDelta(input, *cfg, manifest, closure)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish root-delta construction failed: %w", err)
	}
	metrics.RootDeltaConstruction = time.Since(start)
	if err := validateColumnManifestRootDeltaForPlan(rootDelta, input.CurrentManifestRecords, input.BaseManifestRootID, *cfg, manifest.Identity); err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: invalid column publish root delta: %w", err)
	}
	durableResourceRequirements, durableResourceMutation, durableResourceRequirementsFallback, durableResourceRequirementWork, err := stableColumnManifestDurablePublication(
		input.CurrentManifestRecords, rootDelta, manifest.Identity.Generation, cfg.AssetManager.Namespace,
	)
	if err != nil {
		return ColumnPublishPlan{}, err
	}

	preparedBytes, err := checkedSumColumnPreparedAssetBytes(manifestPrepared.Assets)
	if err != nil {
		return ColumnPublishPlan{}, err
	}

	plan := ColumnPublishPlan{
		Enabled:                                true,
		Collection:                             input.Collection,
		Operation:                              input.Operation,
		AppliedCommandLSN:                      input.AppliedCommandLSN,
		ManifestRootName:                       rootDelta.RootName,
		ManifestRootBaseID:                     rootDelta.BaseRootID,
		UpdatedActiveManifest:                  manifest.Identity,
		RecoveryAuthoritativeManifest:          manifest.Identity,
		RecoveryAuthoritativeAppliedCommandLSN: input.AppliedCommandLSN,
		RootDelta:                              rootDelta,
		PreparedAssets:                         cloneColumnPreparedAssets(manifestPrepared.Assets),
		RequiredAssetCount:                     closure.RequiredAssets,
		RequiredAssetBytes:                     closure.RequiredBytes,
		RequiredAssetFlush:                     closure.FlushRequired,
		RequiredAssetSync:                      closure.SyncRequired,
		Rows:                                   manifestPrepared.RowCount,
		CommandBytes:                           manifestPrepared.CommandBytes,
		RowRemainderBytes:                      manifestPrepared.RowRemainderBytes,
		ColumnPayloadBytes:                     manifestPrepared.ColumnPayloadBytes,
		ManifestBytes:                          manifest.ManifestBytes,
		ManifestMutationRecords:                columnManifestRootPublishedRecordCount(rootDelta),
		ManifestMutationBytes:                  columnManifestRootPublishedBytes(rootDelta),
		Lifecycle: ColumnPublishLifecycleSummary{
			PublishedRefs:  closure.RequiredAssets,
			PublishedBytes: closure.RequiredBytes,
			PreparedRefs:   len(manifestPrepared.Assets),
			PreparedBytes:  preparedBytes,
		},
		StageMetrics:                        metrics,
		stableResources:                     stableResources,
		durableResourceRequirements:         durableResourceRequirements,
		durableResourceMutation:             durableResourceMutation,
		durableResourceRequirementsFallback: durableResourceRequirementsFallback,
		durableResourceRequirementWork:      durableResourceRequirementWork,
	}

	if input.Hooks.BuildSystemDelta != nil {
		start = time.Now()
		if err := input.Hooks.BuildSystemDelta(ColumnPublishSystemDeltaInput{Plan: cloneColumnPublishPlanForHook(plan)}); err != nil {
			return ColumnPublishPlan{}, fmt.Errorf("collections: column publish system-delta construction failed: %w", err)
		}
		plan.StageMetrics.SystemDeltaConstruction = time.Since(start)
	}
	stableResources = nil
	return plan, nil
}

// OrderedRootPublishInput converts the root delta into a backend ordered-root
// publish input while preserving the already-validated identity record bytes.
func (delta ColumnManifestRootDelta) OrderedRootPublishInput() (backenddb.OrderedRootPublishInput, error) {
	if delta.RootName == "" {
		return backenddb.OrderedRootPublishInput{}, errors.New("collections: column manifest root delta missing root name")
	}
	identity := delta.Identity
	normalizeColumnManifestIdentityDefaults(&identity)
	if err := validateColumnManifestIdentity(identity); err != nil {
		return backenddb.OrderedRootPublishInput{}, err
	}
	if delta.IdentityRecord != encodeColumnManifestIdentityRecordArray(identity) {
		return backenddb.OrderedRootPublishInput{}, errors.New("collections: column manifest root delta identity record does not match identity")
	}
	policy, err := backendRootStoragePolicy(delta.StoragePolicy)
	if err != nil {
		return backenddb.OrderedRootPublishInput{}, err
	}
	return backenddb.OrderedRootPublishInput{
		BaseRoot:      delta.BaseRootID,
		Iter:          columnManifestRootRecordIterator(delta.IdentityRecord, delta.Records),
		StoragePolicy: policy,
	}, nil
}

func columnManifestRootPublishedRecordCount(delta ColumnManifestRootDelta) int {
	if delta.MutationDelta {
		return columnManifestRootMutationRecordCount(delta.Mutations)
	}
	return 1 + len(delta.Records)
}

func columnManifestRootPublishedBytes(delta ColumnManifestRootDelta) int64 {
	if delta.MutationDelta {
		return columnManifestRootMutationBytes(delta.Mutations)
	}
	return columnManifestRecordsBytes(delta.Records)
}

func (delta ColumnManifestRootDelta) OrderedRootDeltaPublishInput() (backenddb.OrderedRootDeltaPublishInput, error) {
	if delta.RootName == "" {
		return backenddb.OrderedRootDeltaPublishInput{}, errors.New("collections: column manifest root delta missing root name")
	}
	identity := delta.Identity
	normalizeColumnManifestIdentityDefaults(&identity)
	if err := validateColumnManifestIdentity(identity); err != nil {
		return backenddb.OrderedRootDeltaPublishInput{}, err
	}
	if delta.IdentityRecord != encodeColumnManifestIdentityRecordArray(identity) {
		return backenddb.OrderedRootDeltaPublishInput{}, errors.New("collections: column manifest root delta identity record does not match identity")
	}
	policy, err := backendRootStoragePolicy(delta.StoragePolicy)
	if err != nil {
		return backenddb.OrderedRootDeltaPublishInput{}, err
	}
	var iter iterator.UnsafeIterator
	if delta.MutationDelta {
		iter = columnManifestRootMutationIterator(delta.IdentityRecord, delta.Mutations)
	} else {
		iter = columnManifestRootRecordIterator(delta.IdentityRecord, delta.Records)
	}
	return backenddb.OrderedRootDeltaPublishInput{
		BaseRoot:      delta.BaseRootID,
		Iter:          iter,
		StoragePolicy: policy,
	}, nil
}

func (delta ColumnManifestRootDelta) OrderedRootDeltaBatchPublishInput() (backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	if delta.RootName == "" {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, errors.New("collections: column manifest root delta missing root name")
	}
	identity := delta.Identity
	normalizeColumnManifestIdentityDefaults(&identity)
	if err := validateColumnManifestIdentity(identity); err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, err
	}
	if delta.IdentityRecord != encodeColumnManifestIdentityRecordArray(identity) {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, errors.New("collections: column manifest root delta identity record does not match identity")
	}
	policy, err := backendRootStoragePolicy(delta.StoragePolicy)
	if err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, err
	}
	var iter iterator.UnsafeIterator
	if delta.MutationDelta {
		iter = columnManifestRootMutationIterator(delta.IdentityRecord, delta.Mutations)
	} else {
		iter = columnManifestRootRecordIterator(delta.IdentityRecord, delta.Records)
	}
	defer func() { _ = iter.Close() }()
	deltaBatch, err := backenddb.OrderedRootDeltaBatchFromIterator(iter)
	if err != nil {
		if deltaBatch != nil {
			_ = deltaBatch.Close()
		}
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, err
	}
	cleanup := func() {
		if deltaBatch != nil {
			_ = deltaBatch.Close()
		}
	}
	return backenddb.OrderedRootDeltaBatchPublishInput{
		BaseRoot:      delta.BaseRootID,
		Delta:         deltaBatch,
		StoragePolicy: policy,
	}, cleanup, nil
}

func (c *Collection) buildColumnManifestPublishSystemDeltaIterator(input ColumnManifestPublishSystemDeltaInput, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if c == nil || c.db == nil {
		return nil, backenddb.ErrClosed
	}
	plan := input.Plan
	if !plan.Enabled {
		return nil, errors.New("collections: disabled column publish plan cannot build system delta")
	}
	if len(rootIDs) != 1 {
		return nil, unexpectedOrderedRootCountError(plan.Collection, 1, len(rootIDs))
	}
	if rootIDs[0] == 0 {
		return nil, errors.New("collections: column manifest root publish returned zero root")
	}
	if input.BaseMeta.Options.ColumnStore == nil || input.BaseMeta.Options.ColumnStore.ManifestRoot == nil {
		return nil, errors.New("collections: column manifest publish requires column manifest root metadata")
	}
	if _, err := backendRootStoragePolicy(plan.RootDelta.StoragePolicy); err != nil {
		return nil, err
	}
	if plan.RootDelta.StoragePolicy != input.BaseMeta.Options.ColumnStore.ManifestRoot.StoragePolicy {
		return nil, fmt.Errorf("collections: column publish plan storage policy %q does not match collection manifest root policy %q", plan.RootDelta.StoragePolicy, input.BaseMeta.Options.ColumnStore.ManifestRoot.StoragePolicy)
	}
	if plan.AppliedCommandLSN == 0 {
		return nil, errors.New("collections: column publish plan missing AppliedCommandLSN")
	}
	if plan.RecoveryAuthoritativeAppliedCommandLSN == 0 {
		return nil, errors.New("collections: column publish plan missing recovery-authoritative AppliedCommandLSN")
	}
	if plan.RecoveryAuthoritativeAppliedCommandLSN < plan.AppliedCommandLSN {
		return nil, fmt.Errorf("collections: column publish recovery-authoritative AppliedCommandLSN regression for %q: plan recovery %d < applied %d", input.BaseMeta.Name, plan.RecoveryAuthoritativeAppliedCommandLSN, plan.AppliedCommandLSN)
	}
	rootName := plan.ManifestRootName
	if rootName == "" {
		rootName = plan.RootDelta.RootName
	}
	if rootName == "" {
		return nil, errors.New("collections: column publish plan missing manifest root name")
	}
	if plan.Collection != input.BaseMeta.Name {
		return nil, fmt.Errorf("collections: column publish plan collection %q does not match collection %q", plan.Collection, input.BaseMeta.Name)
	}
	expectedRootName := collectionColumnManifestRootName(input.BaseMeta.Name)
	if rootName != expectedRootName {
		return nil, fmt.Errorf("collections: column publish plan root %q does not match collection root %q", rootName, expectedRootName)
	}
	if plan.RootDelta.RootName != "" && plan.RootDelta.RootName != rootName {
		return nil, fmt.Errorf("collections: column publish plan root %q does not match root delta %q", rootName, plan.RootDelta.RootName)
	}
	if input.BaseManifestRootID != plan.ManifestRootBaseID || input.BaseManifestRootID != plan.RootDelta.BaseRootID {
		return nil, errConcurrentRootModification(input.BaseMeta.Name, rootName)
	}
	rootIdentity := plan.RootDelta.Identity
	normalizeColumnManifestIdentityDefaults(&rootIdentity)
	if err := validateColumnManifestIdentity(rootIdentity); err != nil {
		return nil, err
	}
	if plan.RootDelta.IdentityRecord != encodeColumnManifestIdentityRecordArray(rootIdentity) {
		return nil, errors.New("collections: column publish plan root identity record does not match root identity")
	}
	activeIdentity := plan.UpdatedActiveManifest
	normalizeColumnManifestIdentityDefaults(&activeIdentity)
	if activeIdentity != rootIdentity {
		return nil, errors.New("collections: column publish plan active manifest identity does not match root delta identity")
	}
	recoveryIdentity := plan.RecoveryAuthoritativeManifest
	normalizeColumnManifestIdentityDefaults(&recoveryIdentity)
	if recoveryIdentity != rootIdentity {
		return nil, errors.New("collections: column publish plan recovery-authoritative manifest identity does not match root delta identity")
	}
	if input.BaseMeta.Options.ColumnStore != nil {
		baseRecoveryLSN := input.BaseMeta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN
		if baseRecoveryLSN != 0 && plan.RecoveryAuthoritativeAppliedCommandLSN < baseRecoveryLSN {
			return nil, fmt.Errorf("collections: column publish recovery-authoritative AppliedCommandLSN regression for %q: plan %d < base %d", input.BaseMeta.Name, plan.RecoveryAuthoritativeAppliedCommandLSN, baseRecoveryLSN)
		}
		if baseRecoveryLSN != 0 && plan.AppliedCommandLSN < baseRecoveryLSN {
			return nil, fmt.Errorf("collections: column publish AppliedCommandLSN regression for %q: plan %d < base recovery %d", input.BaseMeta.Name, plan.AppliedCommandLSN, baseRecoveryLSN)
		}
		if baseActive := input.BaseMeta.Options.ColumnStore.ActiveManifest; baseActive != nil {
			normalizedBaseActive := *baseActive
			normalizeColumnManifestIdentityDefaults(&normalizedBaseActive)
			if activeIdentity.Generation <= normalizedBaseActive.Generation {
				return nil, fmt.Errorf("collections: column publish manifest generation regression for %q: plan %d <= base %d", input.BaseMeta.Name, activeIdentity.Generation, normalizedBaseActive.Generation)
			}
		}
	}
	// The root IDs passed here were just built by the ordered-root publish path.
	// Do not read them through the pre-commit snapshot: compressed roots may
	// depend on value-log segment visibility published by the enclosing commit.
	if input.BaseCommitSeq == 0 || input.BaseSystemRoot == 0 {
		return nil, fmt.Errorf("collections: column publish system delta requires BaseCommitSeq and BaseSystemRoot for %q", input.BaseMeta.Name)
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	state, ok := current.StateToken()
	if !ok {
		return nil, backenddb.ErrClosed
	}
	if state.CommitSeq != input.BaseCommitSeq || state.SystemRootPageID != input.BaseSystemRoot {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", input.BaseMeta.Name)
	}
	catalog, err := loadCollectionCatalog(current, input.BaseMeta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, input.BaseMeta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", input.BaseMeta.Name)
	}
	if got := catalog.rootID(rootName); got != input.BaseManifestRootID {
		return nil, errConcurrentRootModification(input.BaseMeta.Name, rootName)
	}

	updatedMeta := copyCollectionMeta(input.BaseMeta)
	if updatedMeta.Options.ColumnStore == nil || !updatedMeta.Options.ColumnStore.Enabled {
		return nil, errColumnPublishPlanRequiresEnabledColumnStore
	}
	cfg := updatedMeta.Options.ColumnStore.copy()
	active := activeIdentity
	recovery := recoveryIdentity
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.RecoveryAuthoritativeAppliedCommandLSN = plan.RecoveryAuthoritativeAppliedCommandLSN
	mutationParts, err := columnPublishPhysicalMutationParts(input.BaseMeta.Options.ColumnStore, plan)
	if err != nil {
		return nil, err
	}
	cfg.PhysicalMutationParts = mutationParts
	updatedMeta.Options.ColumnStore = &cfg
	encodedMeta, err := encodeCollectionMeta(updatedMeta)
	if err != nil {
		return nil, err
	}
	return buildSystemTargetIterator(current, map[string][]byte{
		systemCollectionMetaKey(updatedMeta.Name): encodedMeta,
		systemCollectionRootKey(rootName):         encodeRootID(rootIDs[0]),
	})
}

func validateColumnPublishOperation(op ColumnPublishOperation) error {
	switch op {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete:
		return nil
	default:
		return fmt.Errorf("collections: unsupported column publish operation %q", op)
	}
}

func prepareColumnPublishAssets(input ColumnPublishPlanInput, cfg ColumnStoreConfig) (ColumnPublishPreparedAssets, error) {
	if input.Hooks.PrepareAssets == nil {
		return ColumnPublishPreparedAssets{}, nil
	}
	return input.Hooks.PrepareAssets(ColumnPublishAssetPrepareInput{
		Collection:        input.Collection,
		ColumnStore:       columnPublishHookConfig(cfg),
		Operation:         input.Operation,
		AppliedCommandLSN: input.AppliedCommandLSN,
		CurrentManifest:   cloneColumnManifestIdentityPtr(input.CurrentManifest),
	})
}

func encodeColumnPublishManifest(input ColumnPublishPlanInput, cfg ColumnStoreConfig, prepared ColumnPublishPreparedAssets) (ColumnPublishManifestEncodeResult, error) {
	if input.Hooks.EncodeManifest == nil {
		return ColumnPublishManifestEncodeResult{}, errors.New("collections: column publish manifest encode hook is required")
	}
	return input.Hooks.EncodeManifest(ColumnPublishManifestEncodeInput{
		Collection:               input.Collection,
		ColumnStore:              columnPublishHookConfig(cfg),
		ActiveVectorIndexes:      append([]VectorIndexDefinition(nil), input.ActiveVectorIndexes...),
		ActiveVectorIndexesKnown: input.ActiveVectorIndexesKnown,
		Operation:                input.Operation,
		AppliedCommandLSN:        input.AppliedCommandLSN,
		CurrentManifest:          cloneColumnManifestIdentityPtr(input.CurrentManifest),
		CurrentManifestRecords:   cloneColumnManifestRecords(input.CurrentManifestRecords),
		Prepared:                 cloneColumnPublishPreparedAssets(prepared),
	})
}

func validateColumnPublishClosure(input ColumnPublishPlanInput, cfg ColumnStoreConfig, prepared ColumnPublishPreparedAssets, manifest ColumnPublishManifestEncodeResult) (ColumnPublishDurabilityClosure, error) {
	if input.Hooks.ValidateClosure != nil {
		return input.Hooks.ValidateClosure(ColumnPublishClosureValidationInput{
			Collection:        input.Collection,
			ColumnStore:       columnPublishHookConfig(cfg),
			Operation:         input.Operation,
			AppliedCommandLSN: input.AppliedCommandLSN,
			Prepared:          prepared,
			Manifest:          manifest,
		})
	}
	requiredBytes, err := checkedSumColumnPreparedAssetBytes(prepared.Assets)
	if err != nil {
		return ColumnPublishDurabilityClosure{}, err
	}
	return ColumnPublishDurabilityClosure{
		PreparedAssets: prepared.Assets,
		RequiredAssets: len(prepared.Assets),
		RequiredBytes:  requiredBytes,
		FlushRequired:  len(prepared.Assets) != 0,
		SyncRequired:   len(prepared.Assets) != 0,
	}, nil
}

func buildColumnPublishRootDelta(input ColumnPublishPlanInput, cfg ColumnStoreConfig, manifest ColumnPublishManifestEncodeResult, closure ColumnPublishDurabilityClosure) (ColumnManifestRootDelta, error) {
	if input.Hooks.BuildRootDelta != nil {
		return input.Hooks.BuildRootDelta(ColumnPublishRootDeltaInput{
			Collection:         input.Collection,
			ColumnStore:        columnPublishHookConfig(cfg),
			BaseManifestRootID: input.BaseManifestRootID,
			Manifest:           manifest,
			Closure:            cloneColumnPublishDurabilityClosure(closure),
		})
	}
	if cfg.ManifestRoot == nil {
		return ColumnManifestRootDelta{}, errors.New("missing column manifest root descriptor")
	}
	mutations, err := buildColumnManifestMutationDelta(input.CurrentManifestRecords, manifest.Records)
	if err != nil {
		return ColumnManifestRootDelta{}, err
	}
	return ColumnManifestRootDelta{
		RootName:       cfg.ManifestRoot.Name,
		BaseRootID:     input.BaseManifestRootID,
		StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
		Identity:       manifest.Identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(manifest.Identity),
		Records:        manifest.Records,
		Mutations:      mutations,
		MutationDelta:  true,
	}, nil
}

func columnPublishHookConfig(cfg ColumnStoreConfig) ColumnStoreConfig {
	return cfg.copy()
}

func validateColumnPublishPlanConfig(collection string, cfg *ColumnStoreConfig) error {
	if cfg == nil || !cfg.Enabled {
		return errColumnPublishPlanRequiresEnabledColumnStore
	}
	if cfg.ManifestRoot == nil {
		return errors.New("collections: column publish plan requires column manifest root descriptor")
	}
	if cfg.ManifestRoot.Name == "" {
		return errors.New("collections: column publish plan requires column manifest root name")
	}
	if !columnManifestRootNameMatches(collection, cfg.ManifestRoot.Name) {
		return fmt.Errorf("collections: column manifest root descriptor name %q does not match %q", cfg.ManifestRoot.Name, collectionColumnManifestRootName(collection))
	}
	if _, err := backendRootStoragePolicy(cfg.ManifestRoot.StoragePolicy); err != nil {
		return err
	}
	if cfg.ProfileSupport == "" {
		return errors.New("collections: column publish plan requires normalized profile support")
	}
	switch cfg.ProfileSupport {
	case ColumnStoreProfileDurableOnly, ColumnStoreProfileBenchmarkRelaxed:
	default:
		return fmt.Errorf("collections: unsupported column profile support %q", cfg.ProfileSupport)
	}
	return nil
}

func validateColumnManifestRootDeltaForPlan(delta ColumnManifestRootDelta, currentRecords []columnManifestRecord, baseRootID uint64, cfg ColumnStoreConfig, identity ColumnManifestIdentity) error {
	if cfg.ManifestRoot == nil {
		return errors.New("missing column manifest root descriptor")
	}
	if delta.RootName == "" {
		return errors.New("missing column manifest root name")
	}
	if delta.RootName != cfg.ManifestRoot.Name {
		return fmt.Errorf("root name %q does not match configured manifest root %q", delta.RootName, cfg.ManifestRoot.Name)
	}
	if delta.BaseRootID != baseRootID {
		return fmt.Errorf("base root id=%d does not match expected %d", delta.BaseRootID, baseRootID)
	}
	if delta.StoragePolicy != cfg.ManifestRoot.StoragePolicy {
		return fmt.Errorf("storage policy %q does not match configured manifest root policy %q", delta.StoragePolicy, cfg.ManifestRoot.StoragePolicy)
	}
	if _, err := backendRootStoragePolicy(delta.StoragePolicy); err != nil {
		return err
	}
	if delta.Identity != identity {
		return fmt.Errorf("identity %+v does not match manifest identity %+v", delta.Identity, identity)
	}
	if delta.IdentityRecord != encodeColumnManifestIdentityRecordArray(identity) {
		return errors.New("identity record does not match manifest identity")
	}
	if len(delta.Records) == 0 {
		return errors.New("manifest records omitted")
	}
	snapshot, err := decodeColumnManifestRecords(delta.Records)
	if err != nil {
		return err
	}
	if snapshot.Generation != identity.Generation {
		return fmt.Errorf("manifest records generation=%d does not match identity generation=%d", snapshot.Generation, identity.Generation)
	}
	checksum := checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection: snapshot.Collection,
		ColumnStore: ColumnStoreConfig{
			SchemaHash: snapshot.SchemaHash,
		},
		Operation:         snapshot.Operation,
		AppliedCommandLSN: snapshot.AppliedCommandLSN,
	}, snapshot.Generation, delta.Records)
	if checksum != identity.Checksum {
		return fmt.Errorf("manifest records checksum=%d does not match identity checksum=%d", checksum, identity.Checksum)
	}
	if delta.MutationDelta {
		for i, mutation := range delta.Mutations {
			if len(mutation.record.key) == 0 {
				return fmt.Errorf("manifest mutation[%d] has empty key", i)
			}
			if i > 0 && bytes.Compare(delta.Mutations[i-1].record.key, mutation.record.key) >= 0 {
				return fmt.Errorf("manifest mutations are not strictly sorted at index %d", i)
			}
		}
		expectedMutations, err := buildColumnManifestMutationDelta(currentRecords, delta.Records)
		if err != nil {
			return fmt.Errorf("derive expected manifest mutation delta: %w", err)
		}
		if !columnManifestMutationsEqual(expectedMutations, delta.Mutations) {
			return errors.New("manifest mutation delta does not produce logical post-state")
		}
	}
	return nil
}

func columnManifestRootNameMatches(collection, rootName string) bool {
	if len(rootName) != len(collection)+len(columnManifestRootSuffix) {
		return false
	}
	return rootName[:len(collection)] == collection && rootName[len(collection):] == columnManifestRootSuffix
}

func validateColumnPublishPreparedAssets(prepared ColumnPublishPreparedAssets) error {
	if prepared.RowCount < 0 {
		return errors.New("collections: column publish prepared row count cannot be negative")
	}
	if prepared.CommandBytes < 0 || prepared.RowRemainderBytes < 0 || prepared.ColumnPayloadBytes < 0 {
		return errors.New("collections: column publish prepared byte counts cannot be negative")
	}
	for i, asset := range prepared.Assets {
		if err := validateColumnPreparedAssetForPlan(asset); err != nil {
			return fmt.Errorf("collections: column publish prepared asset[%d]: %w", i, err)
		}
	}
	if _, err := checkedSumColumnPreparedAssetBytes(prepared.Assets); err != nil {
		return err
	}
	return nil
}

func validateColumnPublishDurabilityClosure(closure ColumnPublishDurabilityClosure, cfg ColumnStoreConfig) error {
	if closure.RequiredAssets < 0 || closure.RequiredBytes < 0 {
		return errors.New("collections: column publish closure counts cannot be negative")
	}
	if closure.RequiredAssets != len(closure.PreparedAssets) {
		return fmt.Errorf("collections: column publish closure required assets=%d prepared=%d", closure.RequiredAssets, len(closure.PreparedAssets))
	}
	for i, asset := range closure.PreparedAssets {
		if err := validateColumnPreparedAssetForPlan(asset); err != nil {
			return fmt.Errorf("collections: column publish closure asset[%d]: %w", i, err)
		}
	}
	got, err := checkedSumColumnPreparedAssetBytes(closure.PreparedAssets)
	if err != nil {
		return err
	}
	if got != closure.RequiredBytes {
		return fmt.Errorf("collections: column publish closure required bytes=%d prepared bytes=%d", closure.RequiredBytes, got)
	}
	if cfg.ProfileSupport == ColumnStoreProfileDurableOnly && closure.RequiredAssets != 0 && (!closure.FlushRequired || !closure.SyncRequired) {
		return errors.New("collections: durable column publish closure requires asset flush and sync")
	}
	return nil
}

func validateColumnPublishClosureMatchesPrepared(prepared ColumnPublishPreparedAssets, closure ColumnPublishDurabilityClosure) error {
	if len(closure.PreparedAssets) != len(prepared.Assets) {
		return fmt.Errorf("collections: column publish closure prepared assets=%d manifest prepared assets=%d", len(closure.PreparedAssets), len(prepared.Assets))
	}
	matchesOrder := true
	for i := range prepared.Assets {
		if !columnPreparedAssetsEqual(closure.PreparedAssets[i], prepared.Assets[i]) {
			matchesOrder = false
			break
		}
	}
	if matchesOrder {
		return nil
	}
	preparedCounts := make(map[columnPreparedAssetMatchKey]int, len(prepared.Assets))
	for _, asset := range prepared.Assets {
		preparedCounts[columnPreparedAssetMatchKeyOf(asset)]++
	}
	for i, asset := range closure.PreparedAssets {
		key := columnPreparedAssetMatchKeyOf(asset)
		count := preparedCounts[key]
		if count == 0 {
			return fmt.Errorf("collections: column publish closure prepared asset %d does not match manifest prepared assets", i)
		}
		preparedCounts[key] = count - 1
	}
	return nil
}

type columnPreparedAssetMatchKey struct {
	Ref          ColumnAssetRef
	Rows         int
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
	PartRole     ColumnManifestPartRole
	SortKey      string
}

func columnPreparedAssetsEqual(left, right ColumnPreparedAsset) bool {
	return left == right
}

func columnPreparedAssetMatchKeyOf(asset ColumnPreparedAsset) columnPreparedAssetMatchKey {
	return columnPreparedAssetMatchKey{
		Ref:          asset.Ref,
		Rows:         asset.Rows,
		Bytes:        asset.Bytes,
		PublishID:    asset.PublishID,
		GenerationID: asset.GenerationID,
		PartRole:     asset.PartRole,
		SortKey:      asset.SortKey,
	}
}

func columnSortKeyMatchString(sortKeys []ColumnSortKey) string {
	if len(sortKeys) == 0 {
		return ""
	}
	var out string
	for _, sortKey := range sortKeys {
		out += sortKey.Column + "\x00" + string(sortKey.Direction) + "\x00"
	}
	return out
}

func columnSortKeysFromMatchString(raw string) ([]ColumnSortKey, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, "\x00")
	if len(parts) < 3 || parts[len(parts)-1] != "" || (len(parts)-1)%2 != 0 {
		return nil, fmt.Errorf("collections: malformed column sort key metadata")
	}
	out := make([]ColumnSortKey, 0, (len(parts)-1)/2)
	for i := 0; i < len(parts)-1; i += 2 {
		out = append(out, ColumnSortKey{Column: parts[i], Direction: ColumnSortDirection(parts[i+1])})
	}
	return out, nil
}

func validateColumnPreparedAssetForPlan(asset ColumnPreparedAsset) error {
	if err := validateColumnAssetRefForPlan(asset.Ref); err != nil {
		return err
	}
	switch asset.Ref.Kind {
	case ColumnAssetKindQueryReadyBase, ColumnAssetKindQueryReadyDelta, ColumnAssetKindQueryReadyConsolidatedBase:
		return fmt.Errorf("collections: non-authoritative query-ready asset kind %s cannot enter a manifest publish plan", asset.Ref.Kind)
	}
	if asset.Rows < 0 {
		return fmt.Errorf("collections: column prepared asset rows=%d cannot be negative", asset.Rows)
	}
	if asset.Bytes <= 0 {
		return fmt.Errorf("collections: column prepared asset bytes=%d must be positive", asset.Bytes)
	}
	if asset.Bytes != asset.Ref.Length {
		return fmt.Errorf("collections: column prepared asset bytes=%d does not match ref length=%d", asset.Bytes, asset.Ref.Length)
	}
	if err := validateColumnManifestPartRoleForAsset(asset.PartRole, asset.Ref.Kind, asset.Reason); err != nil {
		return err
	}
	if asset.SortKey != "" && asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
		return fmt.Errorf("collections: column prepared asset sort key is only valid for %s refs, got %s", ColumnAssetKindTCS1TypedColumnPart, asset.Ref.Kind)
	}
	sortKeys, err := columnSortKeysFromMatchString(asset.SortKey)
	if err != nil {
		return err
	}
	if uint64(len(sortKeys)) > columnManifestSortKeyMaxColumns {
		return fmt.Errorf("collections: column prepared asset sort key columns=%d exceeds cap %d", len(sortKeys), columnManifestSortKeyMaxColumns)
	}
	if asset.Ref.Kind == ColumnAssetKindTCS1TypedColumnPart && len(sortKeys) > typedColumnPartSortKeyMaxColumns {
		return fmt.Errorf("collections: column prepared asset sort key columns=%d exceeds cap %d", len(sortKeys), typedColumnPartSortKeyMaxColumns)
	}
	seenSortKeyColumns := make(map[string]struct{}, len(sortKeys))
	for _, sortKey := range sortKeys {
		if sortKey.Column == "" {
			return errors.New("collections: column prepared asset sort key column is required")
		}
		if _, exists := seenSortKeyColumns[sortKey.Column]; exists {
			return fmt.Errorf("collections: column prepared asset duplicate sort key column %q", sortKey.Column)
		}
		seenSortKeyColumns[sortKey.Column] = struct{}{}
		if sortKey.Direction != ColumnSortAscending {
			return fmt.Errorf("collections: column prepared asset sort key column %q direction %q is unsupported", sortKey.Column, sortKey.Direction)
		}
	}
	return nil
}

func validateColumnManifestPartRoleForAsset(role ColumnManifestPartRole, kind ColumnAssetKind, reason string) error {
	switch kind {
	case ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1TypedColumnPart:
		operation, ok := columnPhysicalScanOperationFromBytes([]byte(reason))
		if !ok {
			return fmt.Errorf("collections: unsupported column manifest part reason %q", reason)
		}
		if role == "" {
			return nil
		}
		switch role {
		case ColumnManifestPartRoleBase, ColumnManifestPartRoleDelta, ColumnManifestPartRoleTombstone:
		default:
			return fmt.Errorf("collections: unsupported column manifest part role %q", role)
		}
		if role == ColumnManifestPartRoleTombstone && kind != ColumnAssetKindTCS1PartImage {
			return fmt.Errorf("collections: column manifest tombstone role requires %s ref, got %s", ColumnAssetKindTCS1PartImage, kind)
		}
		want := inferColumnManifestPartRole(kind, string(operation))
		if role != want {
			return fmt.Errorf("collections: column manifest part role=%q does not match operation=%q want role=%q", role, operation, want)
		}
		if role == ColumnManifestPartRoleTombstone && reason != string(ColumnPublishOperationDelete) {
			return fmt.Errorf("collections: column manifest tombstone role requires delete reason, got %q", reason)
		}
		if kind == ColumnAssetKindTCS1TypedColumnPart && role == ColumnManifestPartRoleTombstone {
			return fmt.Errorf("collections: typed-column part cannot have tombstone role")
		}
	default:
		if role == "" {
			return nil
		}
		switch role {
		case ColumnManifestPartRoleBase, ColumnManifestPartRoleDelta, ColumnManifestPartRoleTombstone:
		default:
			return fmt.Errorf("collections: unsupported column manifest part role %q", role)
		}
		return fmt.Errorf("collections: column manifest part role=%q is not allowed for asset kind %s", role, kind)
	}
	return nil
}

func validateColumnAssetRefForPlan(ref ColumnAssetRef) error {
	switch ref.Kind {
	case ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1TypedColumnPart, ColumnAssetKindTCS1AggregateMetadata, ColumnAssetKindTCS1DictionaryCodes, ColumnAssetKindTCS1Int64Values, ColumnAssetKindTCS1HNSWSearchPack, ColumnAssetKindQueryReadyBase, ColumnAssetKindQueryReadyDelta, ColumnAssetKindQueryReadyConsolidatedBase:
	default:
		if ref.Kind == "" {
			return errors.New("collections: column asset ref kind is required")
		}
		return fmt.Errorf("collections: unsupported column asset ref kind %q", ref.Kind)
	}
	if ref.Namespace == "" {
		return errors.New("collections: column asset ref namespace is required")
	}
	if ref.Generation == 0 {
		return errors.New("collections: column asset ref generation is required")
	}
	if ref.PartID == 0 {
		return errors.New("collections: column asset ref part_id is required")
	}
	if ref.FileID == 0 {
		return errors.New("collections: column asset ref file_id is required")
	}
	if ref.Offset < 0 {
		return fmt.Errorf("collections: column asset ref offset=%d cannot be negative", ref.Offset)
	}
	if ref.Length <= 0 {
		return fmt.Errorf("collections: column asset ref length=%d must be positive", ref.Length)
	}
	return nil
}

func checkedSumColumnPreparedAssetBytes(assets []ColumnPreparedAsset) (int64, error) {
	var total int64
	for i, asset := range assets {
		if asset.Bytes > math.MaxInt64-total {
			return 0, fmt.Errorf("collections: column publish asset[%d] bytes overflow", i)
		}
		total += asset.Bytes
	}
	return total, nil
}

func cloneColumnPublishPreparedAssets(prepared ColumnPublishPreparedAssets) ColumnPublishPreparedAssets {
	prepared.stableResources = nil
	prepared.stableResourcesRequired = false
	if len(prepared.Assets) == 0 {
		return prepared
	}
	prepared.Assets = cloneColumnPreparedAssets(prepared.Assets)
	return prepared
}

func cloneColumnManifestIdentityPtr(identity *ColumnManifestIdentity) *ColumnManifestIdentity {
	if identity == nil {
		return nil
	}
	copied := *identity
	return &copied
}

func cloneColumnPreparedAssets(assets []ColumnPreparedAsset) []ColumnPreparedAsset {
	if len(assets) == 0 {
		return assets
	}
	return append([]ColumnPreparedAsset(nil), assets...)
}

func cloneColumnPublishDurabilityClosure(closure ColumnPublishDurabilityClosure) ColumnPublishDurabilityClosure {
	closure.PreparedAssets = cloneColumnPreparedAssets(closure.PreparedAssets)
	return closure
}

func cloneColumnPublishPlanForHook(plan ColumnPublishPlan) ColumnPublishPlan {
	plan.stableResources = nil
	plan.PreparedAssets = cloneColumnPreparedAssets(plan.PreparedAssets)
	return plan
}

func (plan *ColumnPublishPlan) releaseStableResources() {
	if plan == nil || plan.stableResources == nil {
		return
	}
	plan.stableResources.Release()
	plan.stableResources = nil
}

func (plan *ColumnPublishPlan) takeStableResources() *rootpublication.StableResourceSet {
	if plan == nil {
		return nil
	}
	resources := plan.stableResources
	plan.stableResources = nil
	return resources
}
