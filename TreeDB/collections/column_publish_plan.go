package collections

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

const columnManifestRootSuffix = "/column/manifest"

type ColumnPublishOperation string

const (
	ColumnPublishOperationInsert ColumnPublishOperation = "insert"
	ColumnPublishOperationUpdate ColumnPublishOperation = "update"
	ColumnPublishOperationDelete ColumnPublishOperation = "delete"
)

type ColumnAssetKind string

const (
	ColumnAssetKindTCS1PartImage ColumnAssetKind = "tcs1_part_image"
)

type ColumnAssetRef struct {
	Kind     ColumnAssetKind
	FileID   uint32
	Offset   int64
	Length   int64
	Checksum uint32
}

type ColumnPreparedAsset struct {
	Ref          ColumnAssetRef
	Bytes        int
	PublishID    uint64
	GenerationID uint64
	Reason       string
}

type ColumnPublishPlanInput struct {
	Collection            string
	ColumnStore           *ColumnStoreConfig
	ColumnStoreNormalized bool
	Operation             ColumnPublishOperation
	CurrentManifest       *ColumnManifestIdentity
	AppliedCommandLSN     uint64
	BaseManifestRootID    uint64
	MeasureAllocations    bool
	Hooks                 ColumnPublishPlanHooks
}

type ColumnPublishPlanHooks struct {
	ExtractDocuments      func() error
	EncodeDeclaredColumns func(ColumnPublishDeclaredColumnEncodeInput) error
	PrepareAssets         func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error)
	EncodeManifest        func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error)
	ValidateClosure       func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error)
	BuildRootDelta        func(ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error)
	BuildSystemDelta      func(ColumnPublishSystemDeltaInput) error
}

type ColumnPublishDeclaredColumnEncodeInput struct {
	Collection  string
	ColumnStore ColumnStoreConfig
	Operation   ColumnPublishOperation
}

type ColumnPublishAssetPrepareInput struct {
	Collection        string
	ColumnStore       ColumnStoreConfig
	Operation         ColumnPublishOperation
	AppliedCommandLSN uint64
	CurrentManifest   *ColumnManifestIdentity
}

type ColumnPublishPreparedAssets struct {
	Assets             []ColumnPreparedAsset
	RowCount           int
	CommandBytes       int
	RowRemainderBytes  int
	ColumnPayloadBytes int
}

type ColumnPublishManifestEncodeInput struct {
	Collection        string
	ColumnStore       ColumnStoreConfig
	Operation         ColumnPublishOperation
	AppliedCommandLSN uint64
	CurrentManifest   *ColumnManifestIdentity
	Prepared          ColumnPublishPreparedAssets
}

type ColumnPublishManifestEncodeResult struct {
	Identity      ColumnManifestIdentity
	ManifestBytes int
}

type ColumnPublishClosureValidationInput struct {
	Collection        string
	ColumnStore       ColumnStoreConfig
	Operation         ColumnPublishOperation
	AppliedCommandLSN uint64
	Prepared          ColumnPublishPreparedAssets
	Manifest          ColumnPublishManifestEncodeResult
}

type ColumnPublishDurabilityClosure struct {
	// PreparedAssets is owned by the closure/plan boundary; BuildColumnPublishPlan
	// does not defensively clone it on the hot path.
	PreparedAssets []ColumnPreparedAsset
	RequiredAssets int
	RequiredBytes  int
	FlushRequired  bool
	SyncRequired   bool
}

type ColumnPublishRootDeltaInput struct {
	Collection         string
	ColumnStore        ColumnStoreConfig
	BaseManifestRootID uint64
	Manifest           ColumnPublishManifestEncodeResult
	Closure            ColumnPublishDurabilityClosure
}

type ColumnPublishSystemDeltaInput struct {
	Plan ColumnPublishPlan
}

type ColumnManifestPublishSystemDeltaInput struct {
	BaseMeta           CollectionMeta
	BaseCommitSeq      uint64
	BaseSystemRoot     uint64
	BaseManifestRootID uint64
	Plan               ColumnPublishPlan
}

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
	RequiredAssetBytes                     int
	RequiredAssetFlush                     bool
	RequiredAssetSync                      bool
	Rows                                   int
	CommandBytes                           int
	RowRemainderBytes                      int
	ColumnPayloadBytes                     int
	ManifestBytes                          int
	Lifecycle                              ColumnPublishLifecycleSummary
	StageMetrics                           ColumnPublishStageMetrics
}

type ColumnManifestRootDelta struct {
	RootName       string
	BaseRootID     uint64
	StoragePolicy  RootStoragePolicy
	Identity       ColumnManifestIdentity
	IdentityRecord [columnManifestIdentityRecordSize]byte
}

type ColumnPublishLifecycleSummary struct {
	PublishedRefs         int
	PublishedBytes        int
	PreparedRefs          int
	PreparedBytes         int
	SupersededRefs        int
	SupersededBytes       int
	CleanupSafeGeneration uint64
	CleanupSafeRefs       int
	CleanupSafeBytes      int
	RewriteDebtBytes      int
}

type ColumnPublishStageMetrics struct {
	DocumentExtraction      time.Duration
	DeclaredColumnEncoding  time.Duration
	AssetPreparation        time.Duration
	AssetFlushSync          time.Duration
	ManifestEncode          time.Duration
	RootDeltaConstruction   time.Duration
	SystemDeltaConstruction time.Duration
	AllocBytes              uint64
	Allocs                  uint64
}

func BuildColumnPublishPlan(input ColumnPublishPlanInput) (ColumnPublishPlan, error) {
	if input.ColumnStore == nil || !input.ColumnStore.Enabled {
		return ColumnPublishPlan{}, nil
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
	if cfg == nil || !cfg.Enabled {
		return ColumnPublishPlan{}, errors.New("collections: column publish plan requires enabled column_store")
	}
	if cfg.ProfileSupport != ColumnStoreProfileDurableOnly {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish plan requires durable profile support, got %q", cfg.ProfileSupport)
	}

	var metrics ColumnPublishStageMetrics
	var allocStart runtime.MemStats
	if input.MeasureAllocations {
		runtime.ReadMemStats(&allocStart)
	}
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
			ColumnStore: *cfg,
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
	if err := validateColumnPublishPreparedAssets(prepared); err != nil {
		return ColumnPublishPlan{}, err
	}

	start = time.Now()
	manifest, err := encodeColumnPublishManifest(input, *cfg, prepared)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish manifest encode failed: %w", err)
	}
	metrics.ManifestEncode = time.Since(start)
	normalizeColumnManifestIdentityDefaults(&manifest.Identity)
	if err := validateColumnManifestIdentity(manifest.Identity); err != nil {
		return ColumnPublishPlan{}, err
	}

	start = time.Now()
	closure, err := validateColumnPublishClosure(input, *cfg, prepared, manifest)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish asset-closure validation failed: %w", err)
	}
	metrics.AssetFlushSync = time.Since(start)
	if err := validateColumnPublishDurabilityClosure(closure); err != nil {
		return ColumnPublishPlan{}, err
	}
	if err := validateColumnPublishClosureMatchesPrepared(prepared, closure); err != nil {
		return ColumnPublishPlan{}, err
	}

	start = time.Now()
	rootDelta, err := buildColumnPublishRootDelta(input, *cfg, manifest, closure)
	if err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: column publish root-delta construction failed: %w", err)
	}
	metrics.RootDeltaConstruction = time.Since(start)
	if err := validateColumnManifestRootDeltaForPlan(rootDelta, input.BaseManifestRootID, *cfg, manifest.Identity); err != nil {
		return ColumnPublishPlan{}, fmt.Errorf("collections: invalid column publish root delta: %w", err)
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
		PreparedAssets:                         closure.PreparedAssets,
		RequiredAssetCount:                     closure.RequiredAssets,
		RequiredAssetBytes:                     closure.RequiredBytes,
		RequiredAssetFlush:                     closure.FlushRequired,
		RequiredAssetSync:                      closure.SyncRequired,
		Rows:                                   prepared.RowCount,
		CommandBytes:                           prepared.CommandBytes,
		RowRemainderBytes:                      prepared.RowRemainderBytes,
		ColumnPayloadBytes:                     prepared.ColumnPayloadBytes,
		ManifestBytes:                          manifest.ManifestBytes,
		Lifecycle: ColumnPublishLifecycleSummary{
			PublishedRefs:  closure.RequiredAssets,
			PublishedBytes: closure.RequiredBytes,
			PreparedRefs:   len(prepared.Assets),
			PreparedBytes:  sumColumnPreparedAssetBytes(prepared.Assets),
		},
		StageMetrics: metrics,
	}

	if input.Hooks.BuildSystemDelta != nil {
		start = time.Now()
		if err := input.Hooks.BuildSystemDelta(ColumnPublishSystemDeltaInput{Plan: plan}); err != nil {
			return ColumnPublishPlan{}, fmt.Errorf("collections: column publish system-delta construction failed: %w", err)
		}
		plan.StageMetrics.SystemDeltaConstruction = time.Since(start)
	}
	if input.MeasureAllocations {
		var allocEnd runtime.MemStats
		runtime.ReadMemStats(&allocEnd)
		plan.StageMetrics.AllocBytes = allocEnd.TotalAlloc - allocStart.TotalAlloc
		plan.StageMetrics.Allocs = allocEnd.Mallocs - allocStart.Mallocs
	}
	return plan, nil
}

func (delta ColumnManifestRootDelta) OrderedRootPublishInput() (backenddb.OrderedRootPublishInput, error) {
	if delta.RootName == "" {
		return backenddb.OrderedRootPublishInput{}, errors.New("collections: column manifest root delta missing root name")
	}
	identity := delta.Identity
	normalizeColumnManifestIdentityDefaults(&identity)
	if err := validateColumnManifestIdentity(identity); err != nil {
		return backenddb.OrderedRootPublishInput{}, err
	}
	policy, err := backendRootStoragePolicy(delta.StoragePolicy)
	if err != nil {
		return backenddb.OrderedRootPublishInput{}, err
	}
	return backenddb.OrderedRootPublishInput{
		BaseRoot:      delta.BaseRootID,
		Iter:          columnManifestIdentityIterator(identity),
		StoragePolicy: policy,
	}, nil
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
	rootName := plan.ManifestRootName
	if rootName == "" {
		rootName = plan.RootDelta.RootName
	}
	if rootName == "" {
		return nil, errors.New("collections: column publish plan missing manifest root name")
	}

	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
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
		return nil, errors.New("collections: column manifest publish requires enabled column_store metadata")
	}
	cfg := updatedMeta.Options.ColumnStore.copy()
	active := plan.UpdatedActiveManifest
	recovery := plan.RecoveryAuthoritativeManifest
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.RecoveryAuthoritativeAppliedCommandLSN = plan.RecoveryAuthoritativeAppliedCommandLSN
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
		ColumnStore:       cfg,
		Operation:         input.Operation,
		AppliedCommandLSN: input.AppliedCommandLSN,
		CurrentManifest:   input.CurrentManifest,
	})
}

func encodeColumnPublishManifest(input ColumnPublishPlanInput, cfg ColumnStoreConfig, prepared ColumnPublishPreparedAssets) (ColumnPublishManifestEncodeResult, error) {
	if input.Hooks.EncodeManifest == nil {
		return ColumnPublishManifestEncodeResult{}, errors.New("manifest encode hook is required")
	}
	return input.Hooks.EncodeManifest(ColumnPublishManifestEncodeInput{
		Collection:        input.Collection,
		ColumnStore:       cfg,
		Operation:         input.Operation,
		AppliedCommandLSN: input.AppliedCommandLSN,
		CurrentManifest:   input.CurrentManifest,
		Prepared:          prepared,
	})
}

func validateColumnPublishClosure(input ColumnPublishPlanInput, cfg ColumnStoreConfig, prepared ColumnPublishPreparedAssets, manifest ColumnPublishManifestEncodeResult) (ColumnPublishDurabilityClosure, error) {
	if input.Hooks.ValidateClosure != nil {
		return input.Hooks.ValidateClosure(ColumnPublishClosureValidationInput{
			Collection:        input.Collection,
			ColumnStore:       cfg,
			Operation:         input.Operation,
			AppliedCommandLSN: input.AppliedCommandLSN,
			Prepared:          prepared,
			Manifest:          manifest,
		})
	}
	requiredBytes := sumColumnPreparedAssetBytes(prepared.Assets)
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
			ColumnStore:        cfg,
			BaseManifestRootID: input.BaseManifestRootID,
			Manifest:           manifest,
			Closure:            closure,
		})
	}
	if cfg.ManifestRoot == nil {
		return ColumnManifestRootDelta{}, errors.New("missing column manifest root descriptor")
	}
	return ColumnManifestRootDelta{
		RootName:       cfg.ManifestRoot.Name,
		BaseRootID:     input.BaseManifestRootID,
		StoragePolicy:  cfg.ManifestRoot.StoragePolicy,
		Identity:       manifest.Identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(manifest.Identity),
	}, nil
}

func validateColumnPublishPlanConfig(collection string, cfg *ColumnStoreConfig) error {
	if cfg == nil || !cfg.Enabled {
		return errors.New("collections: column publish plan requires enabled column_store")
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
	return nil
}

func validateColumnManifestRootDeltaForPlan(delta ColumnManifestRootDelta, baseRootID uint64, cfg ColumnStoreConfig, identity ColumnManifestIdentity) error {
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
	if delta.Identity != identity {
		return fmt.Errorf("identity %+v does not match manifest identity %+v", delta.Identity, identity)
	}
	if delta.IdentityRecord != encodeColumnManifestIdentityRecordArray(identity) {
		return errors.New("identity record does not match manifest identity")
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
	for _, asset := range prepared.Assets {
		if err := validateColumnPreparedAssetForPlan(asset); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnPublishDurabilityClosure(closure ColumnPublishDurabilityClosure) error {
	if closure.RequiredAssets < 0 || closure.RequiredBytes < 0 {
		return errors.New("collections: column publish closure counts cannot be negative")
	}
	if closure.RequiredAssets != len(closure.PreparedAssets) {
		return fmt.Errorf("collections: column publish closure required assets=%d prepared=%d", closure.RequiredAssets, len(closure.PreparedAssets))
	}
	if got := sumColumnPreparedAssetBytes(closure.PreparedAssets); got != closure.RequiredBytes {
		return fmt.Errorf("collections: column publish closure required bytes=%d prepared bytes=%d", closure.RequiredBytes, got)
	}
	if closure.RequiredAssets != 0 && (!closure.FlushRequired || !closure.SyncRequired) {
		return errors.New("collections: durable column publish closure requires asset flush and sync")
	}
	for _, asset := range closure.PreparedAssets {
		if err := validateColumnPreparedAssetForPlan(asset); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnPublishClosureMatchesPrepared(prepared ColumnPublishPreparedAssets, closure ColumnPublishDurabilityClosure) error {
	if len(closure.PreparedAssets) != len(prepared.Assets) {
		return fmt.Errorf("collections: column publish closure prepared assets=%d manifest prepared assets=%d", len(closure.PreparedAssets), len(prepared.Assets))
	}
	for i := range prepared.Assets {
		if closure.PreparedAssets[i] != prepared.Assets[i] {
			return fmt.Errorf("collections: column publish closure prepared asset %d does not match manifest prepared asset", i)
		}
	}
	return nil
}

func validateColumnPreparedAssetForPlan(asset ColumnPreparedAsset) error {
	if err := validateColumnAssetRefForPlan(asset.Ref); err != nil {
		return err
	}
	if asset.Bytes <= 0 {
		return fmt.Errorf("collections: column prepared asset bytes=%d must be positive", asset.Bytes)
	}
	if int64(asset.Bytes) != asset.Ref.Length {
		return fmt.Errorf("collections: column prepared asset bytes=%d does not match ref length=%d", asset.Bytes, asset.Ref.Length)
	}
	return nil
}

func validateColumnAssetRefForPlan(ref ColumnAssetRef) error {
	switch ref.Kind {
	case ColumnAssetKindTCS1PartImage:
	default:
		if ref.Kind == "" {
			return errors.New("collections: column asset ref kind is required")
		}
		return fmt.Errorf("collections: unsupported column asset ref kind %q", ref.Kind)
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
	if ref.Checksum == 0 {
		return errors.New("collections: column asset ref checksum is required")
	}
	return nil
}

func sumColumnPreparedAssetBytes(assets []ColumnPreparedAsset) int {
	total := 0
	for _, asset := range assets {
		total += asset.Bytes
	}
	return total
}
