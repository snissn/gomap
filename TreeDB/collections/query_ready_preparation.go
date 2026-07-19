package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const (
	defaultQueryReadyColumnPreparationWorkers       = 1
	defaultQueryReadyColumnPreparationInFlightBytes = int64(1 << 30)
)

// QueryReadyColumnPreparationOptions bounds one query-independent build from
// the current recovery-authoritative insert-only typed-column inventory.
// Zero values select conservative defaults.
type QueryReadyColumnPreparationOptions struct {
	MaxWorkers       int
	MaxInFlightBytes int64
}

// QueryReadyColumnPreparationBoundError is returned before retaining the next
// source image or admitting M5 build allocation when their combined estimated
// peak exceeds the configured per-worker share.
type QueryReadyColumnPreparationBoundError struct {
	RequiredBytes int64
	MaxBytes      int64
}

func (e *QueryReadyColumnPreparationBoundError) Error() string {
	if e == nil {
		return "collections: query-ready column preparation bound exceeded"
	}
	return fmt.Sprintf("collections: query-ready column preparation requires %d bytes; bound is %d", e.RequiredBytes, e.MaxBytes)
}

// QueryReadyColumnPreparationStats separates source preparation from M5's
// bounded generation build/publication handoff. It is intended for benchmark
// harness reporting outside timed query attempts.
type QueryReadyColumnPreparationStats struct {
	SourceParts                int           `json:"source_parts"`
	SourceRows                 int64         `json:"source_rows"`
	SourceBytes                int64         `json:"source_bytes"`
	SourceReadTime             time.Duration `json:"source_read_nanoseconds"`
	SourceDecodeTime           time.Duration `json:"source_decode_nanoseconds"`
	Rows                       int64         `json:"rows"`
	InputBytes                 int64         `json:"input_bytes"`
	OutputBytes                int64         `json:"output_bytes"`
	BytesCopied                int64         `json:"bytes_copied"`
	BytesHashed                int64         `json:"bytes_hashed"`
	BytesChecksummed           int64         `json:"bytes_checksummed"`
	ExecutionBytes             int64         `json:"execution_bytes"`
	ExecutionColumns           int           `json:"execution_columns"`
	AssetsProduced             int           `json:"assets_produced"`
	PartsProduced              int           `json:"parts_produced"`
	ReservedInFlightBytes      int64         `json:"build_reserved_in_flight_bytes"`
	EstimatedPeakInFlightBytes int64         `json:"combined_estimated_peak_in_flight_bytes"`
	EncodedBufferPeakBytes     int64         `json:"encoded_buffer_peak_bytes"`
	WorkerLimit                int           `json:"worker_limit"`
	QueueCapacity              int           `json:"queue_capacity"`
	QueueWaitTime              time.Duration `json:"queue_wait_nanoseconds"`
	BuildTime                  time.Duration `json:"build_nanoseconds"`
	AssetPreparationTime       time.Duration `json:"asset_preparation_nanoseconds"`
	ManagerRegistrationTime    time.Duration `json:"manager_registration_nanoseconds"`
	PublicationHandoffTime     time.Duration `json:"publication_handoff_nanoseconds"`
	GenerationOpenTime         time.Duration `json:"generation_open_nanoseconds"`
	OpenPartsDecoded           int           `json:"open_parts_decoded"`
	OpenDictionaryBytesDecoded int64         `json:"open_dictionary_bytes_decoded"`
	OpenDomainsConstructed     int           `json:"open_domains_constructed"`
	TotalTime                  time.Duration `json:"total_nanoseconds"`
}

// QueryReadyColumnPreparedGeneration owns one rebuildable, non-authoritative
// QRBG asset. A successful Close prevents new runners and reclaims the asset.
// Close returns ErrQueryReadyColumnGenerationBusy without changing ownership
// while runners are active, so the owner may retry after they close.
type QueryReadyColumnPreparedGeneration struct {
	files    QueryReadyColumnGenerationFiles
	stats    QueryReadyColumnPreparationStats
	identity ColumnStoreCacheIdentity
	lifetime *queryReadyColumnPreparedLifetime
}

// ErrQueryReadyColumnGenerationBusy reports an attempted owner Close while a
// prepared physical-query runner still pins the M3 mapping.
var ErrQueryReadyColumnGenerationBusy = errors.New("collections: query-ready prepared generation has active runners")

// Files returns the exact nonzero M3 byte range selected by preparation.
func (p *QueryReadyColumnPreparedGeneration) Files() QueryReadyColumnGenerationFiles {
	if p == nil {
		return QueryReadyColumnGenerationFiles{}
	}
	files := p.files
	files.Deltas = append([]QueryReadyColumnGenerationFile(nil), p.files.Deltas...)
	return files
}

// Stats returns the one-time source/build/handoff accounting for this asset.
func (p *QueryReadyColumnPreparedGeneration) Stats() QueryReadyColumnPreparationStats {
	if p == nil {
		return QueryReadyColumnPreparationStats{}
	}
	return p.stats
}

// SnapshotIdentity returns the recovery-authoritative collection identity
// pinned while the source typed-column inventory was selected.
func (p *QueryReadyColumnPreparedGeneration) SnapshotIdentity() ColumnStoreCacheIdentity {
	if p == nil {
		return ColumnStoreCacheIdentity{}
	}
	return p.identity
}

// Close releases the owner lease and is idempotent after successful cleanup.
// A busy Close leaves the owner and file set valid for subsequent runners.
func (p *QueryReadyColumnPreparedGeneration) Close() error {
	if p == nil || p.lifetime == nil {
		return nil
	}
	return p.lifetime.closeOwner()
}

type queryReadyColumnPreparedLifetime struct {
	collection *Collection
	identity   ColumnStoreCacheIdentity
	files      typedcolumn.QueryReadyGenerationOpenFiles
	prepared   *queryReadyPreparedGeneration

	mu       sync.Mutex
	refs     int
	closing  bool
	cleaned  bool
	closeErr error
}

func (l *queryReadyColumnPreparedLifetime) acquire() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing || l.cleaned || l.prepared == nil {
		return errors.New("collections: query-ready prepared generation is closed")
	}
	l.refs++
	return nil
}

func (l *queryReadyColumnPreparedLifetime) closeOwner() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.cleaned {
		err := l.closeErr
		l.mu.Unlock()
		return err
	}
	if l.refs > 1 {
		l.mu.Unlock()
		return ErrQueryReadyColumnGenerationBusy
	}
	l.closing = true
	l.mu.Unlock()
	if err := l.collection.retireCollectionQueryReadyGenerationCache(l.identity, l.files); err != nil {
		l.mu.Lock()
		l.closing = false
		l.mu.Unlock()
		return err
	}
	var cleanupErr error
	if l.prepared != nil {
		cleanupErr = l.prepared.Abort()
	}
	l.mu.Lock()
	l.refs = 0
	l.cleaned = true
	l.prepared = nil
	l.closeErr = errors.Join(l.closeErr, cleanupErr)
	err := l.closeErr
	l.mu.Unlock()
	return err
}

func (l *queryReadyColumnPreparedLifetime) release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.refs > 0 {
		l.refs--
	}
	l.mu.Unlock()
	return nil
}

// PrepareQueryReadyColumnGeneration builds one QRBG from the exact
// recovery-authoritative insert-only typed-column parts pinned by a DB
// snapshot. It neither publishes a manifest/root nor assumes GC ownership.
func (c *Collection) PrepareQueryReadyColumnGeneration(ctx context.Context, options QueryReadyColumnPreparationOptions) (_ *QueryReadyColumnPreparedGeneration, retErr error) {
	started := time.Now()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errCollectionNil
	}
	if options.MaxWorkers == 0 {
		options.MaxWorkers = defaultQueryReadyColumnPreparationWorkers
	}
	if options.MaxInFlightBytes == 0 {
		options.MaxInFlightBytes = defaultQueryReadyColumnPreparationInFlightBytes
	}
	coordinator, err := newQueryReadyBuildCoordinator(c, queryReadyBuildLimits{MaxWorkers: options.MaxWorkers, MaxInFlightBytes: options.MaxInFlightBytes})
	if err != nil {
		return nil, err
	}
	preparationBudget := options.MaxInFlightBytes / int64(options.MaxWorkers)
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(ctx, columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return nil, err
	}
	identity, ok := columnStoreCacheIdentity(view.Catalog, view.SystemRoot, view.CommitSeq)
	if !ok {
		return nil, errors.New("collections: query-ready preparation has no snapshot column-store identity")
	}
	key, ok := collectionQueryReadyGenerationOpenKey(identity)
	if !ok {
		return nil, errors.New("collections: query-ready preparation has incomplete snapshot identity")
	}
	if view.MutationParts != 0 {
		return nil, fmt.Errorf("%w: query-ready preparation requires insert-only recovery state", ErrColumnQueryPlanUnsupported)
	}
	refs := append([]columnManifestAssetRefForScan(nil), view.TypedColumnPartRefs...)
	if len(refs) == 0 {
		return nil, fmt.Errorf("%w: query-ready preparation requires typed-column parts", ErrColumnQueryPlanUnsupported)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Ref.Generation != refs[j].Ref.Generation {
			return refs[i].Ref.Generation < refs[j].Ref.Generation
		}
		return refs[i].Ref.PartID < refs[j].Ref.PartID
	})
	stats := QueryReadyColumnPreparationStats{SourceParts: len(refs)}
	planner, err := typedcolumn.NewQueryReadyBaseStreamingPlanner(key.Identity, len(refs))
	if err != nil {
		return nil, err
	}
	primaryBases := make([]int64, len(refs))
	for index, source := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ref := source.Ref
		if ref.Kind != ColumnAssetKindTCS1TypedColumnPart || source.Reason != ColumnPublishOperationInsert || source.Role != ColumnManifestPartRoleBase {
			return nil, fmt.Errorf("%w: query-ready source[%d] kind=%q reason=%q role=%q is not an insert-only typed-column base", ErrColumnQueryPlanUnsupported, index, ref.Kind, source.Reason, source.Role)
		}
		if ref.Generation == 0 || ref.Generation > identity.ManifestGeneration || source.Rows < 0 {
			return nil, fmt.Errorf("collections: invalid query-ready source[%d] generation=%d rows=%d", index, ref.Generation, source.Rows)
		}
		if ref.Length <= 0 {
			return nil, fmt.Errorf("collections: invalid query-ready source[%d] length=%d", index, ref.Length)
		}
		if ref.Length > preparationBudget {
			return nil, &QueryReadyColumnPreparationBoundError{RequiredBytes: ref.Length, MaxBytes: preparationBudget}
		}
		readStarted := time.Now()
		raw, err := readColumnPhysicalAssetFromManager(view.ColumnAssetRootDir, ref)
		stats.SourceReadTime += time.Since(readStarted)
		if err != nil {
			return nil, fmt.Errorf("collections: read query-ready source[%d]: %w", index, err)
		}
		decodeStarted := time.Now()
		image, err := typedcolumn.ParseColumnPartImage(raw)
		stats.SourceDecodeTime += time.Since(decodeStarted)
		if err != nil {
			return nil, fmt.Errorf("collections: decode query-ready source[%d]: %w", index, err)
		}
		if image.PartID != ref.PartID || image.Rows != source.Rows {
			return nil, fmt.Errorf("collections: query-ready source[%d] image identity part_id=%d rows=%d want part_id=%d rows=%d", index, image.PartID, image.Rows, ref.PartID, source.Rows)
		}
		executionUpper, err := typedcolumn.EstimateQueryReadyExecutionImageUpperBound(image)
		if err != nil {
			return nil, fmt.Errorf("collections: estimate query-ready source[%d] execution: %w", index, err)
		}
		if executionUpper < 0 || executionUpper > math.MaxInt64/2 || int64(len(raw)) > math.MaxInt64-executionUpper*2 {
			return nil, &QueryReadyColumnPreparationBoundError{RequiredBytes: math.MaxInt64, MaxBytes: preparationBudget}
		}
		livePassOne := int64(len(raw)) + executionUpper*2
		metadataBytes := int64(64 << 10)
		index64 := int64(index)
		if index64 > (math.MaxInt64-metadataBytes)/(1024+64)-1 {
			return nil, &QueryReadyColumnPreparationBoundError{RequiredBytes: math.MaxInt64, MaxBytes: preparationBudget}
		}
		metadataBytes += (index64 + 1) * (1024 + 64)
		if livePassOne > math.MaxInt64-metadataBytes {
			return nil, &QueryReadyColumnPreparationBoundError{RequiredBytes: math.MaxInt64, MaxBytes: preparationBudget}
		}
		livePassOne += metadataBytes
		if livePassOne > preparationBudget {
			return nil, &QueryReadyColumnPreparationBoundError{RequiredBytes: livePassOne, MaxBytes: preparationBudget}
		}
		primaryBases[index] = stats.SourceRows
		stats.SourceRows += int64(image.Rows)
		stats.SourceBytes += int64(len(raw))
		if err := planner.Add(typedcolumn.QueryReadyBasePartInput{
			SourceGeneration: ref.Generation,
			Image:            image,
			PrimaryIDMode:    typedcolumn.QueryReadyPrimaryIDDensePartLocal,
			PrimaryIDBase:    stats.SourceRows - int64(image.Rows),
		}); err != nil {
			return nil, fmt.Errorf("collections: plan query-ready source[%d]: %w", index, err)
		}
	}
	plan, err := planner.Finish()
	if err != nil {
		return nil, err
	}
	buildWorkingBytes, err := plan.EstimatedPeakBytes()
	if err != nil {
		return nil, err
	}
	if buildWorkingBytes > preparationBudget {
		return nil, &QueryReadyColumnPreparationBoundError{RequiredBytes: buildWorkingBytes, MaxBytes: preparationBudget}
	}
	request := queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: key.Identity, StreamingBasePlan: plan, StreamingBaseLoad: func(index int) (typedcolumn.QueryReadyBasePartInput, error) {
		if err := ctx.Err(); err != nil {
			return typedcolumn.QueryReadyBasePartInput{}, err
		}
		if index < 0 || index >= len(refs) {
			return typedcolumn.QueryReadyBasePartInput{}, fmt.Errorf("collections: query-ready emit source index=%d outside %d refs", index, len(refs))
		}
		source := refs[index]
		readStarted := time.Now()
		raw, err := readColumnPhysicalAssetFromManager(view.ColumnAssetRootDir, source.Ref)
		stats.SourceReadTime += time.Since(readStarted)
		if err != nil {
			return typedcolumn.QueryReadyBasePartInput{}, fmt.Errorf("collections: reread query-ready source[%d]: %w", index, err)
		}
		decodeStarted := time.Now()
		image, err := typedcolumn.ParseColumnPartImage(raw)
		stats.SourceDecodeTime += time.Since(decodeStarted)
		if err != nil {
			return typedcolumn.QueryReadyBasePartInput{}, fmt.Errorf("collections: redecode query-ready source[%d]: %w", index, err)
		}
		if image.PartID != source.Ref.PartID || image.Rows != source.Rows {
			return typedcolumn.QueryReadyBasePartInput{}, fmt.Errorf("collections: query-ready reread source[%d] image identity part_id=%d rows=%d want part_id=%d rows=%d", index, image.PartID, image.Rows, source.Ref.PartID, source.Rows)
		}
		return typedcolumn.QueryReadyBasePartInput{SourceGeneration: source.Ref.Generation, Image: image, PrimaryIDMode: typedcolumn.QueryReadyPrimaryIDDensePartLocal, PrimaryIDBase: primaryBases[index]}, nil
	}}
	prepared, err := coordinator.prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if retErr != nil && cleanup {
			retErr = errors.Join(retErr, prepared.Abort())
		}
	}()
	descriptor, err := prepared.OpenFileDescriptor()
	if err != nil {
		return nil, err
	}
	if descriptor.Offset <= 0 || descriptor.Length <= 0 || descriptor.Identity.Generation == 0 || descriptor.Kind != typedcolumn.QueryReadyGenerationBase {
		return nil, fmt.Errorf("collections: query-ready preparation produced invalid M3 descriptor %+v", descriptor)
	}
	files := QueryReadyColumnGenerationFiles{Base: QueryReadyColumnGenerationFile{
		Path: descriptor.Path, Offset: descriptor.Offset, Length: descriptor.Length,
		Generation: descriptor.Identity.Generation, Kind: QueryReadyColumnGenerationBase,
	}}
	selectedFiles := typedcolumn.QueryReadyGenerationOpenFiles{
		Key: key, Base: descriptor, SnapshotGeneration: key.Identity.Generation,
		Bound: typedcolumn.DefaultQueryReadyDeltaBoundPolicy(),
	}
	build := prepared.Stats
	stats.Rows, stats.InputBytes, stats.OutputBytes = build.Rows, build.InputBytes, build.OutputBytes
	stats.BytesCopied, stats.BytesHashed, stats.BytesChecksummed = build.BytesCopied, build.BytesHashed, build.BytesChecksummed
	stats.ExecutionBytes, stats.ExecutionColumns = build.ExecutionBytes, build.ExecutionColumns
	stats.AssetsProduced, stats.PartsProduced = build.AssetsProduced, build.PartsProduced
	stats.ReservedInFlightBytes, stats.EstimatedPeakInFlightBytes, stats.EncodedBufferPeakBytes = build.ReservedInFlightBytes, build.EstimatedPeakInFlightBytes, build.EncodedBufferPeakBytes
	stats.WorkerLimit, stats.QueueCapacity, stats.QueueWaitTime = build.WorkerLimit, build.QueueCapacity, build.QueueWaitTime
	stats.BuildTime, stats.AssetPreparationTime = build.BaseBuildTime, build.AssetPreparationTime
	stats.ManagerRegistrationTime, stats.PublicationHandoffTime = build.ManagerRegistrationTime, build.HandoffTime
	openStarted := time.Now()
	openLease, err := c.openCollectionQueryReadyGenerationForIdentity(identity, selectedFiles)
	stats.GenerationOpenTime = time.Since(openStarted)
	if err != nil {
		return nil, err
	}
	if err := openLease.Close(); err != nil {
		retireErr := c.retireCollectionQueryReadyGenerationCache(identity, selectedFiles)
		return nil, errors.Join(err, retireErr)
	}
	openStats := c.collectionQueryReadyGenerationCacheSnapshot().Open
	stats.OpenPartsDecoded = openStats.PartsDecoded
	stats.OpenDictionaryBytesDecoded = openStats.PayloadBytesDecoded
	stats.OpenDomainsConstructed = openStats.DomainsConstructed
	stats.TotalTime = time.Since(started)
	lifetime := &queryReadyColumnPreparedLifetime{collection: c, identity: identity, files: selectedFiles, prepared: prepared, refs: 1}
	files.lifetime = lifetime
	cleanup = false
	return &QueryReadyColumnPreparedGeneration{files: files, stats: stats, identity: identity, lifetime: lifetime}, nil
}
