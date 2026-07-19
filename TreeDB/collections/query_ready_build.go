package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Query-ready generations remain rebuildable prepared assets. This bridge
// deliberately uses the existing column asset manager and prepared-asset
// lifecycle registry; it does not publish a root, select recovery state, or
// claim deletion ownership.
const (
	queryReadyBaseAssetPartID             = uint64(0x51524201)
	queryReadyDeltaAssetPartID            = uint64(0x51524401)
	queryReadyConsolidatedBaseAssetPartID = uint64(0x51524301)
)

type queryReadyBuildKind uint8

const (
	queryReadyBuildBase queryReadyBuildKind = iota + 1
	queryReadyBuildDelta
	queryReadyBuildConsolidatedBase
)

type queryReadyBuildRequest struct {
	Kind              queryReadyBuildKind
	Identity          typedcolumn.QueryReadyBaseIdentity
	Parts             []typedcolumn.QueryReadyBasePartInput
	Tombstones        []typedcolumn.Tombstone
	Base              *typedcolumn.QueryReadyBaseGeneration
	ConsolidatedBase  *typedcolumn.QueryReadyDeltaGeneration
	Deltas            []*typedcolumn.QueryReadyDeltaGeneration
	ThroughGeneration uint64
	Bound             typedcolumn.QueryReadyDeltaBoundPolicy
	StreamingBasePlan *typedcolumn.QueryReadyBaseStreamingPlan
	StreamingBaseLoad func(int) (typedcolumn.QueryReadyBasePartInput, error)
}

type queryReadyBuildLimits struct {
	MaxWorkers       int
	MaxInFlightBytes int64
}

// QueryReadyBuildBoundError is returned before output allocation when one
// request cannot fit its per-worker share of the configured in-flight bound.
type QueryReadyBuildBoundError struct {
	RequiredBytes int64
	MaxBytes      int64
	MaxWorkers    int
}

// QueryReadyBuildBackpressureError reports the deliberate zero-queue
// admission policy. Callers retry or schedule externally; this coordinator
// never grows an internal request queue.
type QueryReadyBuildBackpressureError struct {
	MaxWorkers int
}

func (e *QueryReadyBuildBackpressureError) Error() string {
	return fmt.Sprintf("collections: query-ready build workers saturated (max=%d, queue=0)", e.MaxWorkers)
}

func (e *QueryReadyBuildBoundError) Error() string {
	if e == nil {
		return "collections: query-ready build bound exceeded"
	}
	return fmt.Sprintf("collections: query-ready build requires %d in-flight bytes; per-worker bound is %d with %d workers", e.RequiredBytes, e.MaxBytes, e.MaxWorkers)
}

type queryReadyBuildCoordinatorStats struct {
	ActiveWorkers       int
	PeakWorkers         int
	InFlightBytes       int64
	PeakInFlight        int64
	BoundRejections     int64
	AdmissionRejections int64
	MaxQueueDepth       int
}

type queryReadyBuildCoordinator struct {
	collection     *Collection
	limits         queryReadyBuildLimits
	slots          chan struct{}
	beforeRegister func()
	afterAdmission func()

	mu         sync.Mutex
	statsValue queryReadyBuildCoordinatorStats
}

func newQueryReadyBuildCoordinator(collection *Collection, limits queryReadyBuildLimits) (*queryReadyBuildCoordinator, error) {
	if collection == nil || collection.db == nil {
		return nil, errors.New("collections: query-ready build requires an open collection")
	}
	if limits.MaxWorkers <= 0 {
		return nil, errors.New("collections: query-ready build max workers must be positive")
	}
	if limits.MaxInFlightBytes <= 0 || limits.MaxInFlightBytes < int64(limits.MaxWorkers) {
		return nil, errors.New("collections: query-ready build max in-flight bytes must cover every worker")
	}
	return &queryReadyBuildCoordinator{collection: collection, limits: limits, slots: make(chan struct{}, limits.MaxWorkers)}, nil
}

func (c *queryReadyBuildCoordinator) stats() queryReadyBuildCoordinatorStats {
	if c == nil {
		return queryReadyBuildCoordinatorStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statsValue
}

type queryReadyBuildStats struct {
	Kind                       queryReadyBuildKind
	Rows                       int64
	InputBytes                 int64
	OutputBytes                int64
	BytesCopied                int64
	BytesCompressed            int64
	BytesHashed                int64
	BytesChecksummed           int64
	ExecutionBytes             int64
	ExecutionColumns           int
	BaseBytesRewritten         int64
	AssetsProduced             int
	PartsProduced              int
	WriteAmplification         float64
	ReservedInFlightBytes      int64
	EncodedBufferPeakBytes     int64
	EstimatedPeakInFlightBytes int64
	WorkerLimit                int
	QueueCapacity              int
	QueueWaitTime              time.Duration
	BaseBuildTime              time.Duration
	DeltaBuildTime             time.Duration
	ConsolidationTime          time.Duration
	AssetPreparationTime       time.Duration
	ManagerRegistrationTime    time.Duration
	HandoffTime                time.Duration
	TotalTime                  time.Duration
}

type queryReadyPreparedGeneration struct {
	Asset        ColumnPreparedAsset
	Identity     typedcolumn.QueryReadyBaseIdentity
	AssetPath    string
	Dependencies []typedcolumn.QueryReadyBaseDependency
	Stats        queryReadyBuildStats
	lease        *ColumnAssetLifecycleRegistryLease
	rootDir      string

	mu      sync.Mutex
	aborted bool
}

// OpenFileDescriptor returns M3's exact immutable byte-range descriptor. It
// translates the existing asset-manager kind once at the M5/M3 handoff, so
// callers do not rediscover a current lane by pathname. The asset-manager
// checksum remains available on Asset.Ref and binds this same selected range.
func (p *queryReadyPreparedGeneration) OpenFileDescriptor() (typedcolumn.QueryReadyGenerationFile, error) {
	if p == nil {
		return typedcolumn.QueryReadyGenerationFile{}, errors.New("collections: nil query-ready prepared generation")
	}
	var kind typedcolumn.QueryReadyGenerationKind
	switch p.Asset.Ref.Kind {
	case ColumnAssetKindQueryReadyBase:
		kind = typedcolumn.QueryReadyGenerationBase
	case ColumnAssetKindQueryReadyDelta:
		kind = typedcolumn.QueryReadyGenerationDelta
	case ColumnAssetKindQueryReadyConsolidatedBase:
		kind = typedcolumn.QueryReadyGenerationConsolidatedBase
	default:
		return typedcolumn.QueryReadyGenerationFile{}, fmt.Errorf("collections: unsupported query-ready asset kind %q", p.Asset.Ref.Kind)
	}
	return typedcolumn.QueryReadyGenerationFile{
		Path: p.AssetPath, Offset: p.Asset.Ref.Offset, Length: p.Asset.Ref.Length,
		Kind: kind, Identity: p.Identity,
	}, nil
}

// Abort releases the prepared-asset registration and reclaims the just-written
// tail when it is still safe. It never deletes a published or interleaved
// asset; those remain with the existing reachability/GC owners.
func (p *queryReadyPreparedGeneration) Abort() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.aborted {
		p.mu.Unlock()
		return nil
	}
	p.aborted = true
	lease := p.lease
	p.lease = nil
	asset := p.Asset
	rootDir := p.rootDir
	p.mu.Unlock()
	var releaseErr error
	if lease != nil {
		releaseErr = lease.Close()
	}
	return errors.Join(releaseErr, cleanupColumnPreparedAssets(rootDir, []ColumnPreparedAsset{asset}))
}

func (c *queryReadyBuildCoordinator) prepare(ctx context.Context, request queryReadyBuildRequest) (_ *queryReadyPreparedGeneration, retErr error) {
	started := time.Now()
	if c == nil || c.collection == nil || c.collection.db == nil {
		return nil, errors.New("collections: nil query-ready build coordinator")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	required, err := estimateQueryReadyBuildWorkingBytes(request)
	if err != nil {
		return nil, err
	}
	if request.Kind == queryReadyBuildConsolidatedBase {
		base, _, err := queryReadyConsolidationBase(request)
		if err != nil {
			return nil, err
		}
		expected := typedcolumn.QueryReadyBaseIdentity{Generation: request.ThroughGeneration, SchemaHash: base.Identity.SchemaHash}
		if request.Identity != expected {
			return nil, fmt.Errorf("collections: query-ready consolidation identity=%+v want %+v", request.Identity, expected)
		}
	}
	perWorker := c.limits.MaxInFlightBytes / int64(c.limits.MaxWorkers)
	if required > perWorker {
		c.mu.Lock()
		c.statsValue.BoundRejections++
		c.mu.Unlock()
		return nil, &QueryReadyBuildBoundError{RequiredBytes: required, MaxBytes: perWorker, MaxWorkers: c.limits.MaxWorkers}
	}
	select {
	case c.slots <- struct{}{}:
	default:
		c.mu.Lock()
		c.statsValue.AdmissionRejections++
		c.mu.Unlock()
		return nil, &QueryReadyBuildBackpressureError{MaxWorkers: c.limits.MaxWorkers}
	}
	queueWait := time.Duration(0)
	c.mu.Lock()
	c.statsValue.ActiveWorkers++
	c.statsValue.InFlightBytes += required
	if c.statsValue.ActiveWorkers > c.statsValue.PeakWorkers {
		c.statsValue.PeakWorkers = c.statsValue.ActiveWorkers
	}
	if c.statsValue.InFlightBytes > c.statsValue.PeakInFlight {
		c.statsValue.PeakInFlight = c.statsValue.InFlightBytes
	}
	c.mu.Unlock()
	if c.afterAdmission != nil {
		c.afterAdmission()
	}
	defer func() {
		c.mu.Lock()
		c.statsValue.ActiveWorkers--
		c.statsValue.InFlightBytes -= required
		c.mu.Unlock()
		<-c.slots
	}()

	stats := queryReadyBuildStats{
		Kind: request.Kind, ReservedInFlightBytes: required, EstimatedPeakInFlightBytes: required, WorkerLimit: c.limits.MaxWorkers,
		QueueCapacity: 0, QueueWaitTime: queueWait,
	}
	var payload []byte
	var dependencies []typedcolumn.QueryReadyBaseDependency
	var kind ColumnAssetKind
	var partID uint64
	switch request.Kind {
	case queryReadyBuildBase:
		var built typedcolumn.QueryReadyBaseBuildResult
		var err error
		if request.StreamingBasePlan != nil {
			built, err = request.StreamingBasePlan.Emit(request.StreamingBaseLoad)
		} else {
			built, err = typedcolumn.BuildQueryReadyBaseGeneration(request.Identity, request.Parts)
		}
		if err != nil {
			return nil, err
		}
		payload, dependencies = built.Bytes, built.Dependencies
		kind, partID = ColumnAssetKindQueryReadyBase, queryReadyBaseAssetPartID
		stats.Rows, stats.InputBytes, stats.OutputBytes = built.Stats.Rows, built.Stats.InputBytes, built.Stats.OutputBytes
		stats.BytesCopied, stats.BytesHashed, stats.BytesChecksummed = built.Stats.BytesCopied, built.Stats.BytesHashed, built.Stats.BytesChecksummed
		stats.ExecutionBytes, stats.ExecutionColumns = built.Stats.ExecutionBytes, built.Stats.ExecutionColumns
		stats.EncodedBufferPeakBytes, stats.BaseBuildTime = built.Stats.OutputBytes, built.Stats.BuildTime
	case queryReadyBuildDelta:
		built, err := typedcolumn.BuildQueryReadyDeltaGeneration(request.Identity, request.Parts, request.Tombstones)
		if err != nil {
			return nil, err
		}
		payload, dependencies = built.Bytes, built.Dependencies
		kind, partID = ColumnAssetKindQueryReadyDelta, queryReadyDeltaAssetPartID
		stats.Rows, stats.InputBytes, stats.OutputBytes = built.Stats.Rows, built.Stats.InputBytes, built.Stats.OutputBytes
		stats.BytesCopied, stats.BytesCompressed, stats.BytesHashed, stats.BytesChecksummed = built.Stats.BytesCopied, built.Stats.BytesCompressed, built.Stats.BytesHashed, built.Stats.BytesChecksummed
		stats.ExecutionBytes, stats.ExecutionColumns = built.Stats.ExecutionBytes, built.Stats.ExecutionColumns
		stats.EncodedBufferPeakBytes, stats.WriteAmplification = built.Stats.PeakEncodedBufferBytes, built.Stats.WriteAmplification
		stats.BaseBuildTime, stats.DeltaBuildTime = built.Stats.BaseBuildTime, built.Stats.BuildTime
	case queryReadyBuildConsolidatedBase:
		policy := request.Bound
		if policy == (typedcolumn.QueryReadyDeltaBoundPolicy{}) {
			policy = typedcolumn.DefaultQueryReadyDeltaBoundPolicy()
		}
		var built typedcolumn.QueryReadyConsolidationResult
		var err error
		if request.ConsolidatedBase != nil {
			built, err = typedcolumn.ConsolidateQueryReadyConsolidatedBaseDeltaWithPolicy(request.ConsolidatedBase, request.Deltas, request.ThroughGeneration, policy)
		} else {
			built, err = typedcolumn.ConsolidateQueryReadyBaseDeltaWithPolicy(request.Base, request.Deltas, request.ThroughGeneration, policy)
		}
		if err != nil {
			return nil, err
		}
		payload, dependencies = built.Bytes, built.Dependencies
		kind, partID = ColumnAssetKindQueryReadyConsolidatedBase, queryReadyConsolidatedBaseAssetPartID
		stats.Rows, stats.InputBytes, stats.OutputBytes = built.Stats.RowsMerged, built.Stats.InputBytes, built.Stats.OutputBytes
		stats.BytesCopied, stats.BytesHashed, stats.BytesChecksummed, stats.EncodedBufferPeakBytes = built.Stats.BytesCopied, built.Stats.BytesHashed, built.Stats.BytesChecksummed, built.Stats.PeakEncodedBufferBytes
		stats.ExecutionBytes, stats.ExecutionColumns = built.Stats.ExecutionBytes, built.Stats.ExecutionColumns
		stats.WriteAmplification, stats.ConsolidationTime = built.Stats.WriteAmplification, built.Stats.BuildTime
	default:
		return nil, fmt.Errorf("collections: unsupported query-ready build kind %d", request.Kind)
	}
	if stats.EncodedBufferPeakBytes > required {
		return nil, fmt.Errorf("collections: query-ready encoded buffer used %d bytes above reserved bound %d", stats.EncodedBufferPeakBytes, required)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	meta := c.collection.Meta()
	if meta.Options.ColumnStore == nil || meta.Options.ColumnStore.AssetManager == nil {
		return nil, errors.New("collections: query-ready build requires existing column asset manager")
	}
	prepareStarted := time.Now()
	ref, err := writeColumnAssetToManager(c.collection.db.ColumnAssetRootDir(), *meta.Options.ColumnStore, payload, kind, request.Identity.Generation, partID)
	stats.AssetPreparationTime = time.Since(prepareStarted)
	if err != nil {
		return nil, err
	}
	asset := ColumnPreparedAsset{Ref: ref, Rows: queryReadyRowsAsInt(stats.Rows), Bytes: int64(len(payload)), GenerationID: request.Identity.Generation, Reason: "query_ready_build"}
	assetPath, err := columnAssetSegmentPath(c.collection.db.ColumnAssetRootDir(), ref)
	if err != nil {
		return nil, err
	}
	stats.BytesChecksummed += int64(len(payload))
	cleanup := true
	defer func() {
		if retErr != nil && cleanup {
			retErr = errors.Join(retErr, cleanupColumnPreparedAssets(c.collection.db.ColumnAssetRootDir(), []ColumnPreparedAsset{asset}))
		}
	}()
	if c.beforeRegister != nil {
		c.beforeRegister()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	registerStarted := time.Now()
	lease, err := c.collection.RegisterColumnAssetPreparedAssets(ColumnAssetPreparedAssetRegistrationOptions{
		Owner: fmt.Sprintf("query-ready-generation-%d", request.Identity.Generation), Source: "query_ready_build", Reason: "rebuildable non-authoritative generation", Refs: []ColumnAssetRef{ref},
	})
	stats.ManagerRegistrationTime = time.Since(registerStarted)
	if err != nil {
		return nil, err
	}
	stats.AssetsProduced, stats.PartsProduced = 1, len(dependencies)
	stats.HandoffTime = stats.AssetPreparationTime + stats.ManagerRegistrationTime
	stats.TotalTime = time.Since(started)
	cleanup = false
	return &queryReadyPreparedGeneration{Asset: asset, Identity: request.Identity, AssetPath: assetPath, Dependencies: append([]typedcolumn.QueryReadyBaseDependency(nil), dependencies...), Stats: stats, lease: lease, rootDir: c.collection.db.ColumnAssetRootDir()}, nil
}

func estimateQueryReadyBuildWorkingBytes(request queryReadyBuildRequest) (int64, error) {
	if request.Kind == queryReadyBuildBase && request.StreamingBasePlan != nil {
		return request.StreamingBasePlan.EstimatedPeakBytes()
	}
	// These estimates intentionally cover allocations created after admission,
	// not the immutable input images already owned by the caller. In addition to
	// the encoded output they reserve space for validation/build plans, stable
	// dependencies, consolidation input views, selection metadata, and
	// tombstone normalization maps/slices. They are conservative rather than a
	// claim about exact Go heap use.
	const (
		fixedBuildBytes       = int64(64 << 10)
		perPartMetadataBytes  = int64(1024)
		perDeltaMetadataBytes = int64(256)
		perTombstoneBytes     = int64(256)
		perPartAlignmentBytes = int64(64)
	)
	add := func(total, value int64) (int64, error) {
		if value < 0 || total > math.MaxInt64-value {
			return 0, errors.New("collections: query-ready build size overflow")
		}
		return total + value, nil
	}
	total := fixedBuildBytes
	addPart := func(image typedcolumn.ColumnPartImage) error {
		executionBytes, err := typedcolumn.EstimateQueryReadyExecutionImageUpperBound(image)
		if err != nil {
			return err
		}
		// The build plan retains each generated sidecar while the final
		// generation buffer is allocated and receives a second copy. Charging
		// two conservative sidecar bounds covers both live representations.
		if executionBytes > math.MaxInt64/2 {
			return errors.New("collections: query-ready execution build size overflow")
		}
		partBytes, err := add(int64(len(image.Bytes)), executionBytes*2)
		if err != nil {
			return err
		}
		partBytes, err = add(partBytes, perPartAlignmentBytes+perPartMetadataBytes)
		if err != nil {
			return err
		}
		total, err = add(total, partBytes)
		return err
	}
	tombstones := int64(len(request.Tombstones))
	if request.Kind == queryReadyBuildConsolidatedBase {
		base, inheritedTombstones, err := queryReadyConsolidationBase(request)
		if err != nil {
			return 0, err
		}
		tombstones = int64(len(inheritedTombstones))
		for _, view := range base.Parts {
			if err := addPart(view.Image); err != nil {
				return 0, err
			}
		}
		for _, delta := range request.Deltas {
			if delta == nil || delta.Base == nil {
				return 0, errors.New("collections: query-ready consolidation has nil delta")
			}
			// Consolidation validates the full inventory but only builds the
			// prefix visible through ThroughGeneration. Future payloads must not
			// consume admission capacity for an older selected prefix.
			if delta.Identity.Generation > request.ThroughGeneration {
				continue
			}
			var err error
			total, err = add(total, perDeltaMetadataBytes)
			if err != nil {
				return 0, err
			}
			for _, view := range delta.Base.Parts {
				if err := addPart(view.Image); err != nil {
					return 0, err
				}
			}
			if int64(len(delta.Tombstones)) > math.MaxInt64-tombstones {
				return 0, errors.New("collections: query-ready build tombstone count overflow")
			}
			tombstones += int64(len(delta.Tombstones))
		}
	} else {
		for _, part := range request.Parts {
			if err := addPart(part.Image); err != nil {
				return 0, err
			}
		}
	}
	if request.Kind != queryReadyBuildBase {
		if tombstones > math.MaxInt64/perTombstoneBytes {
			return 0, errors.New("collections: query-ready build tombstone size overflow")
		}
		var err error
		total, err = add(total, fixedBuildBytes+tombstones*perTombstoneBytes)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func queryReadyConsolidationBase(request queryReadyBuildRequest) (*typedcolumn.QueryReadyBaseGeneration, []typedcolumn.Tombstone, error) {
	if request.Base != nil && request.ConsolidatedBase != nil {
		return nil, nil, errors.New("collections: query-ready consolidation requires exactly one base kind")
	}
	if request.ConsolidatedBase != nil {
		if request.ConsolidatedBase.Kind != typedcolumn.QueryReadyGenerationConsolidatedBase || request.ConsolidatedBase.Base == nil {
			return nil, nil, errors.New("collections: query-ready consolidation has invalid consolidated base")
		}
		if len(request.Tombstones) != 0 {
			return nil, nil, errors.New("collections: query-ready consolidation tombstones must come from consolidated base")
		}
		return request.ConsolidatedBase.Base, request.ConsolidatedBase.Tombstones, nil
	}
	if request.Base == nil {
		return nil, nil, errors.New("collections: query-ready consolidation requires base")
	}
	if len(request.Tombstones) != 0 {
		return nil, nil, errors.New("collections: inherited tombstones require consolidated base")
	}
	return request.Base, nil, nil
}

func queryReadyRowsAsInt(rows int64) int {
	if rows <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if rows > int64(maxInt) {
		return maxInt
	}
	return int(rows)
}
