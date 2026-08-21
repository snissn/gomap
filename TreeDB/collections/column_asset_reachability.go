package collections

import (
	"context"
	"errors"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ColumnAssetReachabilityOptions controls protect-only reachability planning.
// CandidateRefs are possible reclamation inputs supplied by the typed column
// asset manager/catalog index; pending, prepared, and pinned refs are always
// retained. Detailed emits per-ref and per-segment entries. SegmentDetails
// emits only per-segment entries and is implied by Detailed.
type ColumnAssetReachabilityOptions struct {
	Detailed       bool
	SegmentDetails bool
	// ProtectCandidateRefsForOlderSnapshots conservatively treats candidate
	// refs as pinned while any active TreeDB snapshot predates the planning
	// snapshot. Destructive GC enables this; non-destructive rewrite leaves it
	// off so it can remap current manifest refs while retaining old segments.
	ProtectCandidateRefsForOlderSnapshots bool
	CandidateRefs                         []ColumnAssetRef
	PendingRefs                           []ColumnAssetRef
	PreparedRefs                          []ColumnAssetRef
	PreparedQueryRefs                     []ColumnAssetRef
	QuarantineRefs                        []ColumnAssetRef
	QuarantineSegments                    []ColumnAssetQuarantineSegment
	PinnedRefs                            []ColumnAssetRef
	releaseVectorPartitionReclaimIDs      map[string]struct{}
}

type columnAssetReachabilityOptionsInternal struct {
	ColumnAssetReachabilityOptions
	omitDetailedEntrySources bool
	omitDetailedEntrySort    bool
}

type ColumnAssetReachabilityStatus string

const (
	ColumnAssetReachabilityProtected   ColumnAssetReachabilityStatus = "protected"
	ColumnAssetReachabilityReclaimable ColumnAssetReachabilityStatus = "reclaimable"
	ColumnAssetReachabilityUncertain   ColumnAssetReachabilityStatus = "uncertain"
)

type ColumnAssetReachabilitySegmentStatus string

const (
	ColumnAssetReachabilitySegmentProtected   ColumnAssetReachabilitySegmentStatus = "protected"
	ColumnAssetReachabilitySegmentReclaimable ColumnAssetReachabilitySegmentStatus = "reclaimable"
	ColumnAssetReachabilitySegmentMixed       ColumnAssetReachabilitySegmentStatus = "mixed"
	ColumnAssetReachabilitySegmentUnknown     ColumnAssetReachabilitySegmentStatus = "unknown"
	ColumnAssetReachabilitySegmentMissing     ColumnAssetReachabilitySegmentStatus = "missing"
)

type ColumnAssetReachabilitySource string

const (
	ColumnAssetReachabilitySourceActiveManifest    ColumnAssetReachabilitySource = "active_manifest"
	ColumnAssetReachabilitySourceRecoveryManifest  ColumnAssetReachabilitySource = "recovery_manifest"
	ColumnAssetReachabilitySourceCandidate         ColumnAssetReachabilitySource = "candidate"
	ColumnAssetReachabilitySourcePinnedSnapshot    ColumnAssetReachabilitySource = "pinned_snapshot"
	ColumnAssetReachabilitySourcePendingPublish    ColumnAssetReachabilitySource = "pending_publish"
	ColumnAssetReachabilitySourcePreparedAsset     ColumnAssetReachabilitySource = "prepared_asset"
	ColumnAssetReachabilitySourcePreparedQuery     ColumnAssetReachabilitySource = "prepared_query"
	ColumnAssetReachabilitySourceQuarantine        ColumnAssetReachabilitySource = "quarantine"
	ColumnAssetReachabilitySourceMappedResourcePin ColumnAssetReachabilitySource = "mappedresource_pin"
	columnAssetReachabilitySourceUnknown           ColumnAssetReachabilitySource = "unknown"
)

type ColumnAssetReachabilityPlan struct {
	ProtectOnly                bool
	Complete                   bool
	Collection                 string
	Namespace                  string
	ManifestRootName           string
	ManifestRootID             uint64
	SystemRoot                 uint64
	PlanCommitSeq              uint64
	ActiveManifestGeneration   uint64
	ActiveManifestChecksum     uint64
	RecoveryManifestGeneration uint64
	RecoveryManifestChecksum   uint64
	ManifestCatalogBytes       int64
	Sources                    ColumnAssetReachabilitySourceStats
	Refs                       ColumnAssetReachabilityRefStats
	Segments                   ColumnAssetReachabilitySegmentStats
	MappedResources            ColumnAssetReachabilityMappedResourceStats
	RewriteDebtBytes           int64
	Entries                    []ColumnAssetReachabilityRefEntry
	SegmentEntries             []ColumnAssetReachabilitySegmentEntry
}

// ColumnAssetReachabilitySourceStats counts unique ref contributions by
// logical reachability source. M15A only plans from an active
// recovery-authoritative manifest view, so active and recovery manifest refs
// are intentionally the same logical liveness set unless a future milestone
// introduces distinct roots.
type ColumnAssetReachabilitySourceStats struct {
	ManifestRoots            int
	ManifestRecords          int
	ActiveManifestRefs       int
	RecoveryManifestRefs     int
	CandidateRefs            int
	PendingRefs              int
	PreparedRefs             int
	PreparedQueryRefs        int
	QuarantineRefs           int
	QuarantineSegmentRecords int
	PinnedRefs               int
	MappedResourcePins       int
	ActiveManifestBytes      int64
	RecoveryManifestBytes    int64
	CandidateBytes           int64
	PendingBytes             int64
	PreparedBytes            int64
	PreparedQueryBytes       int64
	QuarantineBytes          int64
	QuarantineSegmentBytes   int64
	PinnedBytes              int64
	MappedResourcePinBytes   int64
}

type ColumnAssetReachabilityRefStats struct {
	Total            int
	Protected        int
	Reclaimable      int
	Uncertain        int
	BytesTotal       int64
	BytesProtected   int64
	BytesReclaimable int64
	BytesUncertain   int64
}

type ColumnAssetReachabilitySegmentStats struct {
	Total                       int
	Protected                   int
	Reclaimable                 int
	Mixed                       int
	Unknown                     int
	Missing                     int
	OutOfBoundsRefs             int
	QuarantineSegments          int
	QuarantineSegmentMismatches int
	BytesTotal                  int64
	BytesProtected              int64
	BytesReclaimable            int64
	BytesWholeReclaimable       int64
	BytesUnknown                int64
	BytesQuarantined            int64
}

// ColumnAssetReachabilityMappedResourceStats summarizes active #1736
// mappedresource handles observed while building a maintenance plan. The byte
// counters are active process-local handle bytes, not heap allocations by the
// planner itself.
type ColumnAssetReachabilityMappedResourceStats struct {
	ActiveHandles              int
	ActiveMappedBytes          int64
	ActiveHeapCopyBytes        int64
	ActiveDerivedMetadataBytes int64
	PinnedRefs                 int
	PinnedBytes                int64
	UnconvertiblePins          int
	DeniedResources            uint64
	FallbackReads              uint64
}

type ColumnAssetReachabilityRefEntry struct {
	Ref     ColumnAssetRef
	Status  ColumnAssetReachabilityStatus
	Sources []ColumnAssetReachabilitySource
}

type ColumnAssetReachabilitySegmentEntry struct {
	Namespace             string
	FileID                uint32
	Path                  string
	Bytes                 int64
	Status                ColumnAssetReachabilitySegmentStatus
	ProtectedBytes        int64
	ReclaimableBytes      int64
	UnknownBytes          int64
	RefCount              int
	plannedParentIdentity rootpublication.StableIdentity
	plannedChildIdentity  rootpublication.StableIdentity
}

type columnAssetReachabilityRefBuilder struct {
	ref        ColumnAssetRef
	sourceMask columnAssetReachabilitySourceMask
}

type columnAssetReachabilitySourceMask uint64

const (
	columnAssetReachabilitySourceActiveManifestMask columnAssetReachabilitySourceMask = 1 << iota
	columnAssetReachabilitySourceRecoveryManifestMask
	columnAssetReachabilitySourceCandidateMask
	columnAssetReachabilitySourcePinnedSnapshotMask
	columnAssetReachabilitySourcePendingPublishMask
	columnAssetReachabilitySourcePreparedAssetMask
	columnAssetReachabilitySourcePreparedQueryMask
	columnAssetReachabilitySourceQuarantineMask
	columnAssetReachabilitySourceMappedResourcePinMask
	columnAssetReachabilitySourceUnknownMask
)

const columnAssetReachabilityProtectedSourceMask = columnAssetReachabilitySourceActiveManifestMask |
	columnAssetReachabilitySourceRecoveryManifestMask |
	columnAssetReachabilitySourcePinnedSnapshotMask |
	columnAssetReachabilitySourcePendingPublishMask |
	columnAssetReachabilitySourcePreparedAssetMask |
	columnAssetReachabilitySourcePreparedQueryMask |
	columnAssetReachabilitySourceQuarantineMask |
	columnAssetReachabilitySourceMappedResourcePinMask

var columnAssetReachabilitySourceBits = [...]struct {
	source ColumnAssetReachabilitySource
	mask   columnAssetReachabilitySourceMask
}{
	{ColumnAssetReachabilitySourceActiveManifest, columnAssetReachabilitySourceActiveManifestMask},
	{ColumnAssetReachabilitySourceRecoveryManifest, columnAssetReachabilitySourceRecoveryManifestMask},
	{ColumnAssetReachabilitySourceCandidate, columnAssetReachabilitySourceCandidateMask},
	{ColumnAssetReachabilitySourcePinnedSnapshot, columnAssetReachabilitySourcePinnedSnapshotMask},
	{ColumnAssetReachabilitySourcePendingPublish, columnAssetReachabilitySourcePendingPublishMask},
	{ColumnAssetReachabilitySourcePreparedAsset, columnAssetReachabilitySourcePreparedAssetMask},
	{ColumnAssetReachabilitySourcePreparedQuery, columnAssetReachabilitySourcePreparedQueryMask},
	{ColumnAssetReachabilitySourceQuarantine, columnAssetReachabilitySourceQuarantineMask},
	{ColumnAssetReachabilitySourceMappedResourcePin, columnAssetReachabilitySourceMappedResourcePinMask},
	{columnAssetReachabilitySourceUnknown, columnAssetReachabilitySourceUnknownMask},
}

var columnAssetReachabilitySourceMaskBySource = func() map[ColumnAssetReachabilitySource]columnAssetReachabilitySourceMask {
	masks := make(map[ColumnAssetReachabilitySource]columnAssetReachabilitySourceMask, len(columnAssetReachabilitySourceBits))
	for _, entry := range columnAssetReachabilitySourceBits {
		masks[entry.source] = entry.mask
	}
	return masks
}()

type columnAssetReachabilityRange struct {
	start  int64
	end    int64
	status ColumnAssetReachabilityStatus
	kind   ColumnAssetKind
}

type columnAssetReachabilityRangeSet struct {
	first  columnAssetReachabilityRange
	ranges []columnAssetReachabilityRange
	count  int
}

func (set *columnAssetReachabilityRangeSet) appendRange(r columnAssetReachabilityRange) {
	if set.ranges != nil {
		set.ranges = append(set.ranges, r)
		set.count++
		return
	}
	switch set.count {
	case 0:
		set.first = r
		set.count = 1
	case 1:
		set.ranges = append([]columnAssetReachabilityRange{set.first}, r)
		set.count = 2
	}
}

type columnAssetReachabilityInterval struct {
	start int64
	end   int64
}

type columnAssetReachabilitySegment struct {
	fileID         uint32
	name           string
	path           string
	bytes          int64
	parentIdentity rootpublication.StableIdentity
	childIdentity  rootpublication.StableIdentity
}

const columnAssetReachabilityContextCheckInterval = 256

// PlanColumnAssetReachability builds the M15A dry-run/protect-only liveness
// plan for the collection's isolated column asset namespace. It never deletes,
// rewrites, or remaps assets; uncertain or untracked bytes are retained.
func (c *Collection) PlanColumnAssetReachability(ctx context.Context, opts ColumnAssetReachabilityOptions) (ColumnAssetReachabilityPlan, error) {
	var err error
	opts, err = c.columnAssetLifecycleAugmentReachabilityOptions(opts)
	if err != nil {
		return ColumnAssetReachabilityPlan{ProtectOnly: true}, err
	}
	plan, _, err := c.planColumnAssetReachability(ctx, columnAssetReachabilityOptionsInternal{
		ColumnAssetReachabilityOptions: opts,
	})
	return plan, err
}

func (c *Collection) planColumnAssetReachability(ctx context.Context, opts columnAssetReachabilityOptionsInternal) (ColumnAssetReachabilityPlan, map[ColumnAssetRef]columnAssetReachabilitySourceMask, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ColumnAssetReachabilityPlan{ProtectOnly: true}, nil, err
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithContext(ctx)
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		input := columnAssetReachabilityInputFromSnapshotView(view, opts)
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}

	input := columnAssetReachabilityInputFromSnapshotView(view, opts)
	for i, assetRef := range view.AssetRefs {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), input.refs, err
			}
		}
		// prepareColumnPhysicalScanSnapshotView already requires the active and
		// recovery-authoritative manifest identities to match. The same refs are
		// therefore reachable through both logical roots and must be protected as
		// both sources until M15B/M15C introduce destructive actions.
		if input.addRef(assetRef.Ref, ColumnAssetReachabilitySourceActiveManifest) {
			input.activeRefs++
			input.activeBytes = addColumnAssetReachabilityBytes(input.activeBytes, positiveColumnAssetReachabilityLength(assetRef.Ref.Length))
		}
		if input.addRef(assetRef.Ref, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
			input.recoveryBytes = addColumnAssetReachabilityBytes(input.recoveryBytes, positiveColumnAssetReachabilityLength(assetRef.Ref.Length))
		}
	}
	for i, typedPartRef := range view.TypedColumnPartRefs {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), input.refs, err
			}
		}
		if input.addRef(typedPartRef.Ref, ColumnAssetReachabilitySourceActiveManifest) {
			input.activeRefs++
			input.activeBytes = addColumnAssetReachabilityBytes(input.activeBytes, positiveColumnAssetReachabilityLength(typedPartRef.Ref.Length))
		}
		if input.addRef(typedPartRef.Ref, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
			input.recoveryBytes = addColumnAssetReachabilityBytes(input.recoveryBytes, positiveColumnAssetReachabilityLength(typedPartRef.Ref.Length))
		}
	}
	for i, metadataRef := range view.AggregateMetadata {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), input.refs, err
			}
		}
		if input.addRef(metadataRef.AssetRef, ColumnAssetReachabilitySourceActiveManifest) {
			input.activeRefs++
			input.activeBytes = addColumnAssetReachabilityBytes(input.activeBytes, positiveColumnAssetReachabilityLength(metadataRef.AssetRef.Length))
		}
		if input.addRef(metadataRef.AssetRef, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
			input.recoveryBytes = addColumnAssetReachabilityBytes(input.recoveryBytes, positiveColumnAssetReachabilityLength(metadataRef.AssetRef.Length))
		}
	}
	for i, dictionaryRef := range view.DictionaryCodes {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), input.refs, err
			}
		}
		if input.addRef(dictionaryRef.AssetRef, ColumnAssetReachabilitySourceActiveManifest) {
			input.activeRefs++
			input.activeBytes = addColumnAssetReachabilityBytes(input.activeBytes, positiveColumnAssetReachabilityLength(dictionaryRef.AssetRef.Length))
		}
		if input.addRef(dictionaryRef.AssetRef, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
			input.recoveryBytes = addColumnAssetReachabilityBytes(input.recoveryBytes, positiveColumnAssetReachabilityLength(dictionaryRef.AssetRef.Length))
		}
	}
	for i, valuesRef := range view.Int64Values {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), input.refs, err
			}
		}
		if input.addRef(valuesRef.AssetRef, ColumnAssetReachabilitySourceActiveManifest) {
			input.activeRefs++
			input.activeBytes = addColumnAssetReachabilityBytes(input.activeBytes, positiveColumnAssetReachabilityLength(valuesRef.AssetRef.Length))
		}
		if input.addRef(valuesRef.AssetRef, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
			input.recoveryBytes = addColumnAssetReachabilityBytes(input.recoveryBytes, positiveColumnAssetReachabilityLength(valuesRef.AssetRef.Length))
		}
	}
	for i, ref := range view.GraphAssetRefs {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), input.refs, err
			}
		}
		if input.addRef(ref, ColumnAssetReachabilitySourceActiveManifest) {
			input.activeRefs++
			input.activeBytes = addColumnAssetReachabilityBytes(input.activeBytes, positiveColumnAssetReachabilityLength(ref.Length))
		}
		if input.addRef(ref, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
			input.recoveryBytes = addColumnAssetReachabilityBytes(input.recoveryBytes, positiveColumnAssetReachabilityLength(ref.Length))
		}
	}
	if err := input.addRefs(ctx, opts.CandidateRefs, ColumnAssetReachabilitySourceCandidate); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if opts.ProtectCandidateRefsForOlderSnapshots && c.columnAssetReachabilityOlderSnapshotPinned(view.CommitSeq) {
		if err := input.addRefs(ctx, opts.CandidateRefs, ColumnAssetReachabilitySourcePinnedSnapshot); err != nil {
			return columnAssetReachabilityPlanIdentity(input), input.refs, err
		}
	}
	if err := input.addRefs(ctx, opts.PendingRefs, ColumnAssetReachabilitySourcePendingPublish); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if err := input.addRefs(ctx, opts.PreparedRefs, ColumnAssetReachabilitySourcePreparedAsset); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if err := input.addRefs(ctx, opts.PreparedQueryRefs, ColumnAssetReachabilitySourcePreparedQuery); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if err := input.addRefs(ctx, opts.QuarantineRefs, ColumnAssetReachabilitySourceQuarantine); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if err := input.addQuarantineSegments(ctx, opts.QuarantineSegments); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	activePinnedRefs, mappedResourceStats := columnAssetReachabilityMappedResourcePins(input.rootDir, input.namespace)
	input.mappedResources = mappedResourceStats
	if mappedResourceStats.UnconvertiblePins != 0 {
		input.pinStateIncomplete = true
	}
	if err := input.addRefs(ctx, activePinnedRefs, ColumnAssetReachabilitySourceMappedResourcePin); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if err := input.addRefs(ctx, opts.PinnedRefs, ColumnAssetReachabilitySourcePinnedSnapshot); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	if err := ctx.Err(); err != nil {
		return columnAssetReachabilityPlanIdentity(input), input.refs, err
	}
	plan, err := buildColumnAssetReachabilityPlan(ctx, input)
	return plan, input.refs, err
}

func (c *Collection) columnAssetReachabilityOlderSnapshotPinned(planCommitSeq uint64) bool {
	if c == nil || c.db == nil {
		return false
	}
	return c.db.MinPinnedSnapshotCommitSeq() < planCommitSeq
}

func columnAssetReachabilityInputFromSnapshotView(view columnPhysicalScanSnapshotView, opts columnAssetReachabilityOptionsInternal) columnAssetReachabilityInput {
	expectedRefs := len(view.AssetRefs) + len(view.TypedColumnPartRefs) + len(view.AggregateMetadata) + len(view.DictionaryCodes) + len(view.Int64Values) + len(view.GraphAssetRefs) + len(opts.CandidateRefs) + len(opts.PendingRefs) + len(opts.PreparedRefs) + len(opts.PreparedQueryRefs) + len(opts.QuarantineRefs) + len(opts.PinnedRefs)
	input := columnAssetReachabilityInput{
		rootDir:          view.ColumnAssetRootDir,
		collection:       view.CollectionName,
		namespace:        view.AssetNamespace,
		manifestRootName: view.Diagnostics.ManifestRootName,
		manifestRootID:   view.Diagnostics.ManifestRoot,
		systemRoot:       view.SystemRoot,
		planCommitSeq:    view.CommitSeq,
		activeGen:        view.Diagnostics.ManifestGeneration,
		activeChecksum:   view.Diagnostics.ActiveManifestChecksum,
		recoveryGen:      view.Diagnostics.RecoveryManifestGeneration,
		recoveryChecksum: view.Diagnostics.RecoveryManifestChecksum,
		manifestRecs:     view.Diagnostics.ManifestRecords,
		manifestBytes:    view.ManifestCatalogBytes,
		detailed:         opts.Detailed,
		segmentDetails:   opts.Detailed || opts.SegmentDetails,
		omitSources:      opts.omitDetailedEntrySources,
		omitSort:         opts.omitDetailedEntrySort,
	}
	if expectedRefs > 0 {
		input.refs = make(map[ColumnAssetRef]columnAssetReachabilitySourceMask, expectedRefs)
	}
	return input
}

func columnAssetReachabilityMappedResourcePins(rootDir, namespace string) ([]ColumnAssetRef, ColumnAssetReachabilityMappedResourceStats) {
	globalStats := mappedresource.GlobalStats()
	stats := ColumnAssetReachabilityMappedResourceStats{
		DeniedResources: sumMappedResourceDenied(globalStats.DeniedByReason),
		FallbackReads:   globalStats.FallbackReads,
	}
	pins := mappedresource.GlobalPinSummary()
	if len(pins) == 0 {
		return nil, stats
	}
	refs := make([]ColumnAssetRef, 0, len(pins))
	for _, pin := range pins {
		if !columnAssetMappedResourcePinMatchesNamespace(pin, namespace) || !columnAssetMappedResourcePinMatchesRoot(pin, rootDir) {
			continue
		}
		stats.ActiveHandles++
		stats.PinnedBytes = addColumnAssetReachabilityBytes(stats.PinnedBytes, pin.Bytes)
		switch pin.Source {
		case mappedresource.SourceMapped:
			stats.ActiveMappedBytes = addColumnAssetReachabilityBytes(stats.ActiveMappedBytes, pin.Bytes)
		case mappedresource.SourceHeapCopy:
			stats.ActiveHeapCopyBytes = addColumnAssetReachabilityBytes(stats.ActiveHeapCopyBytes, pin.Bytes)
		case mappedresource.SourceDerivedMetadata:
			stats.ActiveDerivedMetadataBytes = addColumnAssetReachabilityBytes(stats.ActiveDerivedMetadataBytes, pin.Bytes)
		}
		ref, ok := columnAssetRefForMappedResourceKey(pin.Key)
		if !ok {
			stats.UnconvertiblePins++
			continue
		}
		if namespace != "" && ref.Namespace != namespace {
			stats.UnconvertiblePins++
			continue
		}
		refs = append(refs, ref)
		stats.PinnedRefs++
	}
	return refs, stats
}

func columnAssetMappedResourcePinMatchesNamespace(pin mappedresource.Pin, namespace string) bool {
	switch pin.Key.Class {
	case mappedresource.ClassTypedRowAsset, mappedresource.ClassTypedColumnAsset:
	default:
		return false
	}
	if namespace == "" {
		return true
	}
	return pin.Key.Namespace == namespace || pin.Scope.Namespace == namespace
}

func columnAssetMappedResourcePinMatchesRoot(pin mappedresource.Pin, rootDir string) bool {
	if rootDir == "" {
		return true
	}
	root := columnAssetPathForRootCompare(rootDir)
	canonicalRoot, canonicalRootOK := columnAssetCanonicalPathForRootCompare(rootDir)
	if pin.Root != "" {
		pinRoot := columnAssetPathForRootCompare(pin.Root)
		if pinRoot == root {
			return true
		}
		if canonicalRootOK {
			if canonicalPinRoot, ok := columnAssetCanonicalPathForRootCompare(pin.Root); ok && canonicalPinRoot == canonicalRoot {
				return true
			}
		}
	}
	if pin.Path == "" {
		return false
	}
	path := columnAssetPathForRootCompare(pin.Path)
	if columnAssetPathWithinRootForCompare(path, root) {
		return true
	}
	if canonicalRootOK {
		if canonicalPath, ok := columnAssetCanonicalPathForRootCompare(pin.Path); ok && columnAssetPathWithinRootForCompare(canonicalPath, canonicalRoot) {
			return true
		}
	}
	return false
}

func columnAssetPathForRootCompare(path string) string {
	if path == "" {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}

func columnAssetCanonicalPathForRootCompare(path string) (string, bool) {
	path = columnAssetPathForRootCompare(path)
	if path == "" {
		return "", false
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", false
	}
	return columnAssetPathForRootCompare(canonical), true
}

func columnAssetPathWithinRootForCompare(path, root string) bool {
	if path == "" || root == "" {
		return false
	}
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return false
	}
	return true
}

func sumMappedResourceDenied(in map[mappedresource.DenyReason]uint64) uint64 {
	var total uint64
	for _, count := range in {
		total += count
	}
	return total
}

type columnAssetReachabilityInput struct {
	rootDir            string
	collection         string
	namespace          string
	manifestRootName   string
	manifestRootID     uint64
	systemRoot         uint64
	planCommitSeq      uint64
	activeGen          uint64
	activeChecksum     uint64
	recoveryGen        uint64
	recoveryChecksum   uint64
	manifestRecs       int
	manifestBytes      int64
	activeRefs         int
	recoveryRefs       int
	activeBytes        int64
	recoveryBytes      int64
	detailed           bool
	segmentDetails     bool
	omitSources        bool
	omitSort           bool
	refs               map[ColumnAssetRef]columnAssetReachabilitySourceMask
	unknownSources     map[ColumnAssetRef][]ColumnAssetReachabilitySource
	sourceCounts       ColumnAssetReachabilitySourceStats
	mappedResources    ColumnAssetReachabilityMappedResourceStats
	quarantineSegments map[uint32]int64
	pinStateIncomplete bool
}

func (in *columnAssetReachabilityInput) addRefs(ctx context.Context, refs []ColumnAssetRef, source ColumnAssetReachabilitySource) error {
	for i, ref := range refs {
		if ctx != nil && i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if in.addRef(ref, source) {
			in.incrementSourceStats(source, ref)
		}
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

func (in *columnAssetReachabilityInput) incrementSourceStats(source ColumnAssetReachabilitySource, ref ColumnAssetRef) {
	refBytes := positiveColumnAssetReachabilityLength(ref.Length)
	switch source {
	case ColumnAssetReachabilitySourceCandidate:
		in.sourceCounts.CandidateRefs++
		in.sourceCounts.CandidateBytes = addColumnAssetReachabilityBytes(in.sourceCounts.CandidateBytes, refBytes)
	case ColumnAssetReachabilitySourcePendingPublish:
		in.sourceCounts.PendingRefs++
		in.sourceCounts.PendingBytes = addColumnAssetReachabilityBytes(in.sourceCounts.PendingBytes, refBytes)
	case ColumnAssetReachabilitySourcePreparedAsset:
		in.sourceCounts.PreparedRefs++
		in.sourceCounts.PreparedBytes = addColumnAssetReachabilityBytes(in.sourceCounts.PreparedBytes, refBytes)
	case ColumnAssetReachabilitySourcePreparedQuery:
		in.sourceCounts.PreparedQueryRefs++
		in.sourceCounts.PreparedQueryBytes = addColumnAssetReachabilityBytes(in.sourceCounts.PreparedQueryBytes, refBytes)
	case ColumnAssetReachabilitySourceQuarantine:
		in.sourceCounts.QuarantineRefs++
		in.sourceCounts.QuarantineBytes = addColumnAssetReachabilityBytes(in.sourceCounts.QuarantineBytes, refBytes)
	case ColumnAssetReachabilitySourcePinnedSnapshot:
		in.sourceCounts.PinnedRefs++
		in.sourceCounts.PinnedBytes = addColumnAssetReachabilityBytes(in.sourceCounts.PinnedBytes, refBytes)
	case ColumnAssetReachabilitySourceMappedResourcePin:
		in.sourceCounts.MappedResourcePins++
		in.sourceCounts.MappedResourcePinBytes = addColumnAssetReachabilityBytes(in.sourceCounts.MappedResourcePinBytes, refBytes)
	}
}

func (in *columnAssetReachabilityInput) addQuarantineSegments(ctx context.Context, segments []ColumnAssetQuarantineSegment) error {
	for i, segment := range segments {
		if ctx != nil && i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		normalized, bytes, err := normalizeColumnAssetQuarantineSegment(segment, in.namespace)
		if err != nil {
			return err
		}
		if in.quarantineSegments == nil {
			in.quarantineSegments = make(map[uint32]int64)
		}
		if bytes > in.quarantineSegments[normalized.FileID] {
			in.quarantineSegments[normalized.FileID] = bytes
		}
		in.sourceCounts.QuarantineSegmentRecords++
		in.sourceCounts.QuarantineSegmentBytes = addColumnAssetReachabilityBytes(in.sourceCounts.QuarantineSegmentBytes, bytes)
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

func (in *columnAssetReachabilityInput) addRef(ref ColumnAssetRef, source ColumnAssetReachabilitySource) bool {
	if in.refs == nil {
		in.refs = make(map[ColumnAssetRef]columnAssetReachabilitySourceMask)
	}
	sourceMask, ok := columnAssetReachabilitySourceBit(source)
	if !ok {
		if !in.detailed {
			mask := in.refs[ref]
			if mask&columnAssetReachabilitySourceUnknownMask != 0 {
				return false
			}
			in.refs[ref] = mask | columnAssetReachabilitySourceUnknownMask
			return true
		}
		if !in.addUnknownRefSource(ref, source) {
			return false
		}
		in.refs[ref] = in.refs[ref] | columnAssetReachabilitySourceUnknownMask
		return true
	}
	mask := in.refs[ref]
	if mask&sourceMask != 0 {
		return false
	}
	in.refs[ref] = mask | sourceMask
	return true
}

func (in *columnAssetReachabilityInput) addUnknownRefSource(ref ColumnAssetRef, source ColumnAssetReachabilitySource) bool {
	if in.unknownSources == nil {
		in.unknownSources = make(map[ColumnAssetRef][]ColumnAssetReachabilitySource)
	}
	sources := in.unknownSources[ref]
	for _, existing := range sources {
		if existing == source {
			return false
		}
	}
	in.unknownSources[ref] = append(sources, source)
	return true
}

func buildColumnAssetReachabilityPlan(ctx context.Context, input columnAssetReachabilityInput) (ColumnAssetReachabilityPlan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}
	plan := columnAssetReachabilityPlanWithStats(input)
	namespace, err := columnAssetManagerNamespaceForRoot(input.rootDir, input.namespace)
	if err != nil {
		plan.Complete = false
		return plan, err
	}
	segments, err := listColumnAssetReachabilitySegments(ctx, namespace.SegmentDir)
	if err != nil {
		plan.Complete = false
		return plan, err
	}

	var rangesByFile map[uint32]columnAssetReachabilityRangeSet
	if precountColumnAssetReachabilityRanges(len(input.refs), len(segments)) {
		rangeCountsCap := len(segments)
		if rangeCountsCap < 1 {
			rangeCountsCap = 1
		}
		if rangeCountsCap > len(input.refs) {
			rangeCountsCap = len(input.refs)
		}
		rangeCounts := make(map[uint32]int, rangeCountsCap)
		i := 0
		for ref := range input.refs {
			if i%columnAssetReachabilityContextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return columnAssetReachabilityPlanIdentity(input), err
				}
			}
			i++
			if columnAssetReachabilityRefCanContributeRange(ref, input.namespace) {
				rangeCounts[ref.FileID]++
			}
		}
		rangesByFile = make(map[uint32]columnAssetReachabilityRangeSet, len(rangeCounts))
		for fileID, count := range rangeCounts {
			if count > 1 {
				rangesByFile[fileID] = columnAssetReachabilityRangeSet{
					ranges: make([]columnAssetReachabilityRange, 0, count),
				}
			}
		}
	} else if len(input.refs) != 0 || len(input.quarantineSegments) != 0 {
		rangesByFile = make(map[uint32]columnAssetReachabilityRangeSet, len(input.refs)+len(input.quarantineSegments))
	}
	if input.detailed {
		plan.Entries = make([]ColumnAssetReachabilityRefEntry, 0, len(input.refs))
	}
	if input.detailed || input.segmentDetails {
		plan.SegmentEntries = make([]ColumnAssetReachabilitySegmentEntry, 0, len(segments)+len(rangesByFile))
	}
	processRef := func(ref ColumnAssetRef, sourceMask columnAssetReachabilitySourceMask) {
		status := columnAssetReachabilityStatusForSourceMask(sourceMask)
		canContributeRange := columnAssetReachabilityRefCanContributeRange(ref, input.namespace)
		if !canContributeRange {
			status = ColumnAssetReachabilityUncertain
		}
		plan.Refs.Total++
		refBytes := positiveColumnAssetReachabilityLength(ref.Length)
		plan.Refs.BytesTotal = addColumnAssetReachabilityBytes(plan.Refs.BytesTotal, refBytes)
		switch status {
		case ColumnAssetReachabilityProtected:
			plan.Refs.Protected++
			plan.Refs.BytesProtected = addColumnAssetReachabilityBytes(plan.Refs.BytesProtected, refBytes)
		case ColumnAssetReachabilityReclaimable:
			plan.Refs.Reclaimable++
			plan.Refs.BytesReclaimable = addColumnAssetReachabilityBytes(plan.Refs.BytesReclaimable, refBytes)
		default:
			plan.Refs.Uncertain++
			plan.Refs.BytesUncertain = addColumnAssetReachabilityBytes(plan.Refs.BytesUncertain, refBytes)
			plan.Complete = false
		}
		if input.detailed {
			entry := ColumnAssetReachabilityRefEntry{
				Ref:    ref,
				Status: status,
			}
			if !input.omitSources {
				entry.Sources = columnAssetReachabilitySourcesForMaskWithUnknown(sourceMask, input.unknownSources[ref])
			}
			plan.Entries = append(plan.Entries, entry)
		}
		if !canContributeRange {
			return
		}
		set := rangesByFile[ref.FileID]
		set.appendRange(columnAssetReachabilityRange{
			start:  ref.Offset,
			end:    ref.Offset + ref.Length,
			status: status,
			kind:   ref.Kind,
		})
		rangesByFile[ref.FileID] = set
	}
	if input.detailed && !input.omitSort {
		refBuilders := make([]columnAssetReachabilityRefBuilder, 0, len(input.refs))
		for ref, sourceMask := range input.refs {
			refBuilders = append(refBuilders, columnAssetReachabilityRefBuilder{ref: ref, sourceMask: sourceMask})
		}
		slices.SortFunc(refBuilders, func(a, b columnAssetReachabilityRefBuilder) int {
			return compareColumnAssetRefs(a.ref, b.ref)
		})
		for i, builder := range refBuilders {
			if i%columnAssetReachabilityContextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return columnAssetReachabilityPlanIdentity(input), err
				}
			}
			processRef(builder.ref, builder.sourceMask)
		}
	} else {
		i := 0
		for ref, sourceMask := range input.refs {
			if i%columnAssetReachabilityContextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return columnAssetReachabilityPlanIdentity(input), err
				}
			}
			i++
			processRef(ref, sourceMask)
		}
	}
	if err := ctx.Err(); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}

	seenQuarantineSegments := make(map[uint32]struct{}, len(input.quarantineSegments))
	for i, segment := range segments {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), err
			}
		}
		rangeSet := rangesByFile[segment.fileID]
		if quarantineBytes, ok := input.quarantineSegments[segment.fileID]; ok {
			seenQuarantineSegments[segment.fileID] = struct{}{}
			plan.Segments.QuarantineSegments++
			plan.Segments.BytesQuarantined = addColumnAssetReachabilityBytes(plan.Segments.BytesQuarantined, segment.bytes)
			if quarantineBytes > 0 && quarantineBytes != segment.bytes {
				plan.Segments.QuarantineSegmentMismatches++
				plan.Complete = false
			}
			if segment.bytes > 0 {
				rangeSet.appendRange(columnAssetReachabilityRange{
					start:  0,
					end:    segment.bytes,
					status: ColumnAssetReachabilityProtected,
				})
				rangesByFile[segment.fileID] = rangeSet
			}
		}
		delete(rangesByFile, segment.fileID)
		plan.Segments.Total++
		plan.Segments.BytesTotal = addColumnAssetReachabilityBytes(plan.Segments.BytesTotal, segment.bytes)
		segmentPlan := classifyColumnAssetReachabilitySegmentSet(segment, rangeSet)
		if segmentPlan.outOfBoundsRefs != 0 {
			plan.Segments.OutOfBoundsRefs += segmentPlan.outOfBoundsRefs
			plan.Complete = false
		}
		if segmentPlan.unknownBytes != 0 || segmentPlan.status == ColumnAssetReachabilitySegmentUnknown {
			plan.Complete = false
		}
		plan.Segments.BytesProtected = addColumnAssetReachabilityBytes(plan.Segments.BytesProtected, segmentPlan.protectedBytes)
		plan.Segments.BytesReclaimable = addColumnAssetReachabilityBytes(plan.Segments.BytesReclaimable, segmentPlan.reclaimableBytes)
		plan.Segments.BytesUnknown = addColumnAssetReachabilityBytes(plan.Segments.BytesUnknown, segmentPlan.unknownBytes)
		switch segmentPlan.status {
		case ColumnAssetReachabilitySegmentProtected:
			plan.Segments.Protected++
		case ColumnAssetReachabilitySegmentReclaimable:
			plan.Segments.Reclaimable++
			plan.Segments.BytesWholeReclaimable = addColumnAssetReachabilityBytes(plan.Segments.BytesWholeReclaimable, segment.bytes)
		case ColumnAssetReachabilitySegmentMixed:
			plan.Segments.Mixed++
			plan.RewriteDebtBytes = addColumnAssetReachabilityBytes(plan.RewriteDebtBytes, segmentPlan.reclaimableBytes)
		default:
			plan.Segments.Unknown++
			if segmentPlan.reclaimableBytes != 0 {
				plan.RewriteDebtBytes = addColumnAssetReachabilityBytes(plan.RewriteDebtBytes, segmentPlan.reclaimableBytes)
			}
		}
		if input.detailed || input.segmentDetails {
			plan.SegmentEntries = append(plan.SegmentEntries, ColumnAssetReachabilitySegmentEntry{
				Namespace:             input.namespace,
				FileID:                segment.fileID,
				Path:                  columnAssetReachabilitySegmentPath(namespace.SegmentDir, segment.name),
				Bytes:                 segment.bytes,
				Status:                segmentPlan.status,
				ProtectedBytes:        segmentPlan.protectedBytes,
				ReclaimableBytes:      segmentPlan.reclaimableBytes,
				UnknownBytes:          segmentPlan.unknownBytes,
				RefCount:              rangeSet.count,
				plannedParentIdentity: segment.parentIdentity,
				plannedChildIdentity:  segment.childIdentity,
			})
		}
	}
	for fileID, quarantineBytes := range input.quarantineSegments {
		if _, ok := seenQuarantineSegments[fileID]; ok {
			continue
		}
		rangeSet := rangesByFile[fileID]
		end := quarantineBytes
		if end <= 0 {
			end = 1
		}
		rangeSet.appendRange(columnAssetReachabilityRange{start: 0, end: end, status: ColumnAssetReachabilityProtected})
		rangesByFile[fileID] = rangeSet
	}
	var missingFileIDs []uint32
	for fileID := range rangesByFile {
		missingFileIDs = append(missingFileIDs, fileID)
	}
	slices.Sort(missingFileIDs)
	for i, fileID := range missingFileIDs {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), err
			}
		}
		ranges := rangesByFile[fileID]
		plan.Segments.Total++
		plan.Segments.Missing++
		plan.Complete = false
		if input.detailed || input.segmentDetails {
			plan.SegmentEntries = append(plan.SegmentEntries, ColumnAssetReachabilitySegmentEntry{
				Namespace: input.namespace,
				FileID:    fileID,
				Path:      columnAssetReachabilitySegmentPath(namespace.SegmentDir, columnAssetSegmentFileName(fileID)),
				Status:    ColumnAssetReachabilitySegmentMissing,
				RefCount:  ranges.count,
			})
		}
	}
	return plan, nil
}

// Pre-counting keeps dense multi-ref segments allocation-stable by pre-sizing
// range slices. Sparse one-ref-per-segment GC plans skip it to avoid a second
// large map and pass over the same refs.
func precountColumnAssetReachabilityRanges(refCount, segmentCount int) bool {
	if refCount == 0 {
		return false
	}
	if segmentCount == 0 {
		return false
	}
	refsPerSegment := refCount / segmentCount
	return refsPerSegment > 4 || (refsPerSegment == 4 && refCount%segmentCount != 0)
}

func columnAssetReachabilityPlanIdentity(input columnAssetReachabilityInput) ColumnAssetReachabilityPlan {
	return ColumnAssetReachabilityPlan{
		ProtectOnly:                true,
		Complete:                   false,
		Collection:                 input.collection,
		Namespace:                  input.namespace,
		ManifestRootName:           input.manifestRootName,
		ManifestRootID:             input.manifestRootID,
		SystemRoot:                 input.systemRoot,
		PlanCommitSeq:              input.planCommitSeq,
		ActiveManifestGeneration:   input.activeGen,
		ActiveManifestChecksum:     input.activeChecksum,
		RecoveryManifestGeneration: input.recoveryGen,
		RecoveryManifestChecksum:   input.recoveryChecksum,
		ManifestCatalogBytes:       input.manifestBytes,
		MappedResources:            input.mappedResources,
	}
}

func columnAssetReachabilityPlanWithStats(input columnAssetReachabilityInput) ColumnAssetReachabilityPlan {
	return ColumnAssetReachabilityPlan{
		ProtectOnly:                true,
		Complete:                   !input.pinStateIncomplete,
		Collection:                 input.collection,
		Namespace:                  input.namespace,
		ManifestRootName:           input.manifestRootName,
		ManifestRootID:             input.manifestRootID,
		SystemRoot:                 input.systemRoot,
		PlanCommitSeq:              input.planCommitSeq,
		ActiveManifestGeneration:   input.activeGen,
		ActiveManifestChecksum:     input.activeChecksum,
		RecoveryManifestGeneration: input.recoveryGen,
		RecoveryManifestChecksum:   input.recoveryChecksum,
		ManifestCatalogBytes:       input.manifestBytes,
		Sources: ColumnAssetReachabilitySourceStats{
			ManifestRoots:            1,
			ManifestRecords:          input.manifestRecs,
			ActiveManifestRefs:       input.activeRefs,
			RecoveryManifestRefs:     input.recoveryRefs,
			CandidateRefs:            input.sourceCounts.CandidateRefs,
			PendingRefs:              input.sourceCounts.PendingRefs,
			PreparedRefs:             input.sourceCounts.PreparedRefs,
			PreparedQueryRefs:        input.sourceCounts.PreparedQueryRefs,
			QuarantineRefs:           input.sourceCounts.QuarantineRefs,
			QuarantineSegmentRecords: input.sourceCounts.QuarantineSegmentRecords,
			PinnedRefs:               input.sourceCounts.PinnedRefs,
			MappedResourcePins:       input.sourceCounts.MappedResourcePins,
			ActiveManifestBytes:      input.activeBytes,
			RecoveryManifestBytes:    input.recoveryBytes,
			CandidateBytes:           input.sourceCounts.CandidateBytes,
			PendingBytes:             input.sourceCounts.PendingBytes,
			PreparedBytes:            input.sourceCounts.PreparedBytes,
			PreparedQueryBytes:       input.sourceCounts.PreparedQueryBytes,
			QuarantineBytes:          input.sourceCounts.QuarantineBytes,
			QuarantineSegmentBytes:   input.sourceCounts.QuarantineSegmentBytes,
			PinnedBytes:              input.sourceCounts.PinnedBytes,
			MappedResourcePinBytes:   input.sourceCounts.MappedResourcePinBytes,
		},
		MappedResources: input.mappedResources,
	}
}

type columnAssetReachabilitySegmentPlan struct {
	status           ColumnAssetReachabilitySegmentStatus
	protectedBytes   int64
	reclaimableBytes int64
	unknownBytes     int64
	outOfBoundsRefs  int
}

func classifyColumnAssetReachabilitySegment(segment columnAssetReachabilitySegment, ranges []columnAssetReachabilityRange) columnAssetReachabilitySegmentPlan {
	if len(ranges) == 0 && segment.fileID != 0 && segment.bytes > 0 {
		return columnAssetReachabilitySegmentPlan{
			status:           ColumnAssetReachabilitySegmentReclaimable,
			reclaimableBytes: segment.bytes,
		}
	}
	if len(ranges) == 1 {
		return classifyColumnAssetReachabilitySingleRangeSegment(segment, ranges[0])
	}
	allProtected := true
	allReclaimable := true
	for _, r := range ranges {
		if r.status != ColumnAssetReachabilityProtected {
			allProtected = false
		}
		if r.status != ColumnAssetReachabilityReclaimable {
			allReclaimable = false
		}
	}
	if len(ranges) != 0 && (allProtected || allReclaimable) {
		coveredBytes, outOfBounds := columnAssetReachabilityRangesCoveredBytes(segment, ranges)
		unknownBytes := segment.bytes - coveredBytes
		if unknownBytes < 0 {
			unknownBytes = 0
		}
		status := ColumnAssetReachabilitySegmentUnknown
		protectedBytes := int64(0)
		reclaimableBytes := int64(0)
		if allProtected {
			protectedBytes = coveredBytes
		} else {
			reclaimableBytes = coveredBytes
		}
		switch {
		case unknownBytes != 0 || outOfBounds != 0:
			status = ColumnAssetReachabilitySegmentUnknown
		case allProtected:
			status = ColumnAssetReachabilitySegmentProtected
		default:
			status = ColumnAssetReachabilitySegmentReclaimable
		}
		return columnAssetReachabilitySegmentPlan{
			status:           status,
			protectedBytes:   protectedBytes,
			reclaimableBytes: reclaimableBytes,
			unknownBytes:     unknownBytes,
			outOfBoundsRefs:  outOfBounds,
		}
	}

	var protected []columnAssetReachabilityInterval
	var reclaimable []columnAssetReachabilityInterval
	all := make([]columnAssetReachabilityInterval, 0, len(ranges))
	outOfBounds := 0
	for _, r := range ranges {
		interval, outOfBoundsRange, ok := clipColumnAssetReachabilityRange(segment.bytes, r)
		if outOfBoundsRange {
			outOfBounds++
		}
		if !ok {
			continue
		}
		all = append(all, interval)
		switch r.status {
		case ColumnAssetReachabilityProtected:
			protected = append(protected, interval)
		case ColumnAssetReachabilityReclaimable:
			reclaimable = append(reclaimable, interval)
		}
	}
	padding := deterministicColumnAssetReachabilityPaddingIntervals(segment, ranges)
	for _, interval := range padding {
		all = append(all, interval.interval)
		switch interval.status {
		case ColumnAssetReachabilityProtected:
			protected = append(protected, interval.interval)
		case ColumnAssetReachabilityReclaimable:
			reclaimable = append(reclaimable, interval.interval)
		}
	}
	allUnion := mergeColumnAssetReachabilityIntervals(all)
	protectedUnion := mergeColumnAssetReachabilityIntervals(protected)
	reclaimableUnion := subtractColumnAssetReachabilityIntervals(
		mergeColumnAssetReachabilityIntervals(reclaimable),
		protectedUnion,
	)
	protectedBytes := columnAssetReachabilityIntervalsLength(protectedUnion)
	reclaimableBytes := columnAssetReachabilityIntervalsLength(reclaimableUnion)
	coveredBytes := columnAssetReachabilityIntervalsLength(allUnion)
	unknownBytes := segment.bytes - coveredBytes
	if unknownBytes < 0 {
		unknownBytes = 0
	}
	status := ColumnAssetReachabilitySegmentUnknown
	switch {
	case unknownBytes != 0 || outOfBounds != 0:
		status = ColumnAssetReachabilitySegmentUnknown
	case protectedBytes != 0 && reclaimableBytes != 0:
		status = ColumnAssetReachabilitySegmentMixed
	case protectedBytes != 0:
		status = ColumnAssetReachabilitySegmentProtected
	case reclaimableBytes != 0:
		status = ColumnAssetReachabilitySegmentReclaimable
	}
	return columnAssetReachabilitySegmentPlan{
		status:           status,
		protectedBytes:   protectedBytes,
		reclaimableBytes: reclaimableBytes,
		unknownBytes:     unknownBytes,
		outOfBoundsRefs:  outOfBounds,
	}
}

func classifyColumnAssetReachabilitySegmentSet(segment columnAssetReachabilitySegment, ranges columnAssetReachabilityRangeSet) columnAssetReachabilitySegmentPlan {
	if ranges.count == 1 && ranges.ranges == nil {
		return classifyColumnAssetReachabilitySingleRangeSegment(segment, ranges.first)
	}
	return classifyColumnAssetReachabilitySegment(segment, ranges.ranges)
}

func classifyColumnAssetReachabilitySingleRangeSegment(segment columnAssetReachabilitySegment, r columnAssetReachabilityRange) columnAssetReachabilitySegmentPlan {
	interval, outOfBoundsRange, ok := clipColumnAssetReachabilityRange(segment.bytes, r)
	outOfBounds := 0
	if outOfBoundsRange {
		outOfBounds = 1
	}
	coveredBytes := int64(0)
	if ok {
		coveredBytes = interval.end - interval.start
	}
	unknownBytes := segment.bytes - coveredBytes
	if unknownBytes < 0 {
		unknownBytes = 0
	}
	protectedBytes := int64(0)
	reclaimableBytes := int64(0)
	switch r.status {
	case ColumnAssetReachabilityProtected:
		protectedBytes = coveredBytes
	case ColumnAssetReachabilityReclaimable:
		reclaimableBytes = coveredBytes
	}
	status := ColumnAssetReachabilitySegmentUnknown
	switch {
	case unknownBytes != 0 || outOfBounds != 0:
		status = ColumnAssetReachabilitySegmentUnknown
	case r.status == ColumnAssetReachabilityProtected:
		status = ColumnAssetReachabilitySegmentProtected
	case r.status == ColumnAssetReachabilityReclaimable:
		status = ColumnAssetReachabilitySegmentReclaimable
	}
	return columnAssetReachabilitySegmentPlan{
		status:           status,
		protectedBytes:   protectedBytes,
		reclaimableBytes: reclaimableBytes,
		unknownBytes:     unknownBytes,
		outOfBoundsRefs:  outOfBounds,
	}
}

// columnAssetReachabilitySourceBit returns ok only for recognized non-unknown
// sources. The unknown sentinel still maps to the unknown mask, but ok is false
// so callers do not accidentally treat it as a precise durable source.
func columnAssetReachabilitySourceBit(source ColumnAssetReachabilitySource) (columnAssetReachabilitySourceMask, bool) {
	mask, ok := columnAssetReachabilitySourceMaskBySource[source]
	if !ok {
		return columnAssetReachabilitySourceUnknownMask, false
	}
	return mask, source != columnAssetReachabilitySourceUnknown
}

func columnAssetReachabilityStatusForSourceMask(mask columnAssetReachabilitySourceMask) ColumnAssetReachabilityStatus {
	if mask&columnAssetReachabilitySourceUnknownMask != 0 {
		return ColumnAssetReachabilityUncertain
	}
	if mask&columnAssetReachabilityProtectedSourceMask != 0 {
		return ColumnAssetReachabilityProtected
	}
	if mask&columnAssetReachabilitySourceCandidateMask != 0 {
		return ColumnAssetReachabilityReclaimable
	}
	return ColumnAssetReachabilityUncertain
}

func columnAssetReachabilitySourcesForMask(mask columnAssetReachabilitySourceMask) []ColumnAssetReachabilitySource {
	return columnAssetReachabilitySourcesForMaskWithUnknown(mask, nil)
}

func columnAssetReachabilitySourcesForMaskWithUnknown(mask columnAssetReachabilitySourceMask, unknownSources []ColumnAssetReachabilitySource) []ColumnAssetReachabilitySource {
	if mask == 0 {
		return nil
	}
	count := columnAssetReachabilitySourceMaskCount(mask)
	hasUnknown := mask&columnAssetReachabilitySourceUnknownMask != 0
	if hasUnknown && len(unknownSources) != 0 {
		count += len(unknownSources) - 1
	}
	sources := make([]ColumnAssetReachabilitySource, 0, count)
	for _, entry := range columnAssetReachabilitySourceBits {
		if entry.source == columnAssetReachabilitySourceUnknown && hasUnknown && len(unknownSources) != 0 {
			continue
		}
		if mask&entry.mask != 0 {
			sources = append(sources, entry.source)
		}
	}
	if hasUnknown {
		sources = append(sources, unknownSources...)
	}
	return sources
}

func columnAssetReachabilitySourceMaskCount(mask columnAssetReachabilitySourceMask) int {
	return bits.OnesCount64(uint64(mask))
}

func listColumnAssetReachabilitySegments(ctx context.Context, segmentDir string) (_ []columnAssetReachabilitySegment, retErr error) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		return listColumnAssetReachabilitySegmentsLegacy(ctx, segmentDir)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := os.Open(segmentDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, dir.Close()) }()
	parentIdentity, err := rootpublication.StableIdentityFromFile(dir)
	if err != nil {
		return nil, err
	}
	infos, readErr := dir.Readdir(-1)
	if readErr != nil {
		return nil, readErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	segments := make([]columnAssetReachabilitySegment, 0, len(infos))
	for i, info := range infos {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		name := info.Name()
		fileID, ok := columnAssetReachabilitySegmentFileID(name)
		path := columnAssetReachabilitySegmentPath(segmentDir, name)
		appendSegment := func(bytes int64, childIdentity rootpublication.StableIdentity) {
			if ok {
				segments = append(segments, columnAssetReachabilitySegment{
					fileID: fileID, name: name, path: path, bytes: bytes,
					parentIdentity: parentIdentity, childIdentity: childIdentity,
				})
			} else {
				segments = append(segments, columnAssetReachabilitySegment{
					name: name, path: path, bytes: bytes,
					parentIdentity: parentIdentity, childIdentity: childIdentity,
				})
			}
		}
		if info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			appendSegment(0, rootpublication.StableIdentity{})
			continue
		}
		resource, err := rootpublication.OpenStableChildFile(dir, name, os.O_RDONLY, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		exactInfo, statErr := resource.Stat()
		childIdentity, identityErr := rootpublication.StableIdentityFromFile(resource)
		closeErr := resource.Close()
		if err := errors.Join(statErr, identityErr, closeErr); err != nil {
			return nil, err
		}
		if !exactInfo.Mode().IsRegular() {
			appendSegment(0, rootpublication.StableIdentity{})
			continue
		}
		appendSegment(exactInfo.Size(), childIdentity)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(segments, func(a, b columnAssetReachabilitySegment) int {
		if a.fileID < b.fileID {
			return -1
		}
		if a.fileID > b.fileID {
			return 1
		}
		if a.name < b.name {
			return -1
		}
		if a.name > b.name {
			return 1
		}
		return 0
	})
	return segments, nil
}

// listColumnAssetReachabilitySegmentsLegacy keeps pre-#3679 read-only
// reporting available where exact relative namespace primitives do not exist.
// It deliberately returns no stable identities and therefore cannot authorize
// destructive GC.
func listColumnAssetReachabilitySegmentsLegacy(ctx context.Context, segmentDir string) ([]columnAssetReachabilitySegment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := os.Open(segmentDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	infos, readErr := dir.Readdir(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	segments := make([]columnAssetReachabilitySegment, 0, len(infos))
	for i, info := range infos {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		name := info.Name()
		path := columnAssetReachabilitySegmentPath(segmentDir, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		fileID, ok := columnAssetReachabilitySegmentFileID(name)
		appendSegment := func(bytes int64) {
			segment := columnAssetReachabilitySegment{name: name, path: path, bytes: bytes}
			if ok {
				segment.fileID = fileID
			}
			segments = append(segments, segment)
		}
		if info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			appendSegment(0)
			continue
		}
		appendSegment(info.Size())
	}
	slices.SortFunc(segments, func(a, b columnAssetReachabilitySegment) int {
		if a.fileID < b.fileID {
			return -1
		}
		if a.fileID > b.fileID {
			return 1
		}
		return strings.Compare(a.name, b.name)
	})
	return segments, ctx.Err()
}

func columnAssetReachabilitySegmentPath(segmentDir, name string) string {
	// Keep segment path construction behind one helper so GC/rewrite path
	// canonicalization and tests use the same join semantics.
	return filepath.Join(segmentDir, name)
}

func columnAssetReachabilityRefCanContributeRange(ref ColumnAssetRef, namespace string) bool {
	return (ref.Kind == ColumnAssetKindTCS1PartImage || ref.Kind == ColumnAssetKindTCS1TypedColumnPart || ref.Kind == ColumnAssetKindTCS1AggregateMetadata || ref.Kind == ColumnAssetKindTCS1DictionaryCodes || ref.Kind == ColumnAssetKindTCS1Int64Values || ref.Kind == ColumnAssetKindTCS1HNSWSearchPack || ref.Kind == ColumnAssetKindQueryReadyBase || ref.Kind == ColumnAssetKindQueryReadyDelta || ref.Kind == ColumnAssetKindQueryReadyConsolidatedBase) &&
		ref.Namespace == namespace &&
		ref.Generation != 0 &&
		ref.PartID != 0 &&
		ref.FileID != 0 &&
		ref.Offset >= 0 &&
		ref.Length > 0 &&
		ref.Offset <= math.MaxInt64-ref.Length
}

type columnAssetReachabilityPaddingInterval struct {
	interval columnAssetReachabilityInterval
	status   ColumnAssetReachabilityStatus
}

func deterministicColumnAssetReachabilityPaddingIntervals(segment columnAssetReachabilitySegment, ranges []columnAssetReachabilityRange) []columnAssetReachabilityPaddingInterval {
	if len(ranges) < 2 || segment.path == "" {
		return nil
	}
	clipped := make([]columnAssetReachabilityRange, 0, len(ranges))
	for _, r := range ranges {
		interval, _, ok := clipColumnAssetReachabilityRange(segment.bytes, r)
		if !ok {
			continue
		}
		clipped = append(clipped, columnAssetReachabilityRange{start: interval.start, end: interval.end, status: r.status, kind: r.kind})
	}
	if len(clipped) < 2 {
		return nil
	}
	slices.SortFunc(clipped, func(a, b columnAssetReachabilityRange) int {
		return compareColumnAssetReachabilityIntervalBounds(a.start, a.end, b.start, b.end)
	})
	out := make([]columnAssetReachabilityPaddingInterval, 0, len(clipped)-1)
	coveredEnd := clipped[0].end
	for _, r := range clipped[1:] {
		if r.start > coveredEnd && columnAssetReachabilityRangeFollowsDeterministicZeroPadding(segment, coveredEnd, r) {
			out = append(out, columnAssetReachabilityPaddingInterval{
				interval: columnAssetReachabilityInterval{start: coveredEnd, end: r.start},
				status:   r.status,
			})
		}
		if r.end > coveredEnd {
			coveredEnd = r.end
		}
	}
	return out
}

func columnAssetReachabilityRangeFollowsDeterministicZeroPadding(segment columnAssetReachabilitySegment, previousEnd int64, r columnAssetReachabilityRange) bool {
	if r.kind == ColumnAssetKindTCS1TypedColumnPart && columnAssetReachabilityRangeFollowsZeroPaddingAtAlignment(segment, previousEnd, r, columnVectorGraphScalarU8CodesAlignment) {
		return true
	}
	alignment := columnAssetReachabilityRangeDeterministicPaddingAlignment(r.kind)
	return columnAssetReachabilityRangeFollowsZeroPaddingAtAlignment(segment, previousEnd, r, alignment)
}

func columnAssetReachabilityRangeFollowsZeroPaddingAtAlignment(segment columnAssetReachabilitySegment, previousEnd int64, r columnAssetReachabilityRange, alignment int64) bool {
	if alignment <= 1 || r.start <= previousEnd || r.start%alignment != 0 {
		return false
	}
	padding := int64(columnAssetSegmentPrefixPadding(previousEnd, alignment))
	if padding == 0 || r.start-previousEnd != padding {
		return false
	}
	return columnAssetReachabilitySegmentRangeIsZero(segment.path, previousEnd, padding)
}

func columnAssetReachabilityRangeDeterministicPaddingAlignment(kind ColumnAssetKind) int64 {
	switch kind {
	case ColumnAssetKindTCS1DictionaryCodes:
		return dictionaryCodesDirectViewAssetAlignment
	case ColumnAssetKindTCS1Int64Values:
		return int64ValuesDirectViewAssetAlignment
	case ColumnAssetKindTCS1TypedColumnPart:
		return typedColumnPartDirectViewAssetAlignment
	case ColumnAssetKindTCS1HNSWSearchPack:
		return int64(columnHNSWSearchPackVectorSectionAlignment)
	default:
		return 0
	}
}

func columnAssetReachabilityMaxDeterministicPaddingAlignment() int64 {
	maxAlignment := int64(typedColumnPartDirectViewAssetAlignment)
	for _, alignment := range []int64{dictionaryCodesDirectViewAssetAlignment, int64ValuesDirectViewAssetAlignment, int64(columnHNSWSearchPackVectorSectionAlignment), columnVectorGraphScalarU8CodesAlignment} {
		if alignment > maxAlignment {
			maxAlignment = alignment
		}
	}
	return maxAlignment
}

func columnAssetReachabilitySegmentRangeIsZero(path string, offset, length int64) bool {
	if path == "" || offset < 0 || length <= 0 || length > int64(columnAssetReachabilityMaxDeterministicPaddingAlignment()-1) {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	buf := make([]byte, int(length))
	if _, err := file.ReadAt(buf, offset); err != nil {
		return false
	}
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func clippedColumnAssetReachabilityIntervals(segment columnAssetReachabilitySegment, ranges []columnAssetReachabilityRange) ([]columnAssetReachabilityInterval, int) {
	intervals := make([]columnAssetReachabilityInterval, 0, len(ranges))
	outOfBounds := 0
	for _, r := range ranges {
		interval, outOfBoundsRange, ok := clipColumnAssetReachabilityRange(segment.bytes, r)
		if outOfBoundsRange {
			outOfBounds++
		}
		if !ok {
			continue
		}
		intervals = append(intervals, interval)
	}
	return intervals, outOfBounds
}

// columnAssetReachabilityRangesCoveredBytes computes covered bytes within the
// segment bounds. It sorts ranges in place; callers must treat ranges as
// consumed after calling.
func columnAssetReachabilityRangesCoveredBytes(segment columnAssetReachabilitySegment, ranges []columnAssetReachabilityRange) (int64, int) {
	if len(ranges) == 1 {
		plan := classifyColumnAssetReachabilitySingleRangeSegment(segment, ranges[0])
		covered := plan.protectedBytes + plan.reclaimableBytes
		if ranges[0].status != ColumnAssetReachabilityProtected && ranges[0].status != ColumnAssetReachabilityReclaimable {
			covered = segment.bytes - plan.unknownBytes
			if covered < 0 {
				covered = 0
			}
		}
		return covered, plan.outOfBoundsRefs
	}
	padding := deterministicColumnAssetReachabilityPaddingIntervals(segment, ranges)
	slices.SortFunc(ranges, func(a, b columnAssetReachabilityRange) int {
		return compareColumnAssetReachabilityIntervalBounds(a.start, a.end, b.start, b.end)
	})
	var covered int64
	var cur columnAssetReachabilityInterval
	haveCur := false
	outOfBounds := 0
	for _, r := range ranges {
		interval, outOfBoundsRange, ok := clipColumnAssetReachabilityRange(segment.bytes, r)
		if outOfBoundsRange {
			outOfBounds++
		}
		if !ok {
			continue
		}
		if !haveCur {
			cur = interval
			haveCur = true
			continue
		}
		if interval.start > cur.end {
			covered = addColumnAssetReachabilityBytes(covered, cur.end-cur.start)
			cur = interval
			continue
		}
		if interval.end > cur.end {
			cur.end = interval.end
		}
	}
	if haveCur {
		covered = addColumnAssetReachabilityBytes(covered, cur.end-cur.start)
	}
	for _, interval := range padding {
		covered = addColumnAssetReachabilityBytes(covered, interval.interval.end-interval.interval.start)
	}
	return covered, outOfBounds
}

func clipColumnAssetReachabilityRange(segmentBytes int64, r columnAssetReachabilityRange) (columnAssetReachabilityInterval, bool, bool) {
	outOfBounds := r.start < 0 || r.end <= r.start || r.start >= segmentBytes || r.end > segmentBytes
	start := r.start
	if start < 0 {
		start = 0
	}
	end := r.end
	if end > segmentBytes {
		end = segmentBytes
	}
	if start >= end {
		return columnAssetReachabilityInterval{}, outOfBounds, false
	}
	return columnAssetReachabilityInterval{start: start, end: end}, outOfBounds, true
}

func columnAssetReachabilitySegmentFileID(name string) (uint32, bool) {
	if !strings.HasPrefix(name, columnAssetSegmentFilePrefix) || !strings.HasSuffix(name, columnAssetSegmentFileSuffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, columnAssetSegmentFilePrefix), columnAssetSegmentFileSuffix)
	if len(raw) < 6 || (len(raw) > 6 && raw[0] == '0') {
		return 0, false
	}
	id, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || id == 0 {
		return 0, false
	}
	fileID := uint32(id)
	if fileID < 1_000_000 && len(raw) != 6 {
		return 0, false
	}
	return fileID, true
}

// mergeColumnAssetReachabilityIntervals returns non-overlapping intervals. It
// sorts and reuses the input slice in place; callers must treat input as
// consumed after calling it.
func mergeColumnAssetReachabilityIntervals(in []columnAssetReachabilityInterval) []columnAssetReachabilityInterval {
	if len(in) == 0 {
		return nil
	}
	if len(in) == 1 {
		if in[0].end <= in[0].start {
			return nil
		}
		return in
	}
	slices.SortFunc(in, func(a, b columnAssetReachabilityInterval) int {
		return compareColumnAssetReachabilityIntervalBounds(a.start, a.end, b.start, b.end)
	})
	merged := in[:0]
	for _, interval := range in {
		if interval.end <= interval.start {
			continue
		}
		if len(merged) == 0 || interval.start > merged[len(merged)-1].end {
			merged = append(merged, interval)
			continue
		}
		if interval.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = interval.end
		}
	}
	return merged
}

func compareColumnAssetReachabilityIntervalBounds(aStart, aEnd, bStart, bEnd int64) int {
	if aStart < bStart {
		return -1
	}
	if aStart > bStart {
		return 1
	}
	if aEnd < bEnd {
		return -1
	}
	if aEnd > bEnd {
		return 1
	}
	return 0
}

// subtractColumnAssetReachabilityIntervals returns intervals from in with
// exclude removed. Both inputs must already be sorted by start and
// non-overlapping, for example as returned by
// mergeColumnAssetReachabilityIntervals; behavior is undefined otherwise.
func subtractColumnAssetReachabilityIntervals(in, exclude []columnAssetReachabilityInterval) []columnAssetReachabilityInterval {
	if len(in) == 0 || len(exclude) == 0 {
		return in
	}
	out := make([]columnAssetReachabilityInterval, 0, len(in))
	excludeIdx := 0
	for _, interval := range in {
		start := interval.start
		for excludeIdx < len(exclude) && exclude[excludeIdx].end <= start {
			excludeIdx++
		}
		for j := excludeIdx; j < len(exclude) && exclude[j].start < interval.end; j++ {
			if exclude[j].end <= start {
				continue
			}
			if exclude[j].start > start {
				out = append(out, columnAssetReachabilityInterval{start: start, end: minColumnAssetReachabilityInt64(exclude[j].start, interval.end)})
			}
			if exclude[j].end > start {
				start = exclude[j].end
			}
			if start >= interval.end {
				break
			}
		}
		for excludeIdx < len(exclude) && exclude[excludeIdx].end <= interval.end {
			excludeIdx++
		}
		if start < interval.end {
			out = append(out, columnAssetReachabilityInterval{start: start, end: interval.end})
		}
	}
	return out
}

func columnAssetReachabilityIntervalsLength(intervals []columnAssetReachabilityInterval) int64 {
	var total int64
	for _, interval := range intervals {
		if interval.end > interval.start {
			total = addColumnAssetReachabilityBytes(total, interval.end-interval.start)
		}
	}
	return total
}

func compareColumnAssetRefs(a, b ColumnAssetRef) int {
	if a.Kind != b.Kind {
		if a.Kind < b.Kind {
			return -1
		}
		return 1
	}
	if a.Namespace != b.Namespace {
		if a.Namespace < b.Namespace {
			return -1
		}
		return 1
	}
	if a.FileID != b.FileID {
		if a.FileID < b.FileID {
			return -1
		}
		return 1
	}
	if a.Offset != b.Offset {
		if a.Offset < b.Offset {
			return -1
		}
		return 1
	}
	if a.Length != b.Length {
		if a.Length < b.Length {
			return -1
		}
		return 1
	}
	if a.Generation != b.Generation {
		if a.Generation < b.Generation {
			return -1
		}
		return 1
	}
	if a.PartID != b.PartID {
		if a.PartID < b.PartID {
			return -1
		}
		return 1
	}
	if a.Checksum != b.Checksum {
		if a.Checksum < b.Checksum {
			return -1
		}
		return 1
	}
	return 0
}

func positiveColumnAssetReachabilityLength(length int64) int64 {
	if length > 0 {
		return length
	}
	return 0
}

func addColumnAssetReachabilityBytes(total, delta int64) int64 {
	if delta <= 0 {
		return total
	}
	if total > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return total + delta
}

func minColumnAssetReachabilityInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
