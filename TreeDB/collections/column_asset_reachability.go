package collections

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
	PinnedRefs                            []ColumnAssetRef
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
	ColumnAssetReachabilitySourceActiveManifest   ColumnAssetReachabilitySource = "active_manifest"
	ColumnAssetReachabilitySourceRecoveryManifest ColumnAssetReachabilitySource = "recovery_manifest"
	ColumnAssetReachabilitySourceCandidate        ColumnAssetReachabilitySource = "candidate"
	ColumnAssetReachabilitySourcePinnedSnapshot   ColumnAssetReachabilitySource = "pinned_snapshot"
	ColumnAssetReachabilitySourcePendingPublish   ColumnAssetReachabilitySource = "pending_publish"
	ColumnAssetReachabilitySourcePreparedAsset    ColumnAssetReachabilitySource = "prepared_asset"
	columnAssetReachabilitySourceUnknown          ColumnAssetReachabilitySource = "unknown"
)

type ColumnAssetReachabilityPlan struct {
	ProtectOnly                bool
	Complete                   bool
	Collection                 string
	Namespace                  string
	ActiveManifestGeneration   uint64
	RecoveryManifestGeneration uint64
	Sources                    ColumnAssetReachabilitySourceStats
	Refs                       ColumnAssetReachabilityRefStats
	Segments                   ColumnAssetReachabilitySegmentStats
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
	ManifestRoots        int
	ManifestRecords      int
	ActiveManifestRefs   int
	RecoveryManifestRefs int
	CandidateRefs        int
	PendingRefs          int
	PreparedRefs         int
	PinnedRefs           int
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
	Total                 int
	Protected             int
	Reclaimable           int
	Mixed                 int
	Unknown               int
	Missing               int
	OutOfBoundsRefs       int
	BytesTotal            int64
	BytesProtected        int64
	BytesReclaimable      int64
	BytesWholeReclaimable int64
	BytesUnknown          int64
}

type ColumnAssetReachabilityRefEntry struct {
	Ref     ColumnAssetRef
	Status  ColumnAssetReachabilityStatus
	Sources []ColumnAssetReachabilitySource
}

type ColumnAssetReachabilitySegmentEntry struct {
	Namespace        string
	FileID           uint32
	Path             string
	Bytes            int64
	Status           ColumnAssetReachabilitySegmentStatus
	ProtectedBytes   int64
	ReclaimableBytes int64
	UnknownBytes     int64
	RefCount         int
}

type columnAssetReachabilityRefBuilder struct {
	ref        ColumnAssetRef
	sourceMask columnAssetReachabilitySourceMask
}

type columnAssetReachabilitySourceMask uint32

const (
	columnAssetReachabilitySourceActiveManifestMask columnAssetReachabilitySourceMask = 1 << iota
	columnAssetReachabilitySourceRecoveryManifestMask
	columnAssetReachabilitySourceCandidateMask
	columnAssetReachabilitySourcePinnedSnapshotMask
	columnAssetReachabilitySourcePendingPublishMask
	columnAssetReachabilitySourcePreparedAssetMask
	columnAssetReachabilitySourceUnknownMask
)

const columnAssetReachabilityProtectedSourceMask = columnAssetReachabilitySourceActiveManifestMask |
	columnAssetReachabilitySourceRecoveryManifestMask |
	columnAssetReachabilitySourcePinnedSnapshotMask |
	columnAssetReachabilitySourcePendingPublishMask |
	columnAssetReachabilitySourcePreparedAssetMask

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
	{columnAssetReachabilitySourceUnknown, columnAssetReachabilitySourceUnknownMask},
}

type columnAssetReachabilityRange struct {
	start  int64
	end    int64
	status ColumnAssetReachabilityStatus
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
	fileID uint32
	name   string
	bytes  int64
}

const columnAssetReachabilityContextCheckInterval = 256

// PlanColumnAssetReachability builds the M15A dry-run/protect-only liveness
// plan for the collection's isolated column asset namespace. It never deletes,
// rewrites, or remaps assets; uncertain or untracked bytes are retained.
func (c *Collection) PlanColumnAssetReachability(ctx context.Context, opts ColumnAssetReachabilityOptions) (ColumnAssetReachabilityPlan, error) {
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
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return ColumnAssetReachabilityPlan{ProtectOnly: true}, nil, err
	}

	expectedRefs := len(view.AssetRefs) + len(opts.CandidateRefs) + len(opts.PendingRefs) + len(opts.PreparedRefs) + len(opts.PinnedRefs)
	input := columnAssetReachabilityInput{
		rootDir:        view.ColumnAssetRootDir,
		collection:     view.CollectionName,
		namespace:      view.AssetNamespace,
		activeGen:      view.Diagnostics.ManifestGeneration,
		recoveryGen:    view.Diagnostics.RecoveryManifestGeneration,
		manifestRecs:   view.Diagnostics.ManifestRecords,
		detailed:       opts.Detailed,
		segmentDetails: opts.Detailed || opts.SegmentDetails,
		omitSources:    opts.omitDetailedEntrySources,
		omitSort:       opts.omitDetailedEntrySort,
	}
	if expectedRefs > 0 {
		input.refs = make(map[ColumnAssetRef]columnAssetReachabilitySourceMask, expectedRefs)
	}
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
		}
		if input.addRef(assetRef.Ref, ColumnAssetReachabilitySourceRecoveryManifest) {
			input.recoveryRefs++
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

type columnAssetReachabilityInput struct {
	rootDir        string
	collection     string
	namespace      string
	activeGen      uint64
	recoveryGen    uint64
	manifestRecs   int
	activeRefs     int
	recoveryRefs   int
	detailed       bool
	segmentDetails bool
	omitSources    bool
	omitSort       bool
	refs           map[ColumnAssetRef]columnAssetReachabilitySourceMask
	sourceCounts   ColumnAssetReachabilitySourceStats
}

func (in *columnAssetReachabilityInput) addRefs(ctx context.Context, refs []ColumnAssetRef, source ColumnAssetReachabilitySource) error {
	for i, ref := range refs {
		if ctx != nil && i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if in.addRef(ref, source) {
			in.incrementSourceCount(source)
		}
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

func (in *columnAssetReachabilityInput) incrementSourceCount(source ColumnAssetReachabilitySource) {
	switch source {
	case ColumnAssetReachabilitySourceCandidate:
		in.sourceCounts.CandidateRefs++
	case ColumnAssetReachabilitySourcePendingPublish:
		in.sourceCounts.PendingRefs++
	case ColumnAssetReachabilitySourcePreparedAsset:
		in.sourceCounts.PreparedRefs++
	case ColumnAssetReachabilitySourcePinnedSnapshot:
		in.sourceCounts.PinnedRefs++
	}
}

func (in *columnAssetReachabilityInput) addRef(ref ColumnAssetRef, source ColumnAssetReachabilitySource) bool {
	if in.refs == nil {
		in.refs = make(map[ColumnAssetRef]columnAssetReachabilitySourceMask)
	}
	sourceMask, ok := columnAssetReachabilitySourceBit(source)
	if !ok {
		sourceMask = columnAssetReachabilitySourceUnknownMask
	}
	mask := in.refs[ref]
	if mask&sourceMask != 0 {
		return false
	}
	in.refs[ref] = mask | sourceMask
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
	segments, err := listColumnAssetReachabilitySegments(namespace.SegmentDir)
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
	} else if len(input.refs) != 0 {
		rangesByFile = make(map[uint32]columnAssetReachabilityRangeSet, len(input.refs))
	}
	if input.detailed {
		plan.Entries = make([]ColumnAssetReachabilityRefEntry, 0, len(input.refs))
	}
	if input.detailed || input.segmentDetails {
		plan.SegmentEntries = make([]ColumnAssetReachabilitySegmentEntry, 0, len(segments))
	}
	processRef := func(ref ColumnAssetRef, sourceMask columnAssetReachabilitySourceMask) {
		status := columnAssetReachabilityStatusForSourceMask(sourceMask)
		if !columnAssetReachabilityRefCanContributeRange(ref, input.namespace) {
			status = ColumnAssetReachabilityUncertain
		}
		plan.Refs.Total++
		plan.Refs.BytesTotal += positiveColumnAssetReachabilityLength(ref.Length)
		switch status {
		case ColumnAssetReachabilityProtected:
			plan.Refs.Protected++
			plan.Refs.BytesProtected += positiveColumnAssetReachabilityLength(ref.Length)
		case ColumnAssetReachabilityReclaimable:
			plan.Refs.Reclaimable++
			plan.Refs.BytesReclaimable += positiveColumnAssetReachabilityLength(ref.Length)
		default:
			plan.Refs.Uncertain++
			plan.Refs.BytesUncertain += positiveColumnAssetReachabilityLength(ref.Length)
			plan.Complete = false
		}
		if input.detailed {
			entry := ColumnAssetReachabilityRefEntry{
				Ref:    ref,
				Status: status,
			}
			if !input.omitSources {
				entry.Sources = columnAssetReachabilitySourcesForMask(sourceMask)
			}
			plan.Entries = append(plan.Entries, entry)
		}
		if status == ColumnAssetReachabilityUncertain {
			return
		}
		set := rangesByFile[ref.FileID]
		set.appendRange(columnAssetReachabilityRange{
			start:  ref.Offset,
			end:    ref.Offset + ref.Length,
			status: status,
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

	for i, segment := range segments {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), err
			}
		}
		rangeSet := rangesByFile[segment.fileID]
		delete(rangesByFile, segment.fileID)
		plan.Segments.Total++
		plan.Segments.BytesTotal += segment.bytes
		segmentPlan := classifyColumnAssetReachabilitySegmentSet(segment, rangeSet)
		if segmentPlan.outOfBoundsRefs != 0 {
			plan.Segments.OutOfBoundsRefs += segmentPlan.outOfBoundsRefs
			plan.Complete = false
		}
		if segmentPlan.unknownBytes != 0 || segmentPlan.status == ColumnAssetReachabilitySegmentUnknown {
			plan.Complete = false
		}
		plan.Segments.BytesProtected += segmentPlan.protectedBytes
		plan.Segments.BytesReclaimable += segmentPlan.reclaimableBytes
		plan.Segments.BytesUnknown += segmentPlan.unknownBytes
		switch segmentPlan.status {
		case ColumnAssetReachabilitySegmentProtected:
			plan.Segments.Protected++
		case ColumnAssetReachabilitySegmentReclaimable:
			plan.Segments.Reclaimable++
			plan.Segments.BytesWholeReclaimable += segment.bytes
		case ColumnAssetReachabilitySegmentMixed:
			plan.Segments.Mixed++
			plan.RewriteDebtBytes += segmentPlan.reclaimableBytes
		default:
			plan.Segments.Unknown++
		}
		if input.detailed || input.segmentDetails {
			plan.SegmentEntries = append(plan.SegmentEntries, ColumnAssetReachabilitySegmentEntry{
				Namespace:        input.namespace,
				FileID:           segment.fileID,
				Path:             columnAssetReachabilitySegmentPath(namespace.SegmentDir, segment.name),
				Bytes:            segment.bytes,
				Status:           segmentPlan.status,
				ProtectedBytes:   segmentPlan.protectedBytes,
				ReclaimableBytes: segmentPlan.reclaimableBytes,
				UnknownBytes:     segmentPlan.unknownBytes,
				RefCount:         rangeSet.count,
			})
		}
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
		ActiveManifestGeneration:   input.activeGen,
		RecoveryManifestGeneration: input.recoveryGen,
	}
}

func columnAssetReachabilityPlanWithStats(input columnAssetReachabilityInput) ColumnAssetReachabilityPlan {
	return ColumnAssetReachabilityPlan{
		ProtectOnly:                true,
		Complete:                   true,
		Collection:                 input.collection,
		Namespace:                  input.namespace,
		ActiveManifestGeneration:   input.activeGen,
		RecoveryManifestGeneration: input.recoveryGen,
		Sources: ColumnAssetReachabilitySourceStats{
			ManifestRoots:        1,
			ManifestRecords:      input.manifestRecs,
			ActiveManifestRefs:   input.activeRefs,
			RecoveryManifestRefs: input.recoveryRefs,
			CandidateRefs:        input.sourceCounts.CandidateRefs,
			PendingRefs:          input.sourceCounts.PendingRefs,
			PreparedRefs:         input.sourceCounts.PreparedRefs,
			PinnedRefs:           input.sourceCounts.PinnedRefs,
		},
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

func columnAssetReachabilitySourceBit(source ColumnAssetReachabilitySource) (columnAssetReachabilitySourceMask, bool) {
	switch source {
	case ColumnAssetReachabilitySourceCandidate:
		return columnAssetReachabilitySourceCandidateMask, true
	case ColumnAssetReachabilitySourceActiveManifest:
		return columnAssetReachabilitySourceActiveManifestMask, true
	case ColumnAssetReachabilitySourceRecoveryManifest:
		return columnAssetReachabilitySourceRecoveryManifestMask, true
	case ColumnAssetReachabilitySourcePendingPublish:
		return columnAssetReachabilitySourcePendingPublishMask, true
	case ColumnAssetReachabilitySourcePreparedAsset:
		return columnAssetReachabilitySourcePreparedAssetMask, true
	case ColumnAssetReachabilitySourcePinnedSnapshot:
		return columnAssetReachabilitySourcePinnedSnapshotMask, true
	default:
		return columnAssetReachabilitySourceUnknownMask, false
	}
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
	if mask == 0 {
		return nil
	}
	sources := make([]ColumnAssetReachabilitySource, 0, columnAssetReachabilitySourceMaskCount(mask))
	for _, entry := range columnAssetReachabilitySourceBits {
		if mask&entry.mask != 0 {
			sources = append(sources, entry.source)
		}
	}
	return sources
}

func columnAssetReachabilitySourceMaskCount(mask columnAssetReachabilitySourceMask) int {
	count := 0
	for mask != 0 {
		count++
		mask &= mask - 1
	}
	return count
}

func listColumnAssetReachabilitySegments(segmentDir string) ([]columnAssetReachabilitySegment, error) {
	dir, err := os.Open(segmentDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	infos, readErr := dir.Readdir(-1)
	// Close before handling readErr so failed directory reads do not leak an fd.
	closeErr := dir.Close()
	if readErr != nil {
		if closeErr != nil {
			return nil, errors.Join(readErr, closeErr)
		}
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	segments := make([]columnAssetReachabilitySegment, 0, len(infos))
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		name := info.Name()
		fileID, ok := columnAssetReachabilitySegmentFileID(name)
		if !ok {
			segments = append(segments, columnAssetReachabilitySegment{name: name, bytes: info.Size()})
			continue
		}
		segments = append(segments, columnAssetReachabilitySegment{fileID: fileID, name: name, bytes: info.Size()})
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

func columnAssetReachabilitySegmentPath(segmentDir, name string) string {
	// Keep segment path construction behind one helper so GC/rewrite path
	// canonicalization and tests use the same join semantics.
	return filepath.Join(segmentDir, name)
}

func columnAssetReachabilityRefCanContributeRange(ref ColumnAssetRef, namespace string) bool {
	return ref.Kind == ColumnAssetKindTCS1PartImage &&
		ref.Namespace == namespace &&
		ref.Generation != 0 &&
		ref.PartID != 0 &&
		ref.FileID != 0 &&
		ref.Offset >= 0 &&
		ref.Length > 0 &&
		ref.Offset <= math.MaxInt64-ref.Length
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
			covered += cur.end - cur.start
			cur = interval
			continue
		}
		if interval.end > cur.end {
			cur.end = interval.end
		}
	}
	if haveCur {
		covered += cur.end - cur.start
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
			total += interval.end - interval.start
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

func minColumnAssetReachabilityInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
