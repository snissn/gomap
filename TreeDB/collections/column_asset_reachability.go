package collections

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ColumnAssetReachabilityOptions controls M15A protect-only reachability
// planning. CandidateRefs are possible reclamation inputs supplied by a future
// manager/catalog index; pending, prepared, and pinned refs are always retained.
type ColumnAssetReachabilityOptions struct {
	Detailed      bool
	CandidateRefs []ColumnAssetRef
	PendingRefs   []ColumnAssetRef
	PreparedRefs  []ColumnAssetRef
	PinnedRefs    []ColumnAssetRef
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
	Total            int
	Protected        int
	Reclaimable      int
	Mixed            int
	Unknown          int
	Missing          int
	OutOfBoundsRefs  int
	BytesTotal       int64
	BytesProtected   int64
	BytesReclaimable int64
	BytesUnknown     int64
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
	ref     ColumnAssetRef
	sources []ColumnAssetReachabilitySource
}

type columnAssetReachabilityRange struct {
	start  int64
	end    int64
	status ColumnAssetReachabilityStatus
}

type columnAssetReachabilityInterval struct {
	start int64
	end   int64
}

type columnAssetReachabilitySegment struct {
	fileID uint32
	path   string
	bytes  int64
}

const columnAssetReachabilityContextCheckInterval = 256

// PlanColumnAssetReachability builds the M15A dry-run/protect-only liveness
// plan for the collection's isolated column asset namespace. It never deletes,
// rewrites, or remaps assets; uncertain or untracked bytes are retained.
func (c *Collection) PlanColumnAssetReachability(ctx context.Context, opts ColumnAssetReachabilityOptions) (ColumnAssetReachabilityPlan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ColumnAssetReachabilityPlan{ProtectOnly: true}, err
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithContext(ctx)
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return ColumnAssetReachabilityPlan{ProtectOnly: true}, err
	}

	input := columnAssetReachabilityInput{
		rootDir:      view.ColumnAssetRootDir,
		collection:   view.CollectionName,
		namespace:    view.AssetNamespace,
		activeGen:    view.Diagnostics.ManifestGeneration,
		recoveryGen:  view.Diagnostics.RecoveryManifestGeneration,
		manifestRecs: view.Diagnostics.ManifestRecords,
		detailed:     opts.Detailed,
	}
	for i, assetRef := range view.AssetRefs {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), err
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
		return columnAssetReachabilityPlanIdentity(input), err
	}
	if err := input.addRefs(ctx, opts.PendingRefs, ColumnAssetReachabilitySourcePendingPublish); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}
	if err := input.addRefs(ctx, opts.PreparedRefs, ColumnAssetReachabilitySourcePreparedAsset); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}
	if err := input.addRefs(ctx, opts.PinnedRefs, ColumnAssetReachabilitySourcePinnedSnapshot); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}
	if err := ctx.Err(); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}
	return buildColumnAssetReachabilityPlan(ctx, input)
}

type columnAssetReachabilityInput struct {
	rootDir      string
	collection   string
	namespace    string
	activeGen    uint64
	recoveryGen  uint64
	manifestRecs int
	activeRefs   int
	recoveryRefs int
	detailed     bool
	refs         map[ColumnAssetRef]*columnAssetReachabilityRefBuilder
	sourceCounts ColumnAssetReachabilitySourceStats
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
		in.refs = make(map[ColumnAssetRef]*columnAssetReachabilityRefBuilder)
	}
	builder := in.refs[ref]
	if builder == nil {
		builder = &columnAssetReachabilityRefBuilder{
			ref: ref,
		}
		in.refs[ref] = builder
	}
	for _, seen := range builder.sources {
		if seen == source {
			return false
		}
	}
	builder.sources = append(builder.sources, source)
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

	rangeCounts := make(map[uint32]int, len(segments))
	i := 0
	for _, builder := range input.refs {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), err
			}
		}
		i++
		if columnAssetReachabilityRefCanContributeRange(builder.ref, input.namespace) {
			rangeCounts[builder.ref.FileID]++
		}
	}
	rangesByFile := make(map[uint32][]columnAssetReachabilityRange, len(rangeCounts))
	for fileID, count := range rangeCounts {
		rangesByFile[fileID] = make([]columnAssetReachabilityRange, 0, count)
	}
	processRef := func(builder *columnAssetReachabilityRefBuilder) {
		status := columnAssetReachabilityStatusForSources(builder.sources)
		if !columnAssetReachabilityRefCanContributeRange(builder.ref, input.namespace) {
			status = ColumnAssetReachabilityUncertain
		}
		plan.Refs.Total++
		refBytes := positiveColumnAssetReachabilityLength(builder.ref.Length)
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
			plan.Entries = append(plan.Entries, ColumnAssetReachabilityRefEntry{
				Ref:     builder.ref,
				Status:  status,
				Sources: append([]ColumnAssetReachabilitySource(nil), builder.sources...),
			})
		}
		if status == ColumnAssetReachabilityUncertain {
			return
		}
		rangesByFile[builder.ref.FileID] = append(rangesByFile[builder.ref.FileID], columnAssetReachabilityRange{
			start:  builder.ref.Offset,
			end:    builder.ref.Offset + builder.ref.Length,
			status: status,
		})
	}
	if input.detailed {
		refBuilders := make([]*columnAssetReachabilityRefBuilder, 0, len(input.refs))
		for _, builder := range input.refs {
			refBuilders = append(refBuilders, builder)
		}
		sort.Slice(refBuilders, func(i, j int) bool {
			return compareColumnAssetRefs(refBuilders[i].ref, refBuilders[j].ref) < 0
		})
		for i, builder := range refBuilders {
			if i%columnAssetReachabilityContextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return columnAssetReachabilityPlanIdentity(input), err
				}
			}
			processRef(builder)
		}
	} else {
		i := 0
		for _, builder := range input.refs {
			if i%columnAssetReachabilityContextCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return columnAssetReachabilityPlanIdentity(input), err
				}
			}
			i++
			processRef(builder)
		}
	}
	if err := ctx.Err(); err != nil {
		return columnAssetReachabilityPlanIdentity(input), err
	}

	seenFiles := make(map[uint32]struct{}, len(segments))
	for i, segment := range segments {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return columnAssetReachabilityPlanIdentity(input), err
			}
		}
		seenFiles[segment.fileID] = struct{}{}
		plan.Segments.Total++
		plan.Segments.BytesTotal = addColumnAssetReachabilityBytes(plan.Segments.BytesTotal, segment.bytes)
		segmentPlan := classifyColumnAssetReachabilitySegment(segment, rangesByFile[segment.fileID])
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
		case ColumnAssetReachabilitySegmentMixed:
			plan.Segments.Mixed++
			plan.RewriteDebtBytes = addColumnAssetReachabilityBytes(plan.RewriteDebtBytes, segmentPlan.reclaimableBytes)
		default:
			plan.Segments.Unknown++
			if segmentPlan.reclaimableBytes != 0 {
				plan.RewriteDebtBytes = addColumnAssetReachabilityBytes(plan.RewriteDebtBytes, segmentPlan.reclaimableBytes)
			}
		}
		if input.detailed {
			plan.SegmentEntries = append(plan.SegmentEntries, ColumnAssetReachabilitySegmentEntry{
				Namespace:        input.namespace,
				FileID:           segment.fileID,
				Path:             segment.path,
				Bytes:            segment.bytes,
				Status:           segmentPlan.status,
				ProtectedBytes:   segmentPlan.protectedBytes,
				ReclaimableBytes: segmentPlan.reclaimableBytes,
				UnknownBytes:     segmentPlan.unknownBytes,
				RefCount:         len(rangesByFile[segment.fileID]),
			})
		}
	}
	var missingFileIDs []uint32
	for fileID := range rangesByFile {
		if _, ok := seenFiles[fileID]; ok {
			continue
		}
		missingFileIDs = append(missingFileIDs, fileID)
	}
	sort.Slice(missingFileIDs, func(i, j int) bool {
		return missingFileIDs[i] < missingFileIDs[j]
	})
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
		if input.detailed {
			plan.SegmentEntries = append(plan.SegmentEntries, ColumnAssetReachabilitySegmentEntry{
				Namespace: input.namespace,
				FileID:    fileID,
				Path:      filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)),
				Status:    ColumnAssetReachabilitySegmentMissing,
				RefCount:  len(ranges),
			})
		}
	}
	return plan, nil
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
		intervals, outOfBounds := clippedColumnAssetReachabilityIntervals(segment, ranges)
		union := mergeColumnAssetReachabilityIntervals(intervals)
		coveredBytes := columnAssetReachabilityIntervalsLength(union)
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

func columnAssetReachabilityStatusForSources(sources []ColumnAssetReachabilitySource) ColumnAssetReachabilityStatus {
	seenCandidate := false
	for _, source := range sources {
		switch source {
		case ColumnAssetReachabilitySourceActiveManifest,
			ColumnAssetReachabilitySourceRecoveryManifest,
			ColumnAssetReachabilitySourcePinnedSnapshot,
			ColumnAssetReachabilitySourcePendingPublish,
			ColumnAssetReachabilitySourcePreparedAsset:
			return ColumnAssetReachabilityProtected
		case ColumnAssetReachabilitySourceCandidate:
			seenCandidate = true
		}
	}
	if seenCandidate {
		return ColumnAssetReachabilityReclaimable
	}
	return ColumnAssetReachabilityUncertain
}

func listColumnAssetReachabilitySegments(ctx context.Context, segmentDir string) ([]columnAssetReachabilitySegment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	segments := make([]columnAssetReachabilitySegment, 0, len(entries))
	for i, entry := range entries {
		if i%columnAssetReachabilityContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		path := filepath.Join(segmentDir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		if info.IsDir() {
			continue
		}
		fileID, ok := columnAssetReachabilitySegmentFileID(entry.Name())
		if info.Mode()&os.ModeSymlink != 0 {
			if ok {
				segments = append(segments, columnAssetReachabilitySegment{fileID: fileID, path: path})
			} else {
				segments = append(segments, columnAssetReachabilitySegment{path: path})
			}
			continue
		}
		if !info.Mode().IsRegular() {
			if ok {
				segments = append(segments, columnAssetReachabilitySegment{fileID: fileID, path: path})
			} else {
				segments = append(segments, columnAssetReachabilitySegment{path: path})
			}
			continue
		}
		if !ok {
			segments = append(segments, columnAssetReachabilitySegment{path: path, bytes: info.Size()})
			continue
		}
		segments = append(segments, columnAssetReachabilitySegment{fileID: fileID, path: path, bytes: info.Size()})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].fileID < segments[j].fileID || (segments[i].fileID == segments[j].fileID && segments[i].path < segments[j].path)
	})
	return segments, nil
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
	id, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || id == 0 {
		return 0, false
	}
	fileID := uint32(id)
	if name != columnAssetSegmentFileName(fileID) {
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
	sort.Slice(in, func(i, j int) bool {
		return in[i].start < in[j].start || (in[i].start == in[j].start && in[i].end < in[j].end)
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
