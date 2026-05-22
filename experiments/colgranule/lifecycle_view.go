package colgranule

import (
	"fmt"
	"sort"
)

type ColumnCollectionManifestView struct {
	data []byte
	body []byte
}

type ColumnPreparedAssetRegistryView struct {
	data []byte
	body []byte
}

type ColumnAssetReachabilityViewInput struct {
	ActiveManifest                 *ColumnCollectionManifestView
	ProcessVisibleManifests        []ColumnCollectionManifestView
	PendingManifests               []ColumnCollectionManifestView
	RootPublishedManifests         []ColumnCollectionManifestView
	RecoveryAuthoritativeManifests []ColumnCollectionManifestView
	SnapshotPinnedManifests        []ColumnCollectionManifestView
	SupersededManifests            []ColumnCollectionManifestView
	CleanupSafeManifests           []ColumnCollectionManifestView
	PreparedRegistry               *ColumnPreparedAssetRegistryView
	PreparedAssets                 []ColumnPreparedAsset
	QuarantinedAssets              []ColumnPreparedAsset
}

type ColumnAssetReachabilitySummary struct {
	Stats                  ColumnAssetReachabilityStats `json:"stats"`
	RetainedBytes          int                          `json:"retained_bytes"`
	PreparedBytes          int                          `json:"prepared_bytes"`
	ProcessVisibleBytes    int                          `json:"process_visible_bytes"`
	PendingBytes           int                          `json:"pending_bytes"`
	RootPublishedBytes     int                          `json:"root_published_bytes"`
	QuarantinedBytes       int                          `json:"quarantined_bytes"`
	SupersededBytes        int                          `json:"superseded_bytes"`
	CleanupSafeBytes       int                          `json:"cleanup_safe_bytes"`
	SnapshotProtectedBytes int                          `json:"snapshot_protected_bytes"`
	RewriteDebtBytes       int                          `json:"rewrite_debt_bytes"`
	ReclaimableBytes       int                          `json:"reclaimable_bytes"`
}

type ColumnAssetReachabilitySummaryScratch struct {
	records     []columnAssetReachabilitySummaryRecord
	recordIndex map[ColumnAssetRef]int
	segments    map[uint32]columnAssetSegmentState
}

type columnManifestViewAssetRef struct {
	ref           ColumnAssetRef
	bytes         int
	partID        uint64
	generationID  uint64
	manifestBytes int
}

type columnAssetReachabilitySummaryRecord struct {
	fileID      uint32
	state       columnAssetReachabilitySummaryState
	bytes       int
	live        bool
	candidate   bool
	protected   bool
	quarantined bool
}

type columnAssetReachabilitySummaryState uint8

const (
	columnAssetSummaryStateUnknown columnAssetReachabilitySummaryState = iota
	columnAssetSummaryStateDeleting
	columnAssetSummaryStateSuperseded
	columnAssetSummaryStateReclaimable
	columnAssetSummaryStateCleanupSafe
	columnAssetSummaryStatePrepared
	columnAssetSummaryStatePendingPublish
	columnAssetSummaryStateProcessVisible
	columnAssetSummaryStateRootPublished
	columnAssetSummaryStateSnapshotPinned
	columnAssetSummaryStateActive
	columnAssetSummaryStateRecoveryAuthoritative
	columnAssetSummaryStateQuarantined
)

func DecodeColumnCollectionManifestView(data []byte) (ColumnCollectionManifestView, error) {
	if isColumnControlPlaneBinary(data, columnCollectionManifestBinaryMagic) {
		body, err := decodeColumnControlPlaneEnvelope(data, columnCollectionManifestBinaryMagic, columnCollectionManifestBinaryVersion, "collection manifest")
		if err != nil {
			return ColumnCollectionManifestView{}, err
		}
		if err := validateColumnCollectionManifestViewBody(body); err != nil {
			return ColumnCollectionManifestView{}, err
		}
		return ColumnCollectionManifestView{data: data, body: body}, nil
	}
	manifest, err := DecodeColumnCollectionManifest(data)
	if err != nil {
		return ColumnCollectionManifestView{}, err
	}
	encoded, err := encodeColumnCollectionManifestBinaryEnvelope(manifest)
	if err != nil {
		return ColumnCollectionManifestView{}, err
	}
	return DecodeColumnCollectionManifestView(encoded)
}

func DecodeColumnPreparedAssetRegistryView(data []byte) (ColumnPreparedAssetRegistryView, error) {
	if isColumnControlPlaneBinary(data, columnWorkspacePreparedBinaryMagic) {
		body, err := decodeColumnControlPlaneEnvelope(data, columnWorkspacePreparedBinaryMagic, columnWorkspacePreparedBinaryVersion, "prepared registry")
		if err != nil {
			return ColumnPreparedAssetRegistryView{}, err
		}
		if err := validateColumnPreparedAssetRegistryViewBody(body); err != nil {
			return ColumnPreparedAssetRegistryView{}, err
		}
		return ColumnPreparedAssetRegistryView{data: data, body: body}, nil
	}
	registry, err := decodeColumnPreparedAssetRegistryEnvelope(data)
	if err != nil {
		return ColumnPreparedAssetRegistryView{}, err
	}
	encoded, err := encodeColumnPreparedAssetRegistryBinaryEnvelope(registry)
	if err != nil {
		return ColumnPreparedAssetRegistryView{}, err
	}
	return DecodeColumnPreparedAssetRegistryView(encoded)
}

func (v ColumnCollectionManifestView) BodyBytes() int {
	return len(v.body)
}

func (v ColumnPreparedAssetRegistryView) BodyBytes() int {
	return len(v.body)
}

func validateColumnCollectionManifestViewBody(body []byte) error {
	r := columnBinaryReader{data: body, label: "collection manifest view"}
	manifest := readColumnCollectionManifestBinary(&r)
	if err := r.done(); err != nil {
		return err
	}
	return validateColumnCollectionManifest(manifest)
}

func validateColumnPreparedAssetRegistryViewBody(body []byte) error {
	r := columnBinaryReader{data: body, label: "prepared registry view"}
	registry := readColumnPreparedAssetRegistryBinary(&r)
	if err := r.done(); err != nil {
		return err
	}
	return validateColumnPreparedAssetRegistry(registry)
}

func PlanColumnAssetReachabilityFromViews(input ColumnAssetReachabilityViewInput) (ColumnAssetReachabilityPlan, error) {
	estimatedRefs, err := estimateColumnAssetReachabilityViewRefs(input)
	if err != nil {
		return ColumnAssetReachabilityPlan{}, err
	}
	records := make(map[ColumnAssetRef]columnAssetReachabilityRecord, estimatedRefs)
	segments := make(map[uint32]*columnAssetSegmentState)
	plan := ColumnAssetReachabilityPlan{}

	if input.ActiveManifest != nil {
		plan.Stats.ActiveManifests = 1
		if err := addManifestViewAssetRefs(records, segments, *input.ActiveManifest, ColumnAssetStateActive, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
	}
	for _, manifest := range input.ProcessVisibleManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateProcessVisible, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.ProcessVisibleManifests++
	}
	for _, manifest := range input.PendingManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStatePendingPublish, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.PendingManifests++
	}
	for _, manifest := range input.RootPublishedManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateRootPublished, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.RootPublishedManifests++
	}
	for _, manifest := range input.RecoveryAuthoritativeManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateRecoveryAuthoritative, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.RecoveryAuthoritativeManifests++
	}
	for _, manifest := range input.SnapshotPinnedManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateSnapshotPinned, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.SnapshotPinnedManifests++
	}
	for _, manifest := range input.SupersededManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateSuperseded, false, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.SupersededManifests++
	}
	for _, manifest := range input.CleanupSafeManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateCleanupSafe, false, true, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.CleanupSafeManifests++
	}
	if input.PreparedRegistry != nil {
		prepared, err := scanColumnPreparedRegistryViewAssets(*input.PreparedRegistry)
		if err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		for _, asset := range prepared {
			if err := addPreparedAsset(records, segments, asset, ColumnAssetStatePrepared, true, false, &plan.Stats); err != nil {
				return ColumnAssetReachabilityPlan{}, err
			}
			plan.Stats.PreparedAssets++
		}
	}
	for _, prepared := range input.PreparedAssets {
		if err := addPreparedAsset(records, segments, prepared, ColumnAssetStatePrepared, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.PreparedAssets++
	}
	for _, quarantined := range input.QuarantinedAssets {
		if err := addPreparedAsset(records, segments, quarantined, ColumnAssetStateQuarantined, false, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.QuarantinedAssets++
	}
	finalizeColumnAssetReachabilityRecords(records, segments, &plan)
	return plan, nil
}

func PlanColumnAssetReachabilitySummaryFromViews(input ColumnAssetReachabilityViewInput) (ColumnAssetReachabilitySummary, error) {
	return PlanColumnAssetReachabilitySummaryFromViewsWithScratch(input, nil)
}

func PlanColumnAssetReachabilitySummaryFromViewsWithScratch(input ColumnAssetReachabilityViewInput, scratch *ColumnAssetReachabilitySummaryScratch) (ColumnAssetReachabilitySummary, error) {
	estimatedRefs, err := estimateColumnAssetReachabilityViewRefs(input)
	if err != nil {
		return ColumnAssetReachabilitySummary{}, err
	}
	if scratch == nil {
		scratch = &ColumnAssetReachabilitySummaryScratch{}
	}
	scratch.reset(estimatedRefs)

	summary := ColumnAssetReachabilitySummary{}

	if input.ActiveManifest != nil {
		summary.Stats.ActiveManifests = 1
		if err := addManifestViewAssetRefsToSummary(scratch, *input.ActiveManifest, ColumnAssetStateActive, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
	}
	for _, manifest := range input.ProcessVisibleManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStateProcessVisible, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.ProcessVisibleManifests++
	}
	for _, manifest := range input.PendingManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStatePendingPublish, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.PendingManifests++
	}
	for _, manifest := range input.RootPublishedManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStateRootPublished, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.RootPublishedManifests++
	}
	for _, manifest := range input.RecoveryAuthoritativeManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStateRecoveryAuthoritative, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.RecoveryAuthoritativeManifests++
	}
	for _, manifest := range input.SnapshotPinnedManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStateSnapshotPinned, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.SnapshotPinnedManifests++
	}
	for _, manifest := range input.SupersededManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStateSuperseded, false, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.SupersededManifests++
	}
	for _, manifest := range input.CleanupSafeManifests {
		if err := addManifestViewAssetRefsToSummary(scratch, manifest, ColumnAssetStateCleanupSafe, false, true, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.CleanupSafeManifests++
	}
	if input.PreparedRegistry != nil {
		if err := scanColumnPreparedRegistryViewAssetSummaries(*input.PreparedRegistry, func(ref ColumnAssetRef, bytes int) error {
			if err := addPreparedAssetRefToSummary(scratch, ref, bytes, ColumnAssetStatePrepared, true, false, &summary.Stats); err != nil {
				return err
			}
			summary.Stats.PreparedAssets++
			return nil
		}); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
	}
	for _, prepared := range input.PreparedAssets {
		if err := addPreparedAssetRefToSummary(scratch, prepared.Ref, prepared.Bytes, ColumnAssetStatePrepared, true, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.PreparedAssets++
	}
	for _, quarantined := range input.QuarantinedAssets {
		if err := addPreparedAssetRefToSummary(scratch, quarantined.Ref, quarantined.Bytes, ColumnAssetStateQuarantined, false, false, &summary.Stats); err != nil {
			return ColumnAssetReachabilitySummary{}, err
		}
		summary.Stats.QuarantinedAssets++
	}
	finalizeColumnAssetReachabilitySummary(scratch.records, scratch.segments, &summary)
	return summary, nil
}

func (s *ColumnAssetReachabilitySummaryScratch) reset(recordCap int) {
	if cap(s.records) < recordCap {
		s.records = make([]columnAssetReachabilitySummaryRecord, 0, recordCap)
	} else {
		// Records are pointer-free scratch; keep them that way so reset remains
		// a cheap truncate in the repeated GC summary path.
		s.records = s.records[:0]
	}
	if s.recordIndex == nil {
		s.recordIndex = make(map[ColumnAssetRef]int, recordCap)
	} else {
		clear(s.recordIndex)
	}
	if s.segments == nil {
		s.segments = make(map[uint32]columnAssetSegmentState)
	} else {
		clear(s.segments)
	}
}

func estimateColumnAssetReachabilityViewRefs(input ColumnAssetReachabilityViewInput) (int, error) {
	refs := len(input.PreparedAssets) + len(input.QuarantinedAssets)
	if input.ActiveManifest != nil {
		refs += estimateColumnCollectionManifestViewAssetRefs(*input.ActiveManifest)
	}
	for i := range input.ProcessVisibleManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.ProcessVisibleManifests[i])
	}
	for i := range input.PendingManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.PendingManifests[i])
	}
	for i := range input.RootPublishedManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.RootPublishedManifests[i])
	}
	for i := range input.RecoveryAuthoritativeManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.RecoveryAuthoritativeManifests[i])
	}
	for i := range input.SnapshotPinnedManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.SnapshotPinnedManifests[i])
	}
	for i := range input.SupersededManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.SupersededManifests[i])
	}
	for i := range input.CleanupSafeManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.CleanupSafeManifests[i])
	}
	if input.PreparedRegistry != nil {
		n, err := countColumnPreparedRegistryViewAssets(*input.PreparedRegistry)
		if err != nil {
			return 0, err
		}
		refs += n
	}
	return refs, nil
}

func estimateColumnCollectionManifestViewAssetRefs(view ColumnCollectionManifestView) int {
	// The binary body is fixed-record heavy after the schema prefix. This keeps
	// map growth bounded without adding a second full scan to every GC plan.
	estimate := len(view.body) / 256
	if estimate < 1 {
		return 1
	}
	return estimate
}

func addManifestViewAssetRefs(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, view ColumnCollectionManifestView, state ColumnAssetLifecycleState, live bool, candidate bool, stats *ColumnAssetReachabilityStats) error {
	reasons := columnAssetReachabilityDefaultReasons(state)
	tombstones, err := scanColumnCollectionManifestViewAssetRefs(view, func(ref columnManifestViewAssetRef) error {
		entry := ColumnAssetReachabilityEntry{
			Ref:          ref.ref,
			State:        state,
			Bytes:        ref.bytes,
			PartID:       ref.partID,
			GenerationID: ref.generationID,
			Reasons:      reasons,
		}
		if err := addReachabilityEntry(records, segments, entry, live, candidate, false); err != nil {
			return err
		}
		if stats != nil {
			stats.MetadataBytesScanned += ref.manifestBytes
		}
		return nil
	})
	if err != nil {
		return err
	}
	if stats != nil {
		stats.MetadataBytesScanned += tombstones
	}
	return nil
}

func addManifestViewAssetRefsToSummary(scratch *ColumnAssetReachabilitySummaryScratch, view ColumnCollectionManifestView, state ColumnAssetLifecycleState, live bool, candidate bool, stats *ColumnAssetReachabilityStats) error {
	tombstones, err := scanColumnCollectionManifestViewAssetRefs(view, func(ref columnManifestViewAssetRef) error {
		if err := scratch.addAssetRefToReachabilitySummary(ref.ref, ref.bytes, state, live, candidate, false); err != nil {
			return err
		}
		if stats != nil {
			stats.MetadataBytesScanned += ref.manifestBytes
		}
		return nil
	})
	if err != nil {
		return err
	}
	if stats != nil {
		stats.MetadataBytesScanned += tombstones
	}
	return nil
}

func addPreparedAssetRefToSummary(scratch *ColumnAssetReachabilitySummaryScratch, ref ColumnAssetRef, bytes int, state ColumnAssetLifecycleState, live bool, candidate bool, stats *ColumnAssetReachabilityStats) error {
	if bytes == 0 {
		if ref.Length > int64(^uint(0)>>1) {
			return fmt.Errorf("colgranule: prepared asset length=%d exceeds host int", ref.Length)
		}
		bytes = int(ref.Length)
	}
	if err := scratch.addAssetRefToReachabilitySummary(ref, bytes, state, live, candidate, state == ColumnAssetStateQuarantined); err != nil {
		return err
	}
	if stats != nil {
		stats.MetadataBytesScanned++
	}
	return nil
}

func (s *ColumnAssetReachabilitySummaryScratch) addAssetRefToReachabilitySummary(ref ColumnAssetRef, bytes int, state ColumnAssetLifecycleState, live bool, candidate bool, quarantined bool) error {
	if bytes < 0 {
		return fmt.Errorf("colgranule: negative asset bytes %d", bytes)
	}
	if err := validateColumnAssetRef(ref); err != nil {
		return err
	}
	if index := s.recordIndex[ref]; index > 0 {
		record := &s.records[index-1]
		record.state = strongestColumnAssetReachabilitySummaryState(record.state, state)
		if record.bytes == 0 {
			record.bytes = bytes
		}
		record.live = record.live || live
		record.candidate = record.candidate || candidate
		record.protected = record.protected || columnAssetRefBlocksSegmentDeletion(live, candidate, quarantined)
		record.quarantined = record.quarantined || quarantined
	} else {
		s.records = append(s.records, columnAssetReachabilitySummaryRecord{
			fileID:    ref.FileID,
			state:     columnAssetReachabilitySummaryStateFromLifecycle(state),
			bytes:     bytes,
			live:      live,
			protected: columnAssetRefBlocksSegmentDeletion(live, candidate, quarantined),
		})
		record := &s.records[len(s.records)-1]
		record.candidate = candidate
		record.quarantined = quarantined
		s.recordIndex[ref] = len(s.records)
	}
	if _, ok := s.segments[ref.FileID]; !ok {
		s.segments[ref.FileID] = columnAssetSegmentState{}
	}
	return nil
}

func finalizeColumnAssetReachabilitySummary(records []columnAssetReachabilitySummaryRecord, segments map[uint32]columnAssetSegmentState, summary *ColumnAssetReachabilitySummary) {
	summary.Stats.Manifests = columnAssetReachabilityManifestCount(summary.Stats)
	summary.Stats.AssetRefs = len(records)
	for fileID, seg := range segments {
		seg.liveRefs = 0
		seg.candidateRefs = 0
		seg.protectedRefs = 0
		segments[fileID] = seg
	}
	for _, record := range records {
		seg := segments[record.fileID]
		if record.live {
			seg.liveRefs++
		}
		if record.candidate && !record.live && !record.quarantined {
			seg.candidateRefs++
		}
		if record.protected {
			seg.protectedRefs++
		}
		segments[record.fileID] = seg
	}
	summary.Stats.SegmentRefs = len(segments)
	for _, seg := range segments {
		if seg.liveRefs == 0 && seg.protectedRefs == 0 && seg.candidateRefs > 0 {
			summary.Stats.DirectlyDeletableSegments++
		}
		if (seg.liveRefs > 0 || seg.protectedRefs > 0) && seg.candidateRefs > 0 {
			summary.Stats.MixedLiveDeadSegments++
		}
	}
	for _, record := range records {
		switch {
		case record.quarantined:
			summary.QuarantinedBytes += record.bytes
		case record.live:
			if record.state == columnAssetSummaryStateActive || record.state == columnAssetSummaryStateRecoveryAuthoritative {
				summary.RetainedBytes += record.bytes
			} else {
				switch record.state {
				case columnAssetSummaryStatePrepared:
					summary.PreparedBytes += record.bytes
				case columnAssetSummaryStateProcessVisible:
					summary.ProcessVisibleBytes += record.bytes
				case columnAssetSummaryStatePendingPublish:
					summary.PendingBytes += record.bytes
				case columnAssetSummaryStateRootPublished:
					summary.RootPublishedBytes += record.bytes
				case columnAssetSummaryStateSnapshotPinned:
					summary.SnapshotProtectedBytes += record.bytes
					summary.RetainedBytes += record.bytes
				default:
					summary.RetainedBytes += record.bytes
				}
			}
		case record.candidate:
			summary.CleanupSafeBytes += record.bytes
			seg := segments[record.fileID]
			if seg.liveRefs == 0 && seg.protectedRefs == 0 {
				summary.ReclaimableBytes += record.bytes
			} else {
				summary.RewriteDebtBytes += record.bytes
			}
		case record.state == columnAssetSummaryStateSuperseded:
			summary.SupersededBytes += record.bytes
		}
	}
}

func strongestColumnAssetReachabilitySummaryState(current columnAssetReachabilitySummaryState, next ColumnAssetLifecycleState) columnAssetReachabilitySummaryState {
	nextState := columnAssetReachabilitySummaryStateFromLifecycle(next)
	if nextState > current {
		return nextState
	}
	return current
}

func columnAssetReachabilitySummaryStateFromLifecycle(state ColumnAssetLifecycleState) columnAssetReachabilitySummaryState {
	switch state {
	case ColumnAssetStateQuarantined:
		return columnAssetSummaryStateQuarantined
	case ColumnAssetStateRecoveryAuthoritative:
		return columnAssetSummaryStateRecoveryAuthoritative
	case ColumnAssetStateActive:
		return columnAssetSummaryStateActive
	case ColumnAssetStateSnapshotPinned:
		return columnAssetSummaryStateSnapshotPinned
	case ColumnAssetStateRootPublished:
		return columnAssetSummaryStateRootPublished
	case ColumnAssetStateProcessVisible:
		return columnAssetSummaryStateProcessVisible
	case ColumnAssetStatePendingPublish:
		return columnAssetSummaryStatePendingPublish
	case ColumnAssetStatePrepared:
		return columnAssetSummaryStatePrepared
	case ColumnAssetStateCleanupSafe:
		return columnAssetSummaryStateCleanupSafe
	case ColumnAssetStateReclaimable:
		return columnAssetSummaryStateReclaimable
	case ColumnAssetStateSuperseded:
		return columnAssetSummaryStateSuperseded
	case ColumnAssetStateDeleting:
		return columnAssetSummaryStateDeleting
	default:
		return columnAssetSummaryStateUnknown
	}
}

func finalizeColumnAssetReachabilityRecords(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, plan *ColumnAssetReachabilityPlan) {
	plan.Stats.Manifests = columnAssetReachabilityManifestCount(plan.Stats)
	plan.Stats.AssetRefs = len(records)
	for _, seg := range segments {
		seg.liveRefs = 0
		seg.candidateRefs = 0
		seg.protectedRefs = 0
	}
	for _, record := range records {
		seg := segments[record.entry.Ref.FileID]
		if seg == nil {
			seg = &columnAssetSegmentState{}
			segments[record.entry.Ref.FileID] = seg
		}
		if record.live {
			seg.liveRefs++
		}
		if record.candidate && !record.live && !record.quarantined {
			seg.candidateRefs++
		}
		if record.protected {
			seg.protectedRefs++
		}
	}
	plan.Stats.SegmentRefs = len(segments)
	for _, seg := range segments {
		if seg.liveRefs == 0 && seg.protectedRefs == 0 && seg.candidateRefs > 0 {
			plan.Stats.DirectlyDeletableSegments++
		}
		if (seg.liveRefs > 0 || seg.protectedRefs > 0) && seg.candidateRefs > 0 {
			plan.Stats.MixedLiveDeadSegments++
		}
	}

	plan.Entries = make([]ColumnAssetReachabilityEntry, 0, len(records))
	for _, record := range records {
		entry := record.entry
		switch {
		case record.quarantined:
			entry.State = ColumnAssetStateQuarantined
			plan.QuarantinedBytes += entry.Bytes
		case record.live:
			if entry.State == ColumnAssetStateActive || entry.State == ColumnAssetStateRecoveryAuthoritative {
				plan.RetainedBytes += entry.Bytes
			} else {
				switch entry.State {
				case ColumnAssetStatePrepared:
					plan.PreparedBytes += entry.Bytes
				case ColumnAssetStateProcessVisible:
					plan.ProcessVisibleBytes += entry.Bytes
				case ColumnAssetStatePendingPublish:
					plan.PendingBytes += entry.Bytes
				case ColumnAssetStateRootPublished:
					plan.RootPublishedBytes += entry.Bytes
				case ColumnAssetStateSnapshotPinned:
					plan.SnapshotProtectedBytes += entry.Bytes
					plan.RetainedBytes += entry.Bytes
				default:
					plan.RetainedBytes += entry.Bytes
				}
			}
		case record.candidate:
			plan.CleanupSafeBytes += entry.Bytes
			seg := segments[entry.Ref.FileID]
			if seg != nil && seg.liveRefs == 0 && seg.protectedRefs == 0 {
				entry.DeleteEligible = true
				entry.State = ColumnAssetStateReclaimable
				plan.ReclaimableBytes += entry.Bytes
			} else {
				plan.RewriteDebtBytes += entry.Bytes
			}
		case entry.State == ColumnAssetStateSuperseded:
			plan.SupersededBytes += entry.Bytes
		}
		plan.Entries = append(plan.Entries, entry)
	}
	sort.Slice(plan.Entries, func(i, j int) bool {
		a, b := plan.Entries[i].Ref, plan.Entries[j].Ref
		if a.FileID != b.FileID {
			return a.FileID < b.FileID
		}
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		if a.Length != b.Length {
			return a.Length < b.Length
		}
		return a.Checksum < b.Checksum
	})
}

func columnAssetReachabilityManifestCount(stats ColumnAssetReachabilityStats) int {
	return stats.ActiveManifests +
		stats.ProcessVisibleManifests +
		stats.PendingManifests +
		stats.RootPublishedManifests +
		stats.RecoveryAuthoritativeManifests +
		stats.SnapshotPinnedManifests +
		stats.SupersededManifests +
		stats.CleanupSafeManifests
}

func countColumnCollectionManifestViewAssetRefs(view ColumnCollectionManifestView) (int, error) {
	var count int
	_, err := scanColumnCollectionManifestViewAssetRefs(view, func(columnManifestViewAssetRef) error {
		count++
		return nil
	})
	return count, err
}

func scanColumnCollectionManifestViewAssetRefs(view ColumnCollectionManifestView, fn func(columnManifestViewAssetRef) error) (int, error) {
	if len(view.body) == 0 {
		return 0, fmt.Errorf("colgranule: empty collection manifest view")
	}
	r := columnBinaryReader{data: view.body, label: "collection manifest view"}
	r.skipString()
	r.u32()
	r.u8()
	r.skipStringSlice()
	skipSortKeyColumnsBinary(&r)
	skipColumnDefinitionsBinary(&r)
	r.u64()
	skipColumnCollectionAttachmentBinary(&r)
	tombstones := scanColumnPartSetManifestBinaryView(&r, fn)
	skipColumnCollectionByteAccountingBinary(&r)
	r.i64()
	r.i64()
	if err := r.done(); err != nil {
		return 0, err
	}
	return tombstones, nil
}

func scanColumnPartSetManifestBinaryView(r *columnBinaryReader, fn func(columnManifestViewAssetRef) error) int {
	baseCount := r.count("base parts")
	for i := 0; i < baseCount; i++ {
		scanColumnManifestPartRefBinaryView(r, fn)
	}
	deltaCount := r.count("delta parts")
	for i := 0; i < deltaCount; i++ {
		scanColumnManifestPartRefBinaryView(r, fn)
	}
	tombstoneCount := r.count("tombstones")
	for i := 0; i < tombstoneCount; i++ {
		skipColumnTombstoneBinary(r)
	}
	return tombstoneCount
}

func scanColumnManifestPartRefBinaryView(r *columnBinaryReader, fn func(columnManifestViewAssetRef) error) {
	r.u8()
	generationID := r.u64()
	skipColumnPartCoverageDescriptorBinary(r)
	ref := scanColumnWorkspacePartManifestBinaryView(r)
	ref.generationID = generationID
	if fn != nil && r.err == nil {
		if err := fn(ref); err != nil {
			r.err = err
		}
	}
}

func scanColumnWorkspacePartManifestBinaryView(r *columnBinaryReader) columnManifestViewAssetRef {
	ref := columnManifestViewAssetRef{partID: r.u64()}
	r.intValue("part rows")
	r.intValue("part visible rows")
	r.u32()
	skipSortKeyColumnsBinary(r)
	skipColumnWorkspacePartCoverageBinary(r)
	ref.ref = readColumnAssetRefBinary(r)
	skipTCS1PartRecordBinary(r)
	r.intValue("image bytes")
	ref.manifestBytes = r.intValue("manifest bytes")
	r.intValue("sections")
	ref.bytes = r.intValue("asset bytes")
	r.i64()
	return ref
}

func countColumnPreparedRegistryViewAssets(view ColumnPreparedAssetRegistryView) (int, error) {
	if len(view.body) == 0 {
		return 0, fmt.Errorf("colgranule: empty prepared registry view")
	}
	r := columnBinaryReader{data: view.body, label: "prepared registry view"}
	r.skipString()
	r.u64()
	r.u64()
	r.i64()
	count := r.count("prepared assets")
	if err := r.doneAfterSkippingPreparedAssets(count); err != nil {
		return 0, err
	}
	return count, nil
}

func scanColumnPreparedRegistryViewAssets(view ColumnPreparedAssetRegistryView) ([]ColumnPreparedAsset, error) {
	if len(view.body) == 0 {
		return nil, fmt.Errorf("colgranule: empty prepared registry view")
	}
	r := columnBinaryReader{data: view.body, label: "prepared registry view"}
	r.skipString()
	r.u64()
	r.u64()
	r.i64()
	n := r.count("prepared assets")
	assets := make([]ColumnPreparedAsset, n)
	for i := range assets {
		assets[i] = readColumnPreparedAssetBinary(&r)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return assets, nil
}

func scanColumnPreparedRegistryViewAssetSummaries(view ColumnPreparedAssetRegistryView, fn func(ColumnAssetRef, int) error) error {
	if len(view.body) == 0 {
		return fmt.Errorf("colgranule: empty prepared registry view")
	}
	r := columnBinaryReader{data: view.body, label: "prepared registry view"}
	r.skipString()
	r.u64()
	r.u64()
	r.i64()
	n := r.count("prepared assets")
	for i := 0; i < n; i++ {
		ref := readColumnAssetRefBinary(&r)
		bytes := r.intValue("prepared bytes")
		r.u64()
		r.u64()
		r.skipString()
		if fn != nil && r.err == nil {
			if err := fn(ref, bytes); err != nil {
				r.err = err
			}
		}
	}
	return r.done()
}

func (r *columnBinaryReader) doneAfterSkippingPreparedAssets(count int) error {
	for i := 0; i < count; i++ {
		skipColumnPreparedAssetBinary(r)
	}
	return r.done()
}

func skipColumnCollectionAttachmentBinary(r *columnBinaryReader) {
	r.skipString()
	r.skipString()
	r.skipString()
	r.skipString()
	r.skipString()
	r.skipStringSlice()
}

func skipColumnPartCoverageDescriptorBinary(r *columnBinaryReader) {
	r.u8()
	r.u64()
	r.u8()
	sourceCount := r.count("source parts")
	for i := 0; i < sourceCount; i++ {
		r.u64()
		r.u64()
	}
	r.u64()
	r.u64()
	r.u64()
	r.i64()
	r.i64()
	r.skipStringSlice()
	r.skipI64Slice()
	r.skipI64Slice()
	r.bool()
	r.intValue("coverage rows")
	r.intValue("coverage visible rows")
	r.intValue("coverage deleted rows")
	assetCount := r.count("coverage asset refs")
	for i := 0; i < assetCount; i++ {
		readColumnAssetRefBinary(r)
	}
	r.skipU32Slice()
}

func skipColumnWorkspacePartCoverageBinary(r *columnBinaryReader) {
	r.i64()
	r.i64()
	r.skipStringSlice()
	r.skipI64Slice()
	r.skipI64Slice()
	r.bool()
}

func skipTCS1PartRecordBinary(r *columnBinaryReader) {
	r.u16()
	r.u16()
	r.u32()
	r.u64()
	r.intValue("tcs1 rows")
	r.u16()
	r.intValue("payload bytes")
	r.intValue("total bytes")
	r.u32()
	readColumnAssetRefBinary(r)
}

func skipColumnTombstoneBinary(r *columnBinaryReader) {
	r.i64()
	r.u64()
	r.skipString()
	r.intValue("prepared bytes")
}

func skipColumnPreparedAssetBinary(r *columnBinaryReader) {
	readColumnAssetRefBinary(r)
	r.intValue("prepared bytes")
	r.u64()
	r.u64()
	r.skipString()
}

func skipSortKeyColumnsBinary(r *columnBinaryReader) {
	n := r.count("sort keys")
	for i := 0; i < n; i++ {
		r.skipString()
		r.u8()
		r.u8()
	}
}

func skipColumnDefinitionsBinary(r *columnBinaryReader) {
	n := r.count("column definitions")
	for i := 0; i < n; i++ {
		r.skipString()
		r.u8()
		r.u8()
		r.u8()
		r.u32()
		r.intValue("codec block rows")
	}
}

func skipColumnCollectionByteAccountingBinary(r *columnBinaryReader) {
	for i := 0; i < 12; i++ {
		r.intValue("byte accounting")
	}
}

func (r *columnBinaryReader) skipString() {
	n := r.u32()
	if uint64(n) > uint64(len(r.data)-r.off) {
		r.fail("string length=%d exceeds remaining body bytes=%d", n, len(r.data)-r.off)
		return
	}
	r.bytes(int(n))
}

func (r *columnBinaryReader) skipStringSlice() {
	n := r.count("string slice")
	for i := 0; i < n; i++ {
		r.skipString()
	}
}

func (r *columnBinaryReader) skipI64Slice() {
	n := r.count("int64 slice")
	for i := 0; i < n; i++ {
		r.i64()
	}
}

func (r *columnBinaryReader) skipU32Slice() {
	n := r.count("uint32 slice")
	for i := 0; i < n; i++ {
		r.u32()
	}
}
