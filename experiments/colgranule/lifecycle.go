package colgranule

import (
	"fmt"
	"sort"
)

type ColumnAssetLifecycleState string

const (
	ColumnAssetStatePrepared              ColumnAssetLifecycleState = "prepared"
	ColumnAssetStateProcessVisible        ColumnAssetLifecycleState = "process_visible"
	ColumnAssetStatePendingPublish        ColumnAssetLifecycleState = "pending_publish"
	ColumnAssetStateRootPublished         ColumnAssetLifecycleState = "root_published"
	ColumnAssetStateRecoveryAuthoritative ColumnAssetLifecycleState = "recovery_authoritative"
	ColumnAssetStateActive                ColumnAssetLifecycleState = "active"
	ColumnAssetStateSuperseded            ColumnAssetLifecycleState = "superseded"
	ColumnAssetStateCleanupSafe           ColumnAssetLifecycleState = "cleanup_safe"
	ColumnAssetStateSnapshotPinned        ColumnAssetLifecycleState = "snapshot_pinned"
	ColumnAssetStateReclaimable           ColumnAssetLifecycleState = "reclaimable"
	ColumnAssetStateDeleting              ColumnAssetLifecycleState = "deleting"
	ColumnAssetStateQuarantined           ColumnAssetLifecycleState = "quarantined"
)

type ColumnPreparedAsset struct {
	Ref          ColumnAssetRef `json:"ref"`
	Bytes        int            `json:"bytes"`
	PublishID    uint64         `json:"publish_id,omitempty"`
	GenerationID uint64         `json:"generation_id,omitempty"`
	Reason       string         `json:"reason,omitempty"`
}

type ColumnAssetReachabilityInput struct {
	ActiveManifest                 *ColumnCollectionManifest  `json:"active_manifest,omitempty"`
	ProcessVisibleManifests        []ColumnCollectionManifest `json:"process_visible_manifests,omitempty"`
	PendingManifests               []ColumnCollectionManifest `json:"pending_manifests,omitempty"`
	RootPublishedManifests         []ColumnCollectionManifest `json:"root_published_manifests,omitempty"`
	RecoveryAuthoritativeManifests []ColumnCollectionManifest `json:"recovery_authoritative_manifests,omitempty"`
	SnapshotPinnedManifests        []ColumnCollectionManifest `json:"snapshot_pinned_manifests,omitempty"`
	SupersededManifests            []ColumnCollectionManifest `json:"superseded_manifests,omitempty"`
	CleanupSafeManifests           []ColumnCollectionManifest `json:"cleanup_safe_manifests,omitempty"`
	PreparedAssets                 []ColumnPreparedAsset      `json:"prepared_assets,omitempty"`
	QuarantinedAssets              []ColumnPreparedAsset      `json:"quarantined_assets,omitempty"`
}

type ColumnAssetReachabilityPlan struct {
	Entries                []ColumnAssetReachabilityEntry `json:"entries"`
	Stats                  ColumnAssetReachabilityStats   `json:"stats"`
	RetainedBytes          int                            `json:"retained_bytes"`
	PreparedBytes          int                            `json:"prepared_bytes"`
	ProcessVisibleBytes    int                            `json:"process_visible_bytes"`
	PendingBytes           int                            `json:"pending_bytes"`
	RootPublishedBytes     int                            `json:"root_published_bytes"`
	QuarantinedBytes       int                            `json:"quarantined_bytes"`
	SupersededBytes        int                            `json:"superseded_bytes"`
	CleanupSafeBytes       int                            `json:"cleanup_safe_bytes"`
	SnapshotProtectedBytes int                            `json:"snapshot_protected_bytes"`
	RewriteDebtBytes       int                            `json:"rewrite_debt_bytes"`
	ReclaimableBytes       int                            `json:"reclaimable_bytes"`
}

type ColumnAssetReachabilityStats struct {
	Manifests                      int `json:"manifests"`
	ActiveManifests                int `json:"active_manifests"`
	ProcessVisibleManifests        int `json:"process_visible_manifests"`
	PendingManifests               int `json:"pending_manifests"`
	RootPublishedManifests         int `json:"root_published_manifests"`
	RecoveryAuthoritativeManifests int `json:"recovery_authoritative_manifests"`
	SnapshotPinnedManifests        int `json:"snapshot_pinned_manifests"`
	SupersededManifests            int `json:"superseded_manifests"`
	CleanupSafeManifests           int `json:"cleanup_safe_manifests"`
	PreparedAssets                 int `json:"prepared_assets"`
	QuarantinedAssets              int `json:"quarantined_assets"`
	AssetRefs                      int `json:"asset_refs"`
	SegmentRefs                    int `json:"segment_refs"`
	MetadataBytesScanned           int `json:"metadata_bytes_scanned"`
	TCS1PayloadBytesDecoded        int `json:"tcs1_payload_bytes_decoded"`
	RowsScanned                    int `json:"rows_scanned"`
	ColumnPayloadBlocksRead        int `json:"column_payload_blocks_read"`
	DirectlyDeletableSegments      int `json:"directly_deletable_segments"`
	MixedLiveDeadSegments          int `json:"mixed_live_dead_segments"`
}

type ColumnAssetReachabilityEntry struct {
	Ref            ColumnAssetRef            `json:"ref"`
	State          ColumnAssetLifecycleState `json:"state"`
	Bytes          int                       `json:"bytes"`
	PartID         uint64                    `json:"part_id,omitempty"`
	GenerationID   uint64                    `json:"generation_id,omitempty"`
	DeleteEligible bool                      `json:"delete_eligible,omitempty"`
	Reasons        []string                  `json:"reasons,omitempty"`
}

type ColumnAssetRefDeltaKind string

const (
	ColumnAssetRefDeltaPublished  ColumnAssetRefDeltaKind = "published"
	ColumnAssetRefDeltaSuperseded ColumnAssetRefDeltaKind = "superseded"
	ColumnAssetRefDeltaPrepared   ColumnAssetRefDeltaKind = "prepared"
)

type ColumnAssetRefDeltaInput struct {
	PublishedParts  []ColumnManifestPartRef `json:"published_parts,omitempty"`
	SupersededParts []ColumnManifestPartRef `json:"superseded_parts,omitempty"`
	PreparedAssets  []ColumnPreparedAsset   `json:"prepared_assets,omitempty"`
}

type ColumnAssetRefDeltaPlan struct {
	Entries         []ColumnAssetRefDeltaEntry   `json:"entries"`
	Stats           ColumnAssetReachabilityStats `json:"stats"`
	PublishedBytes  int                          `json:"published_bytes"`
	SupersededBytes int                          `json:"superseded_bytes"`
	PreparedBytes   int                          `json:"prepared_bytes"`
}

type ColumnAssetRefDeltaEntry struct {
	Kind         ColumnAssetRefDeltaKind `json:"kind"`
	Ref          ColumnAssetRef          `json:"ref"`
	Bytes        int                     `json:"bytes"`
	PartID       uint64                  `json:"part_id,omitempty"`
	GenerationID uint64                  `json:"generation_id,omitempty"`
}

type columnAssetReachabilityRecord struct {
	entry       ColumnAssetReachabilityEntry
	live        bool
	candidate   bool
	protected   bool
	quarantined bool
}

var (
	// Shared one-element reason slices are immutable sentinels. Internal
	// builders may carry them, but addReachabilityEntry must clone before any
	// exported reachability entry observes them.
	columnAssetReasonPrepared              = []string{string(ColumnAssetStatePrepared)}
	columnAssetReasonProcessVisible        = []string{string(ColumnAssetStateProcessVisible)}
	columnAssetReasonPendingPublish        = []string{string(ColumnAssetStatePendingPublish)}
	columnAssetReasonRootPublished         = []string{string(ColumnAssetStateRootPublished)}
	columnAssetReasonRecoveryAuthoritative = []string{string(ColumnAssetStateRecoveryAuthoritative)}
	columnAssetReasonActive                = []string{string(ColumnAssetStateActive)}
	columnAssetReasonSuperseded            = []string{string(ColumnAssetStateSuperseded)}
	columnAssetReasonCleanupSafe           = []string{string(ColumnAssetStateCleanupSafe)}
	columnAssetReasonSnapshotPinned        = []string{string(ColumnAssetStateSnapshotPinned)}
	columnAssetReasonReclaimable           = []string{string(ColumnAssetStateReclaimable)}
	columnAssetReasonDeleting              = []string{string(ColumnAssetStateDeleting)}
	columnAssetReasonQuarantined           = []string{string(ColumnAssetStateQuarantined)}
)

func PlanColumnAssetReachability(input ColumnAssetReachabilityInput) (ColumnAssetReachabilityPlan, error) {
	records := make(map[ColumnAssetRef]columnAssetReachabilityRecord, estimateColumnAssetReachabilityRefs(input))
	segments := make(map[uint32]*columnAssetSegmentState)
	plan := ColumnAssetReachabilityPlan{}

	if input.ActiveManifest != nil {
		plan.Stats.ActiveManifests = 1
		if err := addManifestAssetRefs(records, segments, *input.ActiveManifest, ColumnAssetStateActive, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
	}
	for _, manifest := range input.ProcessVisibleManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateProcessVisible, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.ProcessVisibleManifests++
	}
	for _, manifest := range input.PendingManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStatePendingPublish, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.PendingManifests++
	}
	for _, manifest := range input.RootPublishedManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateRootPublished, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.RootPublishedManifests++
	}
	for _, manifest := range input.RecoveryAuthoritativeManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateRecoveryAuthoritative, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.RecoveryAuthoritativeManifests++
	}
	for _, manifest := range input.SnapshotPinnedManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateSnapshotPinned, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.SnapshotPinnedManifests++
	}
	for _, manifest := range input.SupersededManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateSuperseded, false, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.SupersededManifests++
	}
	for _, manifest := range input.CleanupSafeManifests {
		if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateCleanupSafe, false, true, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.CleanupSafeManifests++
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

func PlanColumnAssetRefDelta(input ColumnAssetRefDeltaInput) (ColumnAssetRefDeltaPlan, error) {
	plan := ColumnAssetRefDeltaPlan{}
	plan.Entries = make([]ColumnAssetRefDeltaEntry, 0, len(input.PublishedParts)+len(input.SupersededParts)+len(input.PreparedAssets))
	for _, ref := range input.PublishedParts {
		entry, err := columnAssetRefDeltaEntry(ColumnAssetRefDeltaPublished, ref)
		if err != nil {
			return ColumnAssetRefDeltaPlan{}, err
		}
		plan.PublishedBytes += entry.Bytes
		plan.Stats.AssetRefs++
		plan.Stats.MetadataBytesScanned += ref.Part.ManifestBytes
		plan.Entries = append(plan.Entries, entry)
	}
	for _, ref := range input.SupersededParts {
		entry, err := columnAssetRefDeltaEntry(ColumnAssetRefDeltaSuperseded, ref)
		if err != nil {
			return ColumnAssetRefDeltaPlan{}, err
		}
		plan.SupersededBytes += entry.Bytes
		plan.Stats.AssetRefs++
		plan.Stats.MetadataBytesScanned += ref.Part.ManifestBytes
		plan.Entries = append(plan.Entries, entry)
	}
	for _, prepared := range input.PreparedAssets {
		if err := validateColumnAssetRef(prepared.Ref); err != nil {
			return ColumnAssetRefDeltaPlan{}, err
		}
		bytes := prepared.Bytes
		if bytes == 0 {
			if prepared.Ref.Length > int64(^uint(0)>>1) {
				return ColumnAssetRefDeltaPlan{}, fmt.Errorf("colgranule: prepared asset length=%d exceeds host int", prepared.Ref.Length)
			}
			bytes = int(prepared.Ref.Length)
		}
		plan.PreparedBytes += bytes
		plan.Stats.PreparedAssets++
		plan.Stats.AssetRefs++
		plan.Stats.MetadataBytesScanned++
		plan.Entries = append(plan.Entries, ColumnAssetRefDeltaEntry{
			Kind:         ColumnAssetRefDeltaPrepared,
			Ref:          prepared.Ref,
			Bytes:        bytes,
			GenerationID: prepared.GenerationID,
		})
	}
	sort.Slice(plan.Entries, func(i, j int) bool {
		a, b := plan.Entries[i], plan.Entries[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Ref.FileID != b.Ref.FileID {
			return a.Ref.FileID < b.Ref.FileID
		}
		if a.Ref.Offset != b.Ref.Offset {
			return a.Ref.Offset < b.Ref.Offset
		}
		return a.Ref.Checksum < b.Ref.Checksum
	})
	return plan, nil
}

func ColumnCollectionManifestAssetRefs(manifest ColumnCollectionManifest) ([]ColumnAssetReachabilityEntry, error) {
	stats := ColumnAssetReachabilityStats{}
	records := make(map[ColumnAssetRef]columnAssetReachabilityRecord, len(manifest.PartSet.BaseParts)+len(manifest.PartSet.DeltaParts))
	segments := make(map[uint32]*columnAssetSegmentState)
	if err := addManifestAssetRefs(records, segments, manifest, ColumnAssetStateActive, true, false, &stats); err != nil {
		return nil, err
	}
	out := make([]ColumnAssetReachabilityEntry, 0, len(records))
	for _, record := range records {
		out = append(out, record.entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PartID != out[j].PartID {
			return out[i].PartID < out[j].PartID
		}
		return out[i].Ref.Offset < out[j].Ref.Offset
	})
	return out, nil
}

func columnAssetRefDeltaEntry(kind ColumnAssetRefDeltaKind, ref ColumnManifestPartRef) (ColumnAssetRefDeltaEntry, error) {
	if err := validateColumnManifestPartRef(ref, ref.Role); err != nil {
		return ColumnAssetRefDeltaEntry{}, err
	}
	return ColumnAssetRefDeltaEntry{
		Kind:         kind,
		Ref:          ref.Part.AssetRef,
		Bytes:        ref.Part.AssetBytes,
		PartID:       ref.Part.PartID,
		GenerationID: ref.GenerationID,
	}, nil
}

type columnAssetSegmentState struct {
	liveRefs      int
	candidateRefs int
	protectedRefs int
}

func addManifestAssetRefs(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, manifest ColumnCollectionManifest, state ColumnAssetLifecycleState, live bool, candidate bool, stats *ColumnAssetReachabilityStats) error {
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return err
	}
	reasons := columnAssetReachabilityDefaultReasons(state)
	for i := range manifest.PartSet.BaseParts {
		if err := addManifestPartAssetRef(records, segments, manifest.PartSet.BaseParts[i], state, live, candidate, reasons, stats); err != nil {
			return err
		}
	}
	for i := range manifest.PartSet.DeltaParts {
		if err := addManifestPartAssetRef(records, segments, manifest.PartSet.DeltaParts[i], state, live, candidate, reasons, stats); err != nil {
			return err
		}
	}
	if stats != nil {
		stats.MetadataBytesScanned += len(manifest.PartSet.Tombstones)
	}
	return nil
}

func addManifestPartAssetRef(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, partRef ColumnManifestPartRef, state ColumnAssetLifecycleState, live bool, candidate bool, reasons []string, stats *ColumnAssetReachabilityStats) error {
	entry := ColumnAssetReachabilityEntry{
		Ref:          partRef.Part.AssetRef,
		State:        state,
		Bytes:        partRef.Part.AssetBytes,
		PartID:       partRef.Part.PartID,
		GenerationID: partRef.GenerationID,
		Reasons:      reasons,
	}
	if err := addReachabilityEntry(records, segments, entry, live, candidate, false); err != nil {
		return err
	}
	if stats != nil {
		stats.MetadataBytesScanned += partRef.Part.ManifestBytes
	}
	return nil
}

func addPreparedAsset(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, prepared ColumnPreparedAsset, state ColumnAssetLifecycleState, live bool, candidate bool, stats *ColumnAssetReachabilityStats) error {
	if err := validateColumnAssetRef(prepared.Ref); err != nil {
		return err
	}
	bytes := prepared.Bytes
	if bytes == 0 {
		if prepared.Ref.Length > int64(^uint(0)>>1) {
			return fmt.Errorf("colgranule: prepared asset length=%d exceeds host int", prepared.Ref.Length)
		}
		bytes = int(prepared.Ref.Length)
	}
	entry := ColumnAssetReachabilityEntry{
		Ref:          prepared.Ref,
		State:        state,
		Bytes:        bytes,
		GenerationID: prepared.GenerationID,
		Reasons:      []string{prepared.Reason},
	}
	if prepared.Reason == "" {
		entry.Reasons = columnAssetReachabilityDefaultReasons(state)
	}
	if err := addReachabilityEntry(records, segments, entry, live, candidate, state == ColumnAssetStateQuarantined); err != nil {
		return err
	}
	if stats != nil {
		stats.MetadataBytesScanned++
	}
	return nil
}

func addReachabilityEntry(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, entry ColumnAssetReachabilityEntry, live bool, candidate bool, quarantined bool) error {
	if entry.Bytes < 0 {
		return fmt.Errorf("colgranule: negative asset bytes %d", entry.Bytes)
	}
	if err := validateColumnAssetRef(entry.Ref); err != nil {
		return err
	}
	entry.Reasons = cloneColumnAssetReachabilityReasons(entry.Reasons)
	record, ok := records[entry.Ref]
	if !ok {
		record = columnAssetReachabilityRecord{entry: entry}
	} else {
		record.entry.State = strongestColumnAssetState(record.entry.State, entry.State)
		if record.entry.Bytes == 0 {
			record.entry.Bytes = entry.Bytes
		}
		if record.entry.PartID == 0 {
			record.entry.PartID = entry.PartID
		}
		if record.entry.GenerationID == 0 || entry.GenerationID > record.entry.GenerationID {
			record.entry.GenerationID = entry.GenerationID
		}
		record.entry.Reasons = append(record.entry.Reasons, entry.Reasons...)
	}
	record.live = record.live || live
	record.candidate = record.candidate || candidate
	record.protected = record.protected || columnAssetRefBlocksSegmentDeletion(live, candidate, quarantined)
	record.quarantined = record.quarantined || quarantined
	records[entry.Ref] = record
	if segments[entry.Ref.FileID] == nil {
		segments[entry.Ref.FileID] = &columnAssetSegmentState{}
	}
	return nil
}

func cloneColumnAssetReachabilityReasons(reasons []string) []string {
	if len(reasons) == 0 {
		return nil
	}
	out := make([]string, len(reasons))
	copy(out, reasons)
	return out
}

func columnAssetRefBlocksSegmentDeletion(live bool, candidate bool, quarantined bool) bool {
	// Non-live refs still block whole-segment deletion unless they are purely
	// cleanup-safe candidates. Quarantined refs deliberately block shared segment
	// deletion until quarantine handling has made an explicit cleanup decision.
	if live {
		return false
	}
	if quarantined {
		return true
	}
	return !candidate
}

func estimateColumnAssetReachabilityRefs(input ColumnAssetReachabilityInput) int {
	refs := len(input.PreparedAssets) + len(input.QuarantinedAssets)
	if input.ActiveManifest != nil {
		refs += len(input.ActiveManifest.PartSet.BaseParts) + len(input.ActiveManifest.PartSet.DeltaParts)
	}
	for i := range input.ProcessVisibleManifests {
		refs += len(input.ProcessVisibleManifests[i].PartSet.BaseParts) + len(input.ProcessVisibleManifests[i].PartSet.DeltaParts)
	}
	for i := range input.PendingManifests {
		refs += len(input.PendingManifests[i].PartSet.BaseParts) + len(input.PendingManifests[i].PartSet.DeltaParts)
	}
	for i := range input.RootPublishedManifests {
		refs += len(input.RootPublishedManifests[i].PartSet.BaseParts) + len(input.RootPublishedManifests[i].PartSet.DeltaParts)
	}
	for i := range input.RecoveryAuthoritativeManifests {
		refs += len(input.RecoveryAuthoritativeManifests[i].PartSet.BaseParts) + len(input.RecoveryAuthoritativeManifests[i].PartSet.DeltaParts)
	}
	for i := range input.SnapshotPinnedManifests {
		refs += len(input.SnapshotPinnedManifests[i].PartSet.BaseParts) + len(input.SnapshotPinnedManifests[i].PartSet.DeltaParts)
	}
	for i := range input.SupersededManifests {
		refs += len(input.SupersededManifests[i].PartSet.BaseParts) + len(input.SupersededManifests[i].PartSet.DeltaParts)
	}
	for i := range input.CleanupSafeManifests {
		refs += len(input.CleanupSafeManifests[i].PartSet.BaseParts) + len(input.CleanupSafeManifests[i].PartSet.DeltaParts)
	}
	return refs
}

func columnAssetReachabilityDefaultReasons(state ColumnAssetLifecycleState) []string {
	switch state {
	case ColumnAssetStatePrepared:
		return columnAssetReasonPrepared
	case ColumnAssetStateProcessVisible:
		return columnAssetReasonProcessVisible
	case ColumnAssetStatePendingPublish:
		return columnAssetReasonPendingPublish
	case ColumnAssetStateRootPublished:
		return columnAssetReasonRootPublished
	case ColumnAssetStateRecoveryAuthoritative:
		return columnAssetReasonRecoveryAuthoritative
	case ColumnAssetStateActive:
		return columnAssetReasonActive
	case ColumnAssetStateSuperseded:
		return columnAssetReasonSuperseded
	case ColumnAssetStateCleanupSafe:
		return columnAssetReasonCleanupSafe
	case ColumnAssetStateSnapshotPinned:
		return columnAssetReasonSnapshotPinned
	case ColumnAssetStateReclaimable:
		return columnAssetReasonReclaimable
	case ColumnAssetStateDeleting:
		return columnAssetReasonDeleting
	case ColumnAssetStateQuarantined:
		return columnAssetReasonQuarantined
	default:
		return nil
	}
}

func strongestColumnAssetState(a ColumnAssetLifecycleState, b ColumnAssetLifecycleState) ColumnAssetLifecycleState {
	if columnAssetStateRank(b) > columnAssetStateRank(a) {
		return b
	}
	return a
}

func columnAssetStateRank(state ColumnAssetLifecycleState) int {
	switch state {
	case ColumnAssetStateQuarantined:
		// Quarantine is reported as the dominant state so unsafe/isolated refs
		// remain visible even if another reachable manifest still names them.
		return 100
	case ColumnAssetStateRecoveryAuthoritative:
		return 90
	case ColumnAssetStateActive:
		return 80
	case ColumnAssetStateSnapshotPinned:
		return 70
	case ColumnAssetStateRootPublished:
		return 60
	case ColumnAssetStateProcessVisible:
		return 55
	case ColumnAssetStatePendingPublish:
		return 50
	case ColumnAssetStatePrepared:
		return 40
	case ColumnAssetStateCleanupSafe:
		return 30
	case ColumnAssetStateReclaimable:
		return 20
	case ColumnAssetStateSuperseded:
		// Superseded refs are still protected from direct segment deletion, but
		// cleanup-safe/reclaimable states win when a later lifecycle scan proves
		// the containing segment can be reclaimed or rewritten safely.
		return 10
	case ColumnAssetStateDeleting:
		return 0
	default:
		return -1
	}
}
