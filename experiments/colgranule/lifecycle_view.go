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
	ActiveManifest          *ColumnCollectionManifestView
	PendingManifests        []ColumnCollectionManifestView
	SnapshotPinnedManifests []ColumnCollectionManifestView
	SupersededManifests     []ColumnCollectionManifestView
	PreparedRegistry        *ColumnPreparedAssetRegistryView
	PreparedAssets          []ColumnPreparedAsset
	QuarantinedAssets       []ColumnPreparedAsset
}

type columnManifestViewAssetRef struct {
	ref           ColumnAssetRef
	bytes         int
	partID        uint64
	generationID  uint64
	manifestBytes int
}

func DecodeColumnCollectionManifestView(data []byte) (ColumnCollectionManifestView, error) {
	if isColumnControlPlaneBinary(data, columnCollectionManifestBinaryMagic) {
		body, err := decodeColumnControlPlaneEnvelope(data, columnCollectionManifestBinaryMagic, columnCollectionManifestBinaryVersion, "collection manifest")
		if err != nil {
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
	for _, manifest := range input.PendingManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStatePendingPublish, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.PendingManifests++
	}
	for _, manifest := range input.SnapshotPinnedManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateSnapshotPinned, true, false, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.SnapshotPinnedManifests++
	}
	for _, manifest := range input.SupersededManifests {
		if err := addManifestViewAssetRefs(records, segments, manifest, ColumnAssetStateSuperseded, false, true, &plan.Stats); err != nil {
			return ColumnAssetReachabilityPlan{}, err
		}
		plan.Stats.SupersededManifests++
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

func estimateColumnAssetReachabilityViewRefs(input ColumnAssetReachabilityViewInput) (int, error) {
	refs := len(input.PreparedAssets) + len(input.QuarantinedAssets)
	if input.ActiveManifest != nil {
		refs += estimateColumnCollectionManifestViewAssetRefs(*input.ActiveManifest)
	}
	for i := range input.PendingManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.PendingManifests[i])
	}
	for i := range input.SnapshotPinnedManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.SnapshotPinnedManifests[i])
	}
	for i := range input.SupersededManifests {
		refs += estimateColumnCollectionManifestViewAssetRefs(input.SupersededManifests[i])
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
	tombstones, err := scanColumnCollectionManifestViewAssetRefs(view, func(ref columnManifestViewAssetRef) error {
		entry := ColumnAssetReachabilityEntry{
			Ref:          ref.ref,
			State:        state,
			Bytes:        ref.bytes,
			PartID:       ref.partID,
			GenerationID: ref.generationID,
			Reasons:      columnAssetReachabilityDefaultReasons(state),
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

func finalizeColumnAssetReachabilityRecords(records map[ColumnAssetRef]columnAssetReachabilityRecord, segments map[uint32]*columnAssetSegmentState, plan *ColumnAssetReachabilityPlan) {
	plan.Stats.Manifests = plan.Stats.ActiveManifests + plan.Stats.PendingManifests + plan.Stats.SnapshotPinnedManifests + plan.Stats.SupersededManifests
	plan.Stats.AssetRefs = len(records)
	for _, seg := range segments {
		seg.liveRefs = 0
		seg.candidateRefs = 0
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
	}
	plan.Stats.SegmentRefs = len(segments)
	for _, seg := range segments {
		if seg.liveRefs == 0 && seg.candidateRefs > 0 {
			plan.Stats.DirectlyDeletableSegments++
		}
		if seg.liveRefs > 0 && seg.candidateRefs > 0 {
			plan.Stats.MixedLiveDeadSegments++
		}
	}

	plan.Entries = make([]ColumnAssetReachabilityEntry, 0, len(records))
	for _, record := range records {
		entry := record.entry
		switch {
		case record.quarantined:
			plan.QuarantinedBytes += entry.Bytes
		case record.live:
			switch entry.State {
			case ColumnAssetStatePrepared, ColumnAssetStatePendingPublish:
				plan.PreparedBytes += entry.Bytes
			case ColumnAssetStateSnapshotPinned:
				plan.SnapshotProtectedBytes += entry.Bytes
				plan.RetainedBytes += entry.Bytes
			default:
				plan.RetainedBytes += entry.Bytes
			}
		case record.candidate:
			plan.SupersededBytes += entry.Bytes
			seg := segments[entry.Ref.FileID]
			if seg != nil && seg.liveRefs == 0 {
				entry.DeleteEligible = true
				entry.State = ColumnAssetStateReclaimable
				plan.ReclaimableBytes += entry.Bytes
			} else {
				plan.RewriteDebtBytes += entry.Bytes
			}
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
