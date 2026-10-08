package collections

import (
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// columnPhysicalAssetImagePlan contains the same ordinary encoder's owned byte
// images before opening, appending, observing, registering or installing any
// physical asset. Its installation receives the operational Collection only on
// the call stack. No retained callback or DB/producer pointer is part of a plan.
type columnPhysicalAssetImagePlan struct {
	prepared               ColumnPublishPreparedAssets
	projectedRefs          []ColumnAssetRef // exact tentative facts, never installed authority
	assets                 []pendingColumnAsset
	generation             uint64
	commandLSN             uint64
	unboundCommandIdentity bool
	isolatedTypedOutput    bool
}

type pendingColumnAsset struct {
	payload  []byte
	ref      ColumnAssetRef
	hasRef   bool
	fileID   uint32
	kind     ColumnAssetKind
	partID   uint64
	rows     int
	reason   string
	partRole ColumnManifestPartRole
	sortKey  string
}

// prepareColumnPhysicalAssetImageIdentity binds only the tentative manifest
// key/value facts. The same held namespace allocator stripe must survive until
// installation, which checks every real returned address against these facts.
// The returned assets must never satisfy a durability/resource requirement.
func prepareColumnPhysicalAssetImageIdentity(images *columnPhysicalAssetImagePlan, hook ColumnPublishAssetPrepareInput, reservation *columnPhysicalAssetIdentityReservation, account rootpublication.StableMetadataAccount) ([]ColumnPreparedAsset, error) {
	if images == nil || reservation == nil || account == nil || reservation.lock == nil || reservation.consumed ||
		reservation.fileID == 0 || images.projectedRefs != nil || hook.AppliedCommandLSN == 0 ||
		hook.ColumnStore.AssetManager == nil || hook.ColumnStore.AssetManager.Namespace != reservation.namespaceName ||
		images.unboundCommandIdentity || images.commandLSN != hook.AppliedCommandLSN || len(images.assets) == 0 || len(images.prepared.Assets) != 0 || images.prepared.stableResources != nil {
		return nil, ErrPreparedInsertResourceLimit
	}
	if reservation.shared {
		if images.isolatedTypedOutput || reservation.fileID != columnAssetM12ASegmentFileID || reservation.incarnation == 0 ||
			reservation.start < 0 || reservation.start > columnPhysicalAssetSegmentTargetBytes || len(images.assets) != 1 ||
			images.assets[0].fileID != columnAssetM12ASegmentFileID || images.assets[0].kind != ColumnAssetKindTCS1PartImage {
			return nil, ErrPreparedInsertResourceLimit
		}
	} else if reservation.cache == nil || reservation.cache.nextFileID != reservation.fileID || !images.isolatedTypedOutput {
		return nil, ErrPreparedInsertResourceLimit
	}
	// Whole-image shape validation dominates all copied headers/arrays.
	end := reservation.start
	for _, asset := range images.assets {
		if asset.hasRef || len(asset.payload) == 0 || asset.partID == 0 || images.generation == 0 {
			return nil, ErrPreparedInsertResourceLimit
		}
		if _, _, _, known := stableColumnAssetResourceClassificationFacts(asset.kind); !known {
			return nil, ErrPreparedInsertResourceLimit
		}
		padding := int64(columnAssetSegmentPrefixPadding(end, columnAssetSegmentPayloadAlignment(asset.kind, hook.ColumnStore)))
		if padding > columnPhysicalAssetSegmentTargetBytes-end || int64(len(asset.payload)) > columnPhysicalAssetSegmentTargetBytes-end-padding {
			return nil, ErrPreparedInsertResourceLimit
		}
		end += padding + int64(len(asset.payload))
	}
	count := uint64(len(images.assets))
	var charge uint64
	for _, size := range [...]uint64{uint64(unsafe.Sizeof(columnPhysicalAssetAppendItem{})), uint64(unsafe.Sizeof(ColumnAssetRef{})), uint64(unsafe.Sizeof(ColumnPreparedAsset{}))} {
		if count > ^uint64(0)/size {
			return nil, ErrPreparedInsertResourceLimit
		}
		class, err := rootpublication.StableBackingClassBytes(count*size, true)
		if err != nil || class > ^uint64(0)-charge {
			return nil, ErrPreparedInsertResourceLimit
		}
		charge += class
	}
	if err := account.ReserveStableMetadata(charge); err != nil {
		return nil, err
	}
	items := make([]columnPhysicalAssetAppendItem, len(images.assets))
	for i, asset := range images.assets {
		items[i] = columnPhysicalAssetAppendItem{payload: asset.payload, kind: asset.kind, generation: images.generation, partID: asset.partID}
	}
	// Reference-array credit was included in the complete debit above. The
	// exact same function supplies the installed appender's reference grammar.
	refs, _, _, err := planColumnPhysicalAssetAppendRefs(hook.ColumnStore, reservation.fileID, reservation.start, items, true, nil)
	if err != nil {
		return nil, err
	}
	assets := make([]ColumnPreparedAsset, len(refs))
	for i, ref := range refs {
		source := images.assets[i]
		assets[i] = ColumnPreparedAsset{Ref: ref, Rows: source.rows, Bytes: ref.Length, PublishID: hook.AppliedCommandLSN, GenerationID: images.generation, Reason: source.reason, PartRole: source.partRole, SortKey: source.sortKey}
	}
	images.projectedRefs = refs
	return assets, nil
}

// encodeNativeColumnPhysicalImagesBeforeIdentity performs the bulk row work
// without assigning/burning a command identity. Zero is an intentionally
// unavailable header field in owned unpublished bytes, never a forecast or
// a resource certificate. The existing encoder/installer remain authoritative.
func encodeNativeColumnPhysicalImagesBeforeIdentity(prepared ColumnPublishPreparedAssets, input columnWritePublishInput, hook ColumnPublishAssetPrepareInput, rows []columnDeclaredRow, generation, rowPartID uint64) (columnPhysicalAssetImagePlan, error) {
	if input.nativeSource == nil || hook.ColumnStore.AssetManager == nil || hook.AppliedCommandLSN != 0 || generation == 0 || rowPartID == 0 ||
		len(rows) == 0 || rows[0].Stored == nil || input.preparedTypedBatch != nil ||
		len(prepared.Assets) != 0 || prepared.stableResources != nil || hook.CurrentManifest != nil && hook.CurrentManifest.Generation >= generation {
		return columnPhysicalAssetImagePlan{}, ErrPreparedInsertResourceLimit
	}
	images, err := encodeColumnPhysicalAssetImagePlan(prepared, input, hook, rows, nil, nil, generation, rowPartID, typedColumnPartAssetPartID)
	if err != nil {
		return columnPhysicalAssetImagePlan{}, err
	}
	if images.isolatedTypedOutput || len(images.assets) != 1 || images.assets[0].kind != ColumnAssetKindTCS1PartImage || images.assets[0].hasRef {
		return columnPhysicalAssetImagePlan{}, ErrPreparedInsertResourceLimit
	}
	images.unboundCommandIdentity = true
	return images, nil
}

// bindNativeColumnPhysicalImagesCommandIdentity runs under the actual command
// serializer, before C13 sees the final manifest/locator keys. It edits only
// the fixed header field in already-owned bytes and allocates nothing. Neither
// append nor installed authority is implied by a successful binding.
func bindNativeColumnPhysicalImagesCommandIdentity(images *columnPhysicalAssetImagePlan, hook ColumnPublishAssetPrepareInput) error {
	if images == nil || !images.unboundCommandIdentity || images.commandLSN != 0 || hook.AppliedCommandLSN == 0 ||
		images.projectedRefs != nil || images.prepared.stableResources != nil || len(images.prepared.Assets) != 0 ||
		images.isolatedTypedOutput || len(images.assets) != 1 || images.generation == 0 || hook.ColumnStore.AssetManager == nil {
		return ErrPreparedInsertResourceLimit
	}
	asset := &images.assets[0]
	if asset.kind != ColumnAssetKindTCS1PartImage || asset.hasRef || asset.partID == 0 {
		return ErrPreparedInsertResourceLimit
	}
	raw := asset.payload
	if len(raw) < 6 || binary.BigEndian.Uint32(raw[:4]) != columnPhysicalAssetMagic || binary.BigEndian.Uint16(raw[4:6]) != columnPhysicalAssetVersionV10 {
		return ErrPreparedInsertResourceLimit
	}
	offset := 6
	// Check the concrete existing grammar without decoding rows, allocating
	// strings, or retaining a parser/operational edge on the plan.
	for _, expected := range [...]string{hook.Collection, hook.ColumnStore.AssetManager.Namespace} {
		if len(raw)-offset < 8 {
			return ErrPreparedInsertResourceLimit
		}
		n := binary.BigEndian.Uint64(raw[offset : offset+8])
		offset += 8
		if n != uint64(len(expected)) || n > uint64(len(raw)-offset) {
			return ErrPreparedInsertResourceLimit
		}
		for i := 0; i < len(expected); i++ {
			if raw[offset+i] != expected[i] {
				return ErrPreparedInsertResourceLimit
			}
		}
		offset += len(expected)
	}
	if len(raw)-offset < 24 || binary.BigEndian.Uint64(raw[offset:offset+8]) != images.generation ||
		binary.BigEndian.Uint64(raw[offset+8:offset+16]) != asset.partID || binary.BigEndian.Uint64(raw[offset+16:offset+24]) != 0 {
		return ErrPreparedInsertResourceLimit
	}
	// All refusal checks precede the sole mutation. Rebinding is prohibited.
	binary.BigEndian.PutUint64(raw[offset+16:offset+24], hook.AppliedCommandLSN)
	images.commandLSN, images.unboundCommandIdentity = hook.AppliedCommandLSN, false
	return nil
}
