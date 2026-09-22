package collections

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
)

// VectorPartitionGenerationOwnerSearchOpenPlanV2 contains only one owner's
// search metadata. It is deliberately distinct from the complete V1 plan.
// Its global source identity and partition IDs retain their original meaning.
type VectorPartitionGenerationOwnerSearchOpenPlanV2 struct {
	collection            string
	indexName             string
	indexDefinitionDigest string
	integrityDigest       string
	owner                 string
	source                VectorPartitionSourceIdentityV1
	generation            uint64
	partitions            map[uint32]*vectorPartitionOwnerSearchPartitionV2
	members               []vectorPartitionMembershipSourceV1
}

type vectorPartitionOwnerSearchPartitionV2 struct {
	asset   VectorPartitionAssetV1
	assets  []VectorPartitionAssetV1
	present bool
	start   int
	home    int
	overlap int
}

// NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2 prepares local
// search metadata for groupID from an already admitted generation manifest.
// This selection grants no lifecycle or placement authority and must not be
// treated as a complete global search plan. Callers still validate the exact
// generation and placement through the existing admission path.
//
// This constructor scans the admitted V1 input but retains only local metadata.
// It does not make acquisition/decoding of that input owner-local. Paged roots
// and shard-local source readers have separate admission requirements.
func NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(ctx context.Context, manifest VectorPartitionManifestV1, groupID string) (*VectorPartitionGenerationOwnerSearchOpenPlanV2, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limits := DefaultVectorPartitionManifestLimits()
	if groupID == "" || len(groupID) > limits.MaxStringBytes || manifest.Collection == "" || manifest.IndexName == "" || manifest.IndexDefinitionDigest == "" || manifest.Generation == 0 || manifest.PartitionCount == 0 || uint64(manifest.PartitionCount) > uint64(limits.MaxPartitions) || len(manifest.Placements) != int(manifest.PartitionCount) {
		return nil, fmt.Errorf("%w: owner plan identity or placement", ErrVectorPartitionSearchUnavailable)
	}
	if len(manifest.Memberships) > limits.totalMembershipLimit() || len(manifest.OverlapMemberships) > limits.totalMembershipLimit()-min(len(manifest.Memberships), limits.totalMembershipLimit()) || len(manifest.Assets) > limits.MaxAssets {
		return nil, fmt.Errorf("%w: owner plan exceeds manifest limits", ErrVectorPartitionSearchUnavailable)
	}
	// Never preallocate from global partition or membership counts here.
	anchorForLocalPack := make(map[uint32]uint32)
	for i, placement := range manifest.Placements {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if placement.PartitionID != uint32(i) || placement.GroupID == "" {
			return nil, fmt.Errorf("%w: incomplete owner placement", ErrVectorPartitionSearchUnavailable)
		}
		if placement.GroupID == groupID {
			anchorForLocalPack[placement.PartitionID] = placement.PartitionID
		}
	}
	if len(anchorForLocalPack) == 0 {
		return nil, fmt.Errorf("%w: owner has no partitions", ErrVectorPartitionSearchUnavailable)
	}
	domainMode := false
	for _, asset := range manifest.Assets {
		if strings.Contains(asset.ID, "/section/") && strings.HasPrefix(asset.ID, vectorPartitionLocalAssetIDV1(asset.PartitionID)+"/section/") {
			domainMode = true
			break
		}
	}
	if domainMode {
		if err := vectorPartitionOwnerDomainAnchorsV2(ctx, manifest, groupID, anchorForLocalPack); err != nil {
			return nil, err
		}
	}
	plan := &VectorPartitionGenerationOwnerSearchOpenPlanV2{
		collection: manifest.Collection, indexName: manifest.IndexName,
		indexDefinitionDigest: manifest.IndexDefinitionDigest, integrityDigest: manifest.IntegrityDigest,
		owner: groupID, generation: manifest.Generation,
		source: VectorPartitionSourceIdentityV1{Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum, SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount},
		partitions: make(map[uint32]*vectorPartitionOwnerSearchPartitionV2),
	}
	for _, anchor := range anchorForLocalPack {
		if plan.partitions[anchor] == nil {
			plan.partitions[anchor] = &vectorPartitionOwnerSearchPartitionV2{}
		}
	}
	for i, asset := range manifest.Assets {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		part := plan.partitions[asset.PartitionID]
		if part == nil {
			continue
		}
		rootID := vectorPartitionLocalAssetIDV1(asset.PartitionID)
		if asset.ID == rootID {
			if part.present {
				return nil, fmt.Errorf("%w: duplicate owner search root", ErrVectorPartitionSearchUnavailable)
			}
			part.asset, part.present = asset, true
		}
		if asset.ID == rootID || strings.HasPrefix(asset.ID, rootID+"/section/") {
			part.assets = append(part.assets, asset)
		}
	}
	kinds := make(map[vectorPartitionServingMembershipKeyV1]VectorPartitionMembershipKindV1)
	add := func(input []VectorPartitionMembershipV1, kind VectorPartitionMembershipKindV1) error {
		for i, member := range input {
			if i&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if member.PartitionID >= manifest.PartitionCount {
				return fmt.Errorf("%w: owner membership partition", ErrVectorPartitionSearchUnavailable)
			}
			anchor, local := anchorForLocalPack[member.PartitionID]
			if !local {
				continue
			}
			key := vectorPartitionServingMembershipKeyV1{ordinal: member.VectorOrdinal, anchor: anchor}
			prior, exists := kinds[key]
			if exists && !domainMode {
				return fmt.Errorf("%w: duplicate owner membership", ErrVectorPartitionSearchUnavailable)
			}
			// Match V1: a home in any colocated pack wins over overlap.
			if !exists || kind == VectorPartitionMembershipHomeV1 || prior != VectorPartitionMembershipHomeV1 {
				kinds[key] = kind
			}
		}
		return nil
	}
	if err := add(manifest.Memberships, VectorPartitionMembershipHomeV1); err != nil {
		return nil, err
	}
	if err := add(manifest.OverlapMemberships, VectorPartitionMembershipOverlapV1); err != nil {
		return nil, err
	}
	for key, kind := range kinds {
		part := plan.partitions[key.anchor]
		if kind == VectorPartitionMembershipHomeV1 {
			part.home++
		} else {
			part.overlap++
		}
	}
	plan.members = make([]vectorPartitionMembershipSourceV1, len(kinds))
	offset := 0
	for _, part := range plan.partitions {
		if !part.present {
			return nil, fmt.Errorf("%w: missing owner search root", ErrVectorPartitionSearchUnavailable)
		}
		part.start = offset
		offset += part.home + part.overlap
	}
	for key, kind := range kinds {
		part := plan.partitions[key.anchor]
		index := part.start
		part.start++
		plan.members[index] = vectorPartitionMembershipSourceV1{ordinal: key.ordinal, kind: kind}
	}
	for _, part := range plan.partitions {
		part.start -= part.home + part.overlap
		members := plan.members[part.start : part.start+part.home+part.overlap]
		slices.SortFunc(members, func(a, b vectorPartitionMembershipSourceV1) int {
			if a.kind != b.kind {
				if a.kind == VectorPartitionMembershipHomeV1 {
					return -1
				}
				return 1
			}
			return cmp.Compare(a.ordinal, b.ordinal)
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return plan, nil
}

func vectorPartitionOwnerDomainAnchorsV2(ctx context.Context, manifest VectorPartitionManifestV1, owner string, anchors map[uint32]uint32) error {
	if manifest.DomainCount == 0 || manifest.DomainCount > manifest.PartitionCount || len(manifest.DomainPacks) != int(manifest.PartitionCount) {
		return fmt.Errorf("%w: owner domain layout", ErrVectorPartitionSearchUnavailable)
	}
	seenLocal := make(map[uint32]struct{}, len(anchors))
	var lastDomain, lastPack, anchor uint32
	var domainOwner string
	for i, mapping := range manifest.DomainPacks {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if mapping.PackID >= manifest.PartitionCount || mapping.DomainID >= manifest.DomainCount || (i == 0 && mapping.DomainID != 0) || (i > 0 && (mapping.DomainID < lastDomain || mapping.DomainID > lastDomain+1 || mapping.DomainID == lastDomain && mapping.PackID <= lastPack)) {
			return fmt.Errorf("%w: owner domain mapping", ErrVectorPartitionSearchUnavailable)
		}
		group := manifest.Placements[mapping.PackID].GroupID
		if i == 0 || mapping.DomainID != lastDomain {
			anchor, domainOwner = mapping.PackID, group
		}
		if group != domainOwner {
			return fmt.Errorf("%w: split domain ownership", ErrVectorPartitionSearchUnavailable)
		}
		if group == owner {
			if _, duplicate := seenLocal[mapping.PackID]; duplicate {
				return fmt.Errorf("%w: duplicate local domain pack", ErrVectorPartitionSearchUnavailable)
			}
			seenLocal[mapping.PackID] = struct{}{}
			anchors[mapping.PackID] = anchor
		}
		lastDomain, lastPack = mapping.DomainID, mapping.PackID
	}
	if lastDomain+1 != manifest.DomainCount || len(seenLocal) != len(anchors) {
		return fmt.Errorf("%w: incomplete owner domain layout", ErrVectorPartitionSearchUnavailable)
	}
	return nil
}

func (p *VectorPartitionGenerationOwnerSearchOpenPlanV2) partition(partition uint32) (*VectorPartitionAssetV1, []vectorPartitionMembershipSourceV1, int, int, error) {
	if p == nil || p.partitions[partition] == nil {
		return nil, nil, 0, 0, fmt.Errorf("%w: partition is not an owner search domain", ErrVectorPartitionSearchUnavailable)
	}
	part := p.partitions[partition]
	return &part.asset, p.members[part.start : part.start+part.home+part.overlap], part.home, part.overlap, nil
}

func (p *VectorPartitionGenerationOwnerSearchOpenPlanV2) partitionAssets(partition uint32) []VectorPartitionAssetV1 {
	if p == nil || p.partitions[partition] == nil {
		return nil
	}
	return p.partitions[partition].assets
}

// OpenVectorPartitionLocalSearcherForGenerationOwnerSearchPlanWithContextV2
// reuses the production asset/source verifier and the existing generation pin.
// It grants neither standalone live-recovery nor offline graph authority.
func (c *Collection) OpenVectorPartitionLocalSearcherForGenerationOwnerSearchPlanWithContextV2(ctx context.Context, index string, generation uint64, partition uint32, owner string, plan *VectorPartitionGenerationOwnerSearchOpenPlanV2, generationPin *VectorPartitionReaderPinV1) (*VectorPartitionLocalSearcherV1, error) {
	if c == nil || c.db == nil || plan == nil || plan.collection != c.name || plan.indexName != index || plan.generation != generation || plan.owner != owner {
		return nil, fmt.Errorf("%w: stale owner search plan", ErrVectorPartitionSearchUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	asset, members, home, overlap, err := plan.partition(partition)
	if err != nil {
		return nil, err
	}
	pin, err := generationPin.cloneForKey(vectorPartitionReaderPinKeyV1(c.db.Dir(), c.name, index, generation))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	searcher, err := c.openVectorPartitionLocalSearcherForPreparedAssetsWithContextV1(ctx, index, generation, partition,
		plan.indexDefinitionDigest, plan.source.Generation, plan.source.Checksum, plan.source.SchemaHash, plan.source.RowCount,
		asset, plan.partitionAssets(partition), members, home, overlap, false, "", false, false)
	if err != nil {
		pin.Release()
		return nil, err
	}
	searcher.partitionPin = pin
	return searcher, nil
}
