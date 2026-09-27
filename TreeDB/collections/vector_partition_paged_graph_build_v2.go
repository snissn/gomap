package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

const vectorPartitionDomainWorkBytesV2 = 256 << 20

// VectorPartitionGraphProfileDigestV2 identifies the sole prepared V2 graph
// profile. Index definition identity separately binds dimensions and query knobs.
func VectorPartitionGraphProfileDigestV2() string {
	sum := sha256.Sum256([]byte("treedb/prepared-source-v2/" + string(VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1)))
	return hex.EncodeToString(sum[:])
}

// buildVectorPartitionANNDirectoryV2 consumes already verified immutable intent
// pages. It holds only the current domain's rows/provenance plus bounded page
// writer state; the callback that originally produced the intent is not reused.
func (s *VectorPartitionPagedSourceSessionV2) buildVectorPartitionANNDirectoryV2(ctx context.Context, input VectorPartitionPreparedInputV2, intent VectorPartitionAssetV1, def VectorIndexDefinition, emit func(VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error), appendPack func([]byte, uint32) (VectorPartitionAssetV1, error)) (VectorPartitionAssetV1, uint64, uint64, uint64, error) {
	if input.GraphProfileDigest != VectorPartitionGraphProfileDigestV2() {
		return VectorPartitionAssetV1{}, 0, 0, 0, fmt.Errorf("%w: unsupported prepared graph profile", ErrVectorPartitionManifestInvalid)
	}
	def.M, def.EfConstruction = columnVamanaConnectivityPreservingPartitionM, columnVamanaConnectivityPreservingPartitionL
	var domains, members, assets uint64
	root, err := writeVectorPartitionDirectoryV2(ctx, "metadata", input.Generation, func(visit func(VectorPartitionDirectoryRecordV2) error) error {
		var declaration *source.ANNDomainV2
		var owner string
		var records []VectorPartitionDirectoryRecordV2
		var workBytes uint64
		flush := func() error {
			if declaration == nil {
				return nil
			}
			if uint64(len(records)) != declaration.MembershipCount {
				return fmt.Errorf("%w: domain member count", ErrVectorPartitionManifestInvalid)
			}
			rows := make([]columnVectorGraphAssetRow, 0, len(records))
			provenance := make(map[string]int, len(records))
			acc, err := source.NewANNOwnerAccumulatorV2(owner)
			if err != nil {
				return err
			}
			if err := acc.BeginDomain(*declaration); err != nil {
				return err
			}
			for i, r := range records {
				row, err := s.ReadSourceRowV2(ctx, *r.Member)
				if err != nil {
					return err
				}
				if len(row.Values) != def.Dimensions {
					return fmt.Errorf("%w: source dimensions", ErrVectorPartitionManifestInvalid)
				}
				id := string(row.DocumentID)
				if _, ok := provenance[id]; ok {
					return fmt.Errorf("%w: duplicate domain document", ErrVectorPartitionManifestInvalid)
				}
				provenance[id] = i
				rowBytes := uint64(len(row.Values))*4 + uint64(len(row.DocumentID)) + 128
				if rowBytes > vectorPartitionDomainWorkBytesV2-workBytes {
					return fmt.Errorf("%w: domain working bytes", ErrVectorPartitionManifestInvalid)
				}
				workBytes += rowBytes
				rows = append(rows, columnVectorGraphAssetRow{ID: slices.Clone(row.DocumentID), Vector: slices.Clone(row.Values)})
				if err := acc.AddMember(source.ANNMemberV2{Source: *r.Member, Kind: r.MembershipKind}); err != nil {
					return err
				}
			}
			digest, err := acc.DomainDigest()
			if err != nil {
				return err
			}
			if err := buildVectorPartitionVamanaV1(ctx, rows, def.Dimensions); err != nil {
				return err
			}
			// The graph core reorders complete rows. Map its output ordinal back to the
			// exact semantic membership rather than manufacturing physical row refs.
			for ordinal, row := range rows {
				i, ok := provenance[string(row.ID)]
				if !ok {
					return fmt.Errorf("%w: graph provenance", ErrVectorPartitionManifestInvalid)
				}
				records[i].GraphOrdinal = uint64(ordinal)
			}
			graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, RowCount: len(rows)}
			pack, err := buildColumnHNSWSearchPackInputBinding(def, graph, rows, true)
			if err != nil {
				return err
			}
			pack.MembershipDigest = digest
			pack.ConnectivityPreservingPartitionVamana = true
			raw, err := encodeColumnHNSWSearchPackRows(pack, rows)
			if err != nil {
				return err
			}
			chunks, err := splitVectorPartitionDomainSearchPackBinding(raw, declaration.DomainID, 1<<20, columnHNSWSearchPackDecodeOptions{SourceV2: true, ExpectedMembershipDigest: digest})
			if err != nil {
				return err
			}
			if err := visit(VectorPartitionDirectoryRecordV2{Owner: owner, DomainID: uint64(declaration.DomainID), Domain: declaration}); err != nil {
				return err
			}
			// Asset IDs encode section/chunk ordinals; lexical ordering is the VDP2 key order.
			slices.SortFunc(chunks, func(a, b vectorPartitionDomainAssetPayloadV1) int {
				if a.id < b.id {
					return -1
				}
				if a.id > b.id {
					return 1
				}
				return 0
			})
			for _, chunk := range chunks {
				asset, err := appendPack(chunk.payload, declaration.DomainID)
				if err != nil {
					return err
				}
				asset.ID, asset.PartitionID, asset.MembershipDigest = chunk.id, declaration.DomainID, hex.EncodeToString(digest[:])
				if chunk.id == vectorPartitionLocalAssetIDV1(declaration.DomainID) {
					asset.GraphVariant = string(VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1)
				}
				if err := visit(VectorPartitionDirectoryRecordV2{Owner: owner, DomainID: uint64(declaration.DomainID), Asset: &asset}); err != nil {
					return err
				}
				assets++
			}
			for _, r := range records {
				if err := visit(r); err != nil {
					return err
				}
			}
			domains++
			members += uint64(len(records))
			records = nil
			workBytes = 0
			return nil
		}
		err := walkVectorPartitionDirectoryV2(ctx, intent, "metadata", "", func(a VectorPartitionAssetV1) ([]byte, error) {
			return readVectorPartitionDirectoryAssetV2(s.collection.db.ColumnAssetRootDir(), a)
		}, nil, func(r VectorPartitionDirectoryRecordV2) error {
			if r.Domain != nil {
				if err := flush(); err != nil {
					return err
				}
				copy := *r.Domain
				declaration = &copy
				owner = r.Owner
				if declaration.MembershipCount > uint64(DefaultVectorPartitionManifestLimits().MaxMemberships) {
					return fmt.Errorf("%w: domain row bound", ErrVectorPartitionManifestInvalid)
				}
				return nil
			}
			if declaration == nil || r.Owner != owner || r.DomainID != uint64(declaration.DomainID) || r.Member == nil || uint64(len(records)) >= declaration.MembershipCount {
				return fmt.Errorf("%w: immutable intent shape", ErrVectorPartitionManifestInvalid)
			}
			// Charge owned semantic records before buffering them, including
			// the ordinal/provenance arrays built for this domain.
			recordBytes := uint64(512 + len(r.Owner) + len(r.Member.SourceOwner) + len(r.Member.ShardID))
			if recordBytes > vectorPartitionDomainWorkBytesV2-workBytes {
				return fmt.Errorf("%w: domain intent working bytes", ErrVectorPartitionManifestInvalid)
			}
			workBytes += recordBytes
			copy := *r.Member
			r.Member = &copy
			records = append(records, r)
			return nil
		})
		if err != nil {
			return err
		}
		return flush()
	}, emit)
	return root, domains, members, assets, err
}
