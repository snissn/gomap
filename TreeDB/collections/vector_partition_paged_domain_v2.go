package collections

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// VectorPartitionPreparedDomainV2 is a local, generation-pinned domain. It does
// not activate the distributed query lifecycle. Provenance is indexed by the
// graph's persisted ordinal, after validating the complete selected domain.
type VectorPartitionPreparedDomainV2 struct {
	mu      sync.Mutex
	view    *columnHNSWSearchPackPreparedView
	pin     *VectorPartitionReaderPinV1
	members []source.ANNMemberV2
	scratch columnVectorGraphNativeSearchScratch
}

type VectorPartitionPreparedResultV2 struct {
	DocumentID     []byte
	Source         VectorPartitionSourceRowIdentityV2
	MembershipKind string
	Score          float64
}

// OpenDomainV2 reads only the selected domain after cold owner verification.
// The returned domain owns its own generation pin and may outlive the session.
func (s *VectorPartitionPagedSourceSessionV2) OpenDomainV2(ctx context.Context, owner string, domain uint32) (_ *VectorPartitionPreparedDomainV2, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil {
		return nil, backenddb.ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshot == nil {
		return nil, backenddb.ErrClosed
	}
	if _, ok := slices.BinarySearch(s.verifiedANNOwners, owner); !ok {
		return nil, fmt.Errorf("%w: unverified ANN owner", ErrVectorPartitionManifestInvalid)
	}
	c := s.collection
	pin, err := c.AcquireVectorPartitionReaderPinWithContextV1(ctx, s.manifest.IndexName, s.manifest.Generation)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			pin.Release()
		}
	}()
	var declaration *source.ANNDomainV2
	var records []VectorPartitionDirectoryRecordV2
	var assets []VectorPartitionAssetV1
	var retained uint64
	err = walkVectorPartitionDirectoryV2(ctx, s.manifest.PagedRootV2.MetadataDirectory, "metadata", vectorPartitionDirectoryDomainPrefixV2(owner, uint64(domain)), func(a VectorPartitionAssetV1) ([]byte, error) {
		return readVectorPartitionDirectoryAssetV2(c.db.ColumnAssetRootDir(), a)
	}, nil, func(r VectorPartitionDirectoryRecordV2) error {
		if r.Owner != owner || r.DomainID != uint64(domain) {
			return fmt.Errorf("%w: selected domain", ErrVectorPartitionManifestInvalid)
		}
		if r.Domain != nil {
			if declaration != nil || r.Domain.MembershipCount > uint64(DefaultVectorPartitionManifestLimits().MaxMemberships) {
				return fmt.Errorf("%w: domain declaration", ErrVectorPartitionManifestInvalid)
			}
			copy := *r.Domain
			declaration = &copy
			return nil
		}
		if declaration == nil {
			return fmt.Errorf("%w: missing domain declaration", ErrVectorPartitionManifestInvalid)
		}
		retained += 512 + uint64(len(r.Owner))
		if r.Member != nil {
			retained += uint64(len(r.Member.SourceOwner) + len(r.Member.ShardID))
			if uint64(len(records)) >= declaration.MembershipCount {
				return fmt.Errorf("%w: excess domain membership", ErrVectorPartitionManifestInvalid)
			}
			copy := *r.Member
			r.Member = &copy
			records = append(records, r)
		} else if r.Asset != nil {
			retained += uint64(len(r.Asset.ID))
			assets = append(assets, *r.Asset)
		}
		if retained > vectorPartitionDomainWorkBytesV2 {
			return fmt.Errorf("%w: domain metadata bound", ErrVectorPartitionManifestInvalid)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if declaration == nil || uint64(len(records)) != declaration.MembershipCount {
		return nil, fmt.Errorf("%w: incomplete selected domain", ErrVectorPartitionManifestInvalid)
	}
	acc, err := source.NewANNOwnerAccumulatorV2(owner)
	if err != nil {
		return nil, err
	}
	if err = acc.BeginDomain(*declaration); err != nil {
		return nil, err
	}
	members := make([]source.ANNMemberV2, len(records))
	seen := make([]bool, len(records))
	for _, r := range records {
		if r.GraphOrdinal >= uint64(len(records)) || seen[r.GraphOrdinal] {
			return nil, fmt.Errorf("%w: graph ordinal permutation", ErrVectorPartitionManifestInvalid)
		}
		seen[r.GraphOrdinal] = true
		member := source.ANNMemberV2{Source: *r.Member, Kind: r.MembershipKind}
		if err = acc.AddMember(member); err != nil {
			return nil, err
		}
		members[r.GraphOrdinal] = member
	}
	digest, err := acc.DomainDigest()
	if err != nil {
		return nil, err
	}
	namespace := c.meta.Options.ColumnStore.AssetManager.Namespace
	if err = verifyVectorPartitionAssetsWithContextV1(ctx, c.db.ColumnAssetRootDir(), namespace, assets); err != nil {
		return nil, err
	}
	for _, a := range assets {
		expectedVariant := ""
		if a.ID == vectorPartitionLocalAssetIDV1(domain) {
			expectedVariant = string(VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1)
		}
		if a.PartitionID != domain || a.Ref.PartID != uint64(domain)+1 || a.Ref.Generation != s.manifest.Generation || a.Ref.Kind != ColumnAssetKindTCS1HNSWSearchPack || a.MembershipDigest != hex.EncodeToString(digest[:]) || a.GraphVariant != expectedVariant {
			return nil, fmt.Errorf("%w: domain pack identity", ErrVectorPartitionManifestInvalid)
		}
	}
	view, err := c.openPagedDomainSectionsV2(ctx, domain, assets, digest)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = view.Close()
		}
	}()
	def, err := c.vectorPartitionRouterDefinitionV1(s.manifest.IndexName)
	if err != nil {
		return nil, err
	}
	if view.Header.Version != columnHNSWSearchPackVersionV7 || view.Header.Rows != len(members) || view.Header.Dimensions != def.Dimensions || view.Header.M != columnVamanaConnectivityPreservingPartitionM || view.Header.EfConstruction != columnVamanaConnectivityPreservingPartitionL || view.Header.EfSearch != def.EfSearch {
		return nil, fmt.Errorf("%w: prepared graph profile", ErrVectorPartitionManifestInvalid)
	}
	// Resolve source rows in canonical membership order, while checking the
	// complete graph-ordinal permutation established above.
	var current vectorPartitionSourceCurrentChunkV2
	for _, record := range records {
		row, readErr := s.readSourceRowWithCurrentChunkV2(ctx, *record.Member, &current)
		if readErr != nil {
			return nil, readErr
		}
		id, ok := view.documentIDForOrdinal(int(record.GraphOrdinal))
		if !ok || !bytes.Equal(id, row.DocumentID) {
			return nil, fmt.Errorf("%w: graph ordinal document provenance", ErrVectorPartitionManifestInvalid)
		}
	}
	return &VectorPartitionPreparedDomainV2{view: view, pin: pin, members: members}, nil
}

func (d *VectorPartitionPreparedDomainV2) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.view == nil {
		return nil
	}
	err := d.view.Close()
	d.view = nil
	d.members = nil
	d.pin.Release()
	d.pin = nil
	return err
}

// SearchLocalV2 uses the existing ordinal-only graph traversal. Results carry
// committed source identities; no legacy physical DocumentRowRef is fabricated.
func (d *VectorPartitionPreparedDomainV2) SearchLocalV2(ctx context.Context, query []float32, topK, efSearch int) ([]VectorPartitionPreparedResultV2, error) {
	if d == nil {
		return nil, backenddb.ErrClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.view == nil {
		return nil, backenddb.ErrClosed
	}
	results, _, err := d.view.searchCosineWithContext(ctx, query, columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch, OmitResultMaterialization: true}, &d.scratch)
	if err != nil {
		return nil, err
	}
	out := make([]VectorPartitionPreparedResultV2, 0, len(results))
	for _, r := range results {
		if r.Ordinal < 0 || r.Ordinal >= len(d.members) {
			return nil, fmt.Errorf("%w: result ordinal", ErrVectorPartitionManifestInvalid)
		}
		id, ok := d.view.documentIDForOrdinal(r.Ordinal)
		if !ok {
			return nil, fmt.Errorf("%w: result document", ErrVectorPartitionManifestInvalid)
		}
		m := d.members[r.Ordinal]
		out = append(out, VectorPartitionPreparedResultV2{DocumentID: slices.Clone(id), Source: m.Source, MembershipKind: m.Kind, Score: r.Score})
	}
	return out, nil
}

func (c *Collection) openPagedDomainSectionsV2(ctx context.Context, domain uint32, assets []VectorPartitionAssetV1, digest [32]byte) (_ *columnHNSWSearchPackPreparedView, err error) {
	manager := mappedresource.NewManager()
	var acquired []*mappedresource.Handle
	defer func() {
		if err != nil {
			for _, h := range acquired {
				_ = h.Release()
			}
		}
	}()
	acquire := func(a VectorPartitionAssetV1) (*mappedresource.Handle, error) {
		if a.Ref.Length <= 0 || a.Ref.Length > vectorPartitionSearchAssetMaxBytesV1 {
			return nil, fmt.Errorf("%w: pack extent", ErrVectorPartitionManifestInvalid)
		}
		path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), a.Ref)
		if err != nil {
			return nil, err
		}
		key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: a.Ref.Namespace, Kind: string(a.Ref.Kind), Generation: a.Ref.Generation, PartID: a.Ref.PartID, FileID: a.Ref.FileID, Offset: a.Ref.Offset, Length: a.Ref.Length, Checksum: uint64(a.Ref.Checksum), Version: columnHNSWSearchPackVersionV7, Encoding: columnVectorIndexStateEncodingHNSWSearchPackV1, Section: mappedresource.Section{Kind: string(columnVectorIndexStateAssetRoleHNSWSearchPack), Category: string(a.Ref.Kind), Name: a.ID}}
		h, err := manager.AcquireFileRange(key, mappedresource.Scope{Kind: mappedresource.ScopePreparedSearch, ID: "vector_partition_source_v2/" + a.ID, Collection: c.name, Namespace: a.Ref.Namespace, Generation: a.Ref.Generation}, path, mappedresource.AcquireOptions{ValidationMode: mappedresource.ValidationVerify, PreferMapped: true, AllowHeapCopy: true, ResourceRoot: c.db.ColumnAssetRootDir(), ResourcePath: path})
		if err == nil {
			acquired = append(acquired, h)
		}
		return h, err
	}
	var root *mappedresource.Handle
	type chunk struct {
		ordinal uint32
		asset   VectorPartitionAssetV1
	}
	groups := make(map[columnHNSWSearchPackSectionKey][]chunk)
	for _, a := range assets {
		if a.ID == vectorPartitionLocalAssetIDV1(domain) {
			if root != nil {
				return nil, fmt.Errorf("%w: duplicate graph root", ErrVectorPartitionManifestInvalid)
			}
			root, err = acquire(a)
			if err != nil {
				return nil, err
			}
			continue
		}
		key, ordinal, parseErr := parseVectorPartitionLocalSectionChunkAssetIDV1(domain, a.ID)
		if parseErr != nil {
			return nil, parseErr
		}
		groups[key] = append(groups[key], chunk{ordinal: ordinal, asset: a})
	}
	if root == nil {
		return nil, fmt.Errorf("%w: missing graph root", ErrVectorPartitionManifestInvalid)
	}
	sections := make(map[columnHNSWSearchPackSectionKey][]*mappedresource.Handle, len(groups))
	for key, chunks := range groups {
		slices.SortFunc(chunks, func(a, b chunk) int {
			if a.ordinal < b.ordinal {
				return -1
			}
			if a.ordinal > b.ordinal {
				return 1
			}
			return 0
		})
		for i, part := range chunks {
			if part.ordinal != uint32(i) {
				return nil, fmt.Errorf("%w: section chunk sequence", ErrVectorPartitionManifestInvalid)
			}
			h, err := acquire(part.asset)
			if err != nil {
				return nil, err
			}
			sections[key] = append(sections[key], h)
		}
	}
	return newColumnHNSWSearchPackPreparedViewFromSectionHandlesWithContext(ctx, manager, root, sections, columnHNSWSearchPackDecodeOptions{SourceV2: true, ExpectedMembershipDigest: digest})
}
