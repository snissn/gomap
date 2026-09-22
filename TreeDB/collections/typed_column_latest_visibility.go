package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

type typedColumnLatestRowResolver struct {
	parts        []typedColumnLatestPhysicalPart
	byGeneration map[uint64]int
	candidates   []typedColumnLatestCandidate
	idArena      []byte
}

type typedColumnLatestPhysicalPart struct {
	Ref         ColumnAssetRef
	Rows        int
	Role        ColumnManifestPartRole
	DocumentIDs [][]byte
	visibleBits []uint64
}

type typedColumnLatestCandidate struct {
	id         []byte
	partIndex  int
	rowIndex   int
	appliedLSN uint64
	generation uint64
	partID     uint64
	ordinal    int
	deleted    bool
}

type typedColumnLatestVisibilityState struct {
	resolver       *typedColumnLatestRowResolver
	PhysicalParts  int
	TombstoneParts int
	CandidateRows  int
	DeletedRows    int
	VisibleRows    int
}

func buildTypedColumnLatestVisibilityState(view columnPhysicalScanSnapshotView, readCache *columnPhysicalAssetReadCache, diag *TypedColumnInt64PredicateScanDiagnostics) (*typedColumnLatestVisibilityState, error) {
	resolver, err := buildTypedColumnLatestRowResolver(view, readCache, diag)
	if err != nil {
		return nil, err
	}
	state := &typedColumnLatestVisibilityState{resolver: resolver, CandidateRows: len(resolver.candidates)}
	for _, part := range resolver.parts {
		state.PhysicalParts++
		if part.Role == ColumnManifestPartRoleTombstone {
			state.TombstoneParts++
		}
		for _, word := range part.visibleBits {
			for word != 0 {
				state.VisibleRows++
				word &= word - 1
			}
		}
	}
	for _, candidate := range resolver.candidates {
		if candidate.deleted {
			state.DeletedRows++
		}
	}
	return state, nil
}

func buildTypedColumnLatestRowResolver(view columnPhysicalScanSnapshotView, readCache *columnPhysicalAssetReadCache, diag *TypedColumnInt64PredicateScanDiagnostics) (*typedColumnLatestRowResolver, error) {
	if readCache == nil {
		return nil, errors.New("collections: typed-column latest-visible resolver requires read cache")
	}
	resolver := &typedColumnLatestRowResolver{
		parts:        make([]typedColumnLatestPhysicalPart, 0, len(view.AssetRefs)),
		byGeneration: make(map[uint64]int, len(view.AssetRefs)),
	}
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(view.Config.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	var rawScratch []byte
	for ordinal, assetRef := range view.AssetRefs {
		if assetRef.Ref.Kind != ColumnAssetKindTCS1PartImage {
			return nil, fmt.Errorf("collections: typed-column latest-visible physical ref kind=%q want %q", assetRef.Ref.Kind, ColumnAssetKindTCS1PartImage)
		}
		partIndex := len(resolver.parts)
		part := typedColumnLatestPhysicalPart{
			Ref:         assetRef.Ref,
			Rows:        assetRef.Rows,
			Role:        assetRef.Role,
			DocumentIDs: make([][]byte, assetRef.Rows),
		}
		if assetRef.Rows > 0 && assetRef.Role != ColumnManifestPartRoleTombstone {
			part.visibleBits = make([]uint64, (assetRef.Rows+63)/64)
		}
		if assetRef.Role != ColumnManifestPartRoleTombstone {
			if _, exists := resolver.byGeneration[assetRef.Ref.Generation]; exists {
				return nil, fmt.Errorf("collections: duplicate live physical row asset for generation=%d", assetRef.Ref.Generation)
			}
			resolver.byGeneration[assetRef.Ref.Generation] = partIndex
		}
		resolver.parts = append(resolver.parts, part)
		raw, err := readCache.read(assetRef.Ref, rawScratch)
		if diag != nil {
			diag.SegmentFileCacheHits = readCache.hits
			diag.SegmentFileCacheMisses = readCache.misses
		}
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column latest-visible physical read generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
		}
		rawScratch = raw
		if diag != nil {
			diag.PhysicalRowAssetReads++
			diag.PhysicalRowIDLookups++
			diag.PhysicalBytesScanned += int64(len(raw))
		}
		_, err = scanColumnPhysicalAssetRowsWithManifestOperation(raw, assetRef.Ref, view.CollectionName, &view.Config, projection, assetRef.Reason, func(row columnPhysicalScanRowView) error {
			if row.RowIndex < 0 || row.RowIndex >= assetRef.Rows {
				return fmt.Errorf("collections: typed-column latest-visible row_index=%d outside manifest rows=%d", row.RowIndex, assetRef.Rows)
			}
			id := resolver.cloneID(row.ID)
			resolver.parts[partIndex].DocumentIDs[row.RowIndex] = id
			resolver.candidates = append(resolver.candidates, typedColumnLatestCandidate{
				id:         id,
				partIndex:  partIndex,
				rowIndex:   row.RowIndex,
				appliedLSN: row.AppliedCommandLSN,
				generation: row.Generation,
				partID:     row.PartID,
				ordinal:    ordinal,
				deleted:    row.Deleted,
			})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column latest-visible physical decode generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
		}
	}
	if err := resolver.buildVisibleBits(); err != nil {
		return nil, err
	}
	return resolver, nil
}

func (r *typedColumnLatestRowResolver) cloneID(id []byte) []byte {
	if len(id) == 0 {
		return nil
	}
	start := len(r.idArena)
	r.idArena = append(r.idArena, id...)
	return r.idArena[start:len(r.idArena)]
}

func (r *typedColumnLatestRowResolver) buildVisibleBits() error {
	if r == nil || len(r.candidates) == 0 {
		return nil
	}
	sort.Slice(r.candidates, func(i, j int) bool {
		cmp := bytes.Compare(r.candidates[i].id, r.candidates[j].id)
		if cmp != 0 {
			return cmp < 0
		}
		return typedColumnLatestCandidateNewer(r.candidates[i], r.candidates[j])
	})
	for start := 0; start < len(r.candidates); {
		end := start + 1
		for end < len(r.candidates) && bytes.Equal(r.candidates[start].id, r.candidates[end].id) {
			end++
		}
		best := r.candidates[start]
		for i := start + 1; i < end; i++ {
			if typedColumnLatestCandidateNewer(r.candidates[i], best) {
				best = r.candidates[i]
			}
		}
		if !best.deleted {
			if best.partIndex < 0 || best.partIndex >= len(r.parts) {
				return fmt.Errorf("collections: typed-column latest-visible part index=%d outside %d", best.partIndex, len(r.parts))
			}
			r.parts[best.partIndex].setVisible(best.rowIndex)
		}
		start = end
	}
	return nil
}

func typedColumnLatestCandidateNewer(a, b typedColumnLatestCandidate) bool {
	if a.appliedLSN != b.appliedLSN {
		return a.appliedLSN > b.appliedLSN
	}
	if a.generation != b.generation {
		return a.generation > b.generation
	}
	if a.partID != b.partID {
		return a.partID > b.partID
	}
	if a.rowIndex != b.rowIndex {
		return a.rowIndex > b.rowIndex
	}
	return a.ordinal > b.ordinal
}

func (p *typedColumnLatestPhysicalPart) setVisible(row int) {
	if row < 0 || row >= p.Rows {
		return
	}
	word := row >> 6
	if word >= len(p.visibleBits) {
		return
	}
	p.visibleBits[word] |= uint64(1) << uint(row&63)
}

func (p *typedColumnLatestPhysicalPart) rowVisible(row int) bool {
	if p == nil || row < 0 || row >= p.Rows {
		return false
	}
	word := row >> 6
	return word < len(p.visibleBits) && (p.visibleBits[word]&(uint64(1)<<uint(row&63))) != 0
}

func (p *typedColumnLatestPhysicalPart) documentID(row int) []byte {
	if p == nil || row < 0 || row >= len(p.DocumentIDs) {
		return nil
	}
	return p.DocumentIDs[row]
}

func (r *typedColumnLatestRowResolver) partForGeneration(generation uint64) (*typedColumnLatestPhysicalPart, bool) {
	if r == nil {
		return nil, false
	}
	idx, ok := r.byGeneration[generation]
	if !ok || idx < 0 || idx >= len(r.parts) {
		return nil, false
	}
	return &r.parts[idx], true
}
