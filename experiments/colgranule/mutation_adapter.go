package colgranule

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

type ColumnMutationAdapterOptions struct {
	Collection        string
	StoreOptions      ColumnStoreOptions
	Dictionaries      map[string]map[string]int64
	ReplayProfile     ColumnMutationReplayProfile
	InitialPartID     uint64
	InitialGeneration uint64
}

type ColumnMutationBatch struct {
	Inserts                 ColumnBatch
	Updates                 ColumnBatch
	Deletes                 []int64
	SourceRowRootGeneration uint64
	SourceRowVersionLower   uint64
	SourceRowVersionUpper   uint64
}

type ColumnMutationApplyResult struct {
	Manifest     ColumnCollectionManifest
	Part         ColumnWorkspacePartManifest
	GenerationID uint64
	InsertedRows int
	UpdatedRows  int
	DeletedRows  int
}

type ColumnMutationAdapter struct {
	workspace     *ColumnWorkspace
	collection    string
	opts          ColumnStoreOptions
	dictionaries  map[string]map[string]int64
	replayProfile ColumnMutationReplayProfile
	baseParts     []ColumnManifestPartRef
	deltaParts    []ColumnManifestPartRef
	tombstones    []ColumnTombstone
	manifest      ColumnCollectionManifest
	nextPartID    uint64
	nextGen       uint64
}

func NewColumnMutationAdapter(workspace *ColumnWorkspace, opts ColumnMutationAdapterOptions) (*ColumnMutationAdapter, error) {
	if workspace == nil {
		return nil, fmt.Errorf("colgranule: nil column workspace")
	}
	normalized, err := normalizeColumnStoreOptions(opts.StoreOptions)
	if err != nil {
		return nil, err
	}
	if err := opts.ReplayProfile.Validate(); err != nil {
		return nil, err
	}
	replayProfile := opts.ReplayProfile.normalized()
	syncMode, err := replayProfile.workspaceManifestSyncMode()
	if err != nil {
		return nil, err
	}
	if workspace.ManifestSyncMode() != syncMode {
		return nil, fmt.Errorf(
			"colgranule: column mutation replay profile %q requires workspace manifest sync mode %q, got %q. "+
				"Use %q for durable replay or %q for benchmark-ceiling replay",
			replayProfile.Label(),
			syncMode,
			workspace.ManifestSyncMode(),
			ColumnWorkspaceManifestSyncDurable,
			ColumnWorkspaceManifestSyncDisabledForBenchmark,
		)
	}
	collection := opts.Collection
	if collection == "" {
		collection = workspace.Manifest().Collection
	}
	if collection == "" {
		return nil, fmt.Errorf("colgranule: empty column mutation collection")
	}
	adapter := &ColumnMutationAdapter{
		workspace:     workspace,
		collection:    collection,
		opts:          normalized,
		dictionaries:  opts.Dictionaries,
		replayProfile: replayProfile,
		nextPartID:    opts.InitialPartID,
		nextGen:       opts.InitialGeneration,
	}
	if adapter.nextPartID == 0 {
		adapter.nextPartID = 1
	}
	if adapter.nextGen == 0 {
		adapter.nextGen = 1
	}
	manifest, err := workspace.LoadCollectionManifest()
	if err == nil {
		adapter.baseParts = cloneColumnManifestPartRefs(manifest.PartSet.BaseParts)
		adapter.deltaParts = cloneColumnManifestPartRefs(manifest.PartSet.DeltaParts)
		adapter.tombstones = append([]ColumnTombstone(nil), manifest.PartSet.Tombstones...)
		adapter.manifest = manifest
		adapter.nextGen = max(adapter.nextGen, manifest.ActiveGeneration+1)
		adapter.nextPartID = max(adapter.nextPartID, columnMutationNextPartID(workspace.Manifest()))
		return adapter, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err = adapter.newManifest()
	if err != nil {
		return nil, err
	}
	if err := workspace.SaveCollectionManifest(manifest); err != nil {
		return nil, err
	}
	adapter.manifest = manifest
	return adapter, nil
}

func (a *ColumnMutationAdapter) ReplayProfile() ColumnMutationReplayProfile {
	if a == nil {
		return ColumnMutationReplayProfile{}
	}
	return a.replayProfile
}

func (a *ColumnMutationAdapter) Manifest() ColumnCollectionManifest {
	if a == nil {
		return ColumnCollectionManifest{}
	}
	return a.manifest
}

func (a *ColumnMutationAdapter) Reader(opts ColumnPartImageReadOptions) (*ColumnPartSetReader, error) {
	if a == nil {
		return nil, fmt.Errorf("colgranule: nil column mutation adapter")
	}
	return OpenColumnPartSetReader(a.workspace, a.Manifest(), opts)
}

func (a *ColumnMutationAdapter) PublishBaseBatch(batch ColumnBatch, coverage ColumnPartCoverageOptions) (ColumnMutationApplyResult, error) {
	if a == nil {
		return ColumnMutationApplyResult{}, fmt.Errorf("colgranule: nil column mutation adapter")
	}
	rows, err := validateColumnBatch(batch, a.opts.Columns)
	if err != nil {
		return ColumnMutationApplyResult{}, err
	}
	if err := validateColumnMutationCoverageOptions(coverage); err != nil {
		return ColumnMutationApplyResult{}, err
	}
	partID := a.nextPartID
	generationID := a.nextGen
	part, err := BuildColumnPart(partID, a.opts, batch)
	if err != nil {
		return ColumnMutationApplyResult{}, err
	}
	entry, err := a.workspace.PublishPart(part, a.dictionaries)
	if err != nil {
		return ColumnMutationApplyResult{}, err
	}
	a.baseParts = append(a.baseParts, NewColumnManifestPartRefWithCoverageOptions(ColumnPartRoleBase, generationID, entry, coverage))
	if err := a.publishManifest(); err != nil {
		return ColumnMutationApplyResult{}, err
	}
	a.nextPartID++
	a.nextGen++
	return ColumnMutationApplyResult{
		Manifest:     a.manifest,
		Part:         entry,
		GenerationID: generationID,
		InsertedRows: rows,
	}, nil
}

func (a *ColumnMutationAdapter) Apply(batch ColumnMutationBatch) (ColumnMutationApplyResult, error) {
	if a == nil {
		return ColumnMutationApplyResult{}, fmt.Errorf("colgranule: nil column mutation adapter")
	}
	if err := validateColumnMutationCoverageOptions(ColumnPartCoverageOptions{
		SourceRowRootGeneration: batch.SourceRowRootGeneration,
		SourceRowVersionLower:   batch.SourceRowVersionLower,
		SourceRowVersionUpper:   batch.SourceRowVersionUpper,
	}); err != nil {
		return ColumnMutationApplyResult{}, err
	}
	rows, inserted, updated, err := a.mergeMutationRows(batch.Inserts, batch.Updates)
	if err != nil {
		return ColumnMutationApplyResult{}, err
	}
	deletes := sortedUniqueInt64s(batch.Deletes)
	if rows.Rows == 0 && len(deletes) == 0 {
		return ColumnMutationApplyResult{Manifest: a.manifest}, nil
	}
	generationID := a.nextGen
	result := ColumnMutationApplyResult{
		GenerationID: generationID,
		InsertedRows: inserted,
		UpdatedRows:  updated,
		DeletedRows:  len(deletes),
	}
	if rows.Rows != 0 {
		partID := a.nextPartID
		part, err := BuildColumnDeltaPart(partID, a.opts, rows)
		if err != nil {
			return ColumnMutationApplyResult{}, err
		}
		entry, err := a.workspace.PublishPart(part, a.dictionaries)
		if err != nil {
			return ColumnMutationApplyResult{}, err
		}
		ref := NewColumnManifestPartRefWithCoverageOptions(ColumnPartRoleDelta, generationID, entry, ColumnPartCoverageOptions{
			SourceRowRootGeneration: batch.SourceRowRootGeneration,
			SourceRowVersionLower:   batch.SourceRowVersionLower,
			SourceRowVersionUpper:   batch.SourceRowVersionUpper,
		})
		a.deltaParts = append(a.deltaParts, ref)
		a.nextPartID++
		result.Part = entry
	}
	for _, primaryID := range deletes {
		a.tombstones = append(a.tombstones, ColumnTombstone{
			PrimaryID:     primaryID,
			GenerationID:  generationID,
			Reason:        "delete",
			PreparedBytes: 0,
		})
	}
	if len(deletes) != 0 {
		sort.Slice(a.tombstones, func(i, j int) bool {
			if a.tombstones[i].GenerationID != a.tombstones[j].GenerationID {
				return a.tombstones[i].GenerationID < a.tombstones[j].GenerationID
			}
			return a.tombstones[i].PrimaryID < a.tombstones[j].PrimaryID
		})
	}
	if err := a.publishManifest(); err != nil {
		return ColumnMutationApplyResult{}, err
	}
	a.nextGen++
	result.Manifest = a.manifest
	return result, nil
}

func (a *ColumnMutationAdapter) mergeMutationRows(inserts ColumnBatch, updates ColumnBatch) (ColumnBatch, int, int, error) {
	insertRows, err := validateOptionalColumnBatch(inserts, a.opts.Columns)
	if err != nil {
		return ColumnBatch{}, 0, 0, err
	}
	updateRows, err := validateOptionalColumnBatch(updates, a.opts.Columns)
	if err != nil {
		return ColumnBatch{}, 0, 0, err
	}
	total := insertRows + updateRows
	if total == 0 {
		return ColumnBatch{}, 0, 0, nil
	}
	columns := make(map[string][]int64, len(a.opts.Columns))
	vectors := make(map[string]Float32VectorColumn)
	adjacencyLists := make(map[string]AdjacencyListColumn)
	for _, def := range a.opts.Columns {
		switch def.Type {
		case ColumnTypeInt64, ColumnTypeLowCardinalityCode, ColumnTypeBool:
			dst := make([]int64, 0, total)
			if insertRows != 0 {
				dst = append(dst, inserts.Columns[def.Name]...)
			}
			if updateRows != 0 {
				dst = append(dst, updates.Columns[def.Name]...)
			}
			columns[def.Name] = dst
		case ColumnTypeFloat32Vector:
			dst := Float32VectorColumn{Dims: def.VectorDims}
			valueCap := total * def.VectorDims
			dst.Values = make([]float32, 0, valueCap)
			if insertRows != 0 {
				dst.Values = append(dst.Values, inserts.Float32Vectors[def.Name].Values...)
			}
			if updateRows != 0 {
				dst.Values = append(dst.Values, updates.Float32Vectors[def.Name].Values...)
			}
			vectors[def.Name] = dst
		case ColumnTypeAdjacencyList:
			dst := AdjacencyListColumn{Offsets: []uint32{0}}
			if insertRows != 0 {
				if err := appendAdjacencyRows(&dst, inserts.AdjacencyLists[def.Name]); err != nil {
					return ColumnBatch{}, 0, 0, fmt.Errorf("colgranule: insert adjacency-list column %s: %w", def.Name, err)
				}
			}
			if updateRows != 0 {
				if err := appendAdjacencyRows(&dst, updates.AdjacencyLists[def.Name]); err != nil {
					return ColumnBatch{}, 0, 0, fmt.Errorf("colgranule: update adjacency-list column %s: %w", def.Name, err)
				}
			}
			adjacencyLists[def.Name] = dst
		}
	}
	if err := validateDistinctPrimaryIDs(ColumnBatch{Rows: total, Columns: columns}, a.opts.LogicalPrimaryKey.Columns[0]); err != nil {
		return ColumnBatch{}, 0, 0, err
	}
	return ColumnBatch{Rows: total, Columns: columns, Float32Vectors: vectors, AdjacencyLists: adjacencyLists}, insertRows, updateRows, nil
}

func appendAdjacencyRows(dst *AdjacencyListColumn, src AdjacencyListColumn) error {
	for row := 0; row+1 < len(src.Offsets); row++ {
		start := int(src.Offsets[row])
		end := int(src.Offsets[row+1])
		dst.Values = append(dst.Values, src.Values[start:end]...)
		if len(dst.Values) > math.MaxUint32 {
			return fmt.Errorf("adjacency values=%d exceed uint32 offsets", len(dst.Values))
		}
		dst.Offsets = append(dst.Offsets, uint32(len(dst.Values)))
	}
	return nil
}

func (a *ColumnMutationAdapter) publishManifest() error {
	manifest, err := a.newManifest()
	if err != nil {
		return err
	}
	if err := a.workspace.SaveCollectionManifest(manifest); err != nil {
		return err
	}
	a.manifest = manifest
	return nil
}

func (a *ColumnMutationAdapter) newManifest() (ColumnCollectionManifest, error) {
	manifest, err := NewColumnCollectionManifest(a.collection, a.opts, a.baseParts, a.deltaParts, a.tombstones)
	if err != nil {
		return ColumnCollectionManifest{}, err
	}
	if a.manifest.CreatedUnix != 0 {
		manifest.CreatedUnix = a.manifest.CreatedUnix
	}
	if len(a.baseParts)+len(a.deltaParts)+len(a.tombstones) != 0 {
		manifest.UpdatedUnix = time.Now().UnixNano()
	}
	return manifest, nil
}

func validateOptionalColumnBatch(batch ColumnBatch, defs []ColumnDefinition) (int, error) {
	if batch.Rows == 0 && len(batch.Columns) == 0 && len(batch.Float32Vectors) == 0 && len(batch.AdjacencyLists) == 0 {
		return 0, nil
	}
	return validateColumnBatch(batch, defs)
}

func validateDistinctPrimaryIDs(batch ColumnBatch, primaryColumn string) error {
	ids := batch.Columns[primaryColumn]
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return fmt.Errorf("colgranule: duplicate mutation primary id %d", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validateColumnMutationCoverageOptions(opts ColumnPartCoverageOptions) error {
	if opts.SourceRowVersionLower != 0 && opts.SourceRowVersionUpper == 0 {
		return fmt.Errorf("colgranule: source row version lower=%d without upper bound", opts.SourceRowVersionLower)
	}
	if opts.SourceRowVersionUpper != 0 && opts.SourceRowVersionLower >= opts.SourceRowVersionUpper {
		return fmt.Errorf("colgranule: source row version lower=%d upper=%d", opts.SourceRowVersionLower, opts.SourceRowVersionUpper)
	}
	if opts.SourceRowRootGeneration == 0 && (opts.SourceRowVersionLower != 0 || opts.SourceRowVersionUpper != 0) {
		return fmt.Errorf("colgranule: source row versions require source row root generation")
	}
	return nil
}

func columnMutationNextPartID(manifest ColumnWorkspaceManifest) uint64 {
	var maxPartID uint64
	for _, part := range manifest.Parts {
		if part.PartID > maxPartID {
			maxPartID = part.PartID
		}
	}
	return maxPartID + 1
}

func sortedUniqueInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	out := append([]int64(nil), values...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	n := 0
	var last int64
	for i, value := range out {
		if i != 0 && value == last {
			continue
		}
		out[n] = value
		n++
		last = value
	}
	return out[:n]
}
