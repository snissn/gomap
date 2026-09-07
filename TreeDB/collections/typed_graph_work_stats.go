package collections

import "github.com/snissn/gomap/TreeDB/internal/workstats"

// ColumnGraphQueryWork is owner-local selected-route work, including an error
// prefix. It is available in Minimal mode and does not sample process counters.
// Route is empty until a scoring/empty branch executes. An exact route can score
// only the suffix when the captured base is empty; it is not a document scan.
type ColumnGraphQueryWork struct {
	Available       bool                     `json:"available"`
	Completed       bool                     `json:"completed"`
	Route           string                   `json:"route"`
	BaseANNScored   uint64                   `json:"base_ann_scored"`
	BaseCandidates  uint64                   `json:"base_candidates"`
	BaseEdges       uint64                   `json:"base_edges"`
	DeltaScored     uint64                   `json:"delta_scored"`
	ExactBaseScored uint64                   `json:"exact_base_scored"`
	BaseShadowed    uint64                   `json:"base_shadowed"`
	BaseResultIDs   uint64                   `json:"base_result_ids"`
	Filter          ColumnGraphFilterWork    `json:"filter"`
	Snapshot        ColumnGraphQuerySnapshot `json:"snapshot"`
}

// ColumnGraphQuerySnapshot is copied from the acquired owner, never the latest
// publication or a requested generation. False means acquisition did not finish.
// SchemaGeneration is the acquired vector index definition's generation, not
// the service's aggregate vector/text generation guard.
type ColumnGraphQuerySnapshot struct {
	Available          bool                    `json:"available"`
	SchemaHash         uint64                  `json:"schema_hash"`
	SchemaGeneration   uint64                  `json:"schema_generation"`
	BaseManifest       ColumnGraphManifestWork `json:"base_manifest"`
	CurrentManifest    ColumnGraphManifestWork `json:"current_manifest"`
	BaseCoverageLSN    uint64                  `json:"base_coverage_lsn"`
	CurrentCoverageLSN uint64                  `json:"current_coverage_lsn"`
}

// ColumnGraphManifestWork keeps zero fields explicit in a versioned work proof.
type ColumnGraphManifestWork struct {
	Generation uint64 `json:"generation"`
	Format     string `json:"format"`
	Version    uint16 `json:"version"`
	Checksum   uint64 `json:"checksum"`
}

func (o *typedGraphReadOwner) querySnapshot() ColumnGraphQuerySnapshot {
	base := o.overlay.base.catalog.meta.Options.ColumnStore
	current := o.overlay.current.catalog.meta.Options.ColumnStore
	manifest := func(id *ColumnManifestIdentity) ColumnGraphManifestWork {
		if id == nil {
			return ColumnGraphManifestWork{}
		}
		return ColumnGraphManifestWork{id.Generation, id.Format, id.Version, id.Checksum}
	}
	return ColumnGraphQuerySnapshot{Available: true, SchemaHash: current.SchemaHash,
		SchemaGeneration: o.overlay.base.reader.def.SchemaGeneration,
		BaseManifest:     manifest(base.ActiveManifest), CurrentManifest: manifest(current.ActiveManifest),
		BaseCoverageLSN:    base.RecoveryAuthoritativeAppliedCommandLSN,
		CurrentCoverageLSN: current.RecoveryAuthoritativeAppliedCommandLSN}
}

// Filter cardinality is final only when Completed is true. MappingWorkCharged
// is the admitted upper bound, not a measured number of comparisons. Retained
// bytes and ordinal growth measure per-call ordinal capacity. Scratch fields
// measure logical peak rows/ID bytes, not Go capacity or cumulative allocation.
type ColumnGraphFilterWork struct {
	Attempted              bool   `json:"attempted"`
	Completed              bool   `json:"completed"`
	EligibleRows           uint64 `json:"eligible_rows"`
	SourceIDs              uint64 `json:"source_ids"`
	SourceBytes            uint64 `json:"source_bytes"`
	InspectedEntries       uint64 `json:"inspected_entries"`
	MappingWorkCharged     uint64 `json:"mapping_work_charged"`
	RetainedBytes          uint64 `json:"retained_bytes"`
	ScratchIDBytes         uint64 `json:"scratch_id_bytes"`
	ScratchRows            uint64 `json:"scratch_rows"`
	OrdinalGrowthPeakBytes uint64 `json:"ordinal_growth_peak_bytes"`
}

func (p *typedGraphPreparedFilter) work(completed bool) ColumnGraphFilterWork {
	w := ColumnGraphFilterWork{Attempted: true, Completed: completed}
	if p == nil {
		return w
	}
	w.EligibleRows, w.SourceIDs, w.SourceBytes = uint64(p.count), uint64(p.sourceIDs), uint64(p.sourceBytes)
	w.InspectedEntries, w.MappingWorkCharged = uint64(p.inspectedEntries), uint64(p.mappingWork)
	w.RetainedBytes, w.ScratchIDBytes, w.ScratchRows, w.OrdinalGrowthPeakBytes = uint64(p.retainedBytes), uint64(p.scratchIDBytes), uint64(p.scratchRows), uint64(p.ordinalGrowthPeakBytes)
	return w
}

func (s typedGraphOverlaySearchStats) work() ColumnGraphQueryWork {
	return ColumnGraphQueryWork{Available: true, Route: s.Route, BaseANNScored: s.Base.PreparedScoreCalls,
		BaseCandidates: s.Base.Candidates, BaseEdges: s.Base.Edges, DeltaScored: uint64(s.DeltaScored),
		ExactBaseScored: uint64(s.ExactBaseScored), BaseShadowed: uint64(s.BaseShadowed), BaseResultIDs: uint64(s.BaseResultIDs)}
}

// Scoring primitives also have direct internal consumers. Count their actual
// work once here; Requests separately counts public selected serving attempts.
func (s *typedGraphOverlaySearchStats) recordWork() {
	switch s.Route {
	case "typed_empty":
		workstats.Graph.Empty.Add(1)
	case "typed_exact":
		workstats.Graph.Exact.Add(1)
	case "typed_hnsw":
		workstats.Graph.HNSW.Add(1)
	}
	workstats.Graph.BaseANNScored.Add(s.Base.PreparedScoreCalls)
	workstats.Graph.BaseCandidates.Add(s.Base.Candidates)
	workstats.Graph.BaseEdges.Add(s.Base.Edges)
	workstats.Graph.DeltaScored.Add(uint64(s.DeltaScored))
	workstats.Graph.ExactBaseScored.Add(uint64(s.ExactBaseScored))
	workstats.Graph.BaseShadowed.Add(uint64(s.BaseShadowed))
	workstats.Graph.BaseResultIDs.Add(uint64(s.BaseResultIDs))
}
