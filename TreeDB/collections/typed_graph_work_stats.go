package collections

import "github.com/snissn/gomap/TreeDB/internal/workstats"

// ColumnGraphQueryWork is owner-local selected-route work, including an error
// prefix. It is available in Minimal mode and does not sample process counters.
// Route is empty until a scoring/empty branch executes. An exact route can score
// only the suffix when the captured base is empty; it is not a document scan.
type ColumnGraphQueryWork struct {
	Available                                                 bool
	Route                                                     string
	BaseANNScored, BaseCandidates, BaseEdges                  uint64
	DeltaScored, ExactBaseScored, BaseShadowed, BaseResultIDs uint64
	Filter                                                    ColumnGraphFilterWork
}

// Filter cardinality is final only when Completed is true. MappingWorkCharged
// is the admitted upper bound, not a measured number of comparisons. Retained
// and scratch fields are per-call capacities/peaks, not cumulative allocation.
type ColumnGraphFilterWork struct {
	Attempted, Completed                                                       bool
	EligibleRows, SourceIDs, SourceBytes, InspectedEntries, MappingWorkCharged uint64
	RetainedBytes, ScratchIDBytes, ScratchRows, OrdinalGrowthPeakBytes         uint64
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
