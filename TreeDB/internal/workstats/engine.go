package workstats

import "sync/atomic"

// OperationCounter separates entry attempts from successful returns. Errors
// include failed admission and partial work; callers never reset these totals.
type OperationCounter struct{ Attempts, Completed, Errors atomic.Uint64 }
type OperationStats struct {
	Attempts  uint64 `json:"attempts"`
	Completed uint64 `json:"completed"`
	Errors    uint64 `json:"errors"`
}

func (c *OperationCounter) Finish(ok bool) {
	if ok {
		c.Completed.Add(1)
	} else {
		c.Errors.Add(1)
	}
}
func (c *OperationCounter) Read() OperationStats {
	return OperationStats{c.Attempts.Load(), c.Completed.Load(), c.Errors.Load()}
}

var Graph struct {
	Requests                 OperationCounter
	Filters                  OperationCounter
	Empty                    atomic.Uint64
	Exact                    atomic.Uint64
	HNSW                     atomic.Uint64
	BaseANNScored            atomic.Uint64
	DeltaScored              atomic.Uint64
	ExactBaseScored          atomic.Uint64
	BaseCandidates           atomic.Uint64
	BaseEdges                atomic.Uint64
	BaseShadowed             atomic.Uint64
	BaseResultIDs            atomic.Uint64
	FilterSourceIDs          atomic.Uint64
	FilterSourceBytes        atomic.Uint64
	FilterInspectedEntries   atomic.Uint64
	FilterMappingWorkCharged atomic.Uint64
}

type GraphStats struct {
	Requests                 OperationStats `json:"requests"`
	Filters                  OperationStats `json:"filters"`
	Empty                    uint64         `json:"empty"`
	Exact                    uint64         `json:"exact"`
	HNSW                     uint64         `json:"hnsw"`
	BaseANNScored            uint64         `json:"base_ann_scored"`
	DeltaScored              uint64         `json:"delta_scored"`
	ExactBaseScored          uint64         `json:"exact_base_scored"`
	BaseCandidates           uint64         `json:"base_candidates"`
	BaseEdges                uint64         `json:"base_edges"`
	BaseShadowed             uint64         `json:"base_shadowed"`
	BaseResultIDs            uint64         `json:"base_result_ids"`
	FilterSourceIDs          uint64         `json:"filter_source_ids"`
	FilterSourceBytes        uint64         `json:"filter_source_bytes"`
	FilterInspectedEntries   uint64         `json:"filter_inspected_entries"`
	FilterMappingWorkCharged uint64         `json:"filter_mapping_work_charged"`
}

func readGraph() GraphStats {
	return GraphStats{
		Requests:                 Graph.Requests.Read(),
		Filters:                  Graph.Filters.Read(),
		Empty:                    Graph.Empty.Load(),
		Exact:                    Graph.Exact.Load(),
		HNSW:                     Graph.HNSW.Load(),
		BaseANNScored:            Graph.BaseANNScored.Load(),
		DeltaScored:              Graph.DeltaScored.Load(),
		ExactBaseScored:          Graph.ExactBaseScored.Load(),
		BaseCandidates:           Graph.BaseCandidates.Load(),
		BaseEdges:                Graph.BaseEdges.Load(),
		BaseShadowed:             Graph.BaseShadowed.Load(),
		BaseResultIDs:            Graph.BaseResultIDs.Load(),
		FilterSourceIDs:          Graph.FilterSourceIDs.Load(),
		FilterSourceBytes:        Graph.FilterSourceBytes.Load(),
		FilterInspectedEntries:   Graph.FilterInspectedEntries.Load(),
		FilterMappingWorkCharged: Graph.FilterMappingWorkCharged.Load(),
	}
}

var Fold struct {
	Build                   OperationCounter
	Public                  OperationCounter
	Renew                   OperationCounter
	Publications            atomic.Uint64
	CandidateBytesCharged   atomic.Uint64
	AppenderAttemptsCharged atomic.Uint64
}

type FoldStats struct {
	Build                   OperationStats `json:"build"`
	Public                  OperationStats `json:"public"`
	Renew                   OperationStats `json:"renew"`
	Publications            uint64         `json:"publications"`
	CandidateBytesCharged   uint64         `json:"candidate_bytes_charged"`
	AppenderAttemptsCharged uint64         `json:"appender_attempts_charged"`
}

func readFold() FoldStats {
	return FoldStats{
		Build:                   Fold.Build.Read(),
		Public:                  Fold.Public.Read(),
		Renew:                   Fold.Renew.Read(),
		Publications:            Fold.Publications.Load(),
		CandidateBytesCharged:   Fold.CandidateBytesCharged.Load(),
		AppenderAttemptsCharged: Fold.AppenderAttemptsCharged.Load(),
	}
}

// Output counters describe materializer work, not serialized HTTP/frame bytes.
// Materialization counts shared collection fetches once; Search and GetMany
// independently attribute their service calls and include completed error prefixes.
var Output struct{ Materialization, Search, GetMany OutputCounter }

type OutputCounter struct {
	OperationCounter
	Requested              atomic.Uint64
	Fetched                atomic.Uint64
	Missing                atomic.Uint64
	OutputBytes            atomic.Uint64
	RetainedPayloadFetches atomic.Uint64
	JSONReconstructionRows atomic.Uint64
	TypedColumnRows        atomic.Uint64
}
type OutputStats struct {
	OperationStats
	Requested              uint64 `json:"requested"`
	Fetched                uint64 `json:"fetched"`
	Missing                uint64 `json:"missing"`
	OutputBytes            uint64 `json:"output_bytes"`
	RetainedPayloadFetches uint64 `json:"retained_payload_fetches"`
	JSONReconstructionRows uint64 `json:"json_reconstruction_rows"`
	TypedColumnRows        uint64 `json:"typed_column_rows"`
}

func (c *OutputCounter) Add(s OutputStats) {
	c.Requested.Add(s.Requested)
	c.Fetched.Add(s.Fetched)
	c.Missing.Add(s.Missing)
	c.OutputBytes.Add(s.OutputBytes)
	c.RetainedPayloadFetches.Add(s.RetainedPayloadFetches)
	c.JSONReconstructionRows.Add(s.JSONReconstructionRows)
	c.TypedColumnRows.Add(s.TypedColumnRows)
}
func (c *OutputCounter) Read() OutputStats {
	return OutputStats{OperationStats: c.OperationCounter.Read(),
		Requested:              c.Requested.Load(),
		Fetched:                c.Fetched.Load(),
		Missing:                c.Missing.Load(),
		OutputBytes:            c.OutputBytes.Load(),
		RetainedPayloadFetches: c.RetainedPayloadFetches.Load(),
		JSONReconstructionRows: c.JSONReconstructionRows.Load(),
		TypedColumnRows:        c.TypedColumnRows.Load(),
	}
}

type OutputWorkStats struct {
	Materialization OutputStats `json:"materialization"`
	Search          OutputStats `json:"search"`
	GetMany         OutputStats `json:"get_many"`
}
