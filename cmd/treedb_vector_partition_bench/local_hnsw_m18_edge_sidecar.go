package main

// M18 query traces are intentionally an offline sidecar.  The serving
// attribution keeps these slices non-serializable so normal search does not
// acquire an evidence-format contract.

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWM18EdgeTraceSchemaV1 = "treedb_local_hnsw_m18_edge_trace_v1"

type localHNSWM18EdgeTraceV1 struct {
	Schema    string                                         `json:"schema"`
	QuerySHA  string                                         `json:"query_fp32_sha256"`
	Partition uint32                                         `json:"partition"`
	Record    localHNSWAttributionQuerySearchV1              `json:"record"`
	Edges     []collections.VectorPartitionSearchEdgeEventV1 `json:"edges"`
	Seeds     []collections.VectorPartitionSearchSeedEventV1 `json:"seeds"`
}

// Validate reconstructs the exact reducer input.  It deliberately compares
// both the persisted record utility and its truth-recovery records, so a
// sidecar cannot swap an origin bucket while preserving aggregate work.
func localHNSWM18EdgeTraceValidateV1(value localHNSWM18EdgeTraceV1, ids []string, origins map[localHNSWAttributionFinalEdgeKeyV1]string, truth map[string]struct{}) error {
	if value.Schema != localHNSWM18EdgeTraceSchemaV1 || !localHNSWAttributionSHA256V1(value.QuerySHA) || len(ids) == 0 || origins == nil {
		return errors.New("invalid M18 edge trace identity")
	}
	if _, err := localHNSWAttributionQueryRecordValidateV1(value.Record, ids, truth); err != nil {
		return fmt.Errorf("M18 edge trace record: %w", err)
	}
	var auxiliary uint64
	for _, edge := range value.Edges {
		if edge.Auxiliary {
			auxiliary++
		}
	}
	trace := collections.VectorPartitionSearchAttributionV1{Schema: "treedb_vector_partition_search_attribution_v1", FrontierAdmissions: value.Record.FrontierAdmissions, SeedCandidates: value.Record.SeedCandidates, SeedAdmissions: value.Record.SeedAdmissions, VisitedRows: uint64(len(value.Record.VisitedOrdinals)), VisitedOrdinalsSHA256: value.Record.VisitedOrdinalsSHA256, TerminationReason: value.Record.TerminationReason, VisitedOrdinals: append([]uint32(nil), value.Record.VisitedOrdinals...), EdgeEvents: append([]collections.VectorPartitionSearchEdgeEventV1(nil), value.Edges...), SeedEvents: append([]collections.VectorPartitionSearchSeedEventV1(nil), value.Seeds...)}
	// QuerySearch.Edges persists total examined work. The reducer consumes the
	// native and auxiliary components separately, so reconstruct native work
	// from the trace before replaying rather than counting auxiliary edges twice.
	if value.Record.Edges < auxiliary {
		return fmt.Errorf("M18 edge trace auxiliary edges exceed total: auxiliary=%d total=%d", auxiliary, value.Record.Edges)
	}
	metrics := collections.VectorPartitionSearchMetricsV1{Candidates: value.Record.Candidates, Edges: value.Record.Edges - auxiliary, AuxiliaryEdges: auxiliary, Route: collections.VectorPartitionSearchRouteHNSWSearchPackV1}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, trace, origins, ids, truth)
	if err != nil {
		return fmt.Errorf("M18 edge trace utility replay: %w", err)
	}
	if utility != value.Record.Utility {
		return fmt.Errorf("M18 edge trace utility replay mismatch: got=%+v want=%+v", utility, value.Record.Utility)
	}
	recoveries := localHNSWAttributionTruthRecoveryRecordsV1(localHNSWAttributionTruthRecoveriesV1(trace, origins, ids, truth))
	if len(recoveries) != len(value.Record.TruthRecoveries) {
		return fmt.Errorf("M18 edge trace recovery replay count: got=%d want=%d", len(recoveries), len(value.Record.TruthRecoveries))
	}
	for i := range recoveries {
		if recoveries[i] != value.Record.TruthRecoveries[i] {
			return fmt.Errorf("M18 edge trace recovery replay index=%d got=%+v want=%+v", i, recoveries[i], value.Record.TruthRecoveries[i])
		}
	}
	return nil
}
