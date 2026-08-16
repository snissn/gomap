package main

// M18 query traces are intentionally an offline sidecar.  The serving
// attribution keeps these slices non-serializable so normal search does not
// acquire an evidence-format contract.

import (
	"errors"

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
	if value.Schema != localHNSWM18EdgeTraceSchemaV1 || len(ids) == 0 || origins == nil {
		return errors.New("invalid M18 edge trace identity")
	}
	if _, err := localHNSWAttributionQueryRecordValidateV1(value.Record, ids, truth); err != nil {
		return err
	}
	var auxiliary uint64
	for _, edge := range value.Edges {
		if edge.Auxiliary {
			auxiliary++
		}
	}
	trace := collections.VectorPartitionSearchAttributionV1{Schema: "treedb_vector_partition_search_attribution_v1", FrontierAdmissions: value.Record.FrontierAdmissions, SeedCandidates: value.Record.SeedCandidates, SeedAdmissions: value.Record.SeedAdmissions, VisitedRows: uint64(len(value.Record.VisitedOrdinals)), VisitedOrdinalsSHA256: value.Record.VisitedOrdinalsSHA256, TerminationReason: value.Record.TerminationReason, VisitedOrdinals: append([]uint32(nil), value.Record.VisitedOrdinals...), EdgeEvents: append([]collections.VectorPartitionSearchEdgeEventV1(nil), value.Edges...), SeedEvents: append([]collections.VectorPartitionSearchSeedEventV1(nil), value.Seeds...)}
	metrics := collections.VectorPartitionSearchMetricsV1{Candidates: value.Record.Candidates, Edges: value.Record.Edges, AuxiliaryEdges: auxiliary, Route: collections.VectorPartitionSearchRouteHNSWSearchPackV1}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, trace, origins, ids, truth)
	if err != nil || utility != value.Record.Utility {
		return errors.New("M18 edge trace utility replay")
	}
	recoveries := localHNSWAttributionTruthRecoveryRecordsV1(localHNSWAttributionTruthRecoveriesV1(trace, origins, ids, truth))
	if len(recoveries) != len(value.Record.TruthRecoveries) {
		return errors.New("M18 edge trace recovery replay")
	}
	for i := range recoveries {
		if recoveries[i] != value.Record.TruthRecoveries[i] {
			return errors.New("M18 edge trace recovery replay")
		}
	}
	return nil
}
