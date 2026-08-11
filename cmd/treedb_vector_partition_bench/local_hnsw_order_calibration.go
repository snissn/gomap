package main

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
)

const localHNSWOrderCalibrationSchemaV1 = "treedb_local_hnsw_order_calibration_v1"

// localHNSWOrderCalibrationQueryV1 deliberately gives the two V3 candidates
// construction-order names. The embedded repair-shaped value is not encoded;
// it only reuses the already fail-closed query and ordinary-timing mechanics.
type localHNSWOrderCalibrationQueryV1 struct {
	Schema          string                               `json:"schema"`
	Ordinal         int                                  `json:"ordinal"`
	QueryFP32SHA256 string                               `json:"query_fp32_sha256"`
	P2Route         []uint32                             `json:"p2_route"`
	P16Route        []uint32                             `json:"p16_route"`
	Truth           []localHNSWAttributionQueryResultV1  `json:"truth"`
	RoutingRecall   float64                              `json:"routing_recall"`
	SourceOrder     localHNSWRepairCalibrationVariantV1  `json:"source_order"`
	StableIDHash    localHNSWRepairCalibrationVariantV1  `json:"stable_id_hash_order"`
	SourceSearches  []localHNSWRepairCalibrationSearchV1 `json:"source_order_searches"`
	HashSearches    []localHNSWRepairCalibrationSearchV1 `json:"stable_id_hash_order_searches"`
	query           []float32
	raw             localHNSWRepairCalibrationQueryV1
}

type localHNSWOrderCalibrationSummaryV1 struct {
	Schema       string                                `json:"schema"`
	SourceOrder  localHNSWRepairCalibrationAggregateV1 `json:"source_order"`
	StableIDHash localHNSWRepairCalibrationAggregateV1 `json:"stable_id_hash_order"`
}

func localHNSWOrderCalibrationQueryV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, sourceOrder, stableIDHash *localHNSWVariantHarnessV1, ordinal int, query []float32, truth []m8CanonicalResultV1) (localHNSWOrderCalibrationQueryV1, error) {
	var out localHNSWOrderCalibrationQueryV1
	if sourceOrder == nil || stableIDHash == nil {
		return out, errors.New("invalid local HNSW order calibration variants")
	}
	raw, err := localHNSWRepairCalibrationQueryV1Build(ctx, source, sourceOrder, stableIDHash, ordinal, query, truth)
	if err != nil {
		return out, err
	}
	out = localHNSWOrderCalibrationQueryV1{
		Schema: localHNSWOrderCalibrationSchemaV1, Ordinal: raw.Ordinal, QueryFP32SHA256: raw.QueryFP32SHA256,
		P2Route: append([]uint32(nil), raw.P2Route...), P16Route: append([]uint32(nil), raw.P16Route...), Truth: append([]localHNSWAttributionQueryResultV1(nil), raw.Truth...), RoutingRecall: raw.RoutingRecall,
		SourceOrder: raw.Overlay, StableIDHash: raw.Repair, SourceSearches: append([]localHNSWRepairCalibrationSearchV1(nil), raw.OverlaySearches...), HashSearches: append([]localHNSWRepairCalibrationSearchV1(nil), raw.RepairSearches...),
		query: append([]float32(nil), raw.Query...), raw: raw,
	}
	if !localHNSWOrderCalibrationQueryV1Valid(out, len(sourceOrder.searchers)) {
		return localHNSWOrderCalibrationQueryV1{}, errors.New("invalid local HNSW order calibration evidence")
	}
	return out, nil
}

func localHNSWOrderCalibrationQueryV1Valid(query localHNSWOrderCalibrationQueryV1, partitions int) bool {
	if query.Schema != localHNSWOrderCalibrationSchemaV1 || !localHNSWRepairCalibrationQueryV1Valid(query.raw, partitions) || query.Ordinal != query.raw.Ordinal || query.QueryFP32SHA256 != query.raw.QueryFP32SHA256 || query.RoutingRecall != query.raw.RoutingRecall || !slices.Equal(query.P2Route, query.raw.P2Route) || !slices.Equal(query.P16Route, query.raw.P16Route) || !slices.Equal(query.query, query.raw.Query) {
		return false
	}
	return localHNSWAttributionQueryResultBitsEqualV1(query.Truth, query.raw.Truth) && localHNSWOrderCalibrationVariantEqualV1(query.SourceOrder, query.raw.Overlay) && localHNSWOrderCalibrationVariantEqualV1(query.StableIDHash, query.raw.Repair) && slices.EqualFunc(query.SourceSearches, query.raw.OverlaySearches, localHNSWOrderCalibrationSearchEqualV1) && slices.EqualFunc(query.HashSearches, query.raw.RepairSearches, localHNSWOrderCalibrationSearchEqualV1)
}

func localHNSWOrderCalibrationVariantEqualV1(left, right localHNSWRepairCalibrationVariantV1) bool {
	return left.P2Recall == right.P2Recall && left.P16Recall == right.P16Recall && localHNSWAttributionQueryResultBitsEqualV1(left.P2Results, right.P2Results) && localHNSWAttributionQueryResultBitsEqualV1(left.P16Results, right.P16Results)
}

func localHNSWAttributionQueryResultBitsEqualV1(left, right []localHNSWAttributionQueryResultV1) bool {
	return slices.EqualFunc(left, right, func(a, b localHNSWAttributionQueryResultV1) bool { return a.ID == b.ID && a.ScoreBits == b.ScoreBits })
}

func localHNSWOrderCalibrationSearchEqualV1(left, right localHNSWRepairCalibrationSearchV1) bool {
	return left.Candidates == right.Candidates && left.NativeEdges == right.NativeEdges && left.AuxiliaryEdges == right.AuxiliaryEdges && left.AuxiliaryCandidates == right.AuxiliaryCandidates && left.AuxiliaryAdmissions == right.AuxiliaryAdmissions && left.FrontierAdmissions == right.FrontierAdmissions && left.TerminationReason == right.TerminationReason && left.VisitedOrdinalsSHA256 == right.VisitedOrdinalsSHA256 && localHNSWAttributionQueryResultBitsEqualV1(left.Results, right.Results)
}

func localHNSWOrderCalibrationSummaryV1Build(ctx context.Context, sidecar string, source *m8ProductionMultiGroupAssetsV1, sourceOrder, stableIDHash *localHNSWVariantHarnessV1, ordinals []int, queries [][]float32, truth [][]m8CanonicalResultV1) (localHNSWAttributionArtifactV1, []localHNSWOrderCalibrationQueryV1, localHNSWOrderCalibrationSummaryV1, error) {
	var out localHNSWOrderCalibrationSummaryV1
	if sidecar == "" || len(ordinals) != 806 || len(ordinals) != len(queries) || len(ordinals) != len(truth) {
		return localHNSWAttributionArtifactV1{}, nil, out, errors.New("invalid local HNSW order calibration alignment")
	}
	out.Schema = localHNSWOrderCalibrationSchemaV1
	rawSummary := localHNSWRepairCalibrationSummaryV1{Schema: localHNSWRepairCalibrationSchemaV1}
	rows := make([]localHNSWOrderCalibrationQueryV1, 0, len(ordinals))
	artifact, err := localHNSWAttributionWriteGzipJSONLV1(sidecar, func(encoder *json.Encoder) (int, error) {
		for i, ordinal := range ordinals {
			if i > 0 && ordinals[i-1] >= ordinal {
				return 0, errors.New("invalid local HNSW order calibration ordinals")
			}
			query, queryErr := localHNSWOrderCalibrationQueryV1Build(ctx, source, sourceOrder, stableIDHash, ordinal, queries[i], truth[i])
			if queryErr != nil || localHNSWRepairCalibrationSummaryAddV1(&rawSummary, query.raw) != nil {
				return 0, errors.Join(queryErr, errors.New("invalid local HNSW order calibration summary"))
			}
			rows = append(rows, query)
			if encodeErr := encoder.Encode(query); encodeErr != nil {
				return 0, encodeErr
			}
		}
		return len(rows), nil
	})
	if err != nil || artifact.Records != len(ordinals) || localHNSWRepairCalibrationSummaryFinishV1(&rawSummary) != nil {
		return localHNSWAttributionArtifactV1{}, nil, localHNSWOrderCalibrationSummaryV1{}, errors.Join(err, errors.New("invalid local HNSW order calibration artifact"))
	}
	out.SourceOrder, out.StableIDHash = rawSummary.Overlay, rawSummary.Repair
	return artifact, rows, out, nil
}

// localHNSWOrderCalibrationTimingV1Build keeps the old helper's ordinary-path
// parity check, then relabels only its already validated public cells.
func localHNSWOrderCalibrationTimingV1Build(ctx context.Context, sourceOrder, stableIDHash *localHNSWVariantHarnessV1, queries []localHNSWOrderCalibrationQueryV1) (localHNSWRepairCalibrationTimingV1, error) {
	raw := make([]localHNSWRepairCalibrationQueryV1, len(queries))
	for i := range queries {
		if !localHNSWOrderCalibrationQueryV1Valid(queries[i], len(sourceOrder.searchers)) {
			return localHNSWRepairCalibrationTimingV1{}, errors.New("invalid local HNSW order timing query")
		}
		raw[i] = queries[i].raw
	}
	timing, err := localHNSWRepairCalibrationTimingV1Build(ctx, sourceOrder, stableIDHash, raw)
	if err != nil {
		return localHNSWRepairCalibrationTimingV1{}, err
	}
	for i := range timing.Cells {
		switch timing.Cells[i].Variant {
		case "overlay_current":
			timing.Cells[i].Variant = "source_order"
		case "auxiliary_navigation":
			timing.Cells[i].Variant = "stable_id_hash_order"
		default:
			return localHNSWRepairCalibrationTimingV1{}, errors.New("invalid local HNSW order timing variant")
		}
	}
	return timing, nil
}
