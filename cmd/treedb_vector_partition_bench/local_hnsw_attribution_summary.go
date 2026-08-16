package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
)

const localHNSWAttributionCalibrationSummarySchemaV1 = "treedb_local_hnsw_attribution_calibration_summary_v1"

type localHNSWAttributionRecallAggregateV1 struct {
	Mean float64 `json:"mean"`
	Min  float64 `json:"min"`
}

type localHNSWAttributionCalibrationVariantV1 struct {
	P2EndToEnd        localHNSWAttributionRecallAggregateV1 `json:"p2_end_to_end"`
	P2Local           localHNSWAttributionRecallAggregateV1 `json:"p2_local"`
	AllGlobal         localHNSWAttributionRecallAggregateV1 `json:"all_global"`
	LowCandidates     uint64                                `json:"low_candidates"`
	LowEdges          uint64                                `json:"low_edges"`
	LowFrontier       uint64                                `json:"low_frontier_admissions"`
	HighCandidates    uint64                                `json:"high_candidates"`
	HighEdges         uint64                                `json:"high_edges"`
	HighFrontier      uint64                                `json:"high_frontier_admissions"`
	TerminationCounts map[string]uint64                     `json:"termination_counts"`
}

type localHNSWAttributionCalibrationWitnessV1 struct {
	QueryOrdinal   int      `json:"query_ordinal"`
	ChangedPackIDs []uint32 `json:"changed_pack_ids"`
	P2TopKChanged  bool     `json:"p2_top_k_changed"`
	AllTopKChanged bool     `json:"all_top_k_changed"`
}

type localHNSWAttributionCalibrationSummaryV1 struct {
	Schema                   string                                    `json:"schema"`
	QueryCount               int                                       `json:"query_count"`
	RoutingRecall            localHNSWAttributionRecallAggregateV1     `json:"routing_recall"`
	Native                   localHNSWAttributionCalibrationVariantV1  `json:"native"`
	Overlay                  localHNSWAttributionCalibrationVariantV1  `json:"overlay"`
	ChangedP2TopK            uint64                                    `json:"changed_p2_top_k"`
	ChangedAllTopK           uint64                                    `json:"changed_all_top_k"`
	ChangedPackVisitedDigest uint64                                    `json:"changed_pack_visited_digest"`
	ChangedPackTermination   uint64                                    `json:"changed_pack_termination"`
	FirstWitness             *localHNSWAttributionCalibrationWitnessV1 `json:"first_witness,omitempty"`
	// NativeUtility aggregates all partition-local native-pack work, before
	// routed result merging; it is not a global unique-document count.
	NativeUtility localHNSWAttributionQueryUtilityV1 `json:"native_utility"`
	// OverlayUtility has the same partition-local scope for overlay-current.
	OverlayUtility localHNSWAttributionQueryUtilityV1 `json:"overlay_utility"`
	HardMisses     []localHNSWAttributionHardMissV1   `json:"hard_misses"`
}

func localHNSWAttributionCalibrationSummaryV1Build(ctx context.Context, sidecarPath string, source *m8ProductionMultiGroupAssetsV1, native, overlay *localHNSWVariantHarnessV1, ordinals []int, queries [][]float32, truths [][]m8CanonicalResultV1) (localHNSWAttributionArtifactV1, []localHNSWAttributionTimingCaseV1, localHNSWAttributionCalibrationSummaryV1, error) {
	var summary localHNSWAttributionCalibrationSummaryV1
	if sidecarPath == "" || len(ordinals) == 0 || len(ordinals) != len(queries) || len(ordinals) != len(truths) {
		return localHNSWAttributionArtifactV1{}, nil, summary, errors.New("invalid local HNSW calibration alignment")
	}
	summary.Schema = localHNSWAttributionCalibrationSummarySchemaV1
	cases := make([]localHNSWAttributionTimingCaseV1, 0, len(ordinals))
	artifact, err := localHNSWAttributionWriteGzipJSONLV1(sidecarPath, func(encoder *json.Encoder) (int, error) {
		for i, ordinal := range ordinals {
			if ordinal < 0 || !localHNSWCalibrationOrdinalV1(ordinal) || i > 0 && ordinals[i-1] >= ordinal || len(queries[i]) == 0 {
				return 0, errors.New("invalid local HNSW calibration ordinal")
			}
			canonical, err := localHNSWAttributionCanonicalResultsV1(truths[i], true)
			ids, scores := m8CanonicalParityV1(truths[i], canonical)
			if err != nil || !ids || !scores {
				return 0, errors.New("noncanonical local HNSW calibration truth")
			}
			evidence, err := localHNSWAttributionQueryEvidenceV1Build(ctx, source, native, overlay, ordinal, queries[i], canonical)
			if err != nil || evidence.Schema != localHNSWAttributionQuerySchemaV1 || evidence.QueryOrdinal != ordinal || evidence.QueryFP32SHA256 != localHNSWAttributionQueryFP32SHA256V1(queries[i]) {
				return 0, errors.New("invalid local HNSW calibration evidence")
			}
			if err := localHNSWAttributionCalibrationSummaryAddV1(&summary, evidence); err != nil {
				return 0, err
			}
			cases = append(cases, localHNSWAttributionTimingCaseV1{Ordinal: ordinal, Query: append([]float32(nil), queries[i]...), QueryFP32SHA256: evidence.QueryFP32SHA256, LowRoute: append([]uint32(nil), evidence.LowRoute...), HighRoute: append([]uint32(nil), evidence.HighRoute...)})
			if err := encoder.Encode(evidence); err != nil {
				return 0, err
			}
		}
		return len(ordinals), nil
	})
	if err != nil || artifact.Records != len(ordinals) || summary.QueryCount != len(ordinals) {
		return localHNSWAttributionArtifactV1{}, nil, localHNSWAttributionCalibrationSummaryV1{}, errors.Join(errors.New("write local HNSW calibration summary"), err)
	}
	if err := localHNSWAttributionTimingCasesV1(cases, int(source.manifest.PartitionCount)); err != nil {
		return localHNSWAttributionArtifactV1{}, nil, localHNSWAttributionCalibrationSummaryV1{}, err
	}
	if err := localHNSWAttributionCalibrationSummaryFinishV1(&summary); err != nil {
		return localHNSWAttributionArtifactV1{}, nil, localHNSWAttributionCalibrationSummaryV1{}, err
	}
	return artifact, cases, summary, nil
}

func localHNSWAttributionCalibrationSummaryAddV1(summary *localHNSWAttributionCalibrationSummaryV1, evidence localHNSWAttributionQueryEvidenceV1) error {
	if len(evidence.Partitions) == 0 || !localHNSWAttributionFiniteRecallV1(evidence.RoutingRecall) || !localHNSWAttributionFiniteRecallV1(evidence.Native.EndToEndRecall) || !localHNSWAttributionFiniteRecallV1(evidence.Native.LocalRecall) || !localHNSWAttributionFiniteRecallV1(evidence.Overlay.EndToEndRecall) || !localHNSWAttributionFiniteRecallV1(evidence.Overlay.LocalRecall) {
		return errors.New("invalid local HNSW calibration recall")
	}
	if err := localHNSWAttributionRecallAddV1(&summary.RoutingRecall, evidence.RoutingRecall, summary.QueryCount); err != nil {
		return err
	}
	if err := localHNSWAttributionCalibrationVariantAddV1(&summary.Native, evidence.Native, evidence.GlobalTruth, summary.QueryCount, evidence.Partitions, true); err != nil {
		return err
	}
	if err := localHNSWAttributionCalibrationVariantAddV1(&summary.Overlay, evidence.Overlay, evidence.GlobalTruth, summary.QueryCount, evidence.Partitions, false); err != nil {
		return err
	}
	p2Changed := !slices.Equal(evidence.Native.LowResults, evidence.Overlay.LowResults)
	allChanged := !slices.Equal(evidence.Native.HighResults, evidence.Overlay.HighResults)
	if p2Changed && summary.ChangedP2TopK == math.MaxUint64 || allChanged && summary.ChangedAllTopK == math.MaxUint64 {
		return errors.New("local HNSW calibration change overflow")
	}
	if p2Changed {
		summary.ChangedP2TopK++
	}
	if allChanged {
		summary.ChangedAllTopK++
	}
	changedPacks := make([]uint32, 0, len(evidence.Partitions))
	for _, partition := range evidence.Partitions {
		if partition.Native.VisitedOrdinalsSHA256 != partition.Overlay.VisitedOrdinalsSHA256 {
			if summary.ChangedPackVisitedDigest == math.MaxUint64 {
				return errors.New("local HNSW calibration change overflow")
			}
			summary.ChangedPackVisitedDigest++
			changedPacks = append(changedPacks, partition.Partition)
		}
		if partition.Native.TerminationReason != partition.Overlay.TerminationReason {
			if summary.ChangedPackTermination == math.MaxUint64 {
				return errors.New("local HNSW calibration change overflow")
			}
			summary.ChangedPackTermination++
			if len(changedPacks) == 0 || changedPacks[len(changedPacks)-1] != partition.Partition {
				changedPacks = append(changedPacks, partition.Partition)
			}
		}
	}
	if summary.FirstWitness == nil && (p2Changed || allChanged || len(changedPacks) != 0) {
		summary.FirstWitness = &localHNSWAttributionCalibrationWitnessV1{QueryOrdinal: evidence.QueryOrdinal, ChangedPackIDs: changedPacks, P2TopKChanged: p2Changed, AllTopKChanged: allChanged}
	}
	if summary.QueryCount == math.MaxInt {
		return errors.New("local HNSW calibration query overflow")
	}
	nativeRecords := make([]localHNSWAttributionQuerySearchV1, len(evidence.Partitions))
	overlayRecords := make([]localHNSWAttributionQuerySearchV1, len(evidence.Partitions))
	for i, partition := range evidence.Partitions {
		nativeRecords[i], overlayRecords[i] = partition.Native, partition.Overlay
	}
	nativeUtility, err := localHNSWAttributionQueryUtilityAggregateV1(nativeRecords)
	if err != nil {
		return err
	}
	overlayUtility, err := localHNSWAttributionQueryUtilityAggregateV1(overlayRecords)
	if err != nil {
		return err
	}
	if err := localHNSWAttributionQueryUtilityAddV1(&summary.NativeUtility, nativeUtility); err != nil {
		return err
	}
	if err := localHNSWAttributionQueryUtilityAddV1(&summary.OverlayUtility, overlayUtility); err != nil {
		return err
	}
	if miss, ok := localHNSWAttributionHardMissV1Build(evidence.QueryOrdinal, evidence.QueryFP32SHA256, "native", localHNSWAttributionQueryBitsRecallV1(evidence.GlobalTruth, evidence.Native.HighResults)); ok {
		summary.HardMisses = append(summary.HardMisses, miss)
	}
	if miss, ok := localHNSWAttributionHardMissV1Build(evidence.QueryOrdinal, evidence.QueryFP32SHA256, "overlay_current", localHNSWAttributionQueryBitsRecallV1(evidence.GlobalTruth, evidence.Overlay.HighResults)); ok {
		summary.HardMisses = append(summary.HardMisses, miss)
	}
	summary.QueryCount++
	return nil
}

func localHNSWAttributionCalibrationVariantAddV1(out *localHNSWAttributionCalibrationVariantV1, value localHNSWAttributionQueryVariantV1, truth []localHNSWAttributionQueryResultV1, count int, partitions []localHNSWAttributionQueryPartitionV1, native bool) error {
	allGlobal := localHNSWAttributionQueryBitsRecallV1(truth, value.HighResults)
	if !localHNSWAttributionFiniteRecallV1(value.EndToEndRecall) || !localHNSWAttributionFiniteRecallV1(value.LocalRecall) || !localHNSWAttributionFiniteRecallV1(allGlobal) {
		return errors.New("invalid local HNSW variant recall")
	}
	if err := localHNSWAttributionRecallAddV1(&out.P2EndToEnd, value.EndToEndRecall, count); err != nil {
		return err
	}
	if err := localHNSWAttributionRecallAddV1(&out.P2Local, value.LocalRecall, count); err != nil {
		return err
	}
	if err := localHNSWAttributionRecallAddV1(&out.AllGlobal, allGlobal, count); err != nil {
		return err
	}
	if math.MaxUint64-out.LowCandidates < value.LowSelectedWork.Candidates || math.MaxUint64-out.LowEdges < value.LowSelectedWork.Edges || math.MaxUint64-out.LowFrontier < value.LowSelectedWork.FrontierAdmissions || math.MaxUint64-out.HighCandidates < value.HighSelectedWork.Candidates || math.MaxUint64-out.HighEdges < value.HighSelectedWork.Edges || math.MaxUint64-out.HighFrontier < value.HighSelectedWork.FrontierAdmissions {
		return errors.New("local HNSW calibration work overflow")
	}
	out.LowCandidates += value.LowSelectedWork.Candidates
	out.LowEdges += value.LowSelectedWork.Edges
	out.LowFrontier += value.LowSelectedWork.FrontierAdmissions
	out.HighCandidates += value.HighSelectedWork.Candidates
	out.HighEdges += value.HighSelectedWork.Edges
	out.HighFrontier += value.HighSelectedWork.FrontierAdmissions
	if out.TerminationCounts == nil {
		out.TerminationCounts = map[string]uint64{}
	}
	for _, partition := range partitions {
		search := partition.Overlay
		if native {
			search = partition.Native
		}
		if !localHNSWAttributionTimingTerminationV1(search.TerminationReason) || math.MaxUint64-out.TerminationCounts[search.TerminationReason] < 1 {
			return errors.New("invalid local HNSW calibration termination")
		}
		out.TerminationCounts[search.TerminationReason]++
	}
	return nil
}

func localHNSWAttributionRecallAddV1(out *localHNSWAttributionRecallAggregateV1, value float64, count int) error {
	if !localHNSWAttributionFiniteRecallV1(value) || count < 0 {
		return errors.New("invalid local HNSW recall aggregate")
	}
	if count == 0 || value < out.Min {
		out.Min = value
	}
	if math.IsInf(out.Mean+value, 0) {
		return errors.New("local HNSW recall aggregate overflow")
	}
	// Mean holds the running sum until localHNSWAttributionCalibrationSummaryFinishV1 divides once.
	out.Mean += value
	return nil
}

func localHNSWAttributionCalibrationSummaryFinishV1(summary *localHNSWAttributionCalibrationSummaryV1) error {
	if summary.QueryCount == 0 {
		return errors.New("empty local HNSW calibration summary")
	}
	divisor := float64(summary.QueryCount)
	for _, aggregate := range []*localHNSWAttributionRecallAggregateV1{&summary.RoutingRecall, &summary.Native.P2EndToEnd, &summary.Native.P2Local, &summary.Native.AllGlobal, &summary.Overlay.P2EndToEnd, &summary.Overlay.P2Local, &summary.Overlay.AllGlobal} {
		aggregate.Mean /= divisor
		if !localHNSWAttributionFiniteRecallV1(aggregate.Mean) || !localHNSWAttributionFiniteRecallV1(aggregate.Min) {
			return errors.New("invalid local HNSW calibration aggregate")
		}
	}
	summary.HardMisses = localHNSWAttributionHardMissesV1(summary.HardMisses)
	return nil
}

func localHNSWAttributionCheckedAddV1(dst *uint64, value uint64) error {
	if dst == nil || math.MaxUint64-*dst < value {
		return errors.New("local HNSW query utility overflow")
	}
	*dst += value
	return nil
}

func localHNSWAttributionQueryOriginUtilityAddV1(dst *localHNSWAttributionQueryOriginUtilityV1, value localHNSWAttributionQueryOriginUtilityV1) error {
	for _, pair := range [][2]*uint64{{&dst.Examined, &value.Examined}, {&dst.NewlyVisited, &value.NewlyVisited}, {&dst.Scored, &value.Scored}, {&dst.TopAdmissions, &value.TopAdmissions}, {&dst.FrontierAdmissions, &value.FrontierAdmissions}, {&dst.StateImprovements, &value.StateImprovements}, {&dst.TruthRecovered, &value.TruthRecovered}} {
		if err := localHNSWAttributionCheckedAddV1(pair[0], *pair[1]); err != nil {
			return err
		}
	}
	return nil
}

func localHNSWAttributionQueryUtilityAddV1(dst *localHNSWAttributionQueryUtilityV1, value localHNSWAttributionQueryUtilityV1) error {
	if dst == nil {
		return errors.New("local HNSW nil query utility")
	}
	for _, pair := range [][2]*uint64{{&dst.ExaminedNative, &value.ExaminedNative}, {&dst.ExaminedAuxiliary, &value.ExaminedAuxiliary}, {&dst.NewlyVisited, &value.NewlyVisited}, {&dst.Scored, &value.Scored}, {&dst.TopAdmissions, &value.TopAdmissions}, {&dst.FrontierAdmissions, &value.FrontierAdmissions}, {&dst.StateImprovements, &value.StateImprovements}, {&dst.TruthRecovered, &value.TruthRecovered}} {
		if err := localHNSWAttributionCheckedAddV1(pair[0], *pair[1]); err != nil {
			return err
		}
	}
	for _, pair := range [][2]*localHNSWAttributionQueryOriginUtilityV1{{&dst.Diversity, &value.Diversity}, {&dst.Backfill, &value.Backfill}, {&dst.Reciprocal, &value.Reciprocal}, {&dst.Repair, &value.Repair}, {&dst.Overlay, &value.Overlay}, {&dst.Auxiliary, &value.Auxiliary}, {&dst.Unattributed, &value.Unattributed}} {
		if err := localHNSWAttributionQueryOriginUtilityAddV1(pair[0], *pair[1]); err != nil {
			return err
		}
	}
	return nil
}

func localHNSWAttributionFiniteRecallV1(value float64) bool {
	return value >= 0 && value <= 1 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func localHNSWAttributionQueryBitsRecallV1(want, got []localHNSWAttributionQueryResultV1) float64 {
	if len(want) == 0 {
		return 0
	}
	seen := make(map[string]bool, len(got))
	for _, result := range got {
		seen[result.ID] = true
	}
	found := 0
	for _, result := range want {
		if seen[result.ID] {
			found++
		}
	}
	return float64(found) / float64(len(want))
}

func localHNSWAttributionTimingTerminationV1(value string) bool {
	switch value {
	case "candidate_limit", "frontier_empty_retained_full", "frontier_empty_no_seed", "distance_bound":
		return true
	}
	return false
}
