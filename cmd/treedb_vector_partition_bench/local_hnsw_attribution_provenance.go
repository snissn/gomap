package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

// This is deliberately a reduced, calibration-only sidecar. Raw construction
// and traversal events remain bounded to the disposable offline harness.
const localHNSWAttributionInstrumentationSchemaV1 = "treedb_local_hnsw_attribution_instrumentation_v1"

var localHNSWAttributionConstructionOriginOrderV1 = [5]string{"diversity_selected", "nearest_backfill", "reciprocal_add", "reciprocity_repair", "overlay_rewrite"}

type localHNSWAttributionConstructionTotalsV1 struct {
	OriginOrder                     [5]string    `json:"origin_order"`
	DiversitySelected               uint64       `json:"diversity_selected"`
	BackfillSelected                uint64       `json:"backfill_selected"`
	InitialAdded                    uint64       `json:"initial_added"`
	ReciprocalAdded                 uint64       `json:"reciprocal_added"`
	VariantRewriteAdded             uint64       `json:"variant_rewrite_added"`
	VariantRewriteDropped           uint64       `json:"variant_rewrite_dropped"`
	PruneKept                       uint64       `json:"prune_kept"`
	PruneDropped                    uint64       `json:"prune_dropped"`
	FinalSurvivors                  uint64       `json:"final_survivors"`
	FinalDiversity                  uint64       `json:"final_diversity_selected"`
	FinalBackfill                   uint64       `json:"final_nearest_backfill"`
	FinalReciprocal                 uint64       `json:"final_reciprocal_add"`
	FinalRepair                     uint64       `json:"final_reciprocity_repair"`
	FinalOverlay                    uint64       `json:"final_overlay_rewrite"`
	InitialAddByOrigin              [5]uint64    `json:"initial_add_by_origin"`
	ReciprocalAddByOrigin           [5]uint64    `json:"reciprocal_add_by_origin"`
	VariantRewriteAddByOrigin       [5]uint64    `json:"variant_rewrite_add_by_origin"`
	VariantRewriteDropByOrigin      [5]uint64    `json:"variant_rewrite_drop_by_origin"`
	PruneKeepByOrigin               [5]uint64    `json:"prune_keep_by_origin"`
	PruneDropByOrigin               [5]uint64    `json:"prune_drop_by_origin"`
	InitialAddAgeByOrigin           [5][4]uint64 `json:"initial_add_age_by_origin"`
	InitialAddDeltaByOrigin         [5][4]uint64 `json:"initial_add_delta_by_origin"`
	ReciprocalAddAgeByOrigin        [5][4]uint64 `json:"reciprocal_add_age_by_origin"`
	ReciprocalAddDeltaByOrigin      [5][4]uint64 `json:"reciprocal_add_delta_by_origin"`
	VariantRewriteAddAgeByOrigin    [5][4]uint64 `json:"variant_rewrite_add_age_by_origin"`
	VariantRewriteAddDeltaByOrigin  [5][4]uint64 `json:"variant_rewrite_add_delta_by_origin"`
	VariantRewriteDropAgeByOrigin   [5][4]uint64 `json:"variant_rewrite_drop_age_by_origin"`
	VariantRewriteDropDeltaByOrigin [5][4]uint64 `json:"variant_rewrite_drop_delta_by_origin"`
	PruneDropAgeByOrigin            [5][4]uint64 `json:"prune_drop_age_by_origin"`
	PruneDropDeltaByOrigin          [5][4]uint64 `json:"prune_drop_delta_by_origin"`
	FinalAgeByOrigin                [5][4]uint64 `json:"final_age_by_origin"`
	FinalDeltaByOrigin              [5][4]uint64 `json:"final_delta_by_origin"`
}

type localHNSWAttributionQueryUtilityV1 struct {
	ExaminedNative     uint64                                   `json:"examined_native"`
	ExaminedAuxiliary  uint64                                   `json:"examined_auxiliary"`
	NewlyVisited       uint64                                   `json:"newly_visited"`
	Scored             uint64                                   `json:"scored"`
	TopAdmissions      uint64                                   `json:"top_admissions"`
	FrontierAdmissions uint64                                   `json:"frontier_admissions"`
	StateImprovements  uint64                                   `json:"state_improvements"`
	TruthRecovered     uint64                                   `json:"truth_recovered"`
	Diversity          localHNSWAttributionQueryOriginUtilityV1 `json:"diversity_selected"`
	Backfill           localHNSWAttributionQueryOriginUtilityV1 `json:"nearest_backfill"`
	Reciprocal         localHNSWAttributionQueryOriginUtilityV1 `json:"reciprocal"`
	Repair             localHNSWAttributionQueryOriginUtilityV1 `json:"reciprocity_repair"`
	Overlay            localHNSWAttributionQueryOriginUtilityV1 `json:"overlay_rewrite"`
	Auxiliary          localHNSWAttributionQueryOriginUtilityV1 `json:"auxiliary"`
	Unattributed       localHNSWAttributionQueryOriginUtilityV1 `json:"unattributed_native"`
}

type localHNSWAttributionQueryOriginUtilityV1 struct {
	Examined           uint64 `json:"examined"`
	NewlyVisited       uint64 `json:"newly_visited"`
	Scored             uint64 `json:"scored"`
	TopAdmissions      uint64 `json:"top_admissions"`
	FrontierAdmissions uint64 `json:"frontier_admissions"`
	StateImprovements  uint64 `json:"state_improvements"`
	TruthRecovered     uint64 `json:"truth_recovered"`
}

// localHNSWAttributionDistanceAggregateV1 intentionally contains only
// aggregations that remain meaningful when per-pack quantiles are combined.
type localHNSWAttributionDistanceAggregateV1 struct {
	Count uint64  `json:"count"`
	Min   float64 `json:"min"`
	Mean  float64 `json:"mean"`
	Max   float64 `json:"max"`
}

// localHNSWAttributionNeighborhoodOracleV1 is an offline-only exact-vector
// audit. Candidate coverage, final overlap, and angular diversity are all
// restricted to the same bounded construction sample; none is request-path
// work.
type localHNSWAttributionNeighborhoodOracleV1 struct {
	Schema                    string                                         `json:"schema"`
	OriginOrder               [5]string                                      `json:"origin_order"`
	ExactK                    int                                            `json:"exact_k"`
	CandidateSamples          uint64                                         `json:"candidate_samples"`
	CandidateTruthNeighbors   uint64                                         `json:"candidate_truth_neighbors"`
	CandidateTruthRecovered   uint64                                         `json:"candidate_truth_recovered"`
	FinalSamples              uint64                                         `json:"final_samples"`
	FinalSampleTruthNeighbors uint64                                         `json:"final_sample_truth_neighbors"`
	FinalSampleTruthRecovered uint64                                         `json:"final_sample_truth_recovered"`
	FinalEdgesByOrigin        [5]uint64                                      `json:"final_edges_by_origin"`
	FinalTruthByOrigin        [5]uint64                                      `json:"final_truth_recovered_by_origin"`
	AngularPairs              uint64                                         `json:"angular_pairs"`
	AngularCosineDistanceMean float64                                        `json:"angular_cosine_distance_mean"`
	PackDiagnostics           []collections.VectorPartitionPackDiagnosticsV1 `json:"pack_diagnostics"`
}

const localHNSWAttributionNeighborhoodOracleSchemaV1 = "treedb_local_hnsw_attribution_neighborhood_oracle_v1"
const localHNSWAttributionNeighborhoodExactKV1 = 10

type localHNSWAttributionHardMissV1 struct {
	QueryOrdinal int    `json:"query_ordinal"`
	QuerySHA256  string `json:"query_sha256"`
	Variant      string `json:"variant"`
	OverlapBits  uint32 `json:"overlap_bits"`
	Rank         string `json:"rank"`
}

type localHNSWAttributionInstrumentationSummaryV1 struct {
	Schema                 string                                   `json:"schema"`
	ManifestIntegrity      string                                   `json:"manifest_integrity_digest"`
	IndexDefinitionDigest  string                                   `json:"index_definition_digest"`
	NativeVariant          string                                   `json:"native_variant"`
	OverlayVariant         string                                   `json:"overlay_variant"`
	NativeConstruction     localHNSWAttributionConstructionTotalsV1 `json:"native_construction"`
	OverlayConstruction    localHNSWAttributionConstructionTotalsV1 `json:"overlay_construction"`
	NativeQuery            localHNSWAttributionQueryUtilityV1       `json:"native_query"`
	OverlayQuery           localHNSWAttributionQueryUtilityV1       `json:"overlay_query"`
	NativeLayer0Distances  localHNSWAttributionDistanceAggregateV1  `json:"native_layer0_distances"`
	OverlayLayer0Distances localHNSWAttributionDistanceAggregateV1  `json:"overlay_layer0_distances"`
	NativeNeighborhood     localHNSWAttributionNeighborhoodOracleV1 `json:"native_neighborhood_oracle"`
	OverlayNeighborhood    localHNSWAttributionNeighborhoodOracleV1 `json:"overlay_neighborhood_oracle"`
	HardMisses             []localHNSWAttributionHardMissV1         `json:"hard_misses"`
}

func localHNSWAttributionConstructionReduceV1(evidence collections.VectorPartitionConstructionEvidenceV1) (localHNSWAttributionConstructionTotalsV1, error) {
	out := localHNSWAttributionConstructionTotalsV1{OriginOrder: localHNSWAttributionConstructionOriginOrderV1}
	if evidence.Schema != "treedb_vector_partition_construction_evidence_v1" || evidence.Variant == "" || !localHNSWAttributionSHA256V1(evidence.ManifestChecksum) || !localHNSWAttributionSHA256V1(evidence.IndexDefinitionDigest) || len(evidence.Partitions) == 0 {
		return out, errors.New("invalid local HNSW construction provenance")
	}
	variant := collections.VectorPartitionLocalGraphVariantV1(evidence.Variant)
	if _, err := collections.VectorPartitionLocalGraphVariantIdentityV1(variant); err != nil {
		return out, errors.New("invalid local HNSW construction variant")
	}
	for _, partition := range evidence.Partitions {
		if len(partition.NativeInsertionOrdinals) == 0 {
			return out, errors.New("missing local HNSW insertion ordinals")
		}
		seenInsertion := make([]bool, len(partition.NativeInsertionOrdinals))
		for _, native := range partition.NativeInsertionOrdinals {
			if native < 0 || native >= len(seenInsertion) || seenInsertion[native] {
				return out, errors.New("invalid local HNSW insertion ordinal permutation")
			}
			seenInsertion[native] = true
		}
		for _, selection := range partition.Selections {
			if selection.Selected != selection.DiversitySelected+selection.BackfillSelected || selection.Selected < 0 {
				return out, errors.New("invalid local HNSW construction selection")
			}
			out.DiversitySelected += uint64(selection.DiversitySelected)
			out.BackfillSelected += uint64(selection.BackfillSelected)
		}
		for _, event := range partition.Events {
			if event.InsertionOrdinal < 0 || event.From < 0 || event.To < 0 || event.From == event.To || event.From >= len(partition.NativeInsertionOrdinals) || event.To >= len(partition.NativeInsertionOrdinals) || event.InsertionOrdinal != max(partition.NativeInsertionOrdinals[event.From], partition.NativeInsertionOrdinals[event.To]) {
				return out, errors.New("invalid local HNSW construction insertion age")
			}
			originIndex := -1
			switch event.Origin {
			case "diversity_selected":
				originIndex = 0
			case "nearest_backfill":
				originIndex = 1
			case "reciprocal_add":
				originIndex = 2
			case "reciprocity_repair":
				originIndex = 3
			case "overlay_rewrite":
				originIndex = 4
			default:
				return out, errors.New("invalid local HNSW construction origin")
			}
			bucket := localHNSWAttributionBucketV1(event.InsertionOrdinal)
			delta := partition.NativeInsertionOrdinals[event.From] - partition.NativeInsertionOrdinals[event.To]
			if delta < 0 {
				delta = -delta
			}
			deltaBucket := localHNSWAttributionBucketV1(delta)
			switch event.Action {
			case "initial_add":
				if originIndex == 2 {
					return out, errors.New("invalid local HNSW initial origin")
				}
				out.InitialAdded++
				out.InitialAddByOrigin[originIndex]++
				out.InitialAddAgeByOrigin[originIndex][bucket]++
				out.InitialAddDeltaByOrigin[originIndex][deltaBucket]++
			case "reciprocal_add":
				if originIndex != 2 {
					return out, errors.New("invalid local HNSW reciprocal origin")
				}
				out.ReciprocalAdded++
				out.ReciprocalAddByOrigin[originIndex]++
				out.ReciprocalAddAgeByOrigin[originIndex][bucket]++
				out.ReciprocalAddDeltaByOrigin[originIndex][deltaBucket]++
			case "reciprocity_repair_add", "overlay_rewrite_add":
				if !localHNSWAttributionVariantMutationAllowedV1(variant, event.Action) {
					return out, errors.New("invalid local HNSW variant rewrite action")
				}
				if (event.Action == "reciprocity_repair_add" && originIndex != 3) || (event.Action == "overlay_rewrite_add" && originIndex != 4) {
					return out, errors.New("invalid local HNSW variant rewrite add origin")
				}
				out.VariantRewriteAdded++
				out.VariantRewriteAddByOrigin[originIndex]++
				out.VariantRewriteAddAgeByOrigin[originIndex][bucket]++
				out.VariantRewriteAddDeltaByOrigin[originIndex][deltaBucket]++
			case "reciprocity_repair_drop", "overlay_rewrite_drop":
				if !localHNSWAttributionVariantMutationAllowedV1(variant, event.Action) {
					return out, errors.New("invalid local HNSW variant rewrite action")
				}
				out.VariantRewriteDropped++
				out.VariantRewriteDropByOrigin[originIndex]++
				out.VariantRewriteDropAgeByOrigin[originIndex][bucket]++
				out.VariantRewriteDropDeltaByOrigin[originIndex][deltaBucket]++
			case "reciprocal_prune_keep":
				out.PruneKept++
				out.PruneKeepByOrigin[originIndex]++
			case "reciprocal_prune_drop":
				out.PruneDropped++
				out.PruneDropByOrigin[originIndex]++
				out.PruneDropAgeByOrigin[originIndex][bucket]++
				out.PruneDropDeltaByOrigin[originIndex][deltaBucket]++
			case "final_survivor":
				out.FinalSurvivors++
				switch event.Origin {
				case "diversity_selected":
					out.FinalDiversity++
				case "nearest_backfill":
					out.FinalBackfill++
				case "reciprocal_add":
					out.FinalReciprocal++
				case "reciprocity_repair":
					out.FinalRepair++
				case "overlay_rewrite":
					out.FinalOverlay++
				}
				out.FinalAgeByOrigin[originIndex][bucket]++
				out.FinalDeltaByOrigin[originIndex][deltaBucket]++
			default:
				return out, errors.New("invalid local HNSW construction action")
			}
		}
	}
	if out.FinalSurvivors != out.FinalDiversity+out.FinalBackfill+out.FinalReciprocal+out.FinalRepair+out.FinalOverlay {
		return localHNSWAttributionConstructionTotalsV1{}, errors.New("local HNSW final origin conservation")
	}
	if out.InitialAdded != localHNSWAttributionOriginSumV1(out.InitialAddByOrigin) || out.ReciprocalAdded != localHNSWAttributionOriginSumV1(out.ReciprocalAddByOrigin) || out.VariantRewriteAdded != localHNSWAttributionOriginSumV1(out.VariantRewriteAddByOrigin) || out.VariantRewriteDropped != localHNSWAttributionOriginSumV1(out.VariantRewriteDropByOrigin) || out.PruneKept != localHNSWAttributionOriginSumV1(out.PruneKeepByOrigin) || out.PruneDropped != localHNSWAttributionOriginSumV1(out.PruneDropByOrigin) {
		return localHNSWAttributionConstructionTotalsV1{}, errors.New("local HNSW lifecycle origin conservation")
	}
	if localHNSWAttributionBucketsSumV1(out.InitialAddAgeByOrigin) != out.InitialAdded || localHNSWAttributionBucketsSumV1(out.InitialAddDeltaByOrigin) != out.InitialAdded || localHNSWAttributionBucketsSumV1(out.ReciprocalAddAgeByOrigin) != out.ReciprocalAdded || localHNSWAttributionBucketsSumV1(out.ReciprocalAddDeltaByOrigin) != out.ReciprocalAdded || localHNSWAttributionBucketsSumV1(out.VariantRewriteAddAgeByOrigin) != out.VariantRewriteAdded || localHNSWAttributionBucketsSumV1(out.VariantRewriteAddDeltaByOrigin) != out.VariantRewriteAdded || localHNSWAttributionBucketsSumV1(out.VariantRewriteDropAgeByOrigin) != out.VariantRewriteDropped || localHNSWAttributionBucketsSumV1(out.VariantRewriteDropDeltaByOrigin) != out.VariantRewriteDropped || localHNSWAttributionBucketsSumV1(out.PruneDropAgeByOrigin) != out.PruneDropped || localHNSWAttributionBucketsSumV1(out.PruneDropDeltaByOrigin) != out.PruneDropped || localHNSWAttributionBucketsSumV1(out.FinalAgeByOrigin) != out.FinalSurvivors || localHNSWAttributionBucketsSumV1(out.FinalDeltaByOrigin) != out.FinalSurvivors {
		return localHNSWAttributionConstructionTotalsV1{}, errors.New("local HNSW lifecycle bucket conservation")
	}
	return out, nil
}

func localHNSWAttributionVariantMutationAllowedV1(variant collections.VectorPartitionLocalGraphVariantV1, action string) bool {
	switch action {
	case "overlay_rewrite_add", "overlay_rewrite_drop":
		return variant == collections.VectorPartitionLocalGraphVariantOverlayCurrentV1
	case "reciprocity_repair_add", "reciprocity_repair_drop":
		return variant != collections.VectorPartitionLocalGraphVariantNativeV1 && variant != collections.VectorPartitionLocalGraphVariantOverlayCurrentV1
	default:
		return true
	}
}

func localHNSWAttributionOriginSumV1(values [5]uint64) uint64 {
	var total uint64
	for _, value := range values {
		total += value
	}
	return total
}

func localHNSWAttributionBucketV1(value int) int {
	switch {
	case value < 16:
		return 0
	case value < 256:
		return 1
	case value < 4096:
		return 2
	default:
		return 3
	}
}

func localHNSWAttributionBucketsSumV1(values [5][4]uint64) uint64 {
	var total uint64
	for _, origins := range values {
		for _, value := range origins {
			total += value
		}
	}
	return total
}

type localHNSWAttributionFinalEdgeKeyV1 struct{ From, To, Layer int }

func localHNSWAttributionFinalOriginsV1(evidence collections.VectorPartitionConstructionEvidenceV1, partition int) (map[localHNSWAttributionFinalEdgeKeyV1]string, error) {
	if partition < 0 || partition >= len(evidence.Partitions) {
		return nil, errors.New("invalid local HNSW construction partition")
	}
	out := map[localHNSWAttributionFinalEdgeKeyV1]string{}
	for _, event := range evidence.Partitions[partition].Events {
		if event.Action != "final_survivor" {
			continue
		}
		key := localHNSWAttributionFinalEdgeKeyV1{event.From, event.To, event.Layer}
		if _, exists := out[key]; exists || !localHNSWAttributionConstructionOriginValidV1(event.Origin) {
			return nil, errors.New("invalid local HNSW final edge origin")
		}
		out[key] = event.Origin
	}
	if len(out) == 0 {
		return nil, errors.New("empty local HNSW final edge origins")
	}
	return out, nil
}

func localHNSWAttributionQueryUtilityReduceV1(metrics collections.VectorPartitionSearchMetricsV1, attribution collections.VectorPartitionSearchAttributionV1, origins map[localHNSWAttributionFinalEdgeKeyV1]string, ids []string, truth map[string]struct{}) (localHNSWAttributionQueryUtilityV1, error) {
	var out localHNSWAttributionQueryUtilityV1
	if !localHNSWAttributionSearchValidV1(attribution) {
		return out, errors.New("invalid local HNSW query attribution")
	}
	recovered := map[string]struct{}{}
	var layer0NewlyVisited, layer0Scored uint64
	for _, event := range attribution.EdgeEvents {
		if event.DestinationOrdinal < 0 || event.DestinationOrdinal >= len(ids) {
			return localHNSWAttributionQueryUtilityV1{}, errors.New("invalid local HNSW trace ordinal")
		}
		var origin *localHNSWAttributionQueryOriginUtilityV1
		if event.Auxiliary {
			out.ExaminedAuxiliary++
			origin = &out.Auxiliary
		} else {
			out.ExaminedNative++
			switch origins[localHNSWAttributionFinalEdgeKeyV1{event.SourceOrdinal, event.DestinationOrdinal, event.Layer}] {
			case "diversity_selected":
				origin = &out.Diversity
			case "nearest_backfill":
				origin = &out.Backfill
			case "reciprocal_add":
				origin = &out.Reciprocal
			case "reciprocity_repair":
				origin = &out.Repair
			case "overlay_rewrite":
				origin = &out.Overlay
			default:
				return localHNSWAttributionQueryUtilityV1{}, errors.New("unmatched local HNSW native final edge")
			}
		}
		origin.Examined++
		if event.NewlyVisited {
			out.NewlyVisited++
			origin.NewlyVisited++
			if event.Layer == 0 {
				layer0NewlyVisited++
			}
		}
		if event.Scored {
			out.Scored++
			origin.Scored++
			if event.Layer == 0 {
				layer0Scored++
			}
		}
		if event.TopAdmission {
			out.TopAdmissions++
			origin.TopAdmissions++
		}
		if event.FrontierAdmission {
			out.FrontierAdmissions++
			origin.FrontierAdmissions++
		}
		if event.StateImprovement {
			out.StateImprovements++
			origin.StateImprovements++
		}
		if event.Scored {
			if _, wanted := truth[ids[event.DestinationOrdinal]]; wanted {
				if _, seen := recovered[ids[event.DestinationOrdinal]]; !seen {
					recovered[ids[event.DestinationOrdinal]] = struct{}{}
					out.TruthRecovered++
					origin.TruthRecovered++
				}
			}
		}
	}
	// A seed can be the entry reached by upper-layer greedy descent. Edge events
	// are already emitted before that seed, whereas later layer-zero visits to a
	// seeded row are unscored. Crediting scored edges first therefore preserves
	// the actual recovery owner without a second cross-slice event sequence.
	for _, seed := range attribution.SeedEvents {
		if seed.Ordinal < 0 || seed.Ordinal >= len(ids) {
			return out, errors.New("invalid local HNSW seed ordinal")
		}
		if _, wanted := truth[ids[seed.Ordinal]]; wanted {
			if _, seen := recovered[ids[seed.Ordinal]]; !seen {
				recovered[ids[seed.Ordinal]] = struct{}{}
				out.TruthRecovered++
				out.Unattributed.TruthRecovered++
			}
		}
	}
	originsTotal := func(field func(localHNSWAttributionQueryOriginUtilityV1) uint64) uint64 {
		return field(out.Diversity) + field(out.Backfill) + field(out.Reciprocal) + field(out.Repair) + field(out.Overlay) + field(out.Auxiliary) + field(out.Unattributed)
	}
	if out.ExaminedNative+out.ExaminedAuxiliary != metrics.Edges || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.Examined }) != metrics.Edges || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.NewlyVisited }) != out.NewlyVisited || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.Scored }) != out.Scored || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.TopAdmissions }) != out.TopAdmissions || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.FrontierAdmissions }) != out.FrontierAdmissions || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.StateImprovements }) != out.StateImprovements || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.TruthRecovered }) != out.TruthRecovered || out.ExaminedAuxiliary != metrics.AuxiliaryEdges || layer0NewlyVisited+attribution.SeedCandidates != metrics.Candidates || layer0Scored != layer0NewlyVisited || out.FrontierAdmissions+attribution.SeedAdmissions != attribution.FrontierAdmissions || out.TopAdmissions != out.FrontierAdmissions {
		return localHNSWAttributionQueryUtilityV1{}, errors.New("local HNSW query utility conservation")
	}
	return out, nil
}

func localHNSWAttributionTruthRecoveriesV1(attribution collections.VectorPartitionSearchAttributionV1, origins map[localHNSWAttributionFinalEdgeKeyV1]string, ids []string, truth map[string]struct{}) map[string]string {
	out := make(map[string]string)
	for _, event := range attribution.EdgeEvents {
		if !event.Scored || event.DestinationOrdinal < 0 || event.DestinationOrdinal >= len(ids) {
			continue
		}
		id := ids[event.DestinationOrdinal]
		if _, wanted := truth[id]; !wanted {
			continue
		}
		if _, exists := out[id]; exists {
			continue
		}
		origin := "auxiliary"
		if !event.Auxiliary {
			origin = origins[localHNSWAttributionFinalEdgeKeyV1{event.SourceOrdinal, event.DestinationOrdinal, event.Layer}]
		}
		out[id] = origin
	}
	for _, seed := range attribution.SeedEvents {
		if seed.Ordinal >= 0 && seed.Ordinal < len(ids) {
			if _, wanted := truth[ids[seed.Ordinal]]; wanted {
				if _, exists := out[ids[seed.Ordinal]]; !exists {
					out[ids[seed.Ordinal]] = "unattributed"
				}
			}
		}
	}
	return out
}

func localHNSWAttributionQueryUtilityRemoveTruthRecoveryV1(out *localHNSWAttributionQueryUtilityV1, origin string) {
	if out == nil || out.TruthRecovered == 0 {
		return
	}
	out.TruthRecovered--
	switch origin {
	case "diversity_selected":
		out.Diversity.TruthRecovered--
	case "nearest_backfill":
		out.Backfill.TruthRecovered--
	case "reciprocal_add":
		out.Reciprocal.TruthRecovered--
	case "reciprocity_repair":
		out.Repair.TruthRecovered--
	case "overlay_rewrite":
		out.Overlay.TruthRecovered--
	case "auxiliary":
		out.Auxiliary.TruthRecovered--
	case "unattributed":
		out.Unattributed.TruthRecovered--
	}
}

func localHNSWAttributionHardMissesV1(in []localHNSWAttributionHardMissV1) []localHNSWAttributionHardMissV1 {
	// Stable digest ranking makes the sample independent of calibration order.
	sort.Slice(in, func(i, j int) bool {
		if in[i].Rank != in[j].Rank {
			return in[i].Rank < in[j].Rank
		}
		if in[i].Variant != in[j].Variant {
			return in[i].Variant < in[j].Variant
		}
		if in[i].QueryOrdinal != in[j].QueryOrdinal {
			return in[i].QueryOrdinal < in[j].QueryOrdinal
		}
		if in[i].QuerySHA256 != in[j].QuerySHA256 {
			return in[i].QuerySHA256 < in[j].QuerySHA256
		}
		return in[i].OverlapBits < in[j].OverlapBits
	})
	if len(in) > 32 {
		in = in[:32]
	}
	return in
}

func localHNSWAttributionHardMissV1Build(queryOrdinal int, querySHA, variant string, overlap float64) (localHNSWAttributionHardMissV1, bool) {
	if queryOrdinal < 0 || !localHNSWAttributionSHA256V1(querySHA) || (variant != "native" && variant != "overlay_current") || overlap >= 1 || overlap < 0 || math.IsNaN(overlap) {
		return localHNSWAttributionHardMissV1{}, false
	}
	h := sha256.Sum256([]byte("treedb-4170-hard-miss-v1/" + variant + "/" + querySHA))
	return localHNSWAttributionHardMissV1{QueryOrdinal: queryOrdinal, QuerySHA256: querySHA, Variant: variant, OverlapBits: math.Float32bits(float32(overlap)), Rank: hex.EncodeToString(h[:])}, true
}

func localHNSWAttributionInstrumentationSummaryV1Build(source *m8ProductionMultiGroupAssetsV1, native, overlay *localHNSWVariantHarnessV1, graph localHNSWAttributionGraphAggregateV1, calibration localHNSWAttributionCalibrationSummaryV1) (localHNSWAttributionInstrumentationSummaryV1, error) {
	var out localHNSWAttributionInstrumentationSummaryV1
	if source == nil || source.descriptor == nil || native == nil || overlay == nil || source.manifest.IntegrityDigest == "" || len(native.constructionEvidence.Partitions) != int(source.manifest.PartitionCount) || len(overlay.constructionEvidence.Partitions) != int(source.manifest.PartitionCount) {
		return out, errors.New("invalid local HNSW instrumentation inputs")
	}
	nativeConstruction, err := localHNSWAttributionConstructionReduceV1(native.constructionEvidence)
	if err != nil {
		return out, err
	}
	overlayConstruction, err := localHNSWAttributionConstructionReduceV1(overlay.constructionEvidence)
	if err != nil {
		return out, err
	}
	nativeDiagnostics, err := localHNSWAttributionPackDiagnosticsV1(native.searchers)
	if err != nil {
		return out, err
	}
	overlayDiagnostics, err := localHNSWAttributionPackDiagnosticsV1(overlay.searchers)
	if err != nil {
		return out, err
	}
	nativeNeighborhood, err := localHNSWAttributionNeighborhoodOracleV1Build(native, nativeDiagnostics)
	if err != nil {
		return out, err
	}
	overlayNeighborhood, err := localHNSWAttributionNeighborhoodOracleV1Build(overlay, overlayDiagnostics)
	if err != nil {
		return out, err
	}
	if calibration.QueryCount == 0 || calibration.NativeUtility.ExaminedNative+calibration.NativeUtility.ExaminedAuxiliary == 0 || calibration.OverlayUtility.ExaminedNative+calibration.OverlayUtility.ExaminedAuxiliary == 0 {
		return out, errors.New("empty local HNSW instrumentation query utility")
	}
	out = localHNSWAttributionInstrumentationSummaryV1{Schema: localHNSWAttributionInstrumentationSchemaV1, ManifestIntegrity: source.manifest.IntegrityDigest, IndexDefinitionDigest: native.constructionEvidence.IndexDefinitionDigest, NativeVariant: native.constructionEvidence.Variant, OverlayVariant: overlay.constructionEvidence.Variant, NativeConstruction: nativeConstruction, OverlayConstruction: overlayConstruction, NativeQuery: calibration.NativeUtility, OverlayQuery: calibration.OverlayUtility, NativeLayer0Distances: graphDistanceAggregateV1(nativeDiagnostics), OverlayLayer0Distances: graphDistanceAggregateV1(overlayDiagnostics), NativeNeighborhood: nativeNeighborhood, OverlayNeighborhood: overlayNeighborhood, HardMisses: append([]localHNSWAttributionHardMissV1(nil), calibration.HardMisses...)}
	if out.NativeVariant != "native" || out.OverlayVariant != "overlay_current" || !localHNSWAttributionSHA256V1(out.IndexDefinitionDigest) || out.NativeLayer0Distances.Count == 0 || out.OverlayLayer0Distances.Count == 0 || out.NativeConstruction.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || out.OverlayConstruction.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || out.NativeNeighborhood.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || out.OverlayNeighborhood.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || out.NativeNeighborhood.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || out.OverlayNeighborhood.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 {
		return localHNSWAttributionInstrumentationSummaryV1{}, errors.New("invalid local HNSW instrumentation summary")
	}
	return out, nil
}

func localHNSWAttributionNeighborhoodOriginV1(origin string) (int, bool) {
	switch origin {
	case "diversity_selected":
		return 0, true
	case "nearest_backfill":
		return 1, true
	case "reciprocal_add":
		return 2, true
	case "reciprocity_repair":
		return 3, true
	case "overlay_rewrite":
		return 4, true
	}
	return 0, false
}

func localHNSWAttributionConstructionOriginValidV1(origin string) bool {
	_, ok := localHNSWAttributionNeighborhoodOriginV1(origin)
	return ok
}

func localHNSWAttributionCosineDistanceV1(a, b []float32) (float64, bool) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, false
	}
	var dot, an, bn float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		an += float64(a[i]) * float64(a[i])
		bn += float64(b[i]) * float64(b[i])
	}
	if an == 0 || bn == 0 {
		return 0, false
	}
	return 1 - dot/math.Sqrt(an*bn), true
}

func localHNSWAttributionNearestIDsV1(ids []string, vectors map[string][]float32, source string, allowed func(string) bool, k int) ([]string, error) {
	type scored struct {
		id    string
		score float32
	}
	query, ok := vectors[source]
	if !ok {
		return nil, errors.New("missing local HNSW oracle source vector")
	}
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(query)
	if err != nil {
		return nil, err
	}
	all := make([]scored, 0, len(ids))
	for _, id := range ids {
		if id == source || !allowed(id) {
			continue
		}
		score, err := scorer.ScoreV1(vectors[id])
		if err != nil {
			return nil, err
		}
		all = append(all, scored{id, score})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score > all[j].score
		}
		return all[i].id < all[j].id
	})
	if len(all) > k {
		all = all[:k]
	}
	out := make([]string, len(all))
	for i := range all {
		out[i] = all[i].id
	}
	return out, nil
}

func localHNSWAttributionPackDiagnosticsV1(searchers []*collections.VectorPartitionLocalSearcherV1) ([]collections.VectorPartitionPackDiagnosticsV1, error) {
	out := make([]collections.VectorPartitionPackDiagnosticsV1, len(searchers))
	for i, searcher := range searchers {
		var err error
		out[i], err = searcher.PackDiagnosticsV1()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func localHNSWAttributionNeighborhoodOracleV1Build(h *localHNSWVariantHarnessV1, cached ...[]collections.VectorPartitionPackDiagnosticsV1) (localHNSWAttributionNeighborhoodOracleV1, error) {
	out := localHNSWAttributionNeighborhoodOracleV1{Schema: localHNSWAttributionNeighborhoodOracleSchemaV1, OriginOrder: localHNSWAttributionConstructionOriginOrderV1, ExactK: localHNSWAttributionNeighborhoodExactKV1}
	if h == nil || h.assets == nil || len(h.searchers) != len(h.documentIDs) || len(h.searchers) != len(h.constructionEvidence.Partitions) {
		return out, errors.New("invalid local HNSW oracle harness")
	}
	var diagnostics []collections.VectorPartitionPackDiagnosticsV1
	if len(cached) != 0 {
		diagnostics = cached[0]
		if len(diagnostics) != len(h.searchers) {
			return out, errors.New("invalid local HNSW cached diagnostics")
		}
	} else {
		var err error
		diagnostics, err = localHNSWAttributionPackDiagnosticsV1(h.searchers)
		if err != nil {
			return out, err
		}
	}
	_, rows, err := h.assets.collection.ReadVectorPartitionRouterSourceRowsV1(h.assets.manifest.IndexName)
	if err != nil {
		return out, err
	}
	vectors := make(map[string][]float32, len(rows))
	for _, row := range rows {
		vectors[string(row.DocumentID)] = row.Values
	}
	for p, part := range h.constructionEvidence.Partitions {
		ids := h.documentIDs[p]
		if len(ids) != len(part.NativeInsertionOrdinals) {
			return out, errors.New("local HNSW oracle ordinal binding")
		}
		out.PackDiagnostics = append(out.PackDiagnostics, diagnostics[p])
		final := map[int][]struct {
			to     int
			origin string
		}{}
		for _, event := range part.Events {
			if event.Action == "final_survivor" && event.Layer == 0 {
				if event.From < 0 || event.From >= len(ids) || event.To < 0 || event.To >= len(ids) {
					return out, errors.New("local HNSW oracle final ordinal")
				}
				final[event.From] = append(final[event.From], struct {
					to     int
					origin string
				}{event.To, event.Origin})
			}
		}
		samples := make(map[int]collections.VectorPartitionConstructionSelectionV1)
		sampleNodes := make([]int, 0)
		idOrdinal := make(map[string]int, len(ids))
		for ordinal, id := range ids {
			idOrdinal[id] = ordinal
		}
		for _, selection := range part.Selections {
			if !selection.CandidateSampled {
				continue
			}
			if selection.Node < 0 || selection.Node >= len(ids) {
				return out, errors.New("local HNSW oracle sample ordinal")
			}
			if _, duplicate := samples[selection.Node]; duplicate {
				return out, errors.New("duplicate local HNSW oracle sample")
			}
			samples[selection.Node] = selection
			sampleNodes = append(sampleNodes, selection.Node)
			exact, err := localHNSWAttributionNearestIDsV1(ids, vectors, ids[selection.Node], func(id string) bool {
				ordinal, ok := idOrdinal[id]
				return ok && part.NativeInsertionOrdinals[ordinal] < part.NativeInsertionOrdinals[selection.Node]
			}, out.ExactK)
			if err != nil {
				return out, err
			}
			candidate := map[string]struct{}{}
			for _, ordinal := range selection.CandidateOrdinals {
				if ordinal < 0 || ordinal >= len(ids) {
					return out, errors.New("local HNSW oracle candidate ordinal")
				}
				candidate[ids[ordinal]] = struct{}{}
			}
			out.CandidateSamples++
			out.CandidateTruthNeighbors += uint64(len(exact))
			for _, id := range exact {
				if _, ok := candidate[id]; ok {
					out.CandidateTruthRecovered++
				}
			}
		}
		sort.Ints(sampleNodes)
		for _, from := range sampleNodes {
			edges := final[from]
			exact, err := localHNSWAttributionNearestIDsV1(ids, vectors, ids[from], func(string) bool { return true }, out.ExactK)
			if err != nil {
				return out, err
			}
			truth := map[string]struct{}{}
			for _, id := range exact {
				truth[id] = struct{}{}
			}
			out.FinalSamples++
			out.FinalSampleTruthNeighbors += uint64(len(exact))
			for _, edge := range edges {
				oi, ok := localHNSWAttributionNeighborhoodOriginV1(edge.origin)
				if !ok {
					return out, errors.New("local HNSW oracle final origin")
				}
				out.FinalEdgesByOrigin[oi]++
				if _, ok := truth[ids[edge.to]]; ok {
					out.FinalSampleTruthRecovered++
					out.FinalTruthByOrigin[oi]++
				}
			}
			for i := 0; i < len(edges); i++ {
				for j := i + 1; j < len(edges); j++ {
					d, ok := localHNSWAttributionCosineDistanceV1(vectors[ids[edges[i].to]], vectors[ids[edges[j].to]])
					if !ok {
						return out, errors.New("invalid local HNSW angular vector")
					}
					out.AngularPairs++
					out.AngularCosineDistanceMean += d
				}
			}
		}
	}
	if out.CandidateSamples == 0 || out.FinalSamples == 0 || len(out.PackDiagnostics) != len(h.searchers) {
		return localHNSWAttributionNeighborhoodOracleV1{}, errors.New("empty local HNSW oracle")
	}
	if out.AngularPairs != 0 {
		out.AngularCosineDistanceMean /= float64(out.AngularPairs)
	}
	return out, nil
}

func graphDistanceAggregateV1(diagnostics []collections.VectorPartitionPackDiagnosticsV1) localHNSWAttributionDistanceAggregateV1 {
	// PackDiagnostics already computes the authoritative per-pack distances.
	// Keep the report compact: weighted mean and global extrema are sufficient.
	var out localHNSWAttributionDistanceAggregateV1
	for _, d := range diagnostics {
		if d.Layer0Distances.Count == 0 {
			return localHNSWAttributionDistanceAggregateV1{}
		}
		if out.Count == 0 || d.Layer0Distances.Min < out.Min {
			out.Min = d.Layer0Distances.Min
		}
		if d.Layer0Distances.Max > out.Max {
			out.Max = d.Layer0Distances.Max
		}
		out.Mean = (out.Mean*float64(out.Count) + d.Layer0Distances.Mean*float64(d.Layer0Distances.Count)) / float64(out.Count+d.Layer0Distances.Count)
		out.Count += d.Layer0Distances.Count
	}
	return out
}
