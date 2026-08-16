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

type localHNSWAttributionConstructionTotalsV1 struct {
	DiversitySelected     uint64       `json:"diversity_selected"`
	BackfillSelected      uint64       `json:"backfill_selected"`
	InitialAdded          uint64       `json:"initial_added"`
	ReciprocalAdded       uint64       `json:"reciprocal_added"`
	PruneKept             uint64       `json:"prune_kept"`
	PruneDropped          uint64       `json:"prune_dropped"`
	FinalSurvivors        uint64       `json:"final_survivors"`
	FinalDiversity        uint64       `json:"final_diversity_selected"`
	FinalBackfill         uint64       `json:"final_nearest_backfill"`
	FinalReciprocal       uint64       `json:"final_reciprocal_add"`
	InitialAddByOrigin    [3]uint64    `json:"initial_add_by_origin"`
	ReciprocalAddByOrigin [3]uint64    `json:"reciprocal_add_by_origin"`
	PruneKeepByOrigin     [3]uint64    `json:"prune_keep_by_origin"`
	PruneDropByOrigin     [3]uint64    `json:"prune_drop_by_origin"`
	FinalAgeByOrigin      [3][4]uint64 `json:"final_age_by_origin"`
	FinalDeltaByOrigin    [3][4]uint64 `json:"final_delta_by_origin"`
	InsertionAge          [4]uint64    `json:"insertion_age_buckets"`
}

type localHNSWAttributionQueryUtilityV1 struct {
	ExaminedNative     uint64                                   `json:"examined_native"`
	ExaminedAuxiliary  uint64                                   `json:"examined_auxiliary"`
	NewlyVisited       uint64                                   `json:"newly_visited"`
	Scored             uint64                                   `json:"scored"`
	TopAdmissions      uint64                                   `json:"top_admissions"`
	FrontierAdmissions uint64                                   `json:"frontier_admissions"`
	TruthRecovered     uint64                                   `json:"truth_recovered"`
	Diversity          localHNSWAttributionQueryOriginUtilityV1 `json:"diversity_selected"`
	Backfill           localHNSWAttributionQueryOriginUtilityV1 `json:"nearest_backfill"`
	Reciprocal         localHNSWAttributionQueryOriginUtilityV1 `json:"reciprocal"`
	Auxiliary          localHNSWAttributionQueryOriginUtilityV1 `json:"auxiliary"`
	Unattributed       localHNSWAttributionQueryOriginUtilityV1 `json:"unattributed_native"`
}

type localHNSWAttributionQueryOriginUtilityV1 struct {
	Examined           uint64 `json:"examined"`
	NewlyVisited       uint64 `json:"newly_visited"`
	Scored             uint64 `json:"scored"`
	TopAdmissions      uint64 `json:"top_admissions"`
	FrontierAdmissions uint64 `json:"frontier_admissions"`
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
	HardMisses             []localHNSWAttributionHardMissV1         `json:"hard_misses"`
}

func localHNSWAttributionConstructionReduceV1(evidence collections.VectorPartitionConstructionEvidenceV1) (localHNSWAttributionConstructionTotalsV1, error) {
	var out localHNSWAttributionConstructionTotalsV1
	if evidence.Schema != "treedb_vector_partition_construction_evidence_v1" || evidence.Variant == "" || !localHNSWAttributionSHA256V1(evidence.ManifestChecksum) || !localHNSWAttributionSHA256V1(evidence.IndexDefinitionDigest) || len(evidence.Partitions) == 0 {
		return out, errors.New("invalid local HNSW construction provenance")
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
			default:
				return out, errors.New("invalid local HNSW construction origin")
			}
			bucket := 0
			switch {
			case event.InsertionOrdinal < 16:
			case event.InsertionOrdinal < 256:
				bucket = 1
			case event.InsertionOrdinal < 4096:
				bucket = 2
			default:
				bucket = 3
			}
			switch event.Action {
			case "initial_add":
				if originIndex == 2 {
					return out, errors.New("invalid local HNSW initial origin")
				}
				out.InitialAdded++
				out.InitialAddByOrigin[originIndex]++
			case "reciprocal_add":
				if originIndex != 2 {
					return out, errors.New("invalid local HNSW reciprocal origin")
				}
				out.ReciprocalAdded++
				out.ReciprocalAddByOrigin[originIndex]++
			case "reciprocal_prune_keep":
				out.PruneKept++
				out.PruneKeepByOrigin[originIndex]++
			case "reciprocal_prune_drop":
				out.PruneDropped++
				out.PruneDropByOrigin[originIndex]++
			case "final_survivor":
				out.FinalSurvivors++
				switch event.Origin {
				case "diversity_selected":
					out.FinalDiversity++
				case "nearest_backfill":
					out.FinalBackfill++
					originIndex = 1
				case "reciprocal_add":
					out.FinalReciprocal++
					originIndex = 2
				}
				out.FinalAgeByOrigin[originIndex][bucket]++
				delta := partition.NativeInsertionOrdinals[event.From] - partition.NativeInsertionOrdinals[event.To]
				if delta < 0 {
					delta = -delta
				}
				deltaBucket := 0
				if delta >= 16 {
					deltaBucket = 1
				}
				if delta >= 256 {
					deltaBucket = 2
				}
				if delta >= 4096 {
					deltaBucket = 3
				}
				out.FinalDeltaByOrigin[originIndex][deltaBucket]++
			default:
				return out, errors.New("invalid local HNSW construction action")
			}
			out.InsertionAge[bucket]++
		}
	}
	if out.FinalSurvivors != out.FinalDiversity+out.FinalBackfill+out.FinalReciprocal {
		return localHNSWAttributionConstructionTotalsV1{}, errors.New("local HNSW final origin conservation")
	}
	if out.InitialAdded != out.InitialAddByOrigin[0]+out.InitialAddByOrigin[1]+out.InitialAddByOrigin[2] || out.ReciprocalAdded != out.ReciprocalAddByOrigin[0]+out.ReciprocalAddByOrigin[1]+out.ReciprocalAddByOrigin[2] || out.PruneKept != out.PruneKeepByOrigin[0]+out.PruneKeepByOrigin[1]+out.PruneKeepByOrigin[2] || out.PruneDropped != out.PruneDropByOrigin[0]+out.PruneDropByOrigin[1]+out.PruneDropByOrigin[2] {
		return localHNSWAttributionConstructionTotalsV1{}, errors.New("local HNSW lifecycle origin conservation")
	}
	return out, nil
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
		if _, exists := out[key]; exists || (event.Origin != "diversity_selected" && event.Origin != "nearest_backfill" && event.Origin != "reciprocal_add") {
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
		if event.NewlyVisited && event.Scored {
			if _, wanted := truth[ids[event.DestinationOrdinal]]; wanted {
				if _, seen := recovered[ids[event.DestinationOrdinal]]; !seen {
					recovered[ids[event.DestinationOrdinal]] = struct{}{}
					out.TruthRecovered++
					origin.TruthRecovered++
				}
			}
		}
	}
	originsTotal := func(field func(localHNSWAttributionQueryOriginUtilityV1) uint64) uint64 {
		return field(out.Diversity) + field(out.Backfill) + field(out.Reciprocal) + field(out.Auxiliary)
	}
	if out.ExaminedNative+out.ExaminedAuxiliary != metrics.Edges || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.Examined }) != metrics.Edges || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.NewlyVisited }) != out.NewlyVisited || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.Scored }) != out.Scored || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.TopAdmissions }) != out.TopAdmissions || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.FrontierAdmissions }) != out.FrontierAdmissions || originsTotal(func(v localHNSWAttributionQueryOriginUtilityV1) uint64 { return v.TruthRecovered }) != out.TruthRecovered || out.ExaminedAuxiliary != metrics.AuxiliaryEdges || layer0NewlyVisited+attribution.SeedCandidates != metrics.Candidates || layer0Scored != layer0NewlyVisited || out.FrontierAdmissions+attribution.SeedAdmissions != attribution.FrontierAdmissions || out.TopAdmissions != out.FrontierAdmissions {
		return localHNSWAttributionQueryUtilityV1{}, errors.New("local HNSW query utility conservation")
	}
	return out, nil
}

func localHNSWAttributionHardMissesV1(in []localHNSWAttributionHardMissV1) []localHNSWAttributionHardMissV1 {
	// Stable digest ranking makes the sample independent of calibration order.
	sort.Slice(in, func(i, j int) bool { return in[i].Rank < in[j].Rank })
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
	if calibration.QueryCount == 0 || calibration.NativeUtility.ExaminedNative+calibration.NativeUtility.ExaminedAuxiliary == 0 || calibration.OverlayUtility.ExaminedNative+calibration.OverlayUtility.ExaminedAuxiliary == 0 {
		return out, errors.New("empty local HNSW instrumentation query utility")
	}
	out = localHNSWAttributionInstrumentationSummaryV1{Schema: localHNSWAttributionInstrumentationSchemaV1, ManifestIntegrity: source.manifest.IntegrityDigest, IndexDefinitionDigest: native.constructionEvidence.IndexDefinitionDigest, NativeVariant: native.constructionEvidence.Variant, OverlayVariant: overlay.constructionEvidence.Variant, NativeConstruction: nativeConstruction, OverlayConstruction: overlayConstruction, NativeQuery: calibration.NativeUtility, OverlayQuery: calibration.OverlayUtility, NativeLayer0Distances: graphDistanceAggregateV1(native.searchers), OverlayLayer0Distances: graphDistanceAggregateV1(overlay.searchers), HardMisses: append([]localHNSWAttributionHardMissV1(nil), calibration.HardMisses...)}
	if out.NativeVariant != "native" || out.OverlayVariant != "overlay_current" || !localHNSWAttributionSHA256V1(out.IndexDefinitionDigest) || out.NativeLayer0Distances.Count == 0 || out.OverlayLayer0Distances.Count == 0 {
		return localHNSWAttributionInstrumentationSummaryV1{}, errors.New("invalid local HNSW instrumentation summary")
	}
	return out, nil
}

func graphDistanceAggregateV1(searchers []*collections.VectorPartitionLocalSearcherV1) localHNSWAttributionDistanceAggregateV1 {
	// PackDiagnostics already computes the authoritative per-pack distances.
	// Keep the report compact: weighted mean and global extrema are sufficient.
	var out localHNSWAttributionDistanceAggregateV1
	for _, searcher := range searchers {
		d, err := searcher.PackDiagnosticsV1()
		if err != nil || d.Layer0Distances.Count == 0 {
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
