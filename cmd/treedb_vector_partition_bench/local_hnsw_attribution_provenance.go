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
	DiversitySelected uint64    `json:"diversity_selected"`
	BackfillSelected  uint64    `json:"backfill_selected"`
	InitialAdded      uint64    `json:"initial_added"`
	ReciprocalAdded   uint64    `json:"reciprocal_added"`
	PruneKept         uint64    `json:"prune_kept"`
	PruneDropped      uint64    `json:"prune_dropped"`
	FinalSurvivors    uint64    `json:"final_survivors"`
	InsertionAge      [4]uint64 `json:"insertion_age_buckets"`
}

type localHNSWAttributionQueryUtilityV1 struct {
	ExaminedNative     uint64 `json:"examined_native"`
	ExaminedAuxiliary  uint64 `json:"examined_auxiliary"`
	NewlyVisited       uint64 `json:"newly_visited"`
	Scored             uint64 `json:"scored"`
	TopAdmissions      uint64 `json:"top_admissions"`
	FrontierAdmissions uint64 `json:"frontier_admissions"`
}

type localHNSWAttributionHardMissV1 struct {
	QueryOrdinal int    `json:"query_ordinal"`
	QuerySHA256  string `json:"query_sha256"`
	Variant      string `json:"variant"`
	OverlapBits  uint32 `json:"overlap_bits"`
	Rank         string `json:"rank"`
}

type localHNSWAttributionInstrumentationSummaryV1 struct {
	Schema                 string                                                      `json:"schema"`
	ManifestIntegrity      string                                                      `json:"manifest_integrity_digest"`
	IndexDefinitionDigest  string                                                      `json:"index_definition_digest"`
	DescriptorSHA256       string                                                      `json:"descriptor_sha256"`
	CalibrationSHA256      string                                                      `json:"calibration_sha256"`
	TruthSHA256            string                                                      `json:"truth_sha256"`
	NativeVariant          string                                                      `json:"native_variant"`
	OverlayVariant         string                                                      `json:"overlay_variant"`
	NativeConstruction     localHNSWAttributionConstructionTotalsV1                    `json:"native_construction"`
	OverlayConstruction    localHNSWAttributionConstructionTotalsV1                    `json:"overlay_construction"`
	NativeQuery            localHNSWAttributionQueryUtilityV1                          `json:"native_query"`
	OverlayQuery           localHNSWAttributionQueryUtilityV1                          `json:"overlay_query"`
	ExactKNNPoolCoverage   localHNSWAttributionRecallAggregateV1                       `json:"exact_knn_candidate_pool_coverage"`
	FinalExactKNNOverlap   localHNSWAttributionRecallAggregateV1                       `json:"final_exact_knn_overlap"`
	NativeLayer0Distances  collections.VectorPartitionLocalGraphDistanceDistributionV1 `json:"native_layer0_distances"`
	OverlayLayer0Distances collections.VectorPartitionLocalGraphDistanceDistributionV1 `json:"overlay_layer0_distances"`
	HardMisses             []localHNSWAttributionHardMissV1                            `json:"hard_misses"`
}

func localHNSWAttributionConstructionReduceV1(evidence collections.VectorPartitionConstructionEvidenceV1) (localHNSWAttributionConstructionTotalsV1, error) {
	var out localHNSWAttributionConstructionTotalsV1
	if evidence.Schema != "treedb_vector_partition_construction_evidence_v1" || evidence.Variant == "" || !localHNSWAttributionSHA256V1(evidence.ManifestChecksum) || !localHNSWAttributionSHA256V1(evidence.IndexDefinitionDigest) || len(evidence.Partitions) == 0 {
		return out, errors.New("invalid local HNSW construction provenance")
	}
	for _, partition := range evidence.Partitions {
		for _, selection := range partition.Selections {
			if selection.Selected != selection.DiversitySelected+selection.BackfillSelected || selection.Selected < 0 {
				return out, errors.New("invalid local HNSW construction selection")
			}
			out.DiversitySelected += uint64(selection.DiversitySelected)
			out.BackfillSelected += uint64(selection.BackfillSelected)
		}
		for _, event := range partition.Events {
			if event.InsertionOrdinal < 0 {
				return out, errors.New("invalid local HNSW construction insertion age")
			}
			switch event.Action {
			case "initial_add":
				out.InitialAdded++
			case "reciprocal_add":
				out.ReciprocalAdded++
			case "reciprocal_prune_keep":
				out.PruneKept++
			case "reciprocal_prune_drop":
				out.PruneDropped++
			case "final_survivor":
				out.FinalSurvivors++
			default:
				return out, errors.New("invalid local HNSW construction action")
			}
			bucket := 0
			switch {
			case event.InsertionOrdinal < 16:
				bucket = 0
			case event.InsertionOrdinal < 256:
				bucket = 1
			case event.InsertionOrdinal < 4096:
				bucket = 2
			default:
				bucket = 3
			}
			out.InsertionAge[bucket]++
		}
	}
	return out, nil
}

func localHNSWAttributionQueryUtilityReduceV1(metrics collections.VectorPartitionSearchMetricsV1, attribution collections.VectorPartitionSearchAttributionV1) (localHNSWAttributionQueryUtilityV1, error) {
	var out localHNSWAttributionQueryUtilityV1
	if !localHNSWAttributionSearchValidV1(attribution) {
		return out, errors.New("invalid local HNSW query attribution")
	}
	for _, event := range attribution.EdgeEvents {
		if event.Auxiliary {
			out.ExaminedAuxiliary++
		} else {
			out.ExaminedNative++
		}
		if event.NewlyVisited {
			out.NewlyVisited++
		}
		if event.Scored {
			out.Scored++
		}
		if event.TopAdmission {
			out.TopAdmissions++
		}
		if event.FrontierAdmission {
			out.FrontierAdmissions++
		}
	}
	if out.ExaminedNative+out.ExaminedAuxiliary != metrics.Edges || out.ExaminedAuxiliary != metrics.AuxiliaryEdges || out.NewlyVisited+attribution.SeedCandidates != metrics.Candidates || out.FrontierAdmissions+attribution.SeedAdmissions != attribution.FrontierAdmissions || out.Scored != out.NewlyVisited || out.TopAdmissions != out.FrontierAdmissions {
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
	out = localHNSWAttributionInstrumentationSummaryV1{Schema: localHNSWAttributionInstrumentationSchemaV1, ManifestIntegrity: source.manifest.IntegrityDigest, IndexDefinitionDigest: native.constructionEvidence.IndexDefinitionDigest, DescriptorSHA256: localHNSWAttributionDescriptorSHA256V1, CalibrationSHA256: localHNSWAttributionCalibrationSHA256V1, TruthSHA256: localHNSWAttributionTruthSHA256V1, NativeVariant: native.constructionEvidence.Variant, OverlayVariant: overlay.constructionEvidence.Variant, NativeConstruction: nativeConstruction, OverlayConstruction: overlayConstruction, NativeQuery: calibration.NativeUtility, OverlayQuery: calibration.OverlayUtility, ExactKNNPoolCoverage: calibration.RoutingRecall, FinalExactKNNOverlap: calibration.Overlay.AllGlobal, NativeLayer0Distances: graphDistanceAggregateV1(native.searchers), OverlayLayer0Distances: graphDistanceAggregateV1(overlay.searchers), HardMisses: append([]localHNSWAttributionHardMissV1(nil), calibration.HardMisses...)}
	if out.NativeVariant != "native" || out.OverlayVariant != "overlay_current" || !localHNSWAttributionSHA256V1(out.IndexDefinitionDigest) || out.NativeLayer0Distances.Count == 0 || out.OverlayLayer0Distances.Count == 0 {
		return localHNSWAttributionInstrumentationSummaryV1{}, errors.New("invalid local HNSW instrumentation summary")
	}
	return out, nil
}

func graphDistanceAggregateV1(searchers []*collections.VectorPartitionLocalSearcherV1) collections.VectorPartitionLocalGraphDistanceDistributionV1 {
	// PackDiagnostics already computes the authoritative per-pack distances.
	// Keep the report compact: weighted mean and global extrema are sufficient.
	var out collections.VectorPartitionLocalGraphDistanceDistributionV1
	for _, searcher := range searchers {
		d, err := searcher.PackDiagnosticsV1()
		if err != nil || d.Layer0Distances.Count == 0 {
			return collections.VectorPartitionLocalGraphDistanceDistributionV1{}
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
