package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWAttributionQuerySchemaV1 = "treedb_local_hnsw_attribution_query_v1"
const localHNSWAttributionSearchSchemaV1 = "treedb_vector_partition_search_attribution_v1"

type localHNSWAttributionQueryResultV1 struct {
	ID        string `json:"id"`
	ScoreBits uint32 `json:"score_bits"`
}

type localHNSWAttributionQuerySearchV1 struct {
	Results               []localHNSWAttributionQueryResultV1   `json:"results"`
	Candidates            uint64                                `json:"candidates"`
	Edges                 uint64                                `json:"edges"`
	FrontierAdmissions    uint64                                `json:"frontier_admissions"`
	SeedCandidates        uint64                                `json:"seed_candidates"`
	SeedAdmissions        uint64                                `json:"seed_admissions"`
	TerminationReason     string                                `json:"termination_reason"`
	VisitedOrdinalsSHA256 string                                `json:"visited_ordinals_sha256"`
	VisitedOrdinals       []uint32                              `json:"visited_ordinals"`
	Utility               localHNSWAttributionQueryUtilityV1    `json:"utility"`
	TruthRecoveries       []localHNSWAttributionTruthRecoveryV1 `json:"truth_recoveries"`
}

// localHNSWAttributionTruthRecoveryV1 makes the per-query stable-ID owner
// explicit in JSONL so downstream reducers can reproduce overlap de-dup.
type localHNSWAttributionTruthRecoveryV1 struct {
	ID     string `json:"id"`
	Origin string `json:"origin"`
}

type localHNSWAttributionQueryPartitionV1 struct {
	Partition uint32                            `json:"partition_id"`
	Native    localHNSWAttributionQuerySearchV1 `json:"native"`
	Overlay   localHNSWAttributionQuerySearchV1 `json:"overlay"`
}

type localHNSWAttributionQueryWorkV1 struct {
	Candidates         uint64                             `json:"candidates"`
	Edges              uint64                             `json:"edges"`
	FrontierAdmissions uint64                             `json:"frontier_admissions"`
	Utility            localHNSWAttributionQueryUtilityV1 `json:"utility"`
}

type localHNSWAttributionQueryVariantV1 struct {
	LowResults       []localHNSWAttributionQueryResultV1 `json:"low_results"`
	HighResults      []localHNSWAttributionQueryResultV1 `json:"high_results"`
	EndToEndRecall   float64                             `json:"end_to_end_recall"`
	LocalRecall      float64                             `json:"local_recall"`
	RoutingRecall    float64                             `json:"routing_recall"`
	LowSelectedWork  localHNSWAttributionQueryWorkV1     `json:"low_selected_work"`
	HighSelectedWork localHNSWAttributionQueryWorkV1     `json:"high_selected_work"`
}

type localHNSWAttributionQueryEvidenceV1 struct {
	Schema                string                                 `json:"schema"`
	QueryOrdinal          int                                    `json:"query_ordinal"`
	QueryFP32SHA256       string                                 `json:"query_fp32_sha256"`
	LowRoute              []uint32                               `json:"low_route"`
	HighRoute             []uint32                               `json:"high_route"`
	GlobalTruth           []localHNSWAttributionQueryResultV1    `json:"global_truth"`
	NativeExactLocalTruth []localHNSWAttributionQueryResultV1    `json:"native_exact_local_truth"`
	RoutingRecall         float64                                `json:"routing_recall"`
	Partitions            []localHNSWAttributionQueryPartitionV1 `json:"partitions"`
	Native                localHNSWAttributionQueryVariantV1     `json:"native"`
	Overlay               localHNSWAttributionQueryVariantV1     `json:"overlay"`
}

func localHNSWAttributionQueryEvidenceV1Build(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, native, overlay *localHNSWVariantHarnessV1, queryOrdinal int, query []float32, globalTruth []m8CanonicalResultV1) (localHNSWAttributionQueryEvidenceV1, error) {
	var out localHNSWAttributionQueryEvidenceV1
	if queryOrdinal < 0 || source == nil || source.router == nil || source.manifest.PartitionCount == 0 || len(query) == 0 {
		return out, errors.New("invalid local HNSW attribution query")
	}
	if err := localHNSWAttributionGraphHarnessV1(source, native); err != nil {
		return out, err
	}
	if err := localHNSWAttributionGraphHarnessV1(source, overlay); err != nil {
		return out, err
	}
	truth, err := localHNSWAttributionCanonicalResultsV1(globalTruth, true)
	truthIDs, truthScores := m8CanonicalParityV1(globalTruth, truth)
	if err != nil {
		return out, err
	}
	if !truthIDs || !truthScores {
		return out, errors.New("noncanonical local HNSW global truth")
	}
	partitions := int(source.manifest.PartitionCount)
	candidates := min(256, int(source.status.Representatives))
	if candidates < 1 {
		return out, errors.New("invalid retained local HNSW router")
	}
	lowCount := min(2, partitions)
	lowRoute, err := localHNSWAttributionQueryRouteV1(ctx, source, query, candidates, lowCount)
	if err != nil {
		return out, err
	}
	highRoute, err := localHNSWAttributionQueryRouteV1(ctx, source, query, candidates, partitions)
	if err != nil || !localHNSWAttributionRoutePrefixV1(lowRoute, highRoute) || !localHNSWAttributionRoutePermutationV1(highRoute, partitions) {
		return out, errors.New("invalid retained local HNSW query route")
	}
	exactLocal, err := localHNSWAttributionExactLocalV1(ctx, native, query, lowRoute)
	if err != nil {
		return out, err
	}
	truthIDsSet := make(map[string]struct{}, len(truth))
	for _, result := range truth {
		truthIDsSet[result.ID] = struct{}{}
	}
	nativeRecords, nativeResults, err := localHNSWAttributionQueryVariantV1Build(ctx, native, query, truthIDsSet)
	if err != nil {
		return out, err
	}
	overlayRecords, overlayResults, err := localHNSWAttributionQueryVariantV1Build(ctx, overlay, query, truthIDsSet)
	if err != nil {
		return out, err
	}
	if len(nativeRecords) != partitions || len(overlayRecords) != partitions {
		return out, errors.New("incomplete local HNSW query partitions")
	}
	out = localHNSWAttributionQueryEvidenceV1{Schema: localHNSWAttributionQuerySchemaV1, QueryOrdinal: queryOrdinal, QueryFP32SHA256: localHNSWAttributionQueryFP32SHA256V1(query), LowRoute: append([]uint32(nil), lowRoute...), HighRoute: append([]uint32(nil), highRoute...), GlobalTruth: localHNSWAttributionQueryResultBitsV1(truth), NativeExactLocalTruth: localHNSWAttributionQueryResultBitsV1(exactLocal), RoutingRecall: m8CanonicalRecallV1(truth, exactLocal), Partitions: make([]localHNSWAttributionQueryPartitionV1, partitions)}
	for partition := range out.Partitions {
		out.Partitions[partition] = localHNSWAttributionQueryPartitionV1{Partition: uint32(partition), Native: nativeRecords[partition], Overlay: overlayRecords[partition]}
	}
	out.Native, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, nativeRecords, nativeResults, lowRoute, highRoute)
	if err != nil {
		return out, err
	}
	out.Overlay, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, overlayRecords, overlayResults, lowRoute, highRoute)
	if err != nil {
		return out, err
	}
	if err := localHNSWAttributionQueryEvidenceValidateV1(out); err != nil {
		return out, err
	}
	return out, nil
}

func localHNSWAttributionQueryRouteV1(ctx context.Context, source *m8ProductionMultiGroupAssetsV1, query []float32, candidates, probes int) ([]uint32, error) {
	route, err := source.router.SearchWithContextV1(ctx, query, collections.VectorPartitionRouterSearchOptionsV1{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: candidates, PartitionProbes: probes})
	if err != nil || route.Status.Mode != collections.VectorPartitionRouterModeApproxV1 || len(route.Partitions) != probes {
		return nil, errors.New("retained local HNSW router query")
	}
	out := make([]uint32, probes)
	for i, partition := range route.Partitions {
		out[i] = partition.PartitionID
	}
	return out, nil
}

func localHNSWAttributionExactLocalV1(ctx context.Context, native *localHNSWVariantHarnessV1, query []float32, route []uint32) ([]m8CanonicalResultV1, error) {
	var results []m8CanonicalResultV1
	for _, partition := range route {
		if int(partition) >= len(native.searchers) || native.searchers[partition] == nil || native.searchers[partition].Status().SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
			return nil, errors.New("incomplete native local HNSW query route")
		}
		found, _, err := native.searchers[partition].SearchExactWithOptionsV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 128})
		if err != nil {
			return nil, err
		}
		for _, result := range found {
			results = append(results, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	return localHNSWAttributionCanonicalResultsV1(results, false)
}

func localHNSWAttributionQueryVariantV1Build(ctx context.Context, harness *localHNSWVariantHarnessV1, query []float32, truth map[string]struct{}) ([]localHNSWAttributionQuerySearchV1, [][]m8CanonicalResultV1, error) {
	records := make([]localHNSWAttributionQuerySearchV1, len(harness.searchers))
	resultsByPartition := make([][]m8CanonicalResultV1, len(harness.searchers))
	for partition, searcher := range harness.searchers {
		if searcher == nil || searcher.Status().SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
			return nil, nil, errors.New("local HNSW attribution search route")
		}
		results, metrics, attribution, err := searcher.SearchWithAttributionV1(ctx, query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 128})
		if err != nil || metrics.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || !localHNSWAttributionSearchValidV1(attribution) {
			return nil, nil, errors.New("invalid local HNSW attribution search")
		}
		canonical := make([]m8CanonicalResultV1, len(results))
		for i, result := range results {
			canonical[i] = m8CanonicalResultV1{ID: result.ID, Score: result.Score}
		}
		canonical, err = localHNSWAttributionCanonicalResultsV1(canonical, false)
		if err != nil {
			return nil, nil, err
		}
		if partition >= len(harness.finalOrigins) {
			return nil, nil, errors.New("missing local HNSW final origins")
		}
		origins := harness.finalOrigins[partition]
		utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, attribution, origins, harness.documentIDs[partition], truth)
		if err != nil {
			return nil, nil, err
		}
		recoveries := localHNSWAttributionTruthRecoveriesV1(attribution, origins, harness.documentIDs[partition], truth)
		records[partition] = localHNSWAttributionQuerySearchV1{Results: localHNSWAttributionQueryResultBitsV1(canonical), Candidates: metrics.Candidates, Edges: metrics.Edges, FrontierAdmissions: attribution.FrontierAdmissions, SeedCandidates: attribution.SeedCandidates, SeedAdmissions: attribution.SeedAdmissions, TerminationReason: attribution.TerminationReason, VisitedOrdinalsSHA256: attribution.VisitedOrdinalsSHA256, VisitedOrdinals: append([]uint32(nil), attribution.VisitedOrdinals...), Utility: utility, TruthRecoveries: localHNSWAttributionTruthRecoveryRecordsV1(recoveries)}
		resultsByPartition[partition] = canonical
	}
	return records, resultsByPartition, nil
}

func localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal []m8CanonicalResultV1, records []localHNSWAttributionQuerySearchV1, resultsByPartition [][]m8CanonicalResultV1, lowRoute, highRoute []uint32) (localHNSWAttributionQueryVariantV1, error) {
	truthIDs := make(map[string]struct{}, len(truth))
	for _, result := range truth {
		truthIDs[result.ID] = struct{}{}
	}
	lowResults, lowWork, err := localHNSWAttributionQueryMergeV1(records, resultsByPartition, lowRoute, truthIDs)
	if err != nil {
		return localHNSWAttributionQueryVariantV1{}, err
	}
	highResults, highWork, err := localHNSWAttributionQueryMergeV1(records, resultsByPartition, highRoute, truthIDs)
	if err != nil {
		return localHNSWAttributionQueryVariantV1{}, err
	}
	return localHNSWAttributionQueryVariantV1{LowResults: localHNSWAttributionQueryResultBitsV1(lowResults), HighResults: localHNSWAttributionQueryResultBitsV1(highResults), EndToEndRecall: m8CanonicalRecallV1(truth, lowResults), LocalRecall: m8CanonicalRecallV1(exactLocal, lowResults), RoutingRecall: m8CanonicalRecallV1(truth, exactLocal), LowSelectedWork: lowWork, HighSelectedWork: highWork}, nil
}

func localHNSWAttributionQueryMergeV1(records []localHNSWAttributionQuerySearchV1, resultsByPartition [][]m8CanonicalResultV1, route []uint32, truth map[string]struct{}) ([]m8CanonicalResultV1, localHNSWAttributionQueryWorkV1, error) {
	var merged []m8CanonicalResultV1
	var work localHNSWAttributionQueryWorkV1
	recovered := make(map[string]struct{})
	for _, partition := range route {
		if int(partition) >= len(records) || int(partition) >= len(resultsByPartition) {
			return nil, localHNSWAttributionQueryWorkV1{}, errors.New("invalid local HNSW query partition")
		}
		merged = append(merged, resultsByPartition[partition]...)
		work.Candidates += records[partition].Candidates
		work.Edges += records[partition].Edges
		work.FrontierAdmissions += records[partition].FrontierAdmissions
		utility := records[partition].Utility
		recoveries, err := localHNSWAttributionQueryRecordValidateV1(records[partition], truth)
		if err != nil {
			return nil, localHNSWAttributionQueryWorkV1{}, err
		}
		for id, origin := range recoveries {
			if _, seen := recovered[id]; seen {
				if err := localHNSWAttributionQueryUtilityRemoveTruthRecoveryV1(&utility, origin); err != nil {
					return nil, localHNSWAttributionQueryWorkV1{}, err
				}
				continue
			}
			recovered[id] = struct{}{}
		}
		if err := localHNSWAttributionQueryUtilityAddV1(&work.Utility, utility); err != nil {
			return nil, localHNSWAttributionQueryWorkV1{}, err
		}
	}
	return m8CanonicalResultsV1(merged, 10), work, nil
}

// localHNSWAttributionQueryUtilityAggregateV1 counts every searched partition's
// work, but credits a canonical truth ID once per query across home/overlap
// memberships. Non-truth work remains partition-local by design.
func localHNSWAttributionQueryUtilityAggregateV1(records []localHNSWAttributionQuerySearchV1, truth map[string]struct{}) (localHNSWAttributionQueryUtilityV1, error) {
	var out localHNSWAttributionQueryUtilityV1
	recovered := make(map[string]struct{})
	for _, record := range records {
		utility := record.Utility
		recoveries, err := localHNSWAttributionQueryRecordValidateV1(record, truth)
		if err != nil {
			return localHNSWAttributionQueryUtilityV1{}, err
		}
		for id, origin := range recoveries {
			if _, seen := recovered[id]; seen {
				if err := localHNSWAttributionQueryUtilityRemoveTruthRecoveryV1(&utility, origin); err != nil {
					return localHNSWAttributionQueryUtilityV1{}, err
				}
				continue
			}
			recovered[id] = struct{}{}
		}
		if err := localHNSWAttributionQueryUtilityAddV1(&out, utility); err != nil {
			return localHNSWAttributionQueryUtilityV1{}, err
		}
	}
	return out, nil
}

func localHNSWAttributionTruthRecoveryRecordsV1(recoveries map[string]string) []localHNSWAttributionTruthRecoveryV1 {
	if len(recoveries) == 0 {
		return nil
	}
	ids := make([]string, 0, len(recoveries))
	for id := range recoveries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]localHNSWAttributionTruthRecoveryV1, len(ids))
	for i, id := range ids {
		out[i] = localHNSWAttributionTruthRecoveryV1{ID: id, Origin: recoveries[id]}
	}
	return out
}

// localHNSWAttributionTruthRecoveryMapV1 validates the persisted canonical
// records before de-duplicating overlap recoveries. JSONL is the sole source
// of truth; no transient map is retained across serialization.
func localHNSWAttributionQueryRecordValidateV1(record localHNSWAttributionQuerySearchV1, truth map[string]struct{}) (map[string]string, error) {
	if len(truth) == 0 || !localHNSWAttributionQueryUtilityConservedV1(record.Utility, record.Edges) || math.MaxUint64-record.Utility.NewlyVisited < record.SeedCandidates || math.MaxUint64-record.Utility.FrontierAdmissions < record.SeedAdmissions || record.Candidates != uint64(len(record.VisitedOrdinals)) || record.Candidates != record.Utility.NewlyVisited+record.SeedCandidates || record.FrontierAdmissions != record.Utility.FrontierAdmissions+record.SeedAdmissions || !localHNSWAttributionTimingTerminationV1(record.TerminationReason) || record.VisitedOrdinalsSHA256 != localHNSWAttributionVisitedOrdinalsSHA256V1(record.VisitedOrdinals) {
		return nil, errors.New("invalid local HNSW persisted query record")
	}
	if _, err := localHNSWAttributionCanonicalQueryResultBitsV1(record.Results, false); err != nil {
		return nil, err
	}
	for i, ordinal := range record.VisitedOrdinals {
		if i != 0 && ordinal <= record.VisitedOrdinals[i-1] {
			return nil, errors.New("noncanonical local HNSW visited ordinals")
		}
	}
	if uint64(len(record.TruthRecoveries)) != record.Utility.TruthRecovered {
		return nil, errors.New("local HNSW truth recovery count mismatch")
	}
	if len(record.TruthRecoveries) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(record.TruthRecoveries))
	counts := make(map[string]uint64, len(record.TruthRecoveries))
	previous := ""
	for i, recovery := range record.TruthRecoveries {
		if recovery.ID == "" || recovery.Origin == "" || (i != 0 && recovery.ID <= previous) {
			return nil, errors.New("invalid local HNSW truth recovery records")
		}
		if _, wanted := truth[recovery.ID]; !wanted {
			return nil, errors.New("local HNSW truth recovery is not query truth")
		}
		if _, ok := localHNSWAttributionTruthRecoveryBucketV1(&record.Utility, recovery.Origin); !ok {
			return nil, errors.New("invalid local HNSW truth recovery origin")
		}
		out[recovery.ID] = recovery.Origin
		counts[recovery.Origin]++
		previous = recovery.ID
	}
	for _, origin := range []string{"diversity_selected", "nearest_backfill", "reciprocal_add", "reciprocity_repair", "overlay_rewrite", "auxiliary", "unattributed"} {
		bucket, _ := localHNSWAttributionTruthRecoveryBucketV1(&record.Utility, origin)
		if *bucket != counts[origin] {
			return nil, errors.New("local HNSW truth recovery origin count mismatch")
		}
	}
	return out, nil
}

// localHNSWAttributionQueryEvidenceValidateV1 replays every derived query
// field from persisted records. It intentionally consumes only JSON-safe
// payload fields, never the transient search results used during materialization.
func localHNSWAttributionQueryEvidenceValidateV1(evidence localHNSWAttributionQueryEvidenceV1) error {
	if evidence.Schema != localHNSWAttributionQuerySchemaV1 || evidence.QueryOrdinal < 0 || !localHNSWAttributionSHA256V1(evidence.QueryFP32SHA256) || len(evidence.Partitions) == 0 || len(evidence.LowRoute) != min(2, len(evidence.Partitions)) || !localHNSWAttributionRoutePrefixV1(evidence.LowRoute, evidence.HighRoute) || !localHNSWAttributionRoutePermutationV1(evidence.HighRoute, len(evidence.Partitions)) {
		return errors.New("invalid local HNSW persisted query evidence")
	}
	truth, err := localHNSWAttributionCanonicalQueryResultBitsV1(evidence.GlobalTruth, true)
	if err != nil {
		return err
	}
	exactLocal, err := localHNSWAttributionCanonicalQueryResultBitsV1(evidence.NativeExactLocalTruth, false)
	if err != nil {
		return err
	}
	truthIDs := make(map[string]struct{}, len(truth))
	for _, result := range truth {
		truthIDs[result.ID] = struct{}{}
	}
	nativeRecords := make([]localHNSWAttributionQuerySearchV1, len(evidence.Partitions))
	overlayRecords := make([]localHNSWAttributionQuerySearchV1, len(evidence.Partitions))
	nativeResults := make([][]m8CanonicalResultV1, len(evidence.Partitions))
	overlayResults := make([][]m8CanonicalResultV1, len(evidence.Partitions))
	for i, partition := range evidence.Partitions {
		if partition.Partition != uint32(i) {
			return errors.New("noncanonical local HNSW persisted query partition")
		}
		if _, err := localHNSWAttributionQueryRecordValidateV1(partition.Native, truthIDs); err != nil {
			return err
		}
		if _, err := localHNSWAttributionQueryRecordValidateV1(partition.Overlay, truthIDs); err != nil {
			return err
		}
		var err error
		nativeResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Native.Results, false)
		if err != nil {
			return err
		}
		overlayResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Overlay.Results, false)
		if err != nil {
			return err
		}
		nativeRecords[i], overlayRecords[i] = partition.Native, partition.Overlay
	}
	if err := localHNSWAttributionQueryVariantEvidenceValidateV1(evidence.Native, truth, exactLocal, nativeRecords, nativeResults, evidence.LowRoute, evidence.HighRoute); err != nil {
		return err
	}
	if err := localHNSWAttributionQueryVariantEvidenceValidateV1(evidence.Overlay, truth, exactLocal, overlayRecords, overlayResults, evidence.LowRoute, evidence.HighRoute); err != nil {
		return err
	}
	if evidence.RoutingRecall != m8CanonicalRecallV1(truth, exactLocal) || evidence.RoutingRecall != evidence.Native.RoutingRecall || evidence.RoutingRecall != evidence.Overlay.RoutingRecall {
		return errors.New("local HNSW persisted routing recall mismatch")
	}
	return nil
}

func localHNSWAttributionQueryVariantEvidenceValidateV1(value localHNSWAttributionQueryVariantV1, truth, exactLocal []m8CanonicalResultV1, records []localHNSWAttributionQuerySearchV1, results [][]m8CanonicalResultV1, lowRoute, highRoute []uint32) error {
	low, lowWork, err := localHNSWAttributionQueryMergeV1(records, results, lowRoute, localHNSWAttributionResultIDSetV1(truth))
	if err != nil {
		return err
	}
	high, highWork, err := localHNSWAttributionQueryMergeV1(records, results, highRoute, localHNSWAttributionResultIDSetV1(truth))
	if err != nil {
		return err
	}
	if !slices.Equal(value.LowResults, localHNSWAttributionQueryResultBitsV1(low)) || !slices.Equal(value.HighResults, localHNSWAttributionQueryResultBitsV1(high)) || value.LowSelectedWork != lowWork || value.HighSelectedWork != highWork || value.EndToEndRecall != m8CanonicalRecallV1(truth, low) || value.LocalRecall != m8CanonicalRecallV1(exactLocal, low) || value.RoutingRecall != m8CanonicalRecallV1(truth, exactLocal) {
		return errors.New("local HNSW persisted query variant mismatch")
	}
	if _, err := localHNSWAttributionCanonicalQueryResultBitsV1(value.LowResults, false); err != nil {
		return err
	}
	if _, err := localHNSWAttributionCanonicalQueryResultBitsV1(value.HighResults, false); err != nil {
		return err
	}
	return nil
}

func localHNSWAttributionCanonicalQueryResultBitsV1(results []localHNSWAttributionQueryResultV1, requireTen bool) ([]m8CanonicalResultV1, error) {
	decoded := make([]m8CanonicalResultV1, len(results))
	for i, result := range results {
		decoded[i] = m8CanonicalResultV1{ID: result.ID, Score: math.Float32frombits(result.ScoreBits)}
	}
	canonical, err := localHNSWAttributionCanonicalResultsV1(decoded, requireTen)
	if err != nil || !slices.Equal(results, localHNSWAttributionQueryResultBitsV1(canonical)) {
		return nil, errors.New("noncanonical local HNSW persisted query results")
	}
	return canonical, nil
}

func localHNSWAttributionResultIDSetV1(results []m8CanonicalResultV1) map[string]struct{} {
	ids := make(map[string]struct{}, len(results))
	for _, result := range results {
		ids[result.ID] = struct{}{}
	}
	return ids
}

func localHNSWAttributionVisitedOrdinalsSHA256V1(ordinals []uint32) string {
	h := sha256.New()
	h.Write([]byte("treedb_vector_partition_search_attribution_v1/visited/"))
	var raw [4]byte
	for _, ordinal := range ordinals {
		binary.LittleEndian.PutUint32(raw[:], ordinal)
		h.Write(raw[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func localHNSWAttributionQueryUtilityConservedV1(value localHNSWAttributionQueryUtilityV1, edges uint64) bool {
	origins := []localHNSWAttributionQueryOriginUtilityV1{value.Diversity, value.Backfill, value.Reciprocal, value.Repair, value.Overlay, value.Auxiliary, value.Unattributed}
	var examined, newlyVisited, scored, topAdmissions, frontierAdmissions, stateImprovements, truthRecovered uint64
	for i, origin := range origins {
		if origin.Scored > origin.Examined || origin.NewlyVisited > origin.Scored || origin.TopAdmissions != origin.FrontierAdmissions || origin.TopAdmissions > origin.Scored || origin.StateImprovements > origin.Scored {
			return false
		}
		// Only the unattributed bucket owns seed truth. Every edge-origin
		// recovery must have a corresponding scored edge.
		if i != len(origins)-1 && origin.TruthRecovered > origin.Scored {
			return false
		}
		examined += origin.Examined
		newlyVisited += origin.NewlyVisited
		scored += origin.Scored
		topAdmissions += origin.TopAdmissions
		frontierAdmissions += origin.FrontierAdmissions
		stateImprovements += origin.StateImprovements
		truthRecovered += origin.TruthRecovered
	}
	nativeExamined := value.Diversity.Examined + value.Backfill.Examined + value.Reciprocal.Examined + value.Repair.Examined + value.Overlay.Examined
	return value.ExaminedNative+value.ExaminedAuxiliary == edges && value.ExaminedAuxiliary == value.Auxiliary.Examined && value.ExaminedNative == nativeExamined && value.Unattributed.Examined == 0 && value.Unattributed.NewlyVisited == 0 && value.Unattributed.Scored == 0 && value.Unattributed.TopAdmissions == 0 && value.Unattributed.FrontierAdmissions == 0 && value.Unattributed.StateImprovements == 0 && examined == edges && newlyVisited == value.NewlyVisited && scored == value.Scored && topAdmissions == value.TopAdmissions && frontierAdmissions == value.FrontierAdmissions && stateImprovements == value.StateImprovements && truthRecovered == value.TruthRecovered && value.NewlyVisited <= value.Scored && value.TopAdmissions == value.FrontierAdmissions && value.TopAdmissions <= value.Scored && value.StateImprovements <= value.Scored
}

func localHNSWAttributionCanonicalResultsV1(results []m8CanonicalResultV1, requireTen bool) ([]m8CanonicalResultV1, error) {
	canonical := m8CanonicalResultsV1(results, 10)
	if canonical == nil || requireTen && len(canonical) != 10 {
		return nil, errors.New("invalid local HNSW canonical results")
	}
	return canonical, nil
}

func localHNSWAttributionSearchValidV1(attribution collections.VectorPartitionSearchAttributionV1) bool {
	if attribution.Schema != localHNSWAttributionSearchSchemaV1 || attribution.FrontierAdmissions == 0 || attribution.VisitedRows == 0 || attribution.VisitedRows != uint64(len(attribution.VisitedOrdinals)) || !localHNSWAttributionSHA256V1(attribution.VisitedOrdinalsSHA256) {
		return false
	}
	for i, ordinal := range attribution.VisitedOrdinals {
		if i > 0 && attribution.VisitedOrdinals[i-1] >= ordinal {
			return false
		}
	}
	switch attribution.TerminationReason {
	case "candidate_limit", "frontier_empty_retained_full", "frontier_empty_no_seed", "distance_bound":
		return true
	}
	return false
}

func localHNSWAttributionRoutePrefixV1(low, high []uint32) bool {
	if len(low) > len(high) {
		return false
	}
	for i := range low {
		if low[i] != high[i] {
			return false
		}
	}
	return true
}

func localHNSWAttributionRoutePermutationV1(route []uint32, partitions int) bool {
	if len(route) != partitions {
		return false
	}
	seen := make([]bool, partitions)
	for _, partition := range route {
		if int(partition) >= partitions || seen[partition] {
			return false
		}
		seen[partition] = true
	}
	return true
}

func localHNSWAttributionQueryResultBitsV1(results []m8CanonicalResultV1) []localHNSWAttributionQueryResultV1 {
	out := make([]localHNSWAttributionQueryResultV1, len(results))
	for i, result := range results {
		out[i] = localHNSWAttributionQueryResultV1{ID: result.ID, ScoreBits: math.Float32bits(result.Score)}
	}
	return out
}

func localHNSWAttributionQueryFP32SHA256V1(query []float32) string {
	h := sha256.New()
	h.Write([]byte("treedb-4105-local-hnsw-query-fp32-v1/"))
	var raw [4]byte
	for _, value := range query {
		binary.LittleEndian.PutUint32(raw[:], math.Float32bits(value))
		h.Write(raw[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}
