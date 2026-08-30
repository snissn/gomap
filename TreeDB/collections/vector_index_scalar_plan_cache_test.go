package collections

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestNativeScalarPlanCacheWarmHitAndGenerationPublicationInvalidation(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("alpha")}, [][]byte{[]byte(`{"embedding":[1,0],"tenant":"alpha"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	filter := HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}
	cold := searchNativeScalarTest(t, col, def, filter, 1)
	if len(cold.Results) != 1 || string(cold.Results[0].ID) != "alpha" || cold.Stats.ScalarFilterPlanCacheMisses != 1 || cold.Stats.ScalarFilterPlanCacheHits != 0 {
		t.Fatalf("cold results=%+v stats=%+v", cold.Results, cold.Stats)
	}
	warm := searchNativeScalarTest(t, col, def, filter, 1)
	if len(warm.Results) != 1 || string(warm.Results[0].ID) != "alpha" || warm.Stats.ScalarFilterPlanCacheHits != 1 || warm.Stats.ScalarFilterPlanCacheMisses != 0 {
		t.Fatalf("warm results=%+v stats=%+v", warm.Results, warm.Stats)
	}
	if replaced, err := col.Replace([]byte("alpha"), []byte(`{"embedding":[1,0],"tenant":"beta"}`)); err != nil || !replaced {
		t.Fatalf("replace=%v err=%v", replaced, err)
	}
	updated := searchNativeScalarTest(t, col, def, filter, 1)
	if len(updated.Results) != 0 || updated.Stats.ScalarFilterPlanCacheHits != 0 || updated.Stats.ScalarFilterPlanCacheInvalidations+updated.Stats.ScalarFilterPlanCacheGenerationBypasses == 0 {
		t.Fatalf("updated results=%+v stats=%+v", updated.Results, updated.Stats)
	}
	beta := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "beta"}, 1)
	if len(beta.Results) != 1 || string(beta.Results[0].ID) != "alpha" {
		t.Fatalf("beta results=%+v stats=%+v", beta.Results, beta.Stats)
	}
}

func TestNativeScalarPlanCacheConcurrentReuseClonesMutationOwnedState(t *testing.T) {
	col := &Collection{}
	index := &VectorIndex{}
	key := nativeScalarPlanCacheKey{
		vectorIndex: index, sourceGeneration: 7, vectorGeneration: 7,
		vectorSchema: "vector", scalarSchema: "scalar", filterIdentity: "filter",
		probeLimit: nativeScalarProbeLimit, exactSafetyCap: nativeScalarExactSafetyCap,
		annSeedProbeLimit: nativeScalarANNSeedProbeLimit, annSeedLimit: nativeScalarANNSeedLimit,
	}
	immutable := &nativeScalarFilterExecution{
		identity:     NativeScalarFilterPlanMixed,
		finiteIDs:    hybridScalarAllowSet{"alpha": {}, "beta": {}},
		candidateIDs: 2, retainedCandidateIDs: 2, refinedCandidateIDs: 2,
		sourceGeneration: 7,
	}
	first, _ := col.nativeScalarPlanCachePut(key, immutable, nativeScalarPlanCacheStats{misses: 1})
	delete(first.finiteIDs, "alpha")

	const readers = 32
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			plan, stats := col.nativeScalarPlanCacheGet(key)
			if stats.hits != 1 || len(plan.finiteIDs) != 2 {
				errs <- fmt.Errorf("hits=%d finite=%d", stats.hits, len(plan.finiteIDs))
				return
			}
			delete(plan.finiteIDs, "alpha")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	cached, stats := col.nativeScalarPlanCacheGet(key)
	if stats.hits != 1 || len(cached.finiteIDs) != 2 {
		t.Fatalf("cached hits=%d finite=%d", stats.hits, len(cached.finiteIDs))
	}
}

func TestNativeScalarPlanCacheKeySeparatesFilterLimitsSchemaAndIndexIdentity(t *testing.T) {
	index := &VectorIndex{}
	base := nativeScalarPlanCacheKey{
		vectorIndex: index, sourceGeneration: 3, vectorGeneration: 3,
		vectorSchema: "vector-a", scalarSchema: "scalar-a", filterIdentity: "filter-a",
		probeLimit: nativeScalarProbeLimit, exactSafetyCap: nativeScalarExactSafetyCap,
		annSeedProbeLimit: nativeScalarANNSeedProbeLimit, annSeedLimit: nativeScalarANNSeedLimit,
	}
	variants := map[string]nativeScalarPlanCacheKey{}
	filter := base
	filter.filterIdentity = "filter-b"
	variants["filter"] = filter
	limit := base
	limit.probeLimit++
	variants["limit"] = limit
	scalarSchema := base
	scalarSchema.scalarSchema = "scalar-b"
	variants["scalar_schema"] = scalarSchema
	vectorSchema := base
	vectorSchema.vectorSchema = "vector-b"
	variants["vector_schema"] = vectorSchema
	vectorIndex := base
	vectorIndex.vectorIndex = &VectorIndex{}
	variants["vector_index"] = vectorIndex
	generation := base
	generation.sourceGeneration++
	generation.vectorGeneration++
	variants["generation"] = generation

	for name, variant := range variants {
		t.Run(name, func(t *testing.T) {
			col := &Collection{}
			plan := &nativeScalarFilterExecution{identity: NativeScalarFilterPlanCompleteExact, sourceGeneration: base.sourceGeneration}
			if _, stats := col.nativeScalarPlanCachePut(base, plan, nativeScalarPlanCacheStats{misses: 1}); stats.entries != 1 {
				t.Fatalf("put entries=%d", stats.entries)
			}
			if got, stats := col.nativeScalarPlanCacheGet(variant); got != nil || stats.hits != 0 || stats.misses != 1 {
				t.Fatalf("aliased plan=%p stats=%+v", got, stats)
			}
		})
	}
}

func TestNativeScalarPlanCacheBoundsEntriesAndRetainedBytes(t *testing.T) {
	col := &Collection{}
	index := &VectorIndex{}
	var stats nativeScalarPlanCacheStats
	for i := range nativeScalarPlanCacheMaxEntries + 8 {
		key := nativeScalarPlanCacheKey{
			vectorIndex: index, sourceGeneration: 1, vectorGeneration: 1,
			vectorSchema: "vector", scalarSchema: "scalar", filterIdentity: fmt.Sprintf("filter-%d", i),
			probeLimit: nativeScalarProbeLimit, exactSafetyCap: nativeScalarExactSafetyCap,
			annSeedProbeLimit: nativeScalarANNSeedProbeLimit, annSeedLimit: nativeScalarANNSeedLimit,
		}
		_, stats = col.nativeScalarPlanCachePut(key, &nativeScalarFilterExecution{identity: NativeScalarFilterPlanCompleteExact, sourceGeneration: 1}, nativeScalarPlanCacheStats{misses: 1})
	}
	if stats.entries > nativeScalarPlanCacheMaxEntries || stats.retainedBytes > nativeScalarPlanCacheMaxBytes || stats.evictions == 0 {
		t.Fatalf("bounded stats=%+v max_entries=%d max_bytes=%d", stats, nativeScalarPlanCacheMaxEntries, nativeScalarPlanCacheMaxBytes)
	}

	oversized := &Collection{}
	hugeID := strings.Repeat("x", nativeScalarPlanCacheMaxBytes)
	key := nativeScalarPlanCacheKey{vectorIndex: index, sourceGeneration: 1, vectorGeneration: 1, vectorSchema: "vector", scalarSchema: "scalar", filterIdentity: "oversized"}
	plan := &nativeScalarFilterExecution{identity: NativeScalarFilterPlanCompleteFinite, finiteIDs: hybridScalarAllowSet{hugeID: {}}, sourceGeneration: 1}
	_, stats = oversized.nativeScalarPlanCachePut(key, plan, nativeScalarPlanCacheStats{misses: 1})
	if stats.entries != 0 || stats.retainedBytes != 0 {
		t.Fatalf("oversized entry retained stats=%+v", stats)
	}
}
