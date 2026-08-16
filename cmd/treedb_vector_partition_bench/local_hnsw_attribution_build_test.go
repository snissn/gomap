package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionBuildVariantV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := source.dir
	source.owned = false
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sourceDir)
	source, err = openM8ProductionExistingAssetSetV1(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	harness, evidence, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantNativeV1, 9989)
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	if evidence.Schema != localHNSWAttributionBuildSchemaV1 || evidence.Variant != "native" || evidence.VariantIdentity == "" || evidence.FileID != 9989 || evidence.Partitions != 4 || evidence.ElapsedNanos <= 0 || evidence.CloneLogicalBytes <= 0 || evidence.PackBytes == 0 || evidence.MappedBytes == 0 || evidence.CPUAvailable && evidence.CPUDeltaNanos < 0 {
		t.Fatalf("evidence=%+v", evidence)
	}
	construction, err := localHNSWAttributionConstructionReduceV1(harness.constructionEvidence)
	if err != nil || construction.FinalSurvivors == 0 || construction.InitialAdded == 0 || harness.constructionEvidence.Variant != "native" {
		t.Fatalf("construction=%+v evidence=%+v err=%v", construction, harness.constructionEvidence, err)
	}
	sampled := false
	for _, partition := range harness.constructionEvidence.Partitions {
		for _, selection := range partition.Selections {
			if selection.CandidateSampled {
				sampled = true
				if selection.Layer != 0 || selection.CandidateDigest == "" || len(selection.CandidateOrdinals) != selection.Candidates {
					t.Fatalf("sample=%+v", selection)
				}
			}
		}
	}
	if !sampled {
		t.Fatal("missing deterministic candidate sample")
	}
	m18, m18Evidence, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 9992)
	if err != nil {
		t.Fatal(err)
	}
	defer m18.Close()
	if m18Evidence.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) || m18Evidence.VariantIdentity == "" || len(m18.constructionEvidence.Partitions) != 4 {
		t.Fatalf("M18 construction evidence=%+v trace=%+v", m18Evidence, m18.constructionEvidence)
	}
	if m18.constructionEvidence.Partitions[0].TraceMode != "detailed" || m18.constructionEvidence.Partitions[2].TraceMode != "compact" || len(m18.constructionEvidence.Partitions[2].Events) != 0 || len(m18.constructionEvidence.Partitions[2].FinalOrigins) == 0 || m18.constructionEvidence.Partitions[2].PruneKeeps != m18.constructionEvidence.Partitions[2].CompactLifecycle.PruneKeep[0]+m18.constructionEvidence.Partitions[2].CompactLifecycle.PruneKeep[1]+m18.constructionEvidence.Partitions[2].CompactLifecycle.PruneKeep[2]+m18.constructionEvidence.Partitions[2].CompactLifecycle.PruneKeep[3]+m18.constructionEvidence.Partitions[2].CompactLifecycle.PruneKeep[4] {
		t.Fatalf("unexpected bounded M18 evidence: detailed=%+v compact=%+v", m18.constructionEvidence.Partitions[0], m18.constructionEvidence.Partitions[2])
	}
	m18Construction, err := localHNSWAttributionConstructionReduceV1(m18.constructionEvidence)
	if err != nil || m18Construction.FinalSurvivors == 0 {
		t.Fatalf("compact M18 construction reduction=%+v err=%v", m18Construction, err)
	}
	screen, err := materializeRetainedLocalHNSWVariantPartitionsV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MBackfillOnV1, 9996, []uint32{0, 1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer screen.Close()
	for _, partition := range screen.constructionEvidence.Partitions {
		if partition.TraceMode != "compact" || len(partition.Events) != 0 || len(partition.FinalOrigins) == 0 {
			t.Fatalf("#4172 screen arm retained detailed history: %+v", partition)
		}
	}
	for _, partition := range m18.constructionEvidence.Partitions {
		for _, event := range partition.Events {
			if event.Action == "reciprocal_prune_keep" {
				t.Fatalf("per-edge prune keep retained in %s evidence: %+v", partition.TraceMode, event)
			}
		}
	}
	// The bounded mode is strictly observational: untraced, full-detail, and
	// selected-detail/compact construction publish identical packs. Compare the
	// compact per-partition lifecycle counters with its full-detail oracle.
	inputs := make([]collections.VectorPartitionSearchAssetV1, source.manifest.PartitionCount)
	sourceID := collections.VectorPartitionSourceIdentityV1{Generation: source.manifest.SourceGeneration, Checksum: source.manifest.SourceChecksum, SchemaHash: source.manifest.SourceSchemaHash, RowCount: source.manifest.SourceRowCount}
	for p := range inputs {
		inputs[p] = collections.VectorPartitionSearchAssetV1{Source: sourceID, Generation: source.manifest.Generation, PartitionID: uint32(p), Dimensions: 3}
	}
	variant := collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1
	plainAssets, plainResources, err := source.collection.MaterializeVectorPartitionLocalSearchAssetsVariantV1(source.manifest.IndexName, source.manifest, 9993, inputs, variant)
	if err != nil {
		t.Fatal(err)
	}
	defer plainResources.Release()
	fullAssets, fullResources, fullEvidence, err := source.collection.MaterializeVectorPartitionLocalSearchAssetsWithConstructionEvidenceV1(source.manifest.IndexName, source.manifest, 9994, inputs, variant)
	if err != nil {
		t.Fatal(err)
	}
	defer fullResources.Release()
	boundedAssets, boundedResources, boundedEvidence, err := source.collection.MaterializeVectorPartitionLocalSearchAssetsWithBoundedConstructionEvidenceV1(source.manifest.IndexName, source.manifest, 9995, inputs, variant)
	if err != nil {
		t.Fatal(err)
	}
	defer boundedResources.Release()
	if err := source.collection.ValidateVectorPartitionLocalConstructionEvidenceV1(t.Context(), source.manifest.IndexName, source.manifest, boundedAssets, boundedEvidence); err != nil {
		t.Fatal(err)
	}
	for p := range plainAssets {
		if plainAssets[p].Checksum != fullAssets[p].Checksum || plainAssets[p].Checksum != boundedAssets[p].Checksum || plainAssets[p].Bytes != fullAssets[p].Bytes || plainAssets[p].Bytes != boundedAssets[p].Bytes || plainAssets[p].MembershipDigest != fullAssets[p].MembershipDigest || plainAssets[p].MembershipDigest != boundedAssets[p].MembershipDigest {
			t.Fatalf("trace changed M18 pack partition=%d plain=%+v full=%+v bounded=%+v", p, plainAssets[p], fullAssets[p], boundedAssets[p])
		}
		fullPart, boundedPart := fullEvidence, boundedEvidence
		fullPart.Partitions = []collections.VectorPartitionConstructionPartitionEvidenceV1{fullEvidence.Partitions[p]}
		boundedPart.Partitions = []collections.VectorPartitionConstructionPartitionEvidenceV1{boundedEvidence.Partitions[p]}
		fullTotals, fullErr := localHNSWAttributionConstructionReduceV1(fullPart)
		boundedTotals, boundedErr := localHNSWAttributionConstructionReduceV1(boundedPart)
		if fullErr != nil || boundedErr != nil || fullTotals.InitialAdded != boundedTotals.InitialAdded || fullTotals.ReciprocalAdded != boundedTotals.ReciprocalAdded || fullTotals.VariantRewriteAdded != boundedTotals.VariantRewriteAdded || fullTotals.VariantRewriteDropped != boundedTotals.VariantRewriteDropped || fullTotals.PruneKept != boundedTotals.PruneKept || fullTotals.PruneDropped != boundedTotals.PruneDropped || fullTotals.FinalSurvivors != boundedTotals.FinalSurvivors || fullTotals.InitialAddByOrigin != boundedTotals.InitialAddByOrigin || fullTotals.ReciprocalAddByOrigin != boundedTotals.ReciprocalAddByOrigin || fullTotals.VariantRewriteAddByOrigin != boundedTotals.VariantRewriteAddByOrigin || fullTotals.VariantRewriteDropByOrigin != boundedTotals.VariantRewriteDropByOrigin || fullTotals.PruneKeepByOrigin != boundedTotals.PruneKeepByOrigin || fullTotals.PruneDropByOrigin != boundedTotals.PruneDropByOrigin || fullTotals.FinalAgeByOrigin != boundedTotals.FinalAgeByOrigin || fullTotals.FinalDeltaByOrigin != boundedTotals.FinalDeltaByOrigin {
			t.Fatalf("bounded lifecycle differs from full detail partition=%d full=%+v bounded=%+v fullErr=%v boundedErr=%v", p, fullTotals, boundedTotals, fullErr, boundedErr)
		}
	}
	diagnostics, err := localHNSWAttributionPackDiagnosticsV1(harness.searchers)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := localHNSWAttributionNeighborhoodOracleV1Build(harness, diagnostics)
	if err != nil || oracle.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || oracle.CandidateSamples == 0 || oracle.CandidateTruthNeighbors == 0 || oracle.FinalSamples == 0 || len(oracle.PackDiagnostics) != 4 {
		t.Fatalf("oracle=%+v err=%v", oracle, err)
	}
	again, err := localHNSWAttributionNeighborhoodOracleV1Build(harness, diagnostics)
	if err != nil || !reflect.DeepEqual(oracle, again) {
		t.Fatalf("non-deterministic oracle: first=%+v again=%+v err=%v", oracle, again, err)
	}
	oracleVectors, err := localHNSWAttributionNeighborhoodVectorsV1(harness)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := localHNSWAttributionNeighborhoodOracleWithVectorsV1(harness, diagnostics, oracleVectors)
	if err != nil || !reflect.DeepEqual(oracle, shared) {
		t.Fatalf("shared-vector oracle differs: first=%+v shared=%+v err=%v", oracle, shared, err)
	}
	bad := harness.constructionEvidence.Partitions[0].Selections[0]
	harness.constructionEvidence.Partitions[0].Selections[0].CandidateSampled = true
	harness.constructionEvidence.Partitions[0].Selections[0].CandidateOrdinals = []int{len(harness.documentIDs[0])}
	if _, err := localHNSWAttributionNeighborhoodOracleV1Build(harness, diagnostics); err == nil {
		t.Fatal("invalid sampled candidate ordinal accepted")
	}
	harness.constructionEvidence.Partitions[0].Selections[0] = bad
	if len(harness.constructionEvidence.Partitions[0].Selections) > 1 {
		duplicate := harness.constructionEvidence.Partitions[0].Selections[1]
		harness.constructionEvidence.Partitions[0].Selections[1].CandidateSampled = true
		harness.constructionEvidence.Partitions[0].Selections[1].Node = harness.constructionEvidence.Partitions[0].Selections[0].Node
		if _, err := localHNSWAttributionNeighborhoodOracleV1Build(harness, diagnostics); err == nil {
			t.Fatal("duplicate sampled node accepted")
		}
		harness.constructionEvidence.Partitions[0].Selections[1] = duplicate
	}
	repaired, repairEvidence, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 9990)
	if err != nil {
		t.Fatal(err)
	}
	defer repaired.Close()
	if repairEvidence.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) || repairEvidence.VariantIdentity == "" || repairEvidence.FileID != 9990 || repairEvidence.Partitions != 4 || repairEvidence.PackBytes == 0 {
		t.Fatalf("repair evidence=%+v", repairEvidence)
	}
	if _, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantV1("wrong"), 9991); err == nil {
		t.Fatal("expected invalid variant rejection")
	}
}

func TestMaterializeRetainedLocalHNSWVariantSinglePartitionV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	variant := collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1
	h, err := materializeRetainedLocalHNSWVariantPartitionsV1(source, t.TempDir(), variant, 9996, []uint32{3})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if len(h.packAssets) != 1 || len(h.searchers) != 1 || len(h.constructionEvidence.Partitions) != 1 || h.packAssets[0].PartitionID != 3 || h.constructionEvidence.Partitions[0].PartitionID != 3 || h.constructionEvidence.Partitions[0].TraceMode != "detailed" || len(h.constructionEvidence.Partitions[0].Events) == 0 || len(h.finalOrigins) != 1 || len(h.finalOrigins[0]) == 0 {
		t.Fatalf("single retained M18 materialization=%+v evidence=%+v origins=%v", h.packAssets, h.constructionEvidence, h.finalOrigins)
	}
	wantOrigins, err := localHNSWAttributionFinalOriginsV1(h.constructionEvidence, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(h.finalOrigins[0], wantOrigins) {
		t.Fatalf("singleton final origins must use retained partition ID: got=%v want=%v", h.finalOrigins[0], wantOrigins)
	}
	if _, err := materializeRetainedLocalHNSWVariantPartitionsV1(source, t.TempDir(), variant, 9997, []uint32{4}); err == nil {
		t.Fatal("accepted out-of-range retained partition")
	}
}

func TestMaterializeRetainedLocalHNSWVariantAllPartitionsV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	const partitions = 40
	vectors := make([][]float64, partitions*2)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%5 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, partitions)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	h, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 9998)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if len(h.packAssets) != partitions || len(h.searchers) != partitions || len(h.constructionEvidence.Partitions) != partitions || len(h.finalOrigins) != partitions {
		t.Fatalf("full retained M18 materialization assets=%d searchers=%d evidence=%d origins=%d", len(h.packAssets), len(h.searchers), len(h.constructionEvidence.Partitions), len(h.finalOrigins))
	}
	for partition := range h.packAssets {
		if h.packAssets[partition].PartitionID != uint32(partition) || h.constructionEvidence.Partitions[partition].PartitionID != uint32(partition) || h.searchers[partition].Status().PartitionID != uint32(partition) || len(h.finalOrigins[partition]) == 0 {
			t.Fatalf("retained M18 partition mapping=%d asset=%d evidence=%d status=%d origins=%d", partition, h.packAssets[partition].PartitionID, h.constructionEvidence.Partitions[partition].PartitionID, h.searchers[partition].Status().PartitionID, len(h.finalOrigins[partition]))
		}
	}
}
