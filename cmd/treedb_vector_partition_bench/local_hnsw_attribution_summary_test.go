package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionCalibrationSummaryV1Build(t *testing.T) {
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
	native, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantNativeV1, 9989)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	overlay, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 9990)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.Close()
	queries := [][]float32{{1, 1, 1}, {2, 1, 1}}
	truths := make([][]m8CanonicalResultV1, len(queries))
	for i, query := range queries {
		for _, searcher := range native.searchers {
			found, _, err := searcher.SearchExactWithOptionsV1(t.Context(), query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 128})
			if err != nil {
				t.Fatal(err)
			}
			for _, result := range found {
				truths[i] = append(truths[i], m8CanonicalResultV1{ID: result.ID, Score: result.Score})
			}
		}
		truths[i] = m8CanonicalResultsV1(truths[i], 10)
	}
	path := filepath.Join(t.TempDir(), "calibration.jsonl.gz")
	artifact, cases, summary, err := localHNSWAttributionCalibrationSummaryV1Build(t.Context(), path, source, native, overlay, []int{4, 9}, queries, truths)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Schema != localHNSWAttributionSidecarSchemaV1 || artifact.Records != 2 || artifact.Bytes == 0 || len(cases) != 2 || cases[0].Ordinal != 4 || cases[1].Ordinal != 9 || summary.Schema != localHNSWAttributionCalibrationSummarySchemaV1 || summary.QueryCount != 2 || summary.Native.LowCandidates == 0 || summary.Overlay.HighEdges == 0 || summary.Native.TerminationCounts == nil || summary.Overlay.TerminationCounts == nil {
		t.Fatalf("artifact=%+v cases=%+v summary=%+v", artifact, cases, summary)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bufio.NewReader(gz))
	for ordinal := 4; ordinal <= 9; ordinal += 5 {
		var record localHNSWAttributionQueryEvidenceV1
		if err := decoder.Decode(&record); err != nil || record.Schema != localHNSWAttributionQuerySchemaV1 || record.QueryOrdinal != ordinal {
			t.Fatalf("record=%+v err=%v", record, err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := localHNSWAttributionCalibrationSummaryV1Build(t.Context(), filepath.Join(t.TempDir(), "bad.gz"), source, native, overlay, []int{4, 4}, queries, truths); err == nil {
		t.Fatal("expected malformed alignment rejection")
	}
}
