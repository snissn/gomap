package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplicationCellByOrdinalCoversMatrix(t *testing.T) {
	count := applicationCellCount()
	want := 0
	for _, embeddingCell := range applicationEmbeddings {
		want += len(applicationCellMatrix(embeddingCell))
	}
	if count != want || count == 0 {
		t.Fatalf("cell count=%d want %d", count, want)
	}
	first, ok := applicationCellByOrdinal(0)
	if !ok || first.Embedding != applicationEmbeddings[0] {
		t.Fatalf("first cell=%+v ok=%v", first, ok)
	}
	last, ok := applicationCellByOrdinal(count - 1)
	if !ok || last.Embedding != applicationEmbeddings[len(applicationEmbeddings)-1] {
		t.Fatalf("last cell=%+v ok=%v", last, ok)
	}
	if _, ok := applicationCellByOrdinal(-1); ok {
		t.Fatal("negative ordinal accepted")
	}
	if _, ok := applicationCellByOrdinal(count); ok {
		t.Fatal("past-end ordinal accepted")
	}
}

func TestApplicationCellWorkerReportsUnsupportedAndRangeError(t *testing.T) {
	unsupportedOrdinal := -1
	for ordinal := 0; ordinal < applicationCellCount(); ordinal++ {
		cell, _ := applicationCellByOrdinal(ordinal)
		if unsupportedCapability(cell) != nil {
			unsupportedOrdinal = ordinal
			break
		}
	}
	if unsupportedOrdinal < 0 {
		t.Fatal("matrix has no unsupported cell")
	}
	input := strings.NewReader(
		`{"ordinal":` + jsonInt(applicationCellCount()) + `}` + "\n" +
			`{"ordinal":` + jsonInt(unsupportedOrdinal) + `}` + "\n" +
			`{"ordinal":0}` + "\n" +
			`{"ordinal":0}` + "\n",
	)
	var output bytes.Buffer
	cfg := defaultApplicationConfig()
	cfg.FinalEvidence = false
	cfg.Repetitions = 1
	cfg.SamplesPerRep = 1
	cfg.IngestionReps = 1
	cfg.Dir = t.TempDir()
	if err := runApplicationCellWorker(cfg, input, &output); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	decoder := json.NewDecoder(&output)
	var ready applicationCellWorkerReady
	if err := decoder.Decode(&ready); err != nil || !ready.Ready || ready.CellCount != applicationCellCount() {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if ready.EnvironmentPolicy != applicationCellWorkerEnvironmentPolicy || ready.FixtureSHA256 == "" || ready.ConfigSHA256 == "" || ready.SemanticVectorSHA256 == "" {
		t.Fatalf("ready workload identity=%+v", ready)
	}
	var outOfRange applicationCellWorkerResponse
	if err := decoder.Decode(&outOfRange); err != nil {
		t.Fatalf("decode range response: %v", err)
	}
	if outOfRange.Error == "" || outOfRange.Row != nil {
		t.Fatalf("range response=%+v", outOfRange)
	}
	var unsupported applicationCellWorkerResponse
	if err := decoder.Decode(&unsupported); err != nil {
		t.Fatalf("decode unsupported after range error: %v", err)
	}
	if unsupported.Error != "" || unsupported.Row == nil || unsupported.Row.Status != "unsupported" {
		t.Fatalf("unsupported response=%+v", unsupported)
	}
	wantCell, _ := applicationCellByOrdinal(0)
	for attempt := 0; attempt < 2; attempt++ {
		var supported applicationCellWorkerResponse
		if err := decoder.Decode(&supported); err != nil {
			t.Fatalf("decode supported attempt %d: %v", attempt, err)
		}
		if supported.Error != "" || supported.Row == nil || supported.Row.Status != "supported" || supported.Row.Cell != wantCell {
			t.Fatalf("supported attempt %d response=%+v", attempt, supported)
		}
	}
	matches, err := filepath.Glob(filepath.Join(cfg.Dir, "*", "cell-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("fresh cell directories retained: %v", matches)
	}
}

func jsonInt(value int) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}
