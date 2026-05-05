package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCollectLeafLogAnalyze(t *testing.T) {
	dir := t.TempDir()
	mainDir := filepath.Join(dir, "maindb")
	leafDir := filepath.Join(mainDir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), []byte("test"), 0o644); err != nil {
		t.Fatalf("write index.db: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	w, err := valuelog.NewWriter(filepath.Join(leafDir, "value-l255-000001.log"), fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	records := []valuelog.Record{
		{RID: 1, Value: make([]byte, 100)},
		{RID: 2, Value: make([]byte, 50)},
	}
	if _, _, err := w.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records))); err != nil {
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	report, err := collectLeafLogAnalyze(dir, leafLogScanOptions{SkipGzip: true, TopFrames: 10, TopRecordLengths: 10})
	if err != nil {
		t.Fatalf("collectLeafLogAnalyze: %v", err)
	}
	if got, want := report.Totals.FileCount, 1; got != want {
		t.Fatalf("FileCount=%d want %d", got, want)
	}
	if got, want := report.Totals.Frames, int64(1); got != want {
		t.Fatalf("Frames=%d want %d", got, want)
	}
	if got, want := report.Totals.Records, int64(2); got != want {
		t.Fatalf("Records=%d want %d", got, want)
	}
	if got, want := report.Totals.EncodedPayloadBytes, int64(150); got != want {
		t.Fatalf("EncodedPayloadBytes=%d want %d", got, want)
	}
	if got, want := report.Totals.StoredPayloadBytes, int64(150); got != want {
		t.Fatalf("StoredPayloadBytes=%d want %d", got, want)
	}
	if got, want := report.Totals.LogicalLeafBytes, int64(2*page.PageSize); got != want {
		t.Fatalf("LogicalLeafBytes=%d want %d", got, want)
	}
	if len(report.KHistogram) != 1 || report.KHistogram[0].K != 2 || report.KHistogram[0].Frames != 1 {
		t.Fatalf("unexpected K histogram: %+v", report.KHistogram)
	}
	if len(report.TopFrames) != 1 || report.TopFrames[0].K != 2 {
		t.Fatalf("unexpected top frames: %+v", report.TopFrames)
	}
}
