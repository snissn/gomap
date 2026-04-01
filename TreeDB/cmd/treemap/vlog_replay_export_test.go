package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestExportValueLogReplay_PreservesSegmentOrder(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	walDir := filepath.Join(mainDir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile(index.db): %v", err)
	}

	writeSegment := func(name string, lane, seq uint32, ridStart uint64, payloads ...string) {
		t.Helper()
		fileID, err := valuelog.EncodeFileID(lane, seq)
		if err != nil {
			t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
		}
		path := filepath.Join(walDir, name)
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			t.Fatalf("NewWriter(%q): %v", name, err)
		}
		for i, payload := range payloads {
			if _, err := w.Append(0, nil, ridStart+uint64(i), []byte(payload)); err != nil {
				_ = w.Close()
				t.Fatalf("Append(%q): %v", payload, err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%q): %v", name, err)
		}
	}

	writeSegment("value-l0-000001.log", 0, 1, 11, "a-11", "a-12")
	writeSegment("value-l0-000002.log", 0, 2, 21, "b-21")

	outPath := filepath.Join(root, "replay.jsonl")
	report, err := exportValueLogReplay(root, valueLogReplayExportOptions{OutputPath: outPath})
	if err != nil {
		t.Fatalf("exportValueLogReplay: %v", err)
	}
	if report.MainDir != mainDir {
		t.Fatalf("MainDir=%q want %q", report.MainDir, mainDir)
	}
	if report.Records != 3 {
		t.Fatalf("Records=%d want 3", report.Records)
	}

	got := mustReadReplayJSONL(t, outPath)
	if len(got) != 3 {
		t.Fatalf("len(records)=%d want 3", len(got))
	}
	wantSeqs := []uint64{1, 2, 3}
	wantRIDs := []uint64{11, 12, 21}
	wantVals := []string{"a-11", "a-12", "b-21"}
	for i := range got {
		if got[i].Seq != wantSeqs[i] {
			t.Fatalf("record[%d].Seq=%d want %d", i, got[i].Seq, wantSeqs[i])
		}
		if got[i].RID != wantRIDs[i] {
			t.Fatalf("record[%d].RID=%d want %d", i, got[i].RID, wantRIDs[i])
		}
		if got[i].ValueLen != len(wantVals[i]) {
			t.Fatalf("record[%d].ValueLen=%d want %d", i, got[i].ValueLen, len(wantVals[i]))
		}
		if got[i].Encoding != "base64" {
			t.Fatalf("record[%d].Encoding=%q want base64", i, got[i].Encoding)
		}
		if got[i].File == "" || !strings.HasPrefix(got[i].File, "value-l0-") {
			t.Fatalf("record[%d].File=%q want lane file name", i, got[i].File)
		}
		key := mustDecodeReplayBase64(t, got[i].Key)
		if !strings.Contains(string(key), "vlog-replay/") {
			t.Fatalf("record[%d].Key decoded=%q want vlog-replay prefix", i, string(key))
		}
		val := mustDecodeReplayBase64(t, got[i].Val)
		if string(val) != wantVals[i] {
			t.Fatalf("record[%d].Val decoded=%q want %q", i, string(val), wantVals[i])
		}
	}
}

func TestExportValueLogReplay_DecodesDictFramesFromMainDBDir(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	outPath := filepath.Join(dir, "replay.jsonl")
	report, err := exportValueLogReplay(filepath.Join(dir, "maindb"), valueLogReplayExportOptions{OutputPath: outPath})
	if err != nil {
		t.Fatalf("exportValueLogReplay(maindb): %v", err)
	}
	if report.MainDir != filepath.Join(dir, "maindb") {
		t.Fatalf("MainDir=%q want %q", report.MainDir, filepath.Join(dir, "maindb"))
	}
	if report.Segments == 0 || report.Records == 0 || report.RawValueBytes == 0 {
		t.Fatalf("unexpected empty export report: %+v", report)
	}
	audit, err := collectValueLogAudit(dir, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{}, valueLogFrameScanAuditOptions{})
	if err != nil {
		t.Fatalf("collectValueLogAudit: %v", err)
	}
	if report.Records != audit.RIDScan.Records {
		t.Fatalf("Records=%d want audit RID count %d", report.Records, audit.RIDScan.Records)
	}
	records := mustReadReplayJSONL(t, outPath)
	if len(records) != int(report.Records) {
		t.Fatalf("len(records)=%d want %d", len(records), report.Records)
	}
	if records[0].Seq != 1 {
		t.Fatalf("first Seq=%d want 1", records[0].Seq)
	}
	for i, rec := range records {
		if rec.Encoding != "base64" {
			t.Fatalf("record[%d].Encoding=%q want base64", i, rec.Encoding)
		}
		if rec.ValueLen != 16<<10 {
			t.Fatalf("record[%d].ValueLen=%d want %d", i, rec.ValueLen, 16<<10)
		}
		val := mustDecodeReplayBase64(t, rec.Val)
		if len(val) != 16<<10 {
			t.Fatalf("record[%d] decoded len=%d want %d", i, len(val), 16<<10)
		}
	}
}

func mustReadReplayJSONL(t *testing.T, path string) []valueLogReplayExportRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	defer f.Close()

	var out []valueLogReplayExportRecord
	scanner := bufio.NewScanner(f)
	const maxScanLine = 32 << 20
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, maxScanLine)
	for scanner.Scan() {
		var rec valueLogReplayExportRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			t.Fatalf("json.Unmarshal(%q): %v", scanner.Text(), err)
		}
		out = append(out, rec)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Scanner(%q): %v", path, err)
	}
	return out
}

func mustDecodeReplayBase64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("DecodeString(%q): %v", s, err)
	}
	return b
}
