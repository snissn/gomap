package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSweepInput(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.jsonl")
	data := strings.Join(lines, "\n")
	if !strings.HasSuffix(data, "\n") {
		data += "\n"
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	return path
}

func writeSweepCorpus(t *testing.T, payloads ...[]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create(%s): %v", path, err)
	}
	defer f.Close()
	for _, payload := range payloads {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			t.Fatalf("Write length: %v", err)
		}
		if _, err := f.Write(payload); err != nil {
			t.Fatalf("Write payload: %v", err)
		}
	}
	return path
}

func TestResolveInputEncoding_AutoDefaultsToStringWhenEncodingMissing(t *testing.T) {
	rec := kvRecord{Val: "Zm9v"}
	if got := resolveInputEncoding("auto", rec); got != "string" {
		t.Fatalf("resolveInputEncoding(auto)=%q want=string", got)
	}
}

func TestLoadValues_InvalidJSONReturnsError(t *testing.T) {
	path := writeSweepInput(t,
		`{"val":"ok","encoding":"string"}`,
		`not-json`,
	)
	_, _, err := loadValues(path, "auto", 2, 0, 0, "auto")
	if err == nil {
		t.Fatalf("expected invalid-json error")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("expected line number in error, got: %v", err)
	}
}

func TestLoadValues_AutoUsesExplicitRecordEncoding(t *testing.T) {
	path := writeSweepInput(t, `{"val":"Zm9v","encoding":"base64"}`)
	train, eval, err := loadValues(path, "auto", 1, 0, 0, "auto")
	if err != nil {
		t.Fatalf("loadValues: %v", err)
	}
	if len(eval) != 0 {
		t.Fatalf("unexpected eval values: %d", len(eval))
	}
	if len(train) != 1 {
		t.Fatalf("unexpected train values: %d", len(train))
	}
	if got := string(train[0]); got != "foo" {
		t.Fatalf("unexpected decoded payload: got=%q want=%q", got, "foo")
	}
}

func TestNormalizeInputFormat_AutoUsesBinForCorpusFiles(t *testing.T) {
	if got := normalizeInputFormat("/tmp/outer_leaf_pages.bin", "auto"); got != "bin32le" {
		t.Fatalf("normalizeInputFormat(bin, auto)=%q want=bin32le", got)
	}
	if got := normalizeInputFormat("/tmp/input.jsonl", "auto"); got != "jsonl" {
		t.Fatalf("normalizeInputFormat(jsonl, auto)=%q want=jsonl", got)
	}
}

func TestLoadValues_Bin32LELoadsTrainAndEval(t *testing.T) {
	path := writeSweepCorpus(t, []byte("aa"), []byte("bbb"), []byte("cccc"))
	train, eval, err := loadValues(path, "auto", 2, 1, 0, "auto")
	if err != nil {
		t.Fatalf("loadValues(bin): %v", err)
	}
	if len(train) != 2 || len(eval) != 1 {
		t.Fatalf("unexpected split: train=%d eval=%d", len(train), len(eval))
	}
	if got := string(train[0]); got != "aa" {
		t.Fatalf("train[0]=%q want=aa", got)
	}
	if got := string(train[1]); got != "bbb" {
		t.Fatalf("train[1]=%q want=bbb", got)
	}
	if got := string(eval[0]); got != "cccc" {
		t.Fatalf("eval[0]=%q want=cccc", got)
	}
}

func TestLoadValues_Bin32LETruncationIncludesRecordNumber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.bin")
	if err := os.WriteFile(path, []byte{0x04, 0x00, 0x00, 0x00, 'a', 'b'}, 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	_, _, err := loadValues(path, "bin32le", 1, 0, 0, "auto")
	if err == nil {
		t.Fatalf("expected truncation error")
	}
	if !strings.Contains(err.Error(), "record 1") {
		t.Fatalf("expected record number in error, got: %v", err)
	}
}
