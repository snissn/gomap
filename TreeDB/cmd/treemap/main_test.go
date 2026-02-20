package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

type kv struct {
	key []byte
	val []byte
}

func TestResolveOuterLeafMode(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		required bool
		want     string
		wantErr  bool
	}{
		{name: "required_empty", in: "", required: true, wantErr: true},
		{name: "optional_empty", in: "", required: false, want: ""},
		{name: "v1", in: "v1", required: true, want: treedb.IndexOuterLeafModeV1},
		{name: "v2_blockptr_trim_case", in: " V2_BLOCKPTR ", required: true, want: treedb.IndexOuterLeafModeV2BlockPtr},
		{name: "v2_fenceptr", in: "v2_fenceptr", required: true, want: treedb.IndexOuterLeafModeV2FencePtr},
		{name: "invalid", in: "v3", required: true, wantErr: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveOuterLeafMode(tc.in, tc.required)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("mode=%q want %q", got, tc.want)
			}
		})
	}
}

func TestJSONLRoundTripEncodings(t *testing.T) {
	asciiEntries := []kv{
		{key: []byte("alpha"), val: []byte("one")},
		{key: []byte("beta"), val: []byte("two")},
		{key: []byte("gamma"), val: []byte("three")},
	}
	binaryEntries := []kv{
		{key: []byte{0x00, 0x01, 0x02, 0x03}, val: []byte{0xff, 0x00, 0x10, 0x11}},
		{key: []byte("bin\x00key"), val: []byte("line1\nline2")},
		{key: []byte{0x10, 0x11, 0x12, 0x13, 0x7f, 0x80, 0xff}, val: []byte{0x00, 0x01, 0x02, 0x03, 0x04}},
		{key: []byte("utf8-\xf0\x9f\x98\x80"), val: []byte("val-\xe2\x9c\x93")},
	}

	cases := []struct {
		name          string
		entries       []kv
		encoding      string
		omitEncoding  bool
		inputEncoding string
	}{
		{
			name:          "string_ascii",
			entries:       asciiEntries,
			encoding:      "string",
			omitEncoding:  true,
			inputEncoding: "auto",
		},
		{
			name:          "base64_binary",
			entries:       binaryEntries,
			encoding:      "base64",
			omitEncoding:  false,
			inputEncoding: "auto",
		},
		{
			name:          "hex_binary",
			entries:       binaryEntries,
			encoding:      "hex",
			omitEncoding:  false,
			inputEncoding: "auto",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dirA := t.TempDir()
			writeDB(t, dirA, tc.entries)

			jsonlPath := filepath.Join(t.TempDir(), "dump.jsonl")
			dumpJSONL(t, dirA, jsonlPath, tc.encoding, tc.omitEncoding)

			dirB := t.TempDir()
			importJSONLFile(t, dirB, jsonlPath, tc.inputEncoding)

			eq, err := compareDBs(dirA, dirB)
			if err != nil {
				t.Fatalf("compare error: %v", err)
			}
			if !eq {
				_, derr := compareDBsDetailed(dirA, dirB)
				if derr != nil {
					t.Fatalf("databases not equivalent for %s encoding: %v", tc.encoding, derr)
				}
				t.Fatalf("databases not equivalent for %s encoding", tc.encoding)
			}

			if tc.name == "base64_binary" {
				dirC := t.TempDir()
				truncated := filepath.Join(t.TempDir(), "dump_truncated.jsonl")
				truncateJSONL(t, jsonlPath, truncated)
				importJSONLFile(t, dirC, truncated, tc.inputEncoding)
				eq, err := compareDBs(dirA, dirC)
				if err != nil {
					t.Fatalf("compare error: %v", err)
				}
				if eq {
					t.Fatalf("expected truncated import to differ")
				}
			}
		})
	}
}

func TestImportJSONLInvalidBase64(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	path := filepath.Join(t.TempDir(), "bad.jsonl")
	if err := os.WriteFile(path, []byte("{\"key\":\"a\",\"val\":\"***\",\"encoding\":\"base64\"}\n"), 0o600); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	defer f.Close()

	if _, err := importJSONL(db, f, "auto", 0); err == nil {
		t.Fatalf("expected invalid base64 import to fail")
	}
}

func writeDB(t *testing.T, dir string, entries []kv) {
	t.Helper()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	for _, e := range entries {
		if err := db.Set(e.key, e.val); err != nil {
			_ = db.Close()
			t.Fatalf("set error: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}

func dumpJSONL(t *testing.T, dir, path, encoding string, omitEncoding bool) {
	t.Helper()
	db, err := treedb.Open(treedb.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	it, err := db.Iterator(nil, nil)
	if err != nil {
		_ = db.Close()
		t.Fatalf("iterator error: %v", err)
	}
	defer func() { _ = it.Close() }()

	f, err := os.Create(path)
	if err != nil {
		_ = db.Close()
		t.Fatalf("create jsonl: %v", err)
	}
	if _, err := scanJSONL(it, encoding, omitEncoding, 0, f); err != nil {
		_ = f.Close()
		_ = db.Close()
		t.Fatalf("scan jsonl: %v", err)
	}
	if err := f.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("close jsonl: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func importJSONLFile(t *testing.T, dir, path, inputEncoding string) {
	t.Helper()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		_ = db.Close()
		t.Fatalf("open jsonl: %v", err)
	}
	if _, err := importJSONL(db, f, inputEncoding, 128); err != nil {
		_ = f.Close()
		_ = db.Close()
		t.Fatalf("import jsonl: %v", err)
	}
	if err := f.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("close jsonl: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func compareDBs(dirA, dirB string) (bool, error) {
	return compareDBsInternal(dirA, dirB, false)
}

func compareDBsDetailed(dirA, dirB string) (bool, error) {
	return compareDBsInternal(dirA, dirB, true)
}

func compareDBsInternal(dirA, dirB string, detailed bool) (bool, error) {
	dbA, err := treedb.Open(treedb.Options{Dir: dirA, ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer func() { _ = dbA.Close() }()
	dbB, err := treedb.Open(treedb.Options{Dir: dirB, ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer func() { _ = dbB.Close() }()

	itA, err := dbA.Iterator(nil, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = itA.Close() }()

	entriesA := make(map[string][]byte)
	for ; itA.Valid(); itA.Next() {
		key := append([]byte(nil), itA.Key()...)
		val := append([]byte(nil), itA.Value()...)
		entriesA[string(key)] = val
	}
	if err := itA.Error(); err != nil {
		return false, err
	}

	itB, err := dbB.Iterator(nil, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = itB.Close() }()

	entriesB := make(map[string][]byte)
	for ; itB.Valid(); itB.Next() {
		key := append([]byte(nil), itB.Key()...)
		val := append([]byte(nil), itB.Value()...)
		entriesB[string(key)] = val
	}
	if err := itB.Error(); err != nil {
		return false, err
	}

	for k, vA := range entriesA {
		vB, ok := entriesB[k]
		if !ok {
			if detailed {
				return false, fmt.Errorf("missing key in B: %x", []byte(k))
			}
			return false, nil
		}
		if !bytes.Equal(vA, vB) {
			if detailed {
				return false, fmt.Errorf("value mismatch for key %x: %x != %x", []byte(k), vA, vB)
			}
			return false, nil
		}
	}
	for k := range entriesB {
		if _, ok := entriesA[k]; !ok {
			if detailed {
				return false, fmt.Errorf("extra key in B: %x", []byte(k))
			}
			return false, nil
		}
	}
	if len(entriesA) != len(entriesB) {
		if detailed {
			return false, fmt.Errorf("entry count mismatch after compare: %d != %d", len(entriesA), len(entriesB))
		}
		return false, nil
	}
	return true, nil
}

func truncateJSONL(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read jsonl: %v", err)
	}
	lines := bytes.Split(data, []byte("\n"))
	for len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if len(lines) <= 1 {
		t.Fatalf("not enough lines to truncate")
	}
	lines = lines[:len(lines)-1]
	out := bytes.Join(lines, []byte("\n"))
	out = append(out, '\n')
	if err := os.WriteFile(dst, out, 0o600); err != nil {
		t.Fatalf("write truncated jsonl: %v", err)
	}
}
