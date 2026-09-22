package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

type kv struct {
	key []byte
	val []byte
}

func TestApplyCheckpointBenchProfilePreservesLeafPrefixCompressionOverride(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		fast                  bool
		leafPrefixCompression bool
	}{
		{name: "durable_off", fast: false, leafPrefixCompression: false},
		{name: "durable_on", fast: false, leafPrefixCompression: true},
		{name: "fast_off", fast: true, leafPrefixCompression: false},
		{name: "fast_on", fast: true, leafPrefixCompression: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts treedb.Options
			applyCheckpointBenchProfile(&opts, tc.fast, tc.leafPrefixCompression)
			if got := opts.LeafPrefixCompression; got != tc.leafPrefixCompression {
				t.Fatalf("LeafPrefixCompression=%t, want CLI override %t", got, tc.leafPrefixCompression)
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

func TestCompactCommandsExposeFullStoragePath(t *testing.T) {
	if !strings.Contains(usageText, "compact-plan    Preview full storage compaction debt") {
		t.Fatalf("usageText missing compact-plan full-storage wording: %q", usageText)
	}
	if !strings.Contains(usageText, "compact         Run full storage compaction") {
		t.Fatalf("usageText missing compact full-storage wording: %q", usageText)
	}
	if !strings.Contains(usageText, "vlog-gc         Advanced:") || !strings.Contains(usageText, "leafgen-gc      Advanced:") {
		t.Fatalf("usageText does not mark low-level maintenance advanced: %q", usageText)
	}

	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.SetSync([]byte("k"), bytes.Repeat([]byte("v"), 256)); err != nil {
		_ = db.Close()
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	planOut := captureStdout(t, func() {
		runCompactPlan(dir, nil)
	})
	if !strings.Contains(planOut, "compact-storage (plan):") ||
		!strings.Contains(planOut, "remaining-debt:") ||
		!strings.Contains(planOut, "index_vacuum_required=") ||
		!strings.Contains(planOut, "phase: name=index-vacuum status=") ||
		!strings.Contains(planOut, "storage-domain-before: name=value_vlog") ||
		!strings.Contains(planOut, "storage-domain: name=value_vlog") {
		t.Fatalf("compact-plan output missing full-storage report:\n%s", planOut)
	}
	planJSON := captureStdout(t, func() {
		runCompactPlan(dir, []string{"-json"})
	})
	var planStats treedb.CompactStorageStats
	if err := json.Unmarshal([]byte(planJSON), &planStats); err != nil {
		t.Fatalf("decode compact-plan json: %v\n%s", err, planJSON)
	}
	planPhase := compactStorageIndexVacuumPhase(planStats.Phases)
	if planPhase == nil || planPhase.Status == "" || planPhase.Reason == "" ||
		planPhase.Required != planStats.RemainingDebt.IndexVacuumRequired {
		t.Fatalf("compact-plan JSON index disposition does not match debt: phase=%+v debt=%+v", planPhase, planStats.RemainingDebt)
	}

	compactOut := captureStdout(t, func() {
		runCompact(dir, []string{"-rw"})
	})
	if !strings.Contains(compactOut, "compact-storage (applied):") ||
		!strings.Contains(compactOut, "remaining-debt:") ||
		!strings.Contains(compactOut, "index_vacuum_required=") ||
		!strings.Contains(compactOut, "phase: name=index-vacuum status=") ||
		!strings.Contains(compactOut, "storage-domain-before: name=value_vlog") ||
		!strings.Contains(compactOut, "storage-domain: name=value_vlog") {
		t.Fatalf("compact output missing full-storage report:\n%s", compactOut)
	}
}

func compactStorageIndexVacuumPhase(phases []treedb.CompactStoragePhaseStats) *treedb.CompactStoragePhaseStats {
	for i := range phases {
		if phases[i].Name == "index-vacuum" {
			return &phases[i]
		}
	}
	return nil
}

func TestCompactCommandReplaysCommandWALBeforeCompaction(t *testing.T) {
	dir := t.TempDir()
	runCompactPendingWALWriter(t, dir)

	_ = captureStdout(t, func() {
		runCompact(dir, []string{"-rw"})
	})

	opts := treedb.Options{Dir: dir, ReadOnly: true}
	applyPersistedFormatConfig(dir, &opts)
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open read-only after compact: %v", err)
	}
	defer func() { _ = db.Close() }()

	got, err := db.Get([]byte("pending-after-crash"))
	if err != nil {
		t.Fatalf("get replayed key: %v", err)
	}
	if want := bytes.Repeat([]byte("p"), 8192); !bytes.Equal(got, want) {
		t.Fatalf("replayed value mismatch: got=%dB want=%dB", len(got), len(want))
	}
}

func runCompactPendingWALWriter(t *testing.T, dir string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestHelperTreemapCompactPendingWALWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEMAP_COMPACT_WAL_HELPER=1",
		"TREEMAP_COMPACT_WAL_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compact pending WAL helper failed: %v\n%s", err, out)
	}
}

func TestHelperTreemapCompactPendingWALWriter(t *testing.T) {
	if os.Getenv("TREEMAP_COMPACT_WAL_HELPER") != "1" {
		t.Skip("helper")
	}
	dir := os.Getenv("TREEMAP_COMPACT_WAL_DIR")
	if dir == "" {
		t.Fatalf("missing TREEMAP_COMPACT_WAL_DIR")
	}

	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.FlushThreshold = 1 << 30
	opts.JournalLanes = 1
	opts.MaxWALBytes = -1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.SetSync([]byte("pending-after-crash"), bytes.Repeat([]byte("p"), 8192)); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	// Simulate a process crash with cached WAL present and no clean close/checkpoint.
	os.Exit(0)
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = old
	}()

	var buf bytes.Buffer
	readErr := make(chan error, 1)
	go func() {
		_, err := io.Copy(&buf, r)
		readErr <- err
	}()

	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("close stdout pipe writer: %v", err)
	}
	if err := <-readErr; err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close stdout pipe reader: %v", err)
	}
	return buf.String()
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
