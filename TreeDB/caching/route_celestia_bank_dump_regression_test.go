package caching_test

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

type routeDumpEntry struct {
	key    []byte
	valLen int
}

type routeDumpBatch struct {
	expectedCount int
	entries       []routeDumpEntry
}

func loadRouteDumpEntries(t *testing.T, fixture string) []routeDumpEntry {
	t.Helper()
	path := filepath.Join("testdata", fixture)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	out := make([]routeDumpEntry, 0, len(lines))
	for i, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		fields := strings.Fields(ln)
		if len(fields) != 2 {
			t.Fatalf("fixture %s line %d malformed: %q", path, i+1, ln)
		}
		k, err := hex.DecodeString(fields[0])
		if err != nil {
			t.Fatalf("fixture %s line %d decode key: %v", path, i+1, err)
		}
		n, err := strconv.Atoi(fields[1])
		if err != nil {
			t.Fatalf("fixture %s line %d decode val_len: %v", path, i+1, err)
		}
		out = append(out, routeDumpEntry{key: k, valLen: n})
	}
	if len(out) == 0 {
		t.Fatalf("fixture %s is empty", path)
	}
	return out
}

func loadRouteDumpBatches(t *testing.T, fixture string) []routeDumpBatch {
	t.Helper()
	path := filepath.Join("testdata", fixture)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	out := make([]routeDumpBatch, 0, 8)
	for i, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		fields := strings.Fields(ln)
		if len(fields) == 3 && fields[0] == "batch" {
			n, err := strconv.Atoi(fields[2])
			if err != nil {
				t.Fatalf("fixture %s line %d malformed count: %v", path, i+1, err)
			}
			out = append(out, routeDumpBatch{expectedCount: n})
			continue
		}
		if len(fields) == 2 {
			if len(out) == 0 {
				t.Fatalf("fixture %s line %d entry before batch header", path, i+1)
			}
			k, err := hex.DecodeString(fields[0])
			if err != nil {
				t.Fatalf("fixture %s line %d decode key: %v", path, i+1, err)
			}
			n, err := strconv.Atoi(fields[1])
			if err != nil {
				t.Fatalf("fixture %s line %d decode val_len: %v", path, i+1, err)
			}
			out[len(out)-1].entries = append(out[len(out)-1].entries, routeDumpEntry{key: k, valLen: n})
			continue
		}
		t.Fatalf("fixture %s line %d malformed: %q", path, i+1, ln)
	}
	if len(out) == 0 {
		t.Fatalf("fixture %s has no batches", path)
	}
	for i := range out {
		if len(out[i].entries) != out[i].expectedCount {
			t.Fatalf("fixture %s batch=%d entry mismatch got=%d want=%d", path, i+1, len(out[i].entries), out[i].expectedCount)
		}
	}
	return out
}

func deterministicRouteDumpValue(i, n int) []byte {
	if n <= 0 {
		return nil
	}
	v := bytes.Repeat([]byte{byte(i % 251)}, n)
	v[0] = byte((i + 17) % 251)
	return v
}

// Regression: replay the exact 7,471-key bank WriteSync key order captured from
// a failing Celestia snapshot restore. Route mode must keep root-key visibility
// immediately after WriteSync and preserve full read parity after reopen.
func TestRegression_RouteMode_CelestiaBankDumpWriteSyncParityAfterReopen(t *testing.T) {
	entries := loadRouteDumpEntries(t, "celestia_bank_writesync_7471.txt")
	if len(entries) != 7471 {
		t.Fatalf("fixture cardinality changed: got=%d want=7471", len(entries))
	}

	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte, len(entries))
	b := db.NewBatch()
	for i := range entries {
		value := deterministicRouteDumpValue(i, entries[i].valLen)
		if err := b.Set(entries[i].key, value); err != nil {
			_ = b.Close()
			t.Fatalf("set i=%d: %v", i, err)
		}
		expected[string(entries[i].key)] = append([]byte(nil), value...)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("writesync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	rootKey := mustDecodeHex(t, "732f6b3a62616e6b2f73000000000098d30c00000001")
	gotRoot, err := db.Get(rootKey)
	if err != nil {
		t.Fatalf("get root key after writesync: %v", err)
	}
	wantRoot := expected[string(rootKey)]
	if !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("root key missing/mismatch after writesync: got_len=%d want_len=%d", len(gotRoot), len(wantRoot))
	}

	assertRouteParityState(t, db, expected)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	gotRoot, err = reopened.Get(rootKey)
	if err != nil {
		t.Fatalf("get root key after reopen: %v", err)
	}
	if !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("root key missing/mismatch after reopen: got_len=%d want_len=%d", len(gotRoot), len(wantRoot))
	}

	assertRouteParityState(t, reopened, expected)

	// Defensive check: fixture preserves unsorted write order needed by route split
	// path; this should never become fully sorted by key.
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].key, entries[i].key) > 0 {
			return
		}
	}
	t.Fatalf("fixture unexpectedly sorted; expected unsorted key order")
}

// Regression: replay the exact acc/authz/bank three-batch WriteSync sequence
// from a failing Celestia restore and assert root visibility after each batch.
func TestRegression_RouteMode_CelestiaSnapshotThreeBatchReplay(t *testing.T) {
	batches := loadRouteDumpBatches(t, "celestia_snapshot_writesync_3batches.txt")
	if len(batches) != 3 {
		t.Fatalf("batch count changed: got=%d want=3", len(batches))
	}

	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	rootHex := []string{
		"732f6b3a6163632f73000000000098d30c00000001",
		"732f6b3a617574687a2f73000000000098d30c00000001",
		"732f6b3a62616e6b2f73000000000098d30c00000001",
	}
	expected := make(map[string][]byte, 9835+5308+7471)
	valueCounter := 0
	for bi := range batches {
		b := db.NewBatch()
		for i := range batches[bi].entries {
			e := batches[bi].entries[i]
			value := deterministicRouteDumpValue(valueCounter, e.valLen)
			valueCounter++
			if err := b.Set(e.key, value); err != nil {
				_ = b.Close()
				t.Fatalf("batch=%d set i=%d: %v", bi+1, i, err)
			}
			expected[string(e.key)] = append([]byte(nil), value...)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("batch=%d writesync: %v", bi+1, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("batch=%d close: %v", bi+1, err)
		}

		rootKey := mustDecodeHex(t, rootHex[bi])
		got, err := db.Get(rootKey)
		if err != nil {
			t.Fatalf("batch=%d get root: %v", bi+1, err)
		}
		want := expected[string(rootKey)]
		if !bytes.Equal(got, want) {
			t.Fatalf("batch=%d root key missing/mismatch after writesync: got_len=%d want_len=%d", bi+1, len(got), len(want))
		}
	}

	assertRouteParityState(t, db, expected)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	for i := range rootHex {
		rootKey := mustDecodeHex(t, rootHex[i])
		got, err := reopened.Get(rootKey)
		if err != nil {
			t.Fatalf("reopen get root[%d]: %v", i, err)
		}
		want := expected[string(rootKey)]
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen root[%d] mismatch got_len=%d want_len=%d", i, len(got), len(want))
		}
	}
	assertRouteParityState(t, reopened, expected)
}

func TestRouteDumpFixtureIntegrity(t *testing.T) {
	entries := loadRouteDumpEntries(t, "celestia_bank_writesync_7471.txt")
	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		if entries[i].valLen <= 0 {
			t.Fatalf("entry %d has non-positive val_len=%d", i, entries[i].valLen)
		}
		seen[string(entries[i].key)] = struct{}{}
	}
	if len(seen) != len(entries) {
		t.Fatalf("fixture has duplicate keys: total=%d unique=%d", len(entries), len(seen))
	}
	rootKey := "732f6b3a62616e6b2f73000000000098d30c00000001"
	if _, ok := seen[string(mustDecodeHex(t, rootKey))]; !ok {
		t.Fatalf("fixture missing expected root key %s", rootKey)
	}

	batches := loadRouteDumpBatches(t, "celestia_snapshot_writesync_3batches.txt")
	if len(batches) != 3 {
		t.Fatalf("snapshot fixture batch count mismatch got=%d want=3", len(batches))
	}
	if len(batches[0].entries) != 9835 || len(batches[1].entries) != 5308 || len(batches[2].entries) != 7471 {
		t.Fatalf("snapshot fixture cardinality mismatch got=[%d,%d,%d]", len(batches[0].entries), len(batches[1].entries), len(batches[2].entries))
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hex %q: %v", s, err)
	}
	if len(b) == 0 {
		t.Fatalf("decode hex %q returned empty", s)
	}
	return b
}
