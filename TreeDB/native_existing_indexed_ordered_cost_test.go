package treedb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type orderedCostMeasure struct {
	Nanos  uint64 `json:"ns"`
	Bytes  uint64 `json:"bytes"`
	Allocs uint64 `json:"allocs"`
}

func orderedCostStart() (time.Time, runtime.MemStats) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return time.Now(), m
}

func orderedCostEnd(start time.Time, before runtime.MemStats) orderedCostMeasure {
	ns := time.Since(start)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	return orderedCostMeasure{uint64(ns.Nanoseconds()), after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs}
}

// Opt-in, fixed-work public cost discriminator. No calibration or subtest matrix:
// the frozen runner launches exactly one fresh process for each selected cell.
// Native Commit() uses NoWALFast's ordinary volatile ACK; there is no native
// CommitRelaxed argument. This is not a Dgraph adapter benchmark.
func TestNativeExistingIndexedJSONOrderedRowsCost(t *testing.T) {
	rawN, order := os.Getenv("GOMAP_ORDERED_COST_ROWS"), os.Getenv("GOMAP_ORDERED_COST_ORDER")
	if rawN == "" && order == "" {
		t.Skip("opt-in four-cell public cost diagnostic; see artifacts/p3-ordered-public-cost01/prepared")
	}
	n, err := strconv.Atoi(rawN)
	if err != nil || (n != 256 && n != 2048) || (order != "ascending" && order != "descending") {
		t.Fatalf("unsupported cost cell rows=%q order=%q", rawN, order)
	}
	if runtime.GOMAXPROCS(0) != 2 {
		t.Fatal("cost protocol requires GOMAXPROCS=2")
	}
	runNativeIndexedOrderedRows(t, n, order)
}

func runNativeIndexedOrderedRows(t *testing.T, n int, order string) {
	t.Helper()
	// All IDs, documents, callbacks and batch headers are caller inputs, prepared
	// before lifecycle and foreground accounting, with the same bytes per order.
	ids, seed, final := make([][]byte, n), make([][]byte, n), make([][]byte, n)
	calls := make([]int, n)
	members := make([][]collections.UpdateBatchItem, 8)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("u%08d", i))
		seed[i] = []byte(fmt.Sprintf(`{"email":"u%08d@example.com","city":"hnl"}`, i))
		final[i] = []byte(fmt.Sprintf(`{"email":"u%08d@example.com","city":"sea"}`, i))
	}
	for member := range members {
		members[member] = make([]collections.UpdateBatchItem, n/8)
		for j := range members[member] {
			i := member*(n/8) + j
			if order == "descending" {
				i = n - 1 - i
			}
			members[member][j] = collections.UpdateBatchItem{DocumentID: ids[i], Update: func(current []byte) ([]byte, bool, error) {
				calls[i]++
				if calls[i] != 1 || !bytes.Equal(current, seed[i]) {
					return nil, false, fmt.Errorf("callback %d count=%d input=%s", i, calls[i], current)
				}
				return final[i], true, nil
			}}
		}
	}
	rawKey, rawValue := []byte("ordered-cost/raw"), []byte("committed")
	dir := t.TempDir()
	opts := indexedSliceOptions(ProfileNoWALFast, dir)
	if opts.IndexPrimaryDirectory || opts.CommandWAL || !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("unexpected DATA/NoWAL physical options: %+v", opts)
	}
	phases := make(map[string]orderedCostMeasure)
	stats := make(map[string]map[string]string)
	fullStart, fullMemory := orderedCostStart()
	start, memory := orderedCostStart()
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	var manager *collections.CollectionManager
	var held *collections.CollectionReadView
	var tx *collections.NativeJSONTxn
	t.Cleanup(func() {
		if tx != nil {
			if err := tx.Close(); err != nil {
				t.Error(err)
			}
		}
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "users", Indexes: []collections.IndexDefinition{
		{Name: "city", Field: "city", ValueType: collections.IndexValueString},
		{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
	}}); err != nil {
		t.Fatal(err)
	}
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	// Bounded ordinary setup commands/checkpoints are outside the measured
	// transaction. The held view below still captures all n seed documents.
	for first := 0; first < n; first += 256 {
		last := min(first+256, n)
		if got, err := users.InsertBatch(ids[first:last], seed[first:last]); err != nil || len(got) != last-first {
			t.Fatalf("seed %d:%d got%d: %v", first, last, len(got), err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	held, err = users.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	phases["setup_open_activate_create_seed_checkpoint_capture"] = orderedCostEnd(start, memory)
	stats["seed"] = orderedCostStats(t, database, handle, dir)
	logicalBefore := orderedCostUint(t, stats["seed"], "treedb.cache.cow.txn.logical_seq")
	nextBefore := handle.CommandWALNextLSN()
	start, memory = orderedCostStart()
	tx, err = native.Begin("users")
	if err != nil {
		t.Fatal(err)
	}
	phases["begin"] = orderedCostEnd(start, memory)
	start, memory = orderedCostStart()
	for i, items := range members {
		results, err := tx.UpdateBatch(items)
		if err != nil || len(results) != len(items) {
			t.Fatalf("stage%d results%d: %v", i, len(results), err)
		}
		for _, result := range results {
			if !result.Matched || !result.Modified {
				t.Fatalf("stage%d result%+v", i, result)
			}
		}
	}
	if err := tx.SetRaw(rawKey, rawValue); err != nil {
		t.Fatal(err)
	}
	phases["staging_eight_updates_and_raw"] = orderedCostEnd(start, memory)
	// Identically excluded from all chosen foreground segments.
	if got, err := tx.Get(ids[0]); err != nil || !bytes.Equal(got, final[0]) {
		t.Fatalf("staged=%s: %v", got, err)
	}
	start, memory = orderedCostStart()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	phases["commit"] = orderedCostEnd(start, memory)
	start, memory = orderedCostStart()
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	tx = nil
	phases["transaction_close"] = orderedCostEnd(start, memory)
	foreground := orderedCostMeasure{}
	for _, name := range []string{"begin", "staging_eight_updates_and_raw", "commit", "transaction_close"} {
		m := phases[name]
		foreground.Nanos += m.Nanos
		foreground.Bytes += m.Bytes
		foreground.Allocs += m.Allocs
	}
	phases["foreground_sum"] = foreground
	stats["foreground"] = orderedCostStats(t, database, handle, dir)
	if got := orderedCostUint(t, stats["foreground"], "treedb.cache.cow.txn.logical_seq"); got != logicalBefore+1 {
		t.Fatalf("logical=%d before=%d", got, logicalBefore)
	}
	if handle.CommandWALNextLSN() != nextBefore {
		t.Fatal("NoWAL transaction assigned a canonical LSN")
	}
	for i, count := range calls {
		if count != 1 {
			t.Fatalf("callback%d count%d", i, count)
		}
	}
	orderedCostCurrent(t, database, users, ids, final, rawKey, rawValue)
	orderedCostHeld(t, held, ids, seed)
	start, memory = orderedCostStart()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	phases["checkpoint_held"] = orderedCostEnd(start, memory)
	stats["checkpoint"] = orderedCostStats(t, database, handle, dir)
	orderedCostCurrent(t, database, users, ids, final, rawKey, rawValue)
	orderedCostHeld(t, held, ids, seed)
	start, memory = orderedCostStart()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	phases["release_held_manager_database_close"] = orderedCostEnd(start, memory)
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("final pins %+v", s)
	}
	firstFinal := cache.COWMemoryStats()
	start, memory = orderedCostStart()
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed = false
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	users, err = manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	phases["reopen_activate_open_collection"] = orderedCostEnd(start, memory)
	orderedCostCurrent(t, database, users, ids, final, rawKey, rawValue)
	stats["reopened"] = orderedCostStats(t, database, handle, dir)
	start, memory = orderedCostStart()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, registry = database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	phases["reopened_close"] = orderedCostEnd(start, memory)
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopened pins %+v", s)
	}
	phases["full_lifecycle_including_oracles_and_observations"] = orderedCostEnd(fullStart, fullMemory)
	report := map[string]any{"rows": n, "order": order, "profile": ProfileNoWALFast, "native_ack": "Commit() ordinary volatile", "members": 9, "callbacks": n, "fixtures": 1, "phases": phases, "stats": stats, "initial_final_cow": firstFinal, "reopened_final_cow": cache.COWMemoryStats(), "final_pins": registry.Stats(), "foreground_ns_per_row": float64(foreground.Nanos) / float64(n), "foreground_bytes_per_row": float64(foreground.Bytes) / float64(n), "foreground_allocs_per_row": float64(foreground.Allocs) / float64(n)}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ORDERED_COST %s", encoded)
}

func orderedCostUint(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		t.Fatalf("missing/invalid stat %s=%q: %v", key, stats[key], err)
	}
	return n
}

func orderedCostStats(t *testing.T, database *DB, handle *backenddb.DB, dir string) map[string]string {
	t.Helper()
	s := database.Stats()
	for key, want := range map[string]string{"treedb.profile.resolved": "no_wal_fast", "treedb.cache.memtable_mode": "cow_btree", "treedb.cache.redo_log.mode": "disabled_unsafe", "treedb.command_wal.enabled": "false"} {
		if s[key] != want {
			t.Fatalf("activation %s=%q want%q", key, s[key], want)
		}
	}
	for _, key := range []string{"treedb.command_wal.write.bytes_total", "treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total"} {
		if s[key] != "0" {
			t.Fatalf("NoWAL %s=%q", key, s[key])
		}
	}
	snapshot := handle.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("physical snapshot unavailable")
	}
	token, valid := snapshot.StateToken()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if !valid || token.RootPageID < 2 || token.RootPageID >= (uint64(1)<<62) {
		t.Fatalf("not DATA: %+v", token)
	}
	if _, err := os.Stat(filepath.Join(dir, "maindb", "index.db")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "maindb", "index.db.primary")); !os.IsNotExist(err) {
		t.Fatalf("unexpected PRIMARY file: %v", err)
	}
	s["ordered_cost.physical_root"] = strconv.FormatUint(token.RootPageID, 10)
	s["ordered_cost.physical_applied_lsn"] = strconv.FormatUint(token.AppliedCommandLSN, 10)
	out := make(map[string]string)
	for key, value := range s {
		if strings.HasPrefix(key, "treedb.cache.cow.") || strings.HasPrefix(key, "treedb.command_wal.") || strings.Contains(key, "checkpoint") || strings.HasPrefix(key, "ordered_cost.") || strings.HasPrefix(key, "treedb.profile.") || key == "treedb.cache.memtable_mode" || key == "treedb.cache.redo_log.mode" {
			out[key] = value
		}
	}
	return out
}

func orderedCostCurrent(t *testing.T, database *DB, users *collections.Collection, ids, documents [][]byte, rawKey, rawValue []byte) {
	t.Helper()
	for i, id := range ids {
		got, err := users.Get(id)
		if err != nil || !bytes.Equal(got, documents[i]) {
			t.Fatalf("current%d=%s: %v", i, got, err)
		}
		email := fmt.Sprintf("u%08d@example.com", i)
		owners, err := users.FindByIndexValue("email", email)
		if err != nil || len(owners) != 1 || !bytes.Equal(owners[0], id) {
			t.Fatalf("unique%d=%q: %v", i, owners, err)
		}
	}
	owners, err := users.FindByIndexValue("city", "sea")
	if err != nil || len(owners) != len(ids) {
		t.Fatalf("city sea=%d: %v", len(owners), err)
	}
	for i, id := range owners {
		if !bytes.Equal(id, ids[i]) {
			t.Fatalf("city owner%d=%s", i, id)
		}
	}
	if owners, err := users.FindByIndexValue("city", "hnl"); err != nil || len(owners) != 0 {
		t.Fatalf("old city=%d: %v", len(owners), err)
	}
	if got, err := database.Get(rawKey); err != nil || !bytes.Equal(got, rawValue) {
		t.Fatalf("raw=%s: %v", got, err)
	}
}

func orderedCostHeld(t *testing.T, held *collections.CollectionReadView, ids, documents [][]byte) {
	t.Helper()
	old, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
	if err != nil || len(old.Results) != len(ids) {
		t.Fatalf("held rows%d: %v", len(old.Results), err)
	}
	for i, result := range old.Results {
		if !result.Found || !bytes.Equal(result.Document, documents[i]) {
			t.Fatalf("held%d=%s", i, result.Document)
		}
	}
	count := 0
	if err := held.VisitIndexValueIDs("city", "hnl", func(id []byte) error {
		if count >= len(ids) || !bytes.Equal(id, ids[count]) {
			return fmt.Errorf("held city owner%d=%s", count, id)
		}
		count++
		return nil
	}); err != nil || count != len(ids) {
		t.Fatalf("held city count%d: %v", count, err)
	}
}
