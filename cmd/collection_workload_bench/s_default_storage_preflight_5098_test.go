//go:build !windows

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/page"
)

// Unix-only physical census uses syscall.Stat_t device, inode and allocated blocks.
// Test-only encoding/storage preflight. It selects no discovery N, rollover
// qualification, performance claim or finite admission. The original fixture
// and latency harness are unchanged. ROOT supplies any continuous resource
// sampler; these cuts additionally refuse an observed 6 GiB all-file excess.
func TestR1SDefaultStoragePreflight5098(t *testing.T) {
	out := os.Getenv("GOMAP_R1_S_PREFLIGHT_OUT")
	if out == "" {
		t.Skip("opt-in short public storage preflight")
	}
	for _, key := range []string{"TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_WARM_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_COLD_SEGMENT_TARGET_BYTES"} {
		if os.Getenv(key) != "" {
			t.Fatalf("preflight refuses threshold override %s", key)
		}
	}
	const originalHash = "fd5d377dceaa2e53dd37bb3c2b56504f202e411988e17843ed3be1f257ee205c"
	if r1FixtureHash(r1Fixture(4096)) != originalHash {
		t.Fatal("original fixture bytes changed")
	}
	for _, arm := range []string{"original", "exposure"} {
		t.Run(arm, func(t *testing.T) { r1SStoragePreflight5098(t, out, arm, originalHash) })
	}
}

func r1SStoragePreflight5098(t *testing.T, out, arm, originalHash string) {
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(out, arm+"-stages.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var tree *r1Tree
	var held r1Reader
	// Install custody before any DB constructor. All cleanup outcomes, including
	// the stage log's final Close, precede the success-only directory removal.
	defer func() {
		if held != nil {
			if err := held.close(); err != nil {
				t.Error(err)
			}
			held = nil
		}
		if tree != nil && tree.cleanup != nil {
			if err := tree.cleanup(); err != nil {
				t.Error(err)
			} else {
				tree.cleanup = nil
			}
		}
		if err := log.Close(); err != nil {
			t.Error(err)
		}
		if tree != nil && !t.Failed() {
			if err := os.RemoveAll(tree.dir); err != nil {
				t.Error(err)
			}
		}
	}()
	emit := func(stage string, data any) {
		if err := json.NewEncoder(log).Encode(map[string]any{"stage": stage, "arm": arm, "at": time.Now().UTC().Format(time.RFC3339Nano), "data": data, "scope": "fixed short diagnostic; no N, rollover, latency or finite qualification"}); err != nil {
			t.Fatal(err)
		}
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := os.MkdirTemp("", "gomap-r1-tree-")
	if err != nil {
		t.Fatal(err)
	}
	tree = &r1Tree{dir: dir, engine: "typed-row"}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, tree.dir)
	var cacheStats func() map[string]string
	open := func(create bool) {
		var err error
		tree.db, tree.cleanup, cacheStats, err = treedb.OpenBackendWithCachedLeafLogStats(opts)
		if err != nil {
			t.Fatal(err)
		}
		tree.manager = collections.NewCollectionManager(tree.db)
		if create {
			// Exact durable typed-row schema from openR1Tree; setup deliberately
			// uses no helper that deletes the directory on constructor failure.
			meta := collections.CollectionMeta{Name: "r1", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}, Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}, {Name: "city", Field: "city", ValueType: collections.IndexValueString}}}
			meta.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON}
			for _, name := range []string{"email", "city", "name", "bio"} {
				meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, collections.ColumnStoreColumn{Name: name, Path: name, ValueType: collections.ColumnStoreValueString})
			}
			if _, err := tree.manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
		}
		tree.col, err = tree.manager.OpenCollection("r1")
		if err != nil {
			t.Fatal(err)
		}
	}
	open(true)
	stats := cacheStats()
	if stats["treedb.cache.vlog_generation.leaf.segment_target_bytes"] != "33554432" || stats["treedb.cache.vlog_generation.hot.segment_target_bytes"] != "268435456" || stats["treedb.cache.physical_vlog_writers.configured"] == "" {
		t.Fatal("default physical producer contract missing or changed")
	}
	rows := r1Fixture(4096)
	if arm == "exposure" {
		rows = r1SExposureFixture5098()
	}
	emit("producer", map[string]any{"db_dir": tree.dir, "profile": tree.db.ResolvedProfile(), "durability": tree.db.DurabilityMode(), "original_fixture_sha256": originalHash, "fixture_sha256": r1FixtureHash(rows), "cached_stats": stats, "population": 4096, "hot": 64, "batch": 32, "calls": 256, "cuts": []int{64, 128, 192, 256}, "phase_order": []string{"hot", "broad"}})
	known := make(map[string]bool)
	var loadedRetainedBytes, loadedJSONBytes int64
	for start := 0; start < len(rows); start += 32 {
		for _, row := range rows[start : start+32] {
			known[row.Email] = true
		}
		_, retained, _, err := r1TypedInput(rows[start : start+32])
		if err != nil {
			t.Fatal(err)
		}
		for j, value := range retained {
			loadedRetainedBytes += int64(len(value))
			raw, err := json.Marshal(rows[start+j])
			if err != nil {
				t.Fatal(err)
			}
			loadedJSONBytes += int64(len(raw))
		}
		if err := tree.insert(rows[start : start+32]); err != nil {
			t.Fatal(err)
		}
	}
	emit("loaded_input", map[string]any{"fixture_sha256": r1FixtureHash(rows), "rows": len(rows), "retained_json_bytes": loadedRetainedBytes, "full_json_bytes": loadedJSONBytes})
	if err := tree.transition("checkpointed"); err != nil {
		t.Fatal(err)
	}
	held, err = tree.openReader()
	if err != nil {
		t.Fatal(err)
	}
	old := append([]r1Document(nil), rows...)
	verify := func(reader r1Reader, want []r1Document) error {
		for start := 0; start < len(want); start += 32 {
			if err := r1VerifyRows(reader, want[start:start+32]); err != nil {
				return err
			}
		}
		view := reader.(*r1TreeReader).view
		emails, cities := make(map[string][]string), make(map[string][]string)
		for _, row := range want {
			emails[row.Email] = append(emails[row.Email], row.ID)
			cities[row.City] = append(cities[row.City], row.ID)
		}
		check := func(index, value string, expected []string) error {
			var actual []string
			if err := view.VisitIndexValueIDs(index, value, func(id []byte) error { actual = append(actual, string(id)); return nil }); err != nil {
				return err
			}
			sort.Strings(actual)
			sort.Strings(expected)
			if len(actual) != len(expected) {
				return fmt.Errorf("%s=%q posting count mismatch", index, value)
			}
			for i := range actual {
				if actual[i] != expected[i] {
					return fmt.Errorf("%s=%q posting mismatch", index, value)
				}
			}
			return nil
		}
		for value := range known {
			if err := check("email", value, emails[value]); err != nil {
				return err
			}
		}
		for city := 0; city < 8; city++ {
			value := fmt.Sprintf("city-%02d", city)
			if err := check("city", value, cities[value]); err != nil {
				return err
			}
		}
		return nil
	}
	oracle := func(stage string) {
		current, err := tree.openReader()
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(verify(current, rows), current.close()); err != nil {
			t.Fatal(err)
		}
		if held != nil {
			if err := verify(held, old); err != nil {
				t.Fatal(err)
			}
		}
		emit(stage, map[string]any{"complete_current": true, "complete_held": held != nil, "whole_email_history_and_city_postings": true, "known_email_values": len(known)})
	}
	census := func(stage string, captureRoots bool) {
		files := make(map[string]any)
		var apparent, allocated, wal int64
		if err := filepath.WalkDir(tree.dir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("unexpected nonregular fixture path %s", path)
			}
			rel, err := filepath.Rel(tree.dir, path)
			if err != nil {
				return err
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("physical stat unavailable: %s", rel)
			}
			isWAL := strings.Contains(path, string(filepath.Separator)+"wal"+string(filepath.Separator)) || strings.HasSuffix(path, "-wal")
			files[rel] = map[string]any{"apparent_bytes": info.Size(), "allocated_bytes": st.Blocks * 512, "device": st.Dev, "inode": st.Ino, "wal": isWAL}
			apparent += info.Size()
			allocated += st.Blocks * 512
			if isWAL {
				wal += info.Size()
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		data := map[string]any{"db_dir": tree.dir, "all_files": files, "all_file_apparent_bytes": apparent, "all_file_allocated_bytes": allocated, "wal_apparent_bytes": wal, "backend_stats": tree.db.Stats(), "cached_stats": cacheStats(), "limit_bytes": int64(6 << 30)}
		// This fresh public DB uses the root layout with its index in maindb.
		// Raw slot images are diagnostic reads, not a quiescent recovery proof.
		index, err := os.Open(filepath.Join(tree.dir, "maindb", "index.db"))
		if err != nil {
			t.Fatal(err)
		}
		meta := make([]byte, 2*page.PageSize)
		_, readErr := io.ReadFull(index, meta)
		if err := errors.Join(readErr, index.Close()); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(meta)
		data["two_meta_pages_relative_path"] = filepath.Join("maindb", "index.db")
		data["two_meta_pages_hex"] = hex.EncodeToString(meta)
		data["two_meta_pages_sha256"] = hex.EncodeToString(sum[:])
		if captureRoots {
			roots, err := tree.db.CaptureRecoverableRootSetForInspection(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			data["recoverable_roots"] = roots.Roots()
			roots.Release()
		}
		emit(stage, data)
		if apparent > 6<<30 || allocated > 6<<30 {
			t.Fatal("fixture all-file 6 GiB cut guard exceeded; DB retained")
		}
	}
	oracle("loaded_oracle")
	census("loaded", true)
	var retainedTotal, jsonTotal int64
	for n := 0; n < 256; n++ {
		call := 2 * n
		phase := "hot"
		if n >= 128 {
			call = 2*(n-128) + 1
			phase = "broad"
		}
		indices, err := r1SExposureIndices5098(call)
		if err != nil {
			t.Fatal(err)
		}
		var update []r1Document
		if arm == "exposure" {
			indices, update, err = r1SExposureBatch5098(rows, call)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			update = make([]r1Document, 32)
			for j, index := range indices {
				update[j] = rows[index]
				update[j].Revision++
				update[j].Email = fmt.Sprintf("s5098-original-c%d-r%d@example.test", call%8, index)
				update[j].City = fmt.Sprintf("city-%02d", (index+call%8+1)%8)
				update[j].Bio = strings.Repeat(fmt.Sprintf("%02d", call%100), 48)
			}
		}
		ids, retained, _, err := r1TypedInput(update)
		if err != nil {
			t.Fatal(err)
		}
		inputIDs := make([]string, len(ids))
		var retainedBytes, jsonBytes int64
		for j, id := range ids {
			inputIDs[j] = string(id)
			retainedBytes += int64(len(retained[j]))
			raw, err := json.Marshal(update[j])
			if err != nil {
				t.Fatal(err)
			}
			jsonBytes += int64(len(raw))
		}
		ordered := append([]string(nil), inputIDs...)
		sort.Strings(ordered)
		if err := tree.replace(update); err != nil {
			t.Fatal(err)
		}
		for j, index := range indices {
			rows[index] = update[j]
			known[update[j].Email] = true
		}
		retainedTotal += retainedBytes
		jsonTotal += jsonBytes
		emit("input", map[string]any{"completed_calls": n + 1, "helper_call": call, "phase": phase, "indices": indices, "ids": inputIDs, "min_id": ordered[0], "max_id": ordered[len(ordered)-1], "retained_json_bytes": retainedBytes, "full_json_bytes": jsonBytes, "cumulative_retained_json_bytes": retainedTotal, "cumulative_full_json_bytes": jsonTotal})
		if (n+1)%64 == 0 {
			prefix := fmt.Sprintf("cut_%03d", n+1)
			oracle(prefix + "_before_flush_oracle")
			census(prefix+"_before_flush", false)
			if err := tree.manager.FlushAll(); err != nil {
				t.Fatal(err)
			}
			oracle(prefix + "_after_flush_oracle")
			census(prefix+"_after_flush", false)
			if err := tree.db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			oracle(prefix + "_after_checkpoint_oracle")
			census(prefix+"_after_checkpoint", true)
		}
	}
	closeErr := held.close()
	held = nil
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	oracle("drained_oracle")
	census("drained", true)
	if err := tree.cleanup(); err != nil {
		t.Fatal(err)
	}
	tree.cleanup = nil
	open(false)
	oracle("reopened_oracle")
	census("reopened", true)
	emit("preflight_complete", map[string]any{"calls": 256, "hot_calls": 128, "broad_calls": 128, "loaded_retained_json_bytes": loadedRetainedBytes, "loaded_full_json_bytes": loadedJSONBytes, "cumulative_retained_json_bytes": retainedTotal, "cumulative_full_json_bytes": jsonTotal, "qualification": false})
}
