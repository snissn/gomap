package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Opt-in, one fresh process per arm. This is a public producer route-cost
// diagnostic, not a benchmark qualification or a post-Close budget assertion.
func TestPublicColumnDependencyDirectoryIncrementalCost(t *testing.T) {
	out := os.Getenv("TREEDB_PUBLIC_DIRECTORY_COST_OUT")
	if out == "" {
		t.Skip("task-local cost diagnostic; requires an output path")
	}
	arm := os.Getenv("TREEDB_PUBLIC_DIRECTORY_COST_ARM")
	if arm != "v1" && arm != "v2" {
		t.Fatal("arm must be v1 or v2")
	}
	profileDir := os.Getenv("TREEDB_PUBLIC_DIRECTORY_ACK_PROFILE_DIR")
	if profileDir != "" {
		if !filepath.IsAbs(profileDir) {
			t.Fatal("ACK profile directory must be absolute")
		}
		if runtime.MemProfileRate != 64<<10 {
			t.Fatalf("ACK profile requires -test.memprofilerate=65536; observed %d", runtime.MemProfileRate)
		}
		if err := os.MkdirAll(profileDir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// Both collections occur outside ACK's elapsed/MemStats boundary. The
	// runtime allocation profile may lag by two GC cycles. Profile-writing
	// allocations themselves remain identifiable harness stacks in the delta.
	writeACKProfile := func(boundary string) {
		t.Helper()
		if profileDir == "" {
			return
		}
		runtime.GC()
		runtime.GC()
		path := filepath.Join(profileDir, arm+"-ack-"+boundary+".pprof")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		writeErr := pprof.Lookup("allocs").WriteTo(f, 0)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("ACK allocation profile %s: write=%v close=%v", boundary, writeErr, closeErr)
		}
	}
	const batches = 32
	type phase struct {
		Name           string
		Operations     int
		Nanos          int64
		Bytes, Objects uint64
		Error          string `json:",omitempty"`
	}
	type asset struct {
		Ref       ColumnAssetRef
		Path      string
		FileBytes int64
	}
	type point struct {
		Name                                                   string
		GC                                                     bool
		HeapAlloc, HeapObjects, HeapInuse, TotalAlloc, Mallocs uint64
		NumGC                                                  uint32
	}
	result := struct {
		Arm, GoVersion, DBDir       string
		ACKProfileDir               string `json:",omitempty"`
		AllocationSampleRate        int
		ProfiledTimingNonComparable bool
		Batches, BatchSize          int
		Complete                    bool
		Phases                      []phase
		Retention                   []point
		Stats                       map[string]map[string]string
		Work                        []ColumnPublishCandidateResourceWork
		Assets                      map[string][]asset
		SamePhysicalAppend          bool
		Configs                     map[string]backenddb.FormatConfig
	}{Arm: arm, GoVersion: runtime.Version(), DBDir: t.TempDir(), Batches: batches, BatchSize: commandWALBenchBatchSize,
		ACKProfileDir: profileDir, AllocationSampleRate: runtime.MemProfileRate, ProfiledTimingNonComparable: profileDir != "",
		Stats: make(map[string]map[string]string), Assets: make(map[string][]asset), Configs: make(map[string]backenddb.FormatConfig)}
	save := func() {
		t.Helper()
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	defer save() // retain the latest outside-timer observations on assertion failure.
	measure := func(name string, ops int, f func() error) {
		t.Helper()
		if name == "insert_batch_ack" {
			writeACKProfile("before")
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		err := f()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		if name == "insert_batch_ack" {
			writeACKProfile("after")
		}
		p := phase{Name: name, Operations: ops, Nanos: elapsed.Nanoseconds(), Bytes: after.TotalAlloc - before.TotalAlloc, Objects: after.Mallocs - before.Mallocs}
		if err != nil {
			p.Error = err.Error()
		}
		result.Phases = append(result.Phases, p)
		save() // serialization and persistence are outside every measured phase.
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	retained := func(name string) {
		t.Helper()
		for _, gc := range []bool{false, true} {
			if gc {
				runtime.GC()
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			result.Retention = append(result.Retention, point{Name: name, GC: gc, HeapAlloc: m.HeapAlloc, HeapObjects: m.HeapObjects, HeapInuse: m.HeapInuse, TotalAlloc: m.TotalAlloc, Mallocs: m.Mallocs, NumGC: m.NumGC})
		}
		save()
	}
	features := []string{backenddb.RequiredFeatureCommandWALV1}
	if arm == "v2" {
		features = append(features, backenddb.RequiredFeatureDependencyDirectoryV2)
	}
	cfg := backenddb.FormatConfig{RequiredFeatures: features, DurabilityProfile: backenddb.ProfileCommandWALDurable}
	if err := backenddb.SaveFormatConfig(result.DBDir, cfg); err != nil {
		t.Fatal(err)
	}
	opts := backenddb.Options{Dir: result.DBDir, CommandWAL: true, Durability: backenddb.DurabilityDurable,
		ResolvedProfile: backenddb.ProfileCommandWALDurable, DisableBackgroundPrune: true}
	var d *backenddb.DB
	var mgr *CollectionManager
	var old *backenddb.Snapshot
	defer func() {
		if old != nil {
			if err := old.Close(); err != nil {
				t.Errorf("cleanup old: %v", err)
			}
		}
		if mgr != nil {
			if err := mgr.Close(); err != nil {
				t.Errorf("cleanup manager: %v", err)
			}
		}
		if d != nil {
			if err := d.Close(); err != nil {
				t.Errorf("cleanup DB: %v", err)
			}
		}
	}()
	var col *Collection
	measure("setup_open_create", 1, func() (err error) {
		d, err = backenddb.Open(opts)
		if err != nil {
			return err
		}
		mgr = NewCollectionManager(d)
		_, err = mgr.CreateCollection(&CollectionMeta{Name: "bench", Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON, DisableIndexedWriteMemtables: true, ColumnStore: testColumnStoreConfig(nil)}})
		if err != nil {
			return err
		}
		col, err = mgr.OpenCollection("bench")
		return err
	})
	// Identical maintained benchmark documents: 128 rows per batch. The seed
	// establishes an actual original asset and old complete view, outside ACK.
	ids := make([][][]byte, batches+1)
	docs := make([][][]byte, batches+1)
	measure("setup_inputs", 1, func() error {
		for i := range ids {
			ids[i], docs[i], _, _ = columnStoreCommandWALBenchDocuments(i*commandWALBenchBatchSize, commandWALBenchBatchSize, 0)
		}
		return nil
	})
	measure("setup_seed_checkpoint", 1, func() error {
		if _, err := col.InsertBatch(ids[0], docs[0]); err != nil {
			return err
		}
		return d.Checkpoint()
	})
	observe := func(label string) {
		t.Helper()
		stats := d.Stats()
		result.Stats[label] = stats
		actualConfig, exists, err := backenddb.LoadFormatConfig(result.DBDir)
		if err != nil || !exists {
			t.Fatalf("format config: exists=%t err=%v", exists, err)
		}
		result.Configs[label] = actualConfig
		if actualConfig.RequiresDependencyDirectoryV2() != (arm == "v2") {
			t.Fatal("persisted V2 feature mismatch")
		}
		want := "1"
		if arm == "v2" {
			want = "2"
		}
		if stats["treedb.durable_root.format_version"] != want || !d.CommandWALEnabled() || stats["treedb.profile.resolved"] != string(backenddb.ProfileCommandWALDurable) {
			t.Fatalf("wrong publication contract: %v", stats)
		}
		if arm == "v2" && (stats["treedb.durable_root.directory.root_page"] == "0" || stats["treedb.durable_root.directory.logical_count"] == "0" || stats["treedb.durable_root.directory.physical_count"] == "0") {
			t.Fatalf("V2 did not publish nonempty directory: %v", stats)
		}
		token, ok := d.StateToken()
		// Bit 62 is the reserved historical PRIMARY bank namespace. DATA roots
		// must remain ordinary page IDs after representation retirement.
		if !ok || token.RootPageID&(uint64(1)<<62) != 0 {
			t.Fatalf("not DATA: %+v ok=%t", token, ok)
		}
		refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
		if len(refs) == 0 {
			t.Fatal("no physical assets")
		}
		for _, ref := range refs {
			if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
				t.Fatalf("physical asset %+v: %v", ref, err)
			}
			path, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(result.DBDir), ref)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			result.Assets[label] = append(result.Assets[label], asset{Ref: ref, Path: path, FileBytes: info.Size()})
		}
		save()
	}
	observe("seed")
	firstFiles := make(map[string]os.FileInfo)
	firstRefs := make(map[ColumnAssetRef]bool)
	for _, a := range result.Assets["seed"] {
		info, err := os.Stat(a.Path)
		if err != nil {
			t.Fatal(err)
		}
		firstFiles[a.Path] = info
		firstRefs[a.Ref] = true
	}
	first, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatal(err)
	}
	old = d.AcquireSnapshot()
	if old == nil {
		t.Fatal("nil original Snapshot")
	}
	oldCatalog, err := col.catalogForSnapshot(old)
	if err != nil {
		t.Fatal(err)
	}
	retained("seed_old_view_held")
	// No stats, manifest walk, explicit drain, or output persistence inside ACK.
	result.Work = make([]ColumnPublishCandidateResourceWork, batches)
	measure("insert_batch_ack", batches, func() error {
		for i := 1; i <= batches; i++ {
			got, err := col.InsertBatch(ids[i], docs[i])
			if err != nil {
				return err
			}
			if len(got) != len(ids[i]) {
				return fmt.Errorf("batch %d returned %d IDs", i, len(got))
			}
			for j := range got {
				if !bytes.Equal(got[j], ids[i][j]) {
					return fmt.Errorf("batch %d ID %d changed", i, j)
				}
			}
			result.Work[i-1] = col.LastInsertStats().ColumnPublishFinalizeCandidateResourceWork
		}
		return nil
	})
	retained("ack_old_view_held")
	observe("ack")
	for i, w := range result.Work {
		if w.AppendOnlyFastPath == 0 || w.NewlyAdmittedObligations == 0 {
			t.Fatalf("batch %d missed append path: %+v", i, w)
		}
	}
	for _, a := range result.Assets["ack"] {
		if !firstRefs[a.Ref] {
			if before := firstFiles[a.Path]; before != nil {
				after, err := os.Stat(a.Path)
				if err != nil {
					t.Fatal(err)
				}
				if os.SameFile(before, after) && after.Size() > before.Size() {
					result.SamePhysicalAppend = true
				}
			}
		}
	}
	if !result.SamePhysicalAppend {
		t.Fatal("no new part appended to original physical asset")
	}
	verifyCurrent := func() {
		t.Helper()
		for i := range ids {
			for j := range ids[i] {
				got, err := col.Get(ids[i][j])
				if err != nil {
					t.Fatalf("current batch %d row %d id=%q Get: %v want=%q", i, j, ids[i][j], err, docs[i][j])
				}
				assertJSONEqualM13C(t, got, docs[i][j])
			}
		}
		assertColumnManifestStateM10B(t, col, batches+1, d.State().AppliedCommandLSN, mgr)
	}
	verifyOld := func() {
		t.Helper()
		for i := range ids {
			for j := range ids[i] {
				got, found, err := collectionGetAppendAtCatalogRoot(old, oldCatalog, collectionPrimaryRootName("bench"), ids[i][j], nil)
				if err != nil || found != (i == 0) {
					t.Fatalf("old batch %d row %d id=%q found=%t err=%v retained=%q expected_full=%q", i, j, ids[i][j], found, err, got, docs[i][j])
				}
				if found {
					// Raw primary bytes are the retained payload, not the full
					// document. Reuse the maintained original-Snapshot reader;
					// it reconstructs declared fields from that SAME old catalog.
					full, fullFound := typedColumnDocumentAtSnapshot1755(t, col, old, ids[i][j])
					if !fullFound {
						t.Fatalf("old seed id=%q reconstruction missing; retained=%q expected_full=%q", ids[i][j], got, docs[i][j])
					}
					assertJSONEqualM13C(t, full, docs[i][j])
				}
			}
		}
		records, err := loadColumnManifestRecordsFromRoot(old, oldCatalog.rootID(collectionColumnManifestRootName("bench")))
		if err != nil || !equalColumnManifestRecords(records, first.records) {
			t.Fatalf("old manifest changed: %v", err)
		}
	}
	verifyOld()
	verifyCurrent()
	measure("checkpoint_old_view_held", 1, d.Checkpoint)
	observe("checkpoint_held")
	verifyOld()
	verifyCurrent()
	retained("checkpoint_old_view_held")
	oldCatalog = nil
	measure("old_view_close", 1, func() error { err := old.Close(); old = nil; return err })
	measure("checkpoint_old_view_released", 1, d.Checkpoint)
	observe("checkpoint_released")
	retained("checkpoint_old_view_released")
	col = nil
	measure("manager_close", 1, func() error { err := mgr.Close(); mgr = nil; return err })
	measure("db_close", 1, func() error { err := d.Close(); d = nil; return err })
	retained("closed")
	measure("reopen", 1, func() (err error) {
		d, err = backenddb.Open(opts)
		if err != nil {
			return err
		}
		mgr = NewCollectionManager(d)
		col, err = mgr.OpenCollection("bench")
		return err
	})
	observe("reopened")
	verifyCurrent()
	col = nil
	measure("reopened_manager_close", 1, func() error { err := mgr.Close(); mgr = nil; return err })
	measure("reopened_db_close", 1, func() error { err := d.Close(); d = nil; return err })
	runtime.KeepAlive(ids)
	runtime.KeepAlive(docs)
	result.Complete = true
	save()
}
