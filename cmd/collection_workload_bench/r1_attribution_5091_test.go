//go:build linux

package main

// This opt-in characterization is not an acceptance benchmark. It reuses the
// landed public R1 fixture/backend and the existing production diagnostics.
import (
	"context"
	"encoding/json"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestR1Attribution5091(t *testing.T) {
	out := os.Getenv("GOMAP_R1_ATTR_OUT")
	if out == "" {
		t.Skip("opt-in nonqualifying characterization")
	}
	route := os.Getenv("GOMAP_R1_ATTR_ROUTE")
	switch route {
	case "point", "range", "prepared", "generic_update", "typed_replace", "concurrent_update", "concurrent_typed", "checkpoint":
	default:
		t.Fatal("unsupported characterization route")
	}
	engine := os.Getenv("GOMAP_R1_ATTR_ENGINE")
	if engine == "" {
		engine = "typed-row"
	}
	n, err := strconv.Atoi(os.Getenv("GOMAP_R1_ATTR_CALLS"))
	if err != nil || n < 1 || n > 20000 {
		t.Fatal("calls must be in [1,20000]")
	}
	workers := 1
	if route == "concurrent_update" || route == "concurrent_typed" {
		workers = 4
		if n%workers != 0 {
			t.Fatal("calls must divide workers")
		}
	}
	if (route == "generic_update" || route == "typed_replace" || route == "concurrent_update" || route == "concurrent_typed" || route == "checkpoint") && n > 64 {
		t.Fatal("short characterization mutation budget is64")
	}
	if err = os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	tree, err := openR1Tree(r1Config{Durability: "durable"}, engine)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e := tree.close(); e != nil {
			t.Error(e)
		}
	}()
	fixture := r1Fixture(4096)
	for i := 0; i < len(fixture); i += 32 {
		if err = tree.insert(fixture[i : i+32]); err != nil {
			t.Fatal(err)
		}
	}
	if err = tree.transition("flushed"); err != nil {
		t.Fatal(err)
	}
	detailed := os.Getenv("GOMAP_R1_ATTR_DETAIL") != "off"
	tree.manager.SetUpdateBatchDetailedStatsEnabled(detailed)
	var reader r1Reader
	if route == "prepared" {
		reader, err = tree.openReader()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.close()
		if _, _, err = reader.fetch(r1IDs(fixture)); err != nil {
			t.Fatal(err)
		}
	}
	rows := make([]r1Document, n)
	inputs := make([][]collections.UpdateBatchItem, n)
	ids := make([][][]byte, n)
	retained := make([][][]byte, n)
	columns := make([][]collections.TypedColumnBatch, n)
	for i := range n {
		d := fixture[i%len(fixture)]
		d.Email = fmt.Sprintf("attrib-%d@example.test", i)
		d.City = fmt.Sprintf("city-%02d", (i+1)%8)
		d.Revision++
		rows[i] = d
		raw, e := json.Marshal(d)
		if e != nil {
			t.Fatal(e)
		}
		inputs[i] = []collections.UpdateBatchItem{{DocumentID: []byte(d.ID), Update: func([]byte) ([]byte, bool, error) { return raw, true, nil }}}
		ids[i], retained[i], columns[i], err = r1TypedInput([]r1Document{d})
		if err != nil {
			t.Fatal(err)
		}
	}
	if route == "checkpoint" {
		for i := range n {
			if _, err = tree.col.UpdateBatch(inputs[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	stats := func() map[string]string {
		s := tree.db.Stats()
		for k, v := range tree.manager.Stats() {
			s[k] = v
		}
		return s
	}
	beforeStats := stats()
	profile := os.Getenv("GOMAP_R1_ATTR_PROFILE")
	oldRate := runtime.MemProfileRate
	if profile == "alloc" {
		runtime.MemProfileRate = 1
		defer func() { runtime.MemProfileRate = oldRate }()
	}
	heapProfile := func(name string) {
		runtime.GC()
		f, e := os.Create(filepath.Join(out, name))
		if e != nil {
			t.Fatal(e)
		}
		e = pprof.Lookup("allocs").WriteTo(f, 0)
		e2 := f.Close()
		if e != nil || e2 != nil {
			t.Fatal(e, e2)
		}
	}
	if profile == "alloc" {
		heapProfile("allocs-before.pprof")
	}
	var cpuFile *os.File
	if profile == "cpu" {
		cpuFile, err = os.Create(filepath.Join(out, "cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err = pprof.StartCPUProfile(cpuFile); err != nil {
			t.Fatal(err)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var rusageBefore, rusageAfter syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageBefore); err != nil {
		t.Fatal(err)
	}
	var samples []int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errorsFound []error
	run := func(worker int) {
		defer wg.Done()
		localSamples := make([]int64, 0, n/workers)
		var dst []byte
		for i := worker; i < n; i += workers {
			start := time.Now()
			var e error
			switch route {
			case "point":
				var found bool
				dst, found, e = tree.col.GetInto([]byte(fixture[(i*37)%len(fixture)].ID), dst)
				if e == nil && (!found || len(dst) == 0) {
					e = fmt.Errorf("missing point")
				}
			case "range":
				var documents [][]byte
				documents, e = tree.rangeDocuments(fmt.Sprintf("city-%02d", i%8), 10)
				if e == nil && len(documents) != 10 {
					e = fmt.Errorf("incomplete range")
				}
			case "prepared":
				var documents [][]byte
				documents, _, e = reader.fetch(r1IDs(fixture[(i*37)%4064 : (i*37)%4064+32]))
				if e == nil && len(documents) != 32 {
					e = fmt.Errorf("incomplete batch")
				}
			case "generic_update", "concurrent_update":
				var result []collections.UpdateBatchResult
				result, e = tree.col.UpdateBatch(inputs[i])
				if e == nil && (len(result) != 1 || !result[0].Modified) {
					e = fmt.Errorf("no mutation")
				}
			case "typed_replace", "concurrent_typed":
				var result []collections.UpdateBatchResult
				result, e = tree.col.ReplaceTypedBatch(ids[i], retained[i], columns[i])
				if e == nil && (len(result) != 1 || !result[0].Modified) {
					e = fmt.Errorf("no mutation")
				}
			case "checkpoint":
				e = tree.transition("checkpointed")
				i = n
			default:
				e = fmt.Errorf("unknown route")
			}
			localSamples = append(localSamples, time.Since(start).Nanoseconds())
			if e != nil {
				mu.Lock()
				errorsFound = append(errorsFound, e)
				mu.Unlock()
				break
			}
		}
		mu.Lock()
		samples = append(samples, localSamples...)
		mu.Unlock()
	}
	start := time.Now()
	wg.Add(workers)
	for w := range workers {
		go run(w)
	}
	wg.Wait()
	wall := time.Since(start)
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageAfter); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if profile == "cpu" {
		pprof.StopCPUProfile()
		if err = cpuFile.Close(); err != nil {
			t.Fatal(err)
		}
	}
	afterStats := stats()
	if profile == "alloc" {
		heapProfile("allocs-after.pprof")
	}
	if len(errorsFound) > 0 {
		t.Fatal(errorsFound)
	}
	if err = tree.manager.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if route == "generic_update" || route == "concurrent_update" || route == "concurrent_typed" || route == "typed_replace" || route == "checkpoint" {
		for i := range n {
			raw, e := tree.point([]byte(rows[i].ID))
			if e != nil {
				t.Fatal(e)
			}
			if e = r1VerifyDocument(raw, rows[i]); e != nil {
				t.Fatal(e)
			}
		}
	}
	oracleScope := "mutated_primary_rows_post_timing"
	if route == "point" || route == "range" || route == "prepared" {
		point := func(id []byte) ([]byte, error) {
			doc, found, e := tree.col.GetInto(id, nil)
			if e == nil && !found {
				e = fmt.Errorf("missing point oracle row %q", id)
			}
			return doc, e
		}
		if err = r1AttributionReadOracle(route, fixture, point, tree.rangeDocuments, reader); err != nil {
			t.Fatal(err)
		}
		oracleScope = "full_row_fields_ids_and_order_post_timing"
	}
	micros := func(v syscall.Timeval) int64 { return v.Sec*1000000 + v.Usec }
	report := map[string]any{"scope": "short nonqualifying characterization; preencoded equivalent replacements; CPU is process user+system, not exclusive request CPU; profiles exclude fixture/oracle; nested timings nonadditive", "route": route, "engine": engine, "calls": len(samples), "workers": workers, "profile": profile, "detailed_stats": detailed, "wall_ns": wall.Nanoseconds(), "request_samples_ns": samples, "user_cpu_us": micros(rusageAfter.Utime) - micros(rusageBefore.Utime), "system_cpu_us": micros(rusageAfter.Stime) - micros(rusageBefore.Stime), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "heap_before": before.HeapAlloc, "heap_after": after.HeapAlloc, "stats_before": beforeStats, "stats_after": afterStats, "last_update_stats": tree.col.LastUpdateStats(), "fixture_sha256": r1FixtureHash(fixture), "oracle": true, "oracle_scope": oracleScope}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "report.json"), append(raw, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

// This single reduced-target rollover probe establishes physical owner routing;
// it cannot qualify default segment economics or an unlimited plateau.
func TestR1RolloverAttribution5091(t *testing.T) {
	out := os.Getenv("GOMAP_R1_ROLLOVER_OUT")
	if out == "" {
		t.Skip("opt-in reduced-target feasibility probe")
	}
	for _, key := range []string{"TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES", "TREEDB_VLOG_GENERATION_HOT_SEGMENT_TARGET_BYTES"} {
		t.Setenv(key, "65536")
	}
	tree, e := openR1Tree(r1Config{Durability: "durable"}, "typed-row")
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := tree.close(); err != nil {
			t.Error(err)
		}
	}()
	fixture := r1Fixture(4096)
	for i := 0; i < len(fixture); i += 32 {
		if e = tree.insert(fixture[i : i+32]); e != nil {
			t.Fatal(e)
		}
	}
	if e = tree.transition("flushed"); e != nil {
		t.Fatal(e)
	}
	reader, e := tree.openReader()
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if reader != nil {
			reader.close()
		}
	}()
	old := append([]r1Document(nil), fixture[:64]...)
	if _, _, e = reader.fetch(r1IDs(old)); e != nil {
		t.Fatal(e)
	}
	census := func() map[string]int64 {
		m := map[string]int64{}
		e = filepath.WalkDir(tree.dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(tree.dir, p)
			if err != nil {
				return err
			}
			if strings.Contains(rel, "vlog/") {
				m[rel] = info.Size()
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
		return m
	}
	initial := census()
	for i := range 64 {
		d := fixture[i]
		d.Email = fmt.Sprintf("rollover-%d@example.test", i)
		d.City = fmt.Sprintf("city-%02d", (i+1)%8)
		d.Revision++
		fixture[i] = d
		if e = tree.replace([]r1Document{d}); e != nil {
			t.Fatal(e)
		}
	}
	if e = tree.transition("checkpointed"); e != nil {
		t.Fatal(e)
	}
	held := census()
	docs, _, e := reader.fetch(r1IDs(old))
	if e != nil {
		t.Fatal(e)
	}
	if len(docs) != len(old) {
		t.Fatalf("held reader returned %d rows, want %d", len(docs), len(old))
	}
	for i, raw := range docs {
		if e = r1VerifyDocument(raw, old[i]); e != nil {
			t.Fatal(e)
		}
	}
	before, e := tree.db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if e = reader.close(); e != nil {
		t.Fatal(e)
	}
	reader = nil
	if e = tree.transition("checkpointed"); e != nil {
		t.Fatal(e)
	}
	after, e := tree.db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if e != nil {
		t.Fatal(e)
	}
	final := census()
	for _, d := range fixture[:64] {
		raw, e := tree.point([]byte(d.ID))
		if e != nil {
			t.Fatal(e)
		}
		if e = r1VerifyDocument(raw, d); e != nil {
			t.Fatal(e)
		}
	}
	if e = os.MkdirAll(out, 0755); e != nil {
		t.Fatal(e)
	}
	f, e := os.Create(filepath.Join(out, "report.json"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	e = json.NewEncoder(f).Encode(map[string]any{"scope": "single nonqualifying reduced64KiB leaf/hot target rollover probe,64 public mutations, held then released old reader; default economics unavailable", "initial_files": initial, "held_files": held, "final_files": final, "held_gc": before, "released_gc": after, "stats": tree.db.Stats(), "oracle": true})
	if e != nil {
		t.Fatal(e)
	}
}

func r1AttributionReadOracle(route string, fixture []r1Document, point func([]byte) ([]byte, error), rangeDocuments func(string, int) ([][]byte, error), reader r1Reader) error {
	switch route {
	case "point":
		for _, expected := range fixture {
			raw, err := point([]byte(expected.ID))
			if err != nil {
				return err
			}
			if err = r1VerifyDocument(raw, expected); err != nil {
				return err
			}
		}
	case "range":
		for bucket := range 8 {
			city := fmt.Sprintf("city-%02d", bucket)
			documents, err := rangeDocuments(city, 10)
			if err != nil {
				return err
			}
			var expected []r1Document
			for _, row := range fixture {
				if row.City == city && len(expected) < 10 {
					expected = append(expected, row)
				}
			}
			if len(documents) != len(expected) {
				return fmt.Errorf("range oracle row count: got %d want %d", len(documents), len(expected))
			}
			for i, raw := range documents {
				if err = r1VerifyDocument(raw, expected[i]); err != nil {
					return err
				}
			}
		}
	case "prepared":
		return r1VerifyAll(reader, fixture, 32)
	default:
		return fmt.Errorf("unsupported read oracle route %q", route)
	}
	return nil
}

type r1AttributionOracleReader struct {
	point func([]byte) ([]byte, error)
}

func (r r1AttributionOracleReader) fetch(ids [][]byte) ([][]byte, collections.DocumentMaterializationStats, error) {
	docs := make([][]byte, len(ids))
	for i, id := range ids {
		var err error
		docs[i], err = r.point(id)
		if err != nil {
			return nil, collections.DocumentMaterializationStats{}, err
		}
	}
	return docs, collections.DocumentMaterializationStats{}, nil
}
func (r r1AttributionOracleReader) close() error { return nil }

func TestR1Attribution5091ReadOracleRejectsWrongRows(t *testing.T) {
	fixture := r1Fixture(128)
	for _, route := range []string{"point", "range", "prepared"} {
		for _, corruption := range []string{"none", "field", "id", "order"} {
			t.Run(route+"/"+corruption, func(t *testing.T) {
				encoded := make(map[string][]byte, len(fixture))
				for i, row := range fixture {
					switch corruption {
					case "field":
						row.Bio = "incorrect"
					case "id":
						row.ID = "incorrect-id"
					case "order":
						row = fixture[(i+8)%len(fixture)]
					}
					raw, err := json.Marshal(row)
					if err != nil {
						t.Fatal(err)
					}
					encoded[fixture[i].ID] = raw
				}
				point := func(id []byte) ([]byte, error) { return encoded[string(id)], nil }
				rangeDocuments := func(city string, limit int) ([][]byte, error) {
					var docs [][]byte
					for _, row := range fixture {
						if row.City == city && len(docs) < limit {
							docs = append(docs, encoded[row.ID])
						}
					}
					return docs, nil
				}
				err := r1AttributionReadOracle(route, fixture, point, rangeDocuments, r1AttributionOracleReader{point: point})
				if (err != nil) != (corruption != "none") {
					t.Fatalf("oracle corruption=%s: %v", corruption, err)
				}
			})
		}
	}
}
