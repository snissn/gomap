package main

// R1 is a bounded, public-path comparison mode of the native workload harness.
// A packet is evidence only after its source identities are reviewed and frozen.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const r1Schema = "gomap-r1-row-v1"

var r1Engines = []string{"json", "template-v1", "bson", "typed-row", "sqlite-json", "sqlite-row"}
var r1Phases = []string{"load", "point_get_into_complete", "point_complete", "batch_complete", "range_complete", "range_public_complete", "update_nonindexed", "update_indexed", "replace", "delete", "mixed_churn", "checkpoint", "upsert"}

type r1Config struct {
	Documents      int      `json:"documents"`
	Batch          int      `json:"batch_size"`
	Operations     int      `json:"operations"`
	Repetitions    int      `json:"repetitions"`
	Durability     string   `json:"durability"`
	State          string   `json:"read_state"`
	Engines        []string `json:"engines"`
	Qualification  string   `json:"qualification"`
	SourceManifest string   `json:"-"`
}
type r1Source struct {
	Commit        string            `json:"commit"`
	RuntimeSHA256 string            `json:"runtime_sha256"`
	RuntimeBlobs  map[string]string `json:"runtime_blobs"`
	HarnessSHA256 string            `json:"harness_sha256"`
	Clean         bool              `json:"clean"`
}
type r1Packet struct {
	Schema        string   `json:"schema"`
	Source        r1Source `json:"source"`
	Config        r1Config `json:"config"`
	FixtureSHA256 string   `json:"fixture_sha256"`
	GoVersion     string   `json:"go_version"`
	Hostname      string   `json:"hostname"`
	GOOS          string   `json:"goos"`
	GOARCH        string   `json:"goarch"`
	StartedAt     string   `json:"started_at"`
	Cells         []r1Cell `json:"cells"`
}
type r1Cell struct {
	Unsupported     string            `json:"unsupported,omitempty"`
	Rejection       string            `json:"rejection,omitempty"`
	Engine          string            `json:"engine"`
	Repetition      int               `json:"repetition"`
	Ack             string            `json:"acknowledgement"`
	StateTransition r1Measurement     `json:"state_transition"`
	Setup           r1Measurement     `json:"setup"`
	Warmup          r1Measurement     `json:"warmup"`
	Phases          []r1Measurement   `json:"phases"`
	StorageBoundary string            `json:"storage_boundary"`
	PersistentBytes int64             `json:"persistent_bytes"`
	WALBytes        int64             `json:"wal_bytes"`
	TransientBytes  int64             `json:"transient_bytes"`
	Stats           map[string]string `json:"stats,omitempty"`
	OracleVerified  bool              `json:"oracle_verified"`
	Capabilities    map[string]string `json:"capabilities"`
}
type r1Measurement struct {
	Name           string                                   `json:"name"`
	Operations     int                                      `json:"operations"`
	Rows           int                                      `json:"rows"`
	NSPerOp        float64                                  `json:"ns_per_op"`
	OpsPerSec      float64                                  `json:"ops_per_sec"`
	P50NS          int64                                    `json:"p50_ns"`
	P95NS          int64                                    `json:"p95_ns"`
	P99NS          int64                                    `json:"p99_ns"`
	BytesPerOp     float64                                  `json:"bytes_per_op"`
	AllocsPerOp    float64                                  `json:"allocs_per_op"`
	HeapAfterBytes uint64                                   `json:"heap_after_bytes"`
	Counters       collections.DocumentMaterializationStats `json:"materialization_counters"`
	Skipped        string                                   `json:"skipped,omitempty"`
}
type r1Document struct {
	ID       string  `json:"id" bson:"id"`
	Email    string  `json:"email" bson:"email"`
	City     string  `json:"city" bson:"city"`
	Name     string  `json:"name" bson:"name"`
	Bio      string  `json:"bio" bson:"bio"`
	Age      int64   `json:"age" bson:"age"`
	Score    float64 `json:"score" bson:"score"`
	Active   bool    `json:"active" bson:"active"`
	Revision int64   `json:"revision" bson:"revision"`
	// A typed nil pointer in an interface emits explicit null; nil omits the field.
	Optional any `json:"optional,omitempty" bson:"optional,omitempty"`
}

func r1Fixture(n int) []r1Document {
	out := make([]r1Document, n)
	for i := range out {
		out[i] = r1Document{ID: nativeDocumentIDText(i), Email: nativeEmail(i), City: fmt.Sprintf("city-%02d", i%8), Name: fmt.Sprintf("User %06d", i), Bio: strings.Repeat("x", 96), Age: int64(18 + i%67), Score: float64(i%1000) / 10, Active: i%2 == 0}
		if i%2 == 0 {
			out[i].Optional = (*string)(nil)
		}
	}
	return out
}
func r1IDs(rows []r1Document) [][]byte {
	out := make([][]byte, len(rows))
	for i := range rows {
		out[i] = []byte(rows[i].ID)
	}
	return out
}
func r1FixtureHash(rows []r1Document) string {
	raw, _ := json.Marshal(rows)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func r1SHA(s string, n int) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == n }

func parseR1Config(args []string) (r1Config, error) {
	c := r1Config{Documents: 4096, Batch: 32, Operations: 1000, Repetitions: 5, Durability: "durable", State: "flushed", Qualification: "rehearsal"}
	f := flag.NewFlagSet("collection_workload_bench r1", flag.ContinueOnError)
	f.IntVar(&c.Documents, "documents", c.Documents, "fixture documents (at least 16)")
	f.IntVar(&c.Batch, "batch-size", c.Batch, "rows per atomic load batch / complete batch read")
	f.IntVar(&c.Operations, "operations", c.Operations, "calls per timed read/mutation phase")
	f.IntVar(&c.Repetitions, "repetitions", c.Repetitions, "independent fresh database repetitions")
	f.StringVar(&c.Durability, "durability", c.Durability, "durable or relaxed (separate comparisons)")
	f.StringVar(&c.State, "read-state", c.State, "buffered, flushed, or checkpointed; opening the read view drains pending writes")
	f.StringVar(&c.Qualification, "qualification", c.Qualification, "rehearsal or retained (retained requires landed reviewed harness)")
	f.StringVar(&c.SourceManifest, "source-manifest", "", "capture-script source identity JSON")
	engines := strings.Join(r1Engines, ",")
	f.StringVar(&engines, "engines", engines, "ordered cells")
	if err := f.Parse(args); err != nil {
		return c, err
	}
	if f.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	c.Engines = splitCSV(engines)
	if c.Documents < 16 || c.Batch < 1 || c.Batch > c.Documents || c.Operations < 1 || c.Repetitions < 1 {
		return c, errors.New("invalid R1 dimensions")
	}
	if c.Durability != "durable" && c.Durability != "relaxed" {
		return c, errors.New("R1 durability must be durable or relaxed")
	}
	if c.State != "buffered" && c.State != "flushed" && c.State != "checkpointed" {
		return c, errors.New("invalid R1 read state")
	}
	if c.Qualification != "rehearsal" && c.Qualification != "retained" {
		return c, errors.New("invalid R1 qualification")
	}
	seen := map[string]bool{}
	for _, e := range c.Engines {
		if !containsR1(r1Engines, e) || seen[e] {
			return c, fmt.Errorf("unknown or duplicate R1 engine %q", e)
		}
		seen[e] = true
	}
	if len(seen) == 0 {
		return c, errors.New("no R1 engines")
	}
	return c, nil
}
func containsR1(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
func runR1Command(w io.Writer, args []string) error {
	c, err := parseR1Config(args)
	if err != nil {
		return err
	}
	var source r1Source
	if err = readR1JSON(c.SourceManifest, &source); err != nil {
		return fmt.Errorf("source manifest: %w", err)
	}
	p, err := runR1(c, source)
	if err != nil {
		return err
	}
	if err = validateR1Packet(p); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(p)
}
func runR1(c r1Config, source r1Source) (r1Packet, error) {
	host, err := os.Hostname()
	if err != nil {
		return r1Packet{}, err
	}
	fixture := r1Fixture(c.Documents)
	p := r1Packet{Schema: r1Schema, Source: source, Config: c, FixtureSHA256: r1FixtureHash(fixture), GoVersion: runtime.Version(), Hostname: host, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if validateR1Source(source) != nil {
		return p, errors.New("missing/malformed source identity")
	}
	if c.Qualification == "retained" && (!source.Clean || c.Repetitions < 5) {
		return p, errors.New("retained capture requires clean source and at least five repetitions")
	}
	for rep := 0; rep < c.Repetitions; rep++ {
		for _, engine := range c.Engines {
			cell, err := runR1Cell(c, engine, rep, fixture)
			if err != nil {
				if engine == "typed-row" && c.Durability == "relaxed" && errors.Is(err, backenddb.ErrCommandWALRejected) {
					cell = r1Cell{Engine: engine, Repetition: rep, Unsupported: "typed_relaxed_write_admission", Rejection: err.Error()}
				} else {
					return p, fmt.Errorf("%s repetition %d: %w", engine, rep, err)
				}
			}
			p.Cells = append(p.Cells, cell)
		}
	}
	return p, nil
}

type r1Reader interface {
	fetch([][]byte) ([][]byte, collections.DocumentMaterializationStats, error)
	close() error
}
type r1Backend interface {
	insert([]r1Document) error
	replace([]r1Document) error
	update([]r1Document) error
	point([]byte) ([]byte, error)
	rangeDocuments(string, int) ([][]byte, error)
	emailIDs(string) ([][]byte, error)
	capabilities([]r1Document) (map[string]string, error)
	upsert([]r1Document) error
	supportsUpsert() bool
	delete([][]byte) error
	openReader() (r1Reader, error)
	rangeIDs(string, int) ([][]byte, error)
	transition(string) error
	storage() (int64, int64, int64, error)
	stats() map[string]string
	close() error
}

func r1Measure(name string, n int, fn func(int) (int, collections.DocumentMaterializationStats, error)) (r1Measurement, error) {
	samples := make([]int64, n)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	m := r1Measurement{Name: name, Operations: n}
	var elapsed time.Duration
	for i := 0; i < n; i++ {
		s := time.Now()
		rows, counters, err := fn(i)
		samples[i] = time.Since(s).Nanoseconds()
		elapsed += time.Duration(samples[i])
		if err != nil {
			return m, err
		}
		m.Rows += rows
		r1AddCounters(&m.Counters, counters)
	}
	runtime.ReadMemStats(&after)
	m.NSPerOp = float64(elapsed.Nanoseconds()) / float64(n)
	m.OpsPerSec = float64(n) / elapsed.Seconds()
	m.BytesPerOp = float64(after.TotalAlloc-before.TotalAlloc) / float64(n)
	m.AllocsPerOp = float64(after.Mallocs-before.Mallocs) / float64(n)
	m.HeapAfterBytes = after.HeapAlloc
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	m.P50NS = samples[(n-1)*50/100]
	m.P95NS = samples[(n-1)*95/100]
	m.P99NS = samples[(n-1)*99/100]
	return m, nil
}

// Sum work/duration counters; retain the peak active-handle gauge.
func r1AddCounters(dst *collections.DocumentMaterializationStats, src collections.DocumentMaterializationStats) {
	d, s := reflect.ValueOf(dst).Elem(), reflect.ValueOf(src)
	for i := 0; i < d.NumField(); i++ {
		if d.Type().Field(i).Name == "AssetActiveHandles" {
			if s.Field(i).Int() > d.Field(i).Int() {
				d.Field(i).SetInt(s.Field(i).Int())
			}
			continue
		}
		switch d.Field(i).Kind() {
		case reflect.Uint64:
			d.Field(i).SetUint(d.Field(i).Uint() + s.Field(i).Uint())
		case reflect.Int64:
			d.Field(i).SetInt(d.Field(i).Int() + s.Field(i).Int())
		}
	}
}
func validateR1Source(s r1Source) error {
	if !r1SHA(s.Commit, 20) || !r1SHA(s.RuntimeSHA256, 32) || !r1SHA(s.HarnessSHA256, 32) || len(s.RuntimeBlobs) == 0 {
		return errors.New("invalid source identity")
	}
	paths := make([]string, 0, len(s.RuntimeBlobs))
	for path, blob := range s.RuntimeBlobs {
		if !r1SHA(blob, 20) || path == "" {
			return errors.New("invalid runtime source blob")
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		_, _ = fmt.Fprintf(h, "%s%c%s%c", path, 0, s.RuntimeBlobs[path], 0)
	}
	if hex.EncodeToString(h.Sum(nil)) != s.RuntimeSHA256 {
		return errors.New("runtime source digest mismatch")
	}
	return nil
}
func runR1Cell(c r1Config, engine string, rep int, fixture []r1Document) (cell r1Cell, err error) {
	var b r1Backend
	if strings.HasPrefix(engine, "sqlite-") {
		b, err = openR1SQLite(c, engine)
	} else {
		b, err = openR1Tree(c, engine)
	}
	if err != nil {
		return cell, err
	}
	defer func() { err = errors.Join(err, b.close()) }()
	cell.Engine = engine
	cell.Repetition = rep
	cell.Ack = c.Durability + "_at_ack"
	if c.Durability == "relaxed" {
		cell.Ack = "process_visible_no_fsync"
	}
	add := func(name string, n int, fn func(int) (int, collections.DocumentMaterializationStats, error)) error {
		m, e := r1Measure(name, n, fn)
		cell.Phases = append(cell.Phases, m)
		return e
	}
	batches := (len(fixture) + c.Batch - 1) / c.Batch
	if err = add("load", batches, func(i int) (int, collections.DocumentMaterializationStats, error) {
		end := min((i+1)*c.Batch, len(fixture))
		rows := fixture[i*c.Batch : end]
		return len(rows), collections.DocumentMaterializationStats{}, b.insert(rows)
	}); err != nil {
		return cell, err
	}

	cell.StateTransition, err = r1Measure("read_state_transition", 1, func(int) (int, collections.DocumentMaterializationStats, error) {
		return 0, collections.DocumentMaterializationStats{}, b.transition(c.State)
	})
	if err != nil {
		return cell, err
	}
	if err = add("point_get_into_complete", c.Operations, func(op int) (int, collections.DocumentMaterializationStats, error) {
		i := benchmarkDocumentOrdinal(op, 37, len(fixture))
		doc, e := b.point([]byte(fixture[i].ID))
		if e != nil {
			return 0, collections.DocumentMaterializationStats{}, e
		}
		if len(doc) == 0 {
			return 0, collections.DocumentMaterializationStats{}, errors.New("empty public point output")
		}
		return 1, collections.DocumentMaterializationStats{}, nil
	}); err != nil {
		return cell, err
	}
	for _, expected := range fixture {
		raw, e := b.point([]byte(expected.ID))
		if e != nil {
			return cell, e
		}
		if e = r1VerifyDocument(raw, expected); e != nil {
			return cell, fmt.Errorf("ordinary public point: %w", e)
		}
	}
	var reader r1Reader
	cell.Setup, err = r1Measure("read_view_open_flush_setup", 1, func(int) (int, collections.DocumentMaterializationStats, error) {
		reader, err = b.openReader()
		return 0, collections.DocumentMaterializationStats{}, err
	})
	if err != nil {
		return cell, err
	}
	// Cold first complete batch (locator/cache setup) is separate from warmed reads.
	cell.Warmup, err = r1Measure("first_complete_batch", 1, func(int) (int, collections.DocumentMaterializationStats, error) {
		docs, s, e := reader.fetch(r1IDs(fixture[:c.Batch]))
		return len(docs), s, e
	})
	if err != nil {
		_ = reader.close()
		return cell, err
	}
	owned, _, e := reader.fetch(r1IDs(fixture[:1]))
	if e != nil {
		_ = reader.close()
		return cell, e
	}
	originalOwned := bytes.Clone(owned[0])
	if err = r1VerifyAll(reader, fixture, c.Batch); err != nil {
		_ = reader.close()
		return cell, err
	}
	for _, phase := range []string{"point_complete", "batch_complete", "range_complete"} {
		err = add(phase, c.Operations, func(op int) (int, collections.DocumentMaterializationStats, error) {
			i := benchmarkDocumentOrdinal(op, 37, len(fixture))
			ids := [][]byte{[]byte(fixture[i].ID)}
			if phase == "batch_complete" {
				ids = make([][]byte, c.Batch)
				for j := range ids {
					ids[j] = []byte(fixture[(i+j)%len(fixture)].ID)
				}
			}
			if phase == "range_complete" {
				ids, err = b.rangeIDs(fmt.Sprintf("city-%02d", op%8), 10)
				if err != nil {
					return 0, collections.DocumentMaterializationStats{}, err
				}
			}
			docs, s, e := reader.fetch(ids)
			if e != nil {
				return 0, s, e
			}
			for _, doc := range docs {
				if len(doc) == 0 {
					return 0, s, errors.New("missing complete row")
				}
			}
			if len(docs) != len(ids) {
				return 0, s, errors.New("incomplete full-row output")
			}
			return len(docs), s, nil
		})
		if err != nil {
			_ = reader.close()
			return cell, err
		}
	}
	// Verify range result IDs/order and every complete document outside timers.
	for bucket := 0; bucket < 8; bucket++ {
		city := fmt.Sprintf("city-%02d", bucket)
		ids, e := b.rangeIDs(city, 10)
		if e != nil {
			_ = reader.close()
			return cell, e
		}
		expected := []r1Document{}
		for _, d := range fixture {
			if d.City == city && len(expected) < 10 {
				expected = append(expected, d)
			}
		}
		if !reflect.DeepEqual(ids, r1IDs(expected)) {
			_ = reader.close()
			return cell, fmt.Errorf("range oracle mismatch for %s", city)
		}
		if e = r1VerifyRows(reader, expected); e != nil {
			_ = reader.close()
			return cell, e
		}
	}
	if err = reader.close(); err != nil {
		return cell, err
	}
	if !bytes.Equal(owned[0], originalOwned) {
		return cell, errors.New("read output changed after next fetch/close")
	}
	cell.Capabilities, err = b.capabilities(fixture)
	if err != nil {
		return cell, err
	}
	if cell.Capabilities["ordinary_range"] == "rejected_residual_only_full_row_parity_gap" {
		cell.Phases = append(cell.Phases, r1Measurement{Name: "range_public_complete", Skipped: "ordinary typed range returns residual only"})
	} else {
		if err = add("range_public_complete", c.Operations, func(op int) (int, collections.DocumentMaterializationStats, error) {
			docs, e := b.rangeDocuments(fmt.Sprintf("city-%02d", op%8), 10)
			return len(docs), collections.DocumentMaterializationStats{}, e
		}); err != nil {
			return cell, err
		}
		for bucket := 0; bucket < 8; bucket++ {
			city := fmt.Sprintf("city-%02d", bucket)
			docs, e := b.rangeDocuments(city, 10)
			if e != nil {
				return cell, e
			}
			var expected []r1Document
			for _, d := range fixture {
				if d.City == city && len(expected) < 10 {
					expected = append(expected, d)
				}
			}
			if len(docs) != len(expected) {
				return cell, errors.New("public complete range row count mismatch")
			}
			for i, raw := range docs {
				if e = r1VerifyDocument(raw, expected[i]); e != nil {
					return cell, e
				}
			}
		}
	}
	emailHistory := make([]string, 0, len(fixture)+2*c.Operations)
	for _, d := range fixture {
		emailHistory = append(emailHistory, d.Email)
	}
	current := append([]r1Document(nil), fixture...)
	for _, name := range []string{"update_nonindexed", "update_indexed", "replace", "delete", "mixed_churn", "upsert"} {
		if name == "upsert" {
			if err = add("checkpoint", 1, func(int) (int, collections.DocumentMaterializationStats, error) {
				return 0, collections.DocumentMaterializationStats{}, b.transition("checkpointed")
			}); err != nil {
				return cell, err
			}
			cell.PersistentBytes, cell.WALBytes, cell.TransientBytes, err = b.storage()
			if err != nil {
				return cell, err
			}
			cell.Stats = b.stats()
			cell.StorageBoundary = "common_checkpoint_before_upsert"
		}
		if name == "upsert" && !b.supportsUpsert() {
			cell.Phases = append(cell.Phases, r1Measurement{Name: name, Skipped: "no equivalent public atomic retained-document upsert"})
			continue
		}
		n := c.Operations
		if name == "delete" {
			n = min(n, len(current)/4)
		}
		err = add(name, n, func(op int) (int, collections.DocumentMaterializationStats, error) {
			i := benchmarkDocumentOrdinal(op, 37, len(current))
			d := current[i]
			d.Revision++
			switch name {
			case "update_nonindexed":
				d.Score += 1
			case "update_indexed":
				d.City = fmt.Sprintf("city-%02d", (op+3)%8)
				d.Email = fmt.Sprintf("rev%d-%s", d.Revision, fixture[i].Email)
				emailHistory = append(emailHistory, d.Email)
			case "replace":
				d.Bio = strings.Repeat("y", 96)
			case "upsert":
				extra := d
				extra.ID = fmt.Sprintf("extra-%012d", op)
				extra.Email = "extra-" + extra.ID + "@example.test"
				emailHistory = append(emailHistory, extra.Email)
				if e := b.upsert([]r1Document{d, extra}); e != nil {
					return 0, collections.DocumentMaterializationStats{}, e
				}
				current[i] = d
				current = append(current, extra)
				return 2, collections.DocumentMaterializationStats{}, nil
			case "delete":
				d = current[op]
				if e := b.delete([][]byte{[]byte(d.ID)}); e != nil {
					return 0, collections.DocumentMaterializationStats{}, e
				}
				return 1, collections.DocumentMaterializationStats{}, nil
			case "mixed_churn":
				if op%4 == 0 {
					if e := b.delete([][]byte{[]byte(d.ID)}); e != nil {
						return 0, collections.DocumentMaterializationStats{}, e
					}
					if e := b.insert([]r1Document{d}); e != nil {
						return 0, collections.DocumentMaterializationStats{}, e
					}
					current[i] = d
					return 2, collections.DocumentMaterializationStats{}, nil
				}
				d.City = fmt.Sprintf("city-%02d", op%8)
				d.Score += 1
			}
			var e error
			if strings.HasPrefix(name, "update_") {
				e = b.update([]r1Document{d})
			} else {
				e = b.replace([]r1Document{d})
			}
			if e != nil {
				return 0, collections.DocumentMaterializationStats{}, e
			}
			current[i] = d
			return 1, collections.DocumentMaterializationStats{}, nil
		})
		if err != nil {
			return cell, err
		}
		var deleted []r1Document
		if name == "delete" {
			deleted = current[:n]
			current = current[n:]
		}
		reader, err = b.openReader()
		if err != nil {
			return cell, err
		}
		err = r1VerifyAll(reader, current, c.Batch)
		if err == nil {
			err = r1VerifyIndexes(b, reader, current, emailHistory)
		}
		if err == nil && len(deleted) > 0 {
			docs, _, e := reader.fetch(r1IDs(deleted))
			err = e
			for _, doc := range docs {
				if doc != nil {
					err = errors.New("deleted row still visible")
				}
			}
		}
		err = errors.Join(err, reader.close())
		if err != nil {
			return cell, fmt.Errorf("%s oracle: %w", name, err)
		}
	}
	cell.OracleVerified = true
	return cell, nil
}
func r1VerifyAll(r r1Reader, expected []r1Document, batch int) error {
	for start := 0; start < len(expected); start += batch {
		if err := r1VerifyRows(r, expected[start:min(start+batch, len(expected))]); err != nil {
			return err
		}
	}
	return nil
}
func r1VerifyRows(r r1Reader, expected []r1Document) error {
	docs, _, err := r.fetch(r1IDs(expected))
	if err != nil {
		return err
	}
	if len(docs) != len(expected) {
		return errors.New("oracle row count mismatch")
	}
	for i, raw := range docs {
		if e := r1VerifyDocument(raw, expected[i]); e != nil {
			return e
		}
	}
	return nil
}

func r1VerifyDocument(raw []byte, expected r1Document) error {
	var got, want map[string]any
	if e := json.Unmarshal(raw, &got); e != nil {
		return e
	}
	w, e := json.Marshal(expected)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(w, &want); e != nil {
		return e
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("full row oracle mismatch for %s: got %s want %s", expected.ID, raw, w)
	}
	return nil
}

type r1Tree struct {
	dir, engine string
	db          *backenddb.DB
	manager     *collections.CollectionManager
	col         *collections.Collection
	cleanup     func() error
}

func openR1Tree(c r1Config, engine string) (*r1Tree, error) {
	dir, err := os.MkdirTemp("", "gomap-r1-tree-")
	if err != nil {
		return nil, err
	}
	t := &r1Tree{dir: dir, engine: engine}
	profile := treedb.ProfileCommandWALDurable
	if c.Durability == "relaxed" {
		profile = treedb.ProfileCommandWALRelaxed
	}
	opts := treedb.OptionsFor(profile, dir)
	t.db, t.cleanup, err = treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	t.manager = collections.NewCollectionManager(t.db)
	format := collections.DocumentFormat(engine)
	if engine == "typed-row" {
		format = collections.DocumentFormatJSON
	}
	meta := collections.CollectionMeta{Name: "r1", Options: collections.CollectionOptions{DocumentFormat: format}, Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}, {Name: "city", Field: "city", ValueType: collections.IndexValueString}}}
	if engine == "typed-row" {
		meta.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON}
		if c.Durability == "relaxed" {
			meta.Options.ColumnStore.ProfileSupport = collections.ColumnStoreProfileBenchmarkRelaxed
		}
		for _, name := range []string{"email", "city", "name", "bio"} {
			meta.Options.ColumnStore.Columns = append(meta.Options.ColumnStore.Columns, collections.ColumnStoreColumn{Name: name, Path: name, ValueType: collections.ColumnStoreValueString})
		}
	}
	if _, err = t.manager.CreateCollection(&meta); err == nil {
		t.col, err = t.manager.OpenCollection("r1")
	}
	if err != nil {
		_ = t.close()
		return nil, err
	}
	return t, nil
}
func r1TypedInput(rows []r1Document) ([][]byte, [][]byte, []collections.TypedColumnBatch, error) {
	retained := make([][]byte, len(rows))
	cols := []collections.TypedColumnBatch{{Name: "email"}, {Name: "city"}, {Name: "name"}, {Name: "bio"}}
	for i, d := range rows {
		raw, e := json.Marshal(d)
		if e != nil {
			return nil, nil, nil, e
		}
		var obj map[string]any
		_ = json.Unmarshal(raw, &obj)
		for _, name := range []string{"email", "city", "name", "bio"} {
			delete(obj, name)
		}
		retained[i], e = json.Marshal(obj)
		if e != nil {
			return nil, nil, nil, e
		}
		cols[0].Strings = append(cols[0].Strings, d.Email)
		cols[1].Strings = append(cols[1].Strings, d.City)
		cols[2].Strings = append(cols[2].Strings, d.Name)
		cols[3].Strings = append(cols[3].Strings, d.Bio)
	}
	return r1IDs(rows), retained, cols, nil
}
func (t *r1Tree) encoded(rows []r1Document) ([][]byte, error) {
	out := make([][]byte, len(rows))
	for i, d := range rows {
		if t.engine == "bson" {
			raw, e := bson.Marshal(d)
			if e != nil {
				return nil, e
			}
			out[i] = raw
			continue
		}
		raw, e := json.Marshal(d)
		if e != nil {
			return nil, e
		}
		switch t.engine {
		case "json", "typed-row":
			out[i] = raw
		case "template-v1":
			out[i], e = collections.EncodeTemplateV1DocumentJSON(raw)
		default:
			return nil, errors.New("unsupported document encoder")
		}
		if e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (t *r1Tree) insert(rows []r1Document) error {
	if t.engine == "typed-row" {
		ids, retained, cols, e := r1TypedInput(rows)
		if e != nil {
			return e
		}
		_, _, e = t.col.InsertTypedBatchWithStats(ids, retained, cols)
		return e
	}
	docs, e := t.encoded(rows)
	if e != nil {
		return e
	}
	_, e = t.col.InsertBatch(r1IDs(rows), docs)
	return e
}
func (t *r1Tree) replace(rows []r1Document) error {
	if t.engine == "typed-row" {
		ids, retained, cols, e := r1TypedInput(rows)
		if e != nil {
			return e
		}
		_, e = t.col.ReplaceTypedBatch(ids, retained, cols)
		return e
	}
	docs, e := t.encoded(rows)
	if e != nil {
		return e
	}
	for i, d := range rows {
		matched, err := t.col.Replace([]byte(d.ID), docs[i])
		if err != nil {
			return err
		}
		if !matched {
			return errors.New("replace missing row")
		}
	}
	return nil
}
func (t *r1Tree) upsert(rows []r1Document) error {
	ids, retained, cols, e := r1TypedInput(rows)
	if e != nil {
		return e
	}
	_, e = t.col.UpsertTypedBatch(ids, retained, cols)
	return e
}
func (t *r1Tree) supportsUpsert() bool                    { return t.engine == "typed-row" }
func (t *r1Tree) delete(ids [][]byte) error               { _, e := t.col.DeleteBatch(ids); return e }
func (t *r1Tree) emailIDs(email string) ([][]byte, error) { return t.col.FindByIndex("email", email) }
func (t *r1Tree) rangeIDs(city string, limit int) ([][]byte, error) {
	ids, _, e := t.col.FindByIndexRange("city", collections.IndexRangeOptions{Lower: collections.IndexRangeBound{Value: city, Inclusive: true}, Upper: collections.IndexRangeBound{Value: city, Inclusive: true}, Limit: limit})
	return ids, e
}
func (t *r1Tree) transition(state string) error {
	if state == "buffered" {
		return nil
	}
	if e := t.manager.FlushAll(); e != nil {
		return e
	}
	if state == "checkpointed" {
		return t.db.Checkpoint()
	}
	return nil
}
func (t *r1Tree) storage() (int64, int64, int64, error) { return r1Storage(t.dir) }
func (t *r1Tree) stats() map[string]string              { return t.db.Stats() }
func (t *r1Tree) close() error {
	var e error
	if t.cleanup != nil {
		e = t.cleanup()
		t.cleanup = nil
	}
	return errors.Join(e, os.RemoveAll(t.dir))
}

type r1TreeReader struct {
	view         *collections.CollectionReadView
	materializer *collections.StoredDocumentJSONMaterializer
	engine       string
}

func (t *r1Tree) openReader() (r1Reader, error) {
	v, e := t.col.OpenCollectionReadView()
	if e != nil {
		return nil, e
	}
	m, e := t.col.NewStoredDocumentJSONMaterializer()
	if e != nil {
		_ = v.Close()
		return nil, e
	}
	return &r1TreeReader{view: v, materializer: m, engine: t.engine}, nil
}
func (r *r1TreeReader) fetch(ids [][]byte) ([][]byte, collections.DocumentMaterializationStats, error) {
	resp, e := r.view.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
	if e != nil {
		return nil, resp.Stats, e
	}
	out := make([][]byte, len(resp.Results))
	for i, result := range resp.Results {
		if !result.Found {
			continue
		}
		switch r.engine {
		case "template-v1":
			out[i], e = r.materializer.StoredDocumentJSON(result.Document)
		case "bson":
			var row map[string]any
			e = bson.Unmarshal(result.Document, &row)
			if e == nil {
				out[i], e = json.Marshal(row)
			}
		default:
			out[i] = result.Document
		}
		if e != nil {
			return nil, resp.Stats, e
		}
	}
	return out, resp.Stats, nil
}
func (r *r1TreeReader) close() error { return errors.Join(r.materializer.Close(), r.view.Close()) }
func r1Storage(dir string) (persistent, wal, transient int64, err error) {
	err = filepath.Walk(dir, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if info.Mode().IsRegular() {
			if strings.HasSuffix(path, "-shm") {
				// SQLite's WAL index is transient shared memory, not durable payload.
				transient += info.Size()
			} else if strings.Contains(path, string(filepath.Separator)+"wal"+string(filepath.Separator)) || strings.HasSuffix(path, "-wal") {
				wal += info.Size()
			} else {
				persistent += info.Size()
			}
		}
		return nil
	})
	return
}

func validateR1Packet(p r1Packet) error {
	if p.Schema != r1Schema || validateR1Source(p.Source) != nil {
		return errors.New("invalid R1 schema/source binding")
	}
	c := p.Config
	if c.Documents < 16 || c.Batch < 1 || c.Batch > c.Documents || c.Operations < 1 || c.Repetitions < 1 || len(c.Engines) == 0 {
		return errors.New("invalid packet dimensions")
	}
	if c.Durability != "durable" && c.Durability != "relaxed" {
		return errors.New("invalid packet durability")
	}
	if c.State != "buffered" && c.State != "flushed" && c.State != "checkpointed" {
		return errors.New("invalid packet read state")
	}
	if c.Qualification != "rehearsal" && c.Qualification != "retained" {
		return errors.New("invalid packet qualification")
	}
	if c.Qualification == "retained" && (!p.Source.Clean || c.Repetitions < 5) {
		return errors.New("unqualified retained source/repetitions")
	}
	if p.FixtureSHA256 != r1FixtureHash(r1Fixture(c.Documents)) || p.GoVersion == "" || p.Hostname == "" || p.GOOS == "" || p.GOARCH == "" {
		return errors.New("missing/mismatched fixture/runtime")
	}
	if _, e := time.Parse(time.RFC3339Nano, p.StartedAt); e != nil {
		return e
	}
	seen := map[string]bool{}
	for _, engine := range c.Engines {
		if !containsR1(r1Engines, engine) || seen[engine] {
			return errors.New("invalid selected engines")
		}
		seen[engine] = true
	}
	if len(p.Cells) != len(c.Engines)*c.Repetitions {
		return errors.New("incomplete raw cells")
	}
	seen = map[string]bool{}
	for _, cell := range p.Cells {
		key := fmt.Sprintf("%s/%d", cell.Engine, cell.Repetition)
		if seen[key] || !containsR1(c.Engines, cell.Engine) || cell.Repetition < 0 || cell.Repetition >= c.Repetitions {
			return errors.New("duplicate/unselected raw cell")
		}
		seen[key] = true
		if cell.Unsupported != "" {
			if cell.Unsupported != "typed_relaxed_write_admission" || cell.Engine != "typed-row" || c.Durability != "relaxed" || !strings.Contains(cell.Rejection, "relaxed durability modes are unsupported") || len(cell.Phases) != 0 {
				return errors.New("invalid unsupported cell")
			}
			continue
		}
		ack := "durable_at_ack"
		if c.Durability == "relaxed" {
			ack = "process_visible_no_fsync"
		}
		if cell.Ack != ack || !cell.OracleVerified || cell.PersistentBytes <= 0 || cell.WALBytes < 0 || cell.TransientBytes < 0 || cell.StorageBoundary != "common_checkpoint_before_upsert" {
			return errors.New("mismatched acknowledgement/oracle/storage")
		}
		if cell.Capabilities["range_decomposition"] != "quiescent_ids_then_prepared_full_fetch" || cell.Capabilities["ordinary_point"] == "" || cell.Capabilities["ordinary_range"] == "" {
			return errors.New("missing capability/path report")
		}
		if cell.StateTransition.Operations != 1 || cell.StateTransition.Rows != 0 || cell.Setup.Operations != 1 || cell.Setup.Rows != 0 || cell.Warmup.Operations != 1 || cell.Warmup.Rows != c.Batch {
			return errors.New("invalid setup/state/warmup denominator")
		}
		if cell.StateTransition.Name != "read_state_transition" {
			return errors.New("missing state transition")
		}
		if e := validateR1Measurement(cell.StateTransition); e != nil {
			return e
		}
		if cell.Setup.Name != "read_view_open_flush_setup" || cell.Warmup.Name != "first_complete_batch" {
			return errors.New("missing setup/warmup boundary")
		}
		if len(cell.Phases) != len(r1Phases) {
			return errors.New("incomplete phase packet")
		}
		for i, m := range cell.Phases {
			if m.Name != r1Phases[i] {
				return errors.New("missing/misordered phase")
			}
			if m.Skipped != "" {
				publicRangeSkip := m.Name == "range_public_complete" && cell.Engine == "typed-row" && cell.Capabilities["ordinary_range"] == "rejected_residual_only_full_row_parity_gap" && m.Skipped == "ordinary typed range returns residual only"
				retainedUpsertSkip := m.Name == "upsert" && cell.Engine != "typed-row" && !strings.HasPrefix(cell.Engine, "sqlite-")
				if !publicRangeSkip && !retainedUpsertSkip || m.Operations != 0 || m.Rows != 0 || m.NSPerOp != 0 || m.OpsPerSec != 0 {
					return errors.New("unsupported skip")
				}
				continue
			}
			if m.Name == "range_public_complete" && cell.Capabilities["ordinary_range"] == "rejected_residual_only_full_row_parity_gap" {
				return errors.New("residual-only public range mislabeled complete")
			}
			expectedOps, expectedRows := c.Operations, c.Operations
			switch m.Name {
			case "load":
				expectedOps = (c.Documents + c.Batch - 1) / c.Batch
				expectedRows = c.Documents
			case "batch_complete":
				expectedRows = c.Operations * c.Batch
			case "range_complete", "range_public_complete":
				expectedRows = 0
				for op := 0; op < c.Operations; op++ {
					count := (c.Documents + 7 - op%8) / 8
					expectedRows += min(10, count)
				}
			case "upsert":
				expectedRows = 2 * c.Operations
			case "delete":
				size := c.Documents
				expectedOps = min(c.Operations, size/4)
				expectedRows = expectedOps
			case "mixed_churn":
				expectedRows = c.Operations + (c.Operations+3)/4
			case "checkpoint":
				expectedOps = 1
				expectedRows = 0
			}
			if m.Operations != expectedOps || m.Rows != expectedRows {
				return fmt.Errorf("mismatched work denominator %s", m.Name)
			}
			if e := validateR1Measurement(m); e != nil {
				return e
			}
			if strings.HasSuffix(m.Name, "_complete") && m.Rows < m.Operations {
				return errors.New("ID-only/incomplete read measurement")
			}
			if strings.HasSuffix(m.Name, "_complete") && m.Name != "point_get_into_complete" && m.Name != "range_public_complete" && cell.Engine == "typed-row" && m.Counters.FieldsReconstructed == 0 {
				return errors.New("typed full-row path not proven")
			}
		}
		if e := validateR1Measurement(cell.Setup); e != nil {
			return e
		}
		if e := validateR1Measurement(cell.Warmup); e != nil {
			return e
		}
	}
	return nil
}
func validateR1Measurement(m r1Measurement) error {
	if m.Operations <= 0 || m.Rows < 0 || m.NSPerOp <= 0 || m.OpsPerSec <= 0 || m.BytesPerOp < 0 || m.AllocsPerOp < 0 || m.P50NS < 0 || m.P95NS < m.P50NS || m.P99NS < m.P95NS {
		return fmt.Errorf("malformed measurement %s", m.Name)
	}
	for _, v := range []float64{m.NSPerOp, m.OpsPerSec, m.BytesPerOp, m.AllocsPerOp} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("nonfinite measurement")
		}
	}
	return nil
}

func (t *r1Tree) update(rows []r1Document) error {
	docs, e := t.encoded(rows)
	if e != nil {
		return e
	}
	items := make([]collections.UpdateBatchItem, len(rows))
	for i, d := range rows {
		replacement := docs[i]
		items[i] = collections.UpdateBatchItem{DocumentID: []byte(d.ID), Update: func(current []byte) ([]byte, bool, error) {
			if current == nil {
				return nil, false, errors.New("update missing row")
			}
			return replacement, true, nil
		}}
	}
	results, e := t.col.UpdateBatch(items)
	if e != nil {
		return e
	}
	for _, r := range results {
		if !r.Matched || !r.Modified {
			return errors.New("update result mismatch")
		}
	}
	return nil
}
func (t *r1Tree) point(id []byte) ([]byte, error) {
	raw, found, e := t.col.GetInto(id, nil)
	if e != nil {
		return nil, e
	}
	if !found {
		return nil, errors.New("point row missing")
	}
	switch t.engine {
	case "template-v1":
		m, e := t.col.NewStoredDocumentJSONMaterializer()
		if e != nil {
			return nil, e
		}
		doc, e := m.StoredDocumentJSON(raw)
		return doc, errors.Join(e, m.Close())
	case "bson":
		var d map[string]any
		if e = bson.Unmarshal(raw, &d); e != nil {
			return nil, e
		}
		return json.Marshal(d)
	default:
		return raw, nil
	}
}
func (t *r1Tree) rangeDocuments(city string, limit int) ([][]byte, error) {
	records, _, e := t.col.FindDocumentsByIndexRange("city", collections.IndexRangeOptions{Lower: collections.IndexRangeBound{Value: city, Inclusive: true}, Upper: collections.IndexRangeBound{Value: city, Inclusive: true}, Limit: limit})
	if e != nil {
		return nil, e
	}
	out := make([][]byte, len(records))
	var materializer *collections.StoredDocumentJSONMaterializer
	if t.engine == "template-v1" {
		materializer, e = t.col.NewStoredDocumentJSONMaterializer()
		if e != nil {
			return nil, e
		}
	}
	for i, record := range records {
		raw := record.Document
		switch t.engine {
		case "bson":
			var doc map[string]any
			e = bson.Unmarshal(raw, &doc)
			if e == nil {
				raw, e = json.Marshal(doc)
			}
		case "template-v1":
			raw, e = materializer.StoredDocumentJSON(raw)
		}
		if e != nil {
			if materializer != nil {
				_ = materializer.Close()
			}
			return nil, e
		}
		out[i] = raw
	}
	if materializer != nil {
		e = materializer.Close()
	}
	return out, e
}
func (t *r1Tree) capabilities(fixture []r1Document) (map[string]string, error) {
	caps := map[string]string{"range_decomposition": "quiescent_ids_then_prepared_full_fetch", "ordinary_range": "complete_retained_document", "ordinary_point": "owned_complete_GetInto"}
	rejected := false
	for bucket := 0; bucket < 8; bucket++ {
		city := fmt.Sprintf("city-%02d", bucket)
		docs, e := t.rangeDocuments(city, 10)
		if e != nil {
			return nil, e
		}
		var expected []r1Document
		for _, d := range fixture {
			if d.City == city && len(expected) < 10 {
				expected = append(expected, d)
			}
		}
		if len(docs) != len(expected) {
			return nil, errors.New("ordinary range capability row count mismatch")
		}
		for i, raw := range docs {
			if t.engine == "typed-row" {
				var got map[string]any
				if e = json.Unmarshal(raw, &got); e != nil {
					return nil, e
				}
				if _, exists := got["email"]; !exists {
					want, marshalErr := json.Marshal(expected[i])
					if marshalErr != nil {
						return nil, marshalErr
					}
					var residual map[string]any
					_ = json.Unmarshal(want, &residual)
					for _, field := range []string{"email", "city", "name", "bio"} {
						delete(residual, field)
					}
					if !reflect.DeepEqual(got, residual) {
						return nil, errors.New("ordinary range rejected output was not exact residual row")
					}
					rejected = true
					caps["ordinary_range_rejected_document"] = string(raw)
					continue
				}
			}
			if e = r1VerifyDocument(raw, expected[i]); e != nil {
				return nil, fmt.Errorf("ordinary range: %w", e)
			}
		}
	}
	if t.engine == "typed-row" {
		if rejected {
			caps["ordinary_range"] = "rejected_residual_only_full_row_parity_gap"
		} else {
			caps["ordinary_range"] = "complete_typed_row"
		}
	}
	return caps, nil
}

func r1VerifyIndexes(b r1Backend, r r1Reader, current []r1Document, emailHistory []string) error {
	if e := r1VerifyEmailIndex(current, emailHistory, b.emailIDs); e != nil {
		return e
	}
	for bucket := 0; bucket < 8; bucket++ {
		city := fmt.Sprintf("city-%02d", bucket)
		var expected []r1Document
		for _, d := range current {
			if d.City == city {
				expected = append(expected, d)
			}
		}
		sort.Slice(expected, func(i, j int) bool { return expected[i].ID < expected[j].ID })
		expected = expected[:min(10, len(expected))]
		ids, e := b.rangeIDs(city, 10)
		if e != nil {
			return e
		}
		if len(ids) != len(expected) {
			return errors.New("mutation index row count mismatch")
		}
		for i, id := range ids {
			if string(id) != expected[i].ID {
				return errors.New("mutation index ordering/membership mismatch")
			}
		}
		if e = r1VerifyRows(r, expected); e != nil {
			return e
		}
	}
	return nil
}

func r1VerifyEmailIndex(current []r1Document, history []string, lookup func(string) ([][]byte, error)) error {
	live := make(map[string]string, len(current))
	for _, d := range current {
		if _, exists := live[d.Email]; exists {
			return errors.New("duplicate email in oracle model")
		}
		live[d.Email] = d.ID
	}
	seen := make(map[string]bool, len(history)+len(current))
	for _, d := range current {
		seen[d.Email] = true
	}
	for _, email := range history {
		seen[email] = true
	}
	emails := make([]string, 0, len(seen))
	for email := range seen {
		emails = append(emails, email)
	}
	sort.Strings(emails)
	for _, email := range emails {
		ids, e := lookup(email)
		if e != nil {
			return e
		}
		id, present := live[email]
		if present {
			if len(ids) != 1 || string(ids[0]) != id {
				return fmt.Errorf("email index mapping mismatch for %s", email)
			}
		} else if len(ids) != 0 {
			return fmt.Errorf("historical email index entry remains for %s", email)
		}
	}
	return nil
}

func readR1JSON(path string, out any) error {
	raw, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if e = dec.Decode(out); e != nil {
		return e
	}
	var extra any
	if e = dec.Decode(&extra); e != io.EOF {
		return errors.New("trailing JSON input")
	}
	return nil
}
func validateR1Command(w io.Writer, args []string) error {
	f := flag.NewFlagSet("r1-validate", flag.ContinueOnError)
	sourcePath := f.String("source-manifest", "", "independent expected source manifest")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 1 || *sourcePath == "" {
		return errors.New("usage: r1-validate -source-manifest source.json packet.json")
	}
	var p r1Packet
	var source r1Source
	if e := readR1JSON(f.Arg(0), &p); e != nil {
		return e
	}
	if e := readR1JSON(*sourcePath, &source); e != nil {
		return e
	}
	if !reflect.DeepEqual(p.Source, source) {
		return errors.New("packet does not match frozen source manifest")
	}
	if e := validateR1Packet(p); e != nil {
		return e
	}
	_, e := fmt.Fprintln(w, "R1 packet valid:", p.Config.Qualification, p.FixtureSHA256)
	return e
}
