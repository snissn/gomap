// treedb_vector_partition_build consumes the repository-owned exporter corpus
// format and produces a bounded offline M2 artifact/report. It has no server,
// collection, Raft, or runtime-FFI dependency.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const maxManifest = 1 << 20
const maxCorpusBytes int64 = 4 << 30
const maxQueries = 128

type fileInfo struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type manifest struct {
	Version             int                 `json:"version"`
	Generator           string              `json:"generator"`
	CreatedAt           string              `json:"created_at"`
	Docs                int                 `json:"docs"`
	Dimensions          int                 `json:"dimensions"`
	Queries             int                 `json:"queries"`
	TopK                int                 `json:"top_k"`
	Metric              string              `json:"metric"`
	Normalized          bool                `json:"normalized"`
	DocumentIDPattern   string              `json:"document_id_pattern"`
	GroupModulo         int                 `json:"group_modulo"`
	DocumentVectorsFile string              `json:"document_vectors_file"`
	QueryVectorsFile    string              `json:"query_vectors_file"`
	DocumentsJSONLFile  string              `json:"documents_jsonl_file"`
	QueriesJSONLFile    string              `json:"queries_jsonl_file"`
	FloatFormat         string              `json:"float_format"`
	ExactTruthFile      string              `json:"exact_truth_file"`
	ExactTruthKind      string              `json:"exact_truth_kind"`
	ExactTruthQueries   int                 `json:"exact_truth_queries"`
	Files               map[string]fileInfo `json:"files"`
}
type report struct {
	SchemaVersion             int                     `json:"schema_version"`
	ResultKind                string                  `json:"result_kind"`
	Dataset                   manifest                `json:"dataset"`
	Source                    vectorpartition.Source  `json:"source"`
	Config                    vectorpartition.Config  `json:"config"`
	Metrics                   vectorpartition.Metrics `json:"metrics"`
	ArtifactPath              string                  `json:"artifact_path"`
	BuildWallNanos            int64                   `json:"build_wall_nanos"`
	BuildCPUNanos             int64                   `json:"build_cpu_nanos"`
	GraphBuildNanos           int64                   `json:"graph_build_nanos"`
	BackendPartitionNanos     int64                   `json:"backend_partition_nanos"`
	ValidationNanos           int64                   `json:"validation_nanos"`
	PeakRSSBytes              int64                   `json:"peak_rss_bytes"`
	TemporaryBytes            int64                   `json:"temporary_bytes"`
	ArtifactBytes             int64                   `json:"artifact_bytes"`
	ReportBytes               int64                   `json:"report_bytes"`
	FinalBytes                int64                   `json:"final_bytes"`
	BytesPerVector            float64                 `json:"bytes_per_vector"`
	Balance                   float64                 `json:"balance"`
	GraphNeighborRecall       float64                 `json:"graph_neighbor_recall_sample"`
	GraphNeighborSamples      int                     `json:"graph_neighbor_samples"`
	PartitionOracleRecallAt10 float64                 `json:"partition_oracle_recall_at_10"`
	StableHashRecallAt10      float64                 `json:"stable_hash_recall_at_10"`
	Probes                    int                     `json:"probes"`
	ArtifactSHA256            string                  `json:"artifact_sha256"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "treedb-vector-partition-build:", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	var dataset, out string
	var partitions, probes, reps, pivots, leaf, degree int
	var imbalance float64
	var seed int64
	fs := flag.NewFlagSet("treedb_vector_partition_build", flag.ContinueOnError)
	fs.StringVar(&dataset, "dataset", "", "exporter dataset directory")
	fs.StringVar(&out, "out", "", "artifact output directory")
	fs.IntVar(&partitions, "partitions", 16, "partition count")
	fs.IntVar(&probes, "probes", 2, "fixed routing probe budget")
	fs.Int64Var(&seed, "seed", 1, "graph seed")
	fs.IntVar(&reps, "repetitions", 4, "graph repetitions")
	fs.IntVar(&pivots, "pivots", 8, "pivots")
	fs.IntVar(&leaf, "max-leaf-bucket", 128, "leaf cap")
	fs.IntVar(&degree, "degree", 16, "degree cap")
	fs.Float64Var(&imbalance, "imbalance", .05, "imbalance epsilon")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if dataset == "" || out == "" || probes < 1 || probes > partitions {
		return errors.New("dataset/out and probes within partitions required")
	}
	m, vs, qs, err := load(dataset)
	if err != nil {
		return err
	}
	cfg := vectorpartition.DefaultConfig()
	cfg.Partitions = partitions
	cfg.Seed = seed
	cfg.Repetitions = reps
	cfg.Pivots = pivots
	cfg.MaxLeafBucket = leaf
	cfg.Degree = degree
	cfg.Imbalance = imbalance
	start := time.Now()
	cpuStart := cpuNanos()
	art, phases, err := vectorpartition.BuildWithPartitionerPhased(vs, cfg, vectorpartition.Source{SourceID: "exporter_manifest:" + manifestDigest(m)}, vectorpartition.ReferencePartitioner{})
	if err != nil {
		return err
	}
	wall := time.Since(start).Nanoseconds()
	digest, err := vectorpartition.Digest(art)
	if err != nil {
		return err
	}
	raw, err := vectorpartition.CanonicalJSON(art)
	if err != nil {
		return err
	}
	const graphRecallSamples = 64
	gr := graphRecall(vs, art, min(graphRecallSamples, len(vs)))
	pr, hr := oracleRecall(vs, qs, art, probes, 10)
	if pr <= hr {
		return fmt.Errorf("quality gate failed: partition oracle recall@10 %.4f <= stable hash %.4f at probes=%d", pr, hr, probes)
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		return err
	}
	name := "vector_partition_" + digest[:16] + ".json"
	path := filepath.Join(out, name)
	if err := os.WriteFile(path, raw, 0644); err != nil {
		return err
	}
	st, _ := os.Stat(path)
	cap := art.Metrics.Cap
	bal := float64(art.Metrics.MaxPartitionSize) * float64(partitions) / float64(len(vs))
	r := report{SchemaVersion: 1, ResultKind: "offline_partition_builder_exporter_v1", Dataset: m, Source: art.Source, Config: art.Config, Metrics: art.Metrics, ArtifactPath: path, BuildWallNanos: wall, BuildCPUNanos: cpuNanos() - cpuStart, GraphBuildNanos: phases.GraphBuildNanos, BackendPartitionNanos: phases.BackendPartitionNanos, ValidationNanos: phases.ValidationNanos, PeakRSSBytes: peakRSS(), TemporaryBytes: 0, ArtifactBytes: st.Size(), BytesPerVector: float64(st.Size()) / float64(len(vs)), Balance: bal, GraphNeighborRecall: gr, GraphNeighborSamples: min(graphRecallSamples, len(vs)), PartitionOracleRecallAt10: pr, StableHashRecallAt10: hr, Probes: probes, ArtifactSHA256: digest}
	rr, err := json.Marshal(r)
	if err != nil {
		return err
	}
	reportPath := filepath.Join(out, "vector_partition_report_"+digest[:16]+".json")
	// Account for the report itself without embedding the artifact or writing a
	// second giant JSON stream. A bounded fixed-point pass stabilizes FinalBytes.
	for i := 0; i < 2; i++ {
		r.ReportBytes = int64(len(rr))
		r.FinalBytes = r.ArtifactBytes + r.ReportBytes
		rr, err = json.Marshal(r)
		if err != nil {
			return err
		}
	}
	if err = os.WriteFile(reportPath, rr, 0644); err != nil {
		return err
	}
	fmt.Printf("partition artifact=%s report=%s digest=%s final_bytes=%d oracle_recall_at_10=%.4f hash_recall_at_10=%.4f\n", path, reportPath, digest, r.FinalBytes, pr, hr)
	_ = cap
	return nil
}
func load(dir string) (manifest, []vectorpartition.Vector, [][]float64, error) {
	raw, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return manifest{}, nil, nil, e
	}
	if len(raw) > maxManifest {
		return manifest{}, nil, nil, errors.New("manifest too large")
	}
	var m manifest
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if e = d.Decode(&m); e != nil {
		return m, nil, nil, e
	}
	if m.Version != 1 || m.Generator != "treedb_vector_synthetic_v1" || m.Docs < 1 || m.Docs > 1_000_000 || m.Dimensions < 1 || m.Dimensions > 4096 || m.Metric != "cosine" || m.FloatFormat != "float32_le_row_major" || m.DocumentIDPattern != "doc-%06d" {
		return m, nil, nil, errors.New("unsupported exporter manifest")
	}
	docs, e := readF32(filepath.Join(dir, m.DocumentVectorsFile), m.Files[m.DocumentVectorsFile], m.Docs, m.Dimensions)
	if e != nil {
		return m, nil, nil, e
	}
	vs := make([]vectorpartition.Vector, m.Docs)
	for i := range vs {
		row := make([]float64, m.Dimensions)
		for j := range row {
			row[j] = float64(docs[i*m.Dimensions+j])
		}
		vs[i] = vectorpartition.Vector{ID: fmt.Sprintf("doc-%06d", i), Values: row}
	}
	qcount := min(m.Queries, maxQueries)
	qs := [][]float64{}
	if qcount > 0 {
		q, e := readF32(filepath.Join(dir, m.QueryVectorsFile), m.Files[m.QueryVectorsFile], m.Queries, m.Dimensions)
		if e != nil {
			return m, nil, nil, e
		}
		for i := 0; i < qcount; i++ {
			row := make([]float64, m.Dimensions)
			for j := range row {
				row[j] = float64(q[i*m.Dimensions+j])
			}
			qs = append(qs, row)
		}
	}
	return m, vs, qs, nil
}
func readF32(path string, fi fileInfo, rows, dims int) ([]float32, error) {
	want := int64(rows) * int64(dims) * 4
	if want < 1 || want > maxCorpusBytes || fi.Bytes != want || len(fi.SHA256) != 64 {
		return nil, errors.New("invalid corpus file bounds")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(f, want+1), h)
	b, e := io.ReadAll(r)
	if e != nil || int64(len(b)) != want {
		return nil, errors.New("corpus truncated or oversized")
	}
	if hex.EncodeToString(h.Sum(nil)) != fi.SHA256 {
		return nil, errors.New("corpus checksum mismatch")
	}
	a := make([]float32, rows*dims)
	for i := range a {
		a[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return a, nil
}
func manifestDigest(m manifest) string {
	b, _ := json.Marshal(m)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func dot(a, b []float64) float64 {
	var x float64
	for i := range a {
		x = math.FMA(a[i], b[i], x)
	}
	return x
}
func nearest(v []vectorpartition.Vector, q []float64, k int, include func(int) bool) []int {
	type c struct {
		i int
		d float64
	}
	a := make([]c, 0)
	for i, x := range v {
		if include(i) {
			a = append(a, c{i, 1 - dot(x.Values, q)})
		}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].d < a[j].d || a[i].d == a[j].d && a[i].i < a[j].i })
	if len(a) > k {
		a = a[:k]
	}
	o := make([]int, len(a))
	for i := range a {
		o[i] = a[i].i
	}
	return o
}
func graphRecall(v []vectorpartition.Vector, a vectorpartition.Artifact, n int) float64 {
	if n == 0 {
		return 0
	}
	sum := 0.0
	for i := 0; i < n; i++ {
		want := nearest(v, v[i].Values, min(10, len(v)-1), func(j int) bool { return j != i })
		seen := map[int]bool{}
		for _, j := range a.Graph.Neighbors[i] {
			seen[j] = true
		}
		hit := 0
		for _, j := range want {
			if seen[j] {
				hit++
			}
		}
		sum += float64(hit) / float64(len(want))
	}
	return sum / float64(n)
}

// oracleRecall intentionally has no centroid/router step: it uses the global
// exact top-k truth to select the partitions holding most truth neighbors,
// with stable partition-ID ties. That isolates M2 assignment quality from M4.
func oracleRecall(v []vectorpartition.Vector, qs [][]float64, a vectorpartition.Artifact, probes, k int) (float64, float64) {
	if len(qs) == 0 {
		return 0, 0
	}
	hassign := make([]int, len(v))
	for i, x := range v {
		s := sha256.Sum256([]byte(x.ID))
		hassign[i] = int(binary.BigEndian.Uint64(s[:8]) % uint64(a.Config.Partitions))
	}
	var pr, hr float64
	for _, q := range qs {
		truth := nearest(v, q, k, func(int) bool { return true })
		p := oracleParts(truth, a.Assignment, a.Config.Partitions, probes)
		h := oracleParts(truth, hassign, a.Config.Partitions, probes)
		pr += recall(truth, nearest(v, q, k, func(i int) bool {
			for _, x := range p {
				if a.Assignment[i] == x {
					return true
				}
			}
			return false
		}))
		hr += recall(truth, nearest(v, q, k, func(i int) bool {
			for _, x := range h {
				if hassign[i] == x {
					return true
				}
			}
			return false
		}))
	}
	return pr / float64(len(qs)), hr / float64(len(qs))
}
func oracleParts(truth, assignment []int, partitions, probes int) []int {
	counts := make([]int, partitions)
	for _, i := range truth {
		counts[assignment[i]]++
	}
	order := make([]int, partitions)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return counts[order[i]] > counts[order[j]] || counts[order[i]] == counts[order[j]] && order[i] < order[j]
	})
	return order[:probes]
}
func centroids(v []vectorpartition.Vector, a []int, p int) [][]float64 {
	o := make([][]float64, p)
	n := make([]int, p)
	for i, x := range v {
		if o[a[i]] == nil {
			o[a[i]] = make([]float64, len(x.Values))
		}
		for d := range x.Values {
			o[a[i]][d] += x.Values[d]
		}
		n[a[i]]++
	}
	for i := range o {
		for d := range o[i] {
			o[i][d] /= float64(n[i])
		}
	}
	return o
}
func topParts(c [][]float64, q []float64, k int) []int {
	type x struct {
		i int
		d float64
	}
	a := make([]x, len(c))
	for i := range c {
		a[i] = x{i, 1 - dot(c[i], q)}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].d < a[j].d })
	o := make([]int, k)
	for i := range o {
		o[i] = a[i].i
	}
	return o
}
func recall(a, b []int) float64 {
	s := map[int]bool{}
	for _, x := range b {
		s[x] = true
	}
	n := 0
	for _, x := range a {
		if s[x] {
			n++
		}
	}
	return float64(n) / float64(len(a))
}
func peakRSS() int64 {
	runtime.GC()
	b, e := os.ReadFile("/proc/self/status")
	if e != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmHWM:") {
			var kb int64
			fmt.Sscanf(l, "VmHWM: %d kB", &kb)
			return kb * 1024
		}
	}
	return 0
}
func cpuNanos() int64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + (r.Utime.Usec+r.Stime.Usec)*1e3
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
