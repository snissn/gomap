// treedb_vector_partition_build consumes the repository-owned exporter corpus
// format and produces a bounded offline M2 artifact/report. It has no server,
// collection, Raft, or runtime-FFI dependency.
package main

import (
	"container/heap"
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
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const maxManifest = 1 << 20
const maxCorpusBytes int64 = 4 << 30
const maxQueries = 128
const maxQualityWork int64 = 10_000_000_000 // scalar dot-product dimensions
const normalizedTolerance = 1e-3

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
	LoadWallNanos             int64                   `json:"load_wall_nanos"`
	BuildWallNanos            int64                   `json:"build_wall_nanos"`
	BuildCPUNanos             *int64                  `json:"build_cpu_nanos,omitempty"`
	BuildCPUAvailable         bool                    `json:"build_cpu_available"`
	GraphBuildNanos           int64                   `json:"graph_build_nanos"`
	BackendPartitionNanos     int64                   `json:"backend_partition_nanos"`
	ValidationNanos           int64                   `json:"validation_nanos"`
	QualityWallNanos          int64                   `json:"quality_wall_nanos"`
	TotalCommandWallNanos     int64                   `json:"total_command_wall_nanos"`
	TotalCommandCPUNanos      *int64                  `json:"total_command_cpu_nanos,omitempty"`
	TotalCommandCPUAvailable  bool                    `json:"total_command_cpu_available"`
	PeakRSSBytes              *int64                  `json:"peak_rss_bytes,omitempty"`
	PeakRSSAvailable          bool                    `json:"peak_rss_available"`
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
	commandStart := time.Now()
	commandCPUStart, commandCPUAvailable := cpuNanos()
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
	m, err := loadManifest(dataset)
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
	const graphRecallSamples = 64
	if err := vectorpartition.ValidateReferenceInputShape(cfg, m.Docs, m.Dimensions); err != nil {
		return err
	}
	if err := validateQualityWork(m.Docs, min(m.Queries, maxQueries), m.Dimensions, min(graphRecallSamples, m.Docs)); err != nil {
		return err
	}
	loadStart := time.Now()
	vs, qs, err := loadCorpus(dataset, m)
	if err != nil {
		return err
	}
	loadWall := time.Since(loadStart).Nanoseconds()
	start := time.Now()
	cpuStart, buildCPUAvailable := cpuNanos()
	art, phases, err := vectorpartition.BuildWithPartitionerPhased(vs, cfg, vectorpartition.Source{SourceID: "exporter_manifest:" + manifestDigest(m)}, vectorpartition.ReferencePartitioner{})
	if err != nil {
		return err
	}
	wall := time.Since(start).Nanoseconds()
	var buildCPU *int64
	if cpuEnd, ok := cpuNanos(); buildCPUAvailable && ok {
		v := cpuEnd - cpuStart
		buildCPU = &v
	}
	digest, err := vectorpartition.Digest(art)
	if err != nil {
		return err
	}
	raw, err := vectorpartition.CanonicalJSON(art)
	if err != nil {
		return err
	}
	qualityStart := time.Now()
	gr := graphRecall(vs, art, min(graphRecallSamples, len(vs)))
	pr, hr := oracleRecall(vs, qs, art, probes, 10)
	if !passesOracleQualityGate(pr, hr, probes, partitions) {
		return fmt.Errorf("quality gate failed: partition oracle recall@10 %.4f <= stable hash %.4f at probes=%d", pr, hr, probes)
	}
	qualityWall := time.Since(qualityStart).Nanoseconds()
	if err := os.MkdirAll(out, 0755); err != nil {
		return err
	}
	name := "vector_partition_" + digest[:16] + ".json"
	path := filepath.Join(out, name)
	if err := os.WriteFile(path, raw, 0644); err != nil {
		return err
	}
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	bal := float64(art.Metrics.MaxPartitionSize) * float64(partitions) / float64(len(vs))
	peakRSSBytes, peakRSSAvailable := peakRSS()
	var peakRSSValue *int64
	if peakRSSAvailable {
		peakRSSValue = &peakRSSBytes
	}
	r := report{SchemaVersion: 1, ResultKind: "offline_partition_builder_exporter_v1", Dataset: m, Source: art.Source, Config: art.Config, Metrics: art.Metrics, ArtifactPath: path, LoadWallNanos: loadWall, BuildWallNanos: wall, BuildCPUNanos: buildCPU, BuildCPUAvailable: buildCPU != nil, GraphBuildNanos: phases.GraphBuildNanos, BackendPartitionNanos: phases.BackendPartitionNanos, ValidationNanos: phases.ValidationNanos, QualityWallNanos: qualityWall, PeakRSSBytes: peakRSSValue, PeakRSSAvailable: peakRSSAvailable, TemporaryBytes: 0, ArtifactBytes: st.Size(), BytesPerVector: float64(st.Size()) / float64(len(vs)), Balance: bal, GraphNeighborRecall: gr, GraphNeighborSamples: min(graphRecallSamples, len(vs)), PartitionOracleRecallAt10: pr, StableHashRecallAt10: hr, Probes: probes, ArtifactSHA256: digest}
	rr, err := json.Marshal(r)
	if err != nil {
		return err
	}
	reportPath := filepath.Join(out, "vector_partition_report_"+digest[:16]+".json")
	// Account for the report itself without embedding the artifact or writing a
	// second giant JSON stream. A bounded fixed-point pass stabilizes FinalBytes.
	converged := false
	for i := 0; i < 8; i++ {
		r.TotalCommandWallNanos = time.Since(commandStart).Nanoseconds()
		if commandCPUAvailable {
			if cpuEnd, ok := cpuNanos(); ok {
				v := cpuEnd - commandCPUStart
				r.TotalCommandCPUNanos = &v
				r.TotalCommandCPUAvailable = true
			} else {
				commandCPUAvailable = false
				r.TotalCommandCPUNanos = nil
				r.TotalCommandCPUAvailable = false
			}
		}
		r.ReportBytes = int64(len(rr))
		r.FinalBytes = r.ArtifactBytes + r.ReportBytes
		rr, err = json.Marshal(r)
		if err != nil {
			return err
		}
		if int64(len(rr)) == r.ReportBytes {
			converged = true
			break
		}
	}
	if !converged {
		return errors.New("report byte accounting did not converge")
	}
	if err = os.WriteFile(reportPath, rr, 0644); err != nil {
		return err
	}
	actual, err := os.Stat(reportPath)
	if err != nil {
		return err
	}
	if actual.Size() != r.ReportBytes || r.FinalBytes != r.ArtifactBytes+actual.Size() {
		return errors.New("report byte accounting mismatch")
	}
	fmt.Printf("partition artifact=%s report=%s digest=%s final_bytes=%d oracle_recall_at_10=%.4f hash_recall_at_10=%.4f\n", path, reportPath, digest, r.FinalBytes, pr, hr)
	return nil
}

// passesOracleQualityGate requires a strict improvement for partial routing.
// At full coverage both routes inspect every partition, so equality is the
// expected parity result rather than a quality failure.
func passesOracleQualityGate(oracle, stableHash float64, probes, partitions int) bool {
	if probes == partitions {
		return oracle >= stableHash
	}
	return oracle > stableHash
}
func load(dir string) (manifest, []vectorpartition.Vector, [][]float64, error) {
	m, err := loadManifest(dir)
	if err != nil {
		return m, nil, nil, err
	}
	vs, qs, err := loadCorpus(dir, m)
	return m, vs, qs, err
}

func loadManifest(dir string) (manifest, error) {
	f, e := os.Open(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return manifest{}, e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, maxManifest+1))
	if e != nil {
		return manifest{}, e
	}
	if len(raw) > maxManifest {
		return manifest{}, errors.New("manifest too large")
	}
	var m manifest
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if e = d.Decode(&m); e != nil {
		return m, e
	}
	var extra any
	if e = d.Decode(&extra); e != io.EOF {
		return m, errors.New("manifest has trailing JSON")
	}
	if m.Version != 1 || m.Generator != "treedb_vector_synthetic_v1" || m.Docs < 1 || m.Docs > 1_000_000 || m.Dimensions < 1 || m.Dimensions > 4096 || m.Queries < 1 || m.Queries > 1_000_000 || m.TopK < 1 || m.TopK > 1024 || m.GroupModulo < 1 || m.ExactTruthQueries < 0 || m.ExactTruthQueries > m.Queries || m.Metric != "cosine" || !m.Normalized || m.FloatFormat != "float32_le_row_major" || m.DocumentIDPattern != "doc-%06d" || !safeCorpusName(m.DocumentVectorsFile) || !safeCorpusName(m.QueryVectorsFile) || m.DocumentVectorsFile == m.QueryVectorsFile {
		return m, errors.New("unsupported exporter manifest")
	}
	if m.Files == nil {
		return m, errors.New("manifest files missing")
	}
	_, ok := m.Files[m.DocumentVectorsFile]
	_, qok := m.Files[m.QueryVectorsFile]
	if !ok || !qok {
		return m, errors.New("manifest corpus file entry missing")
	}
	for _, name := range []string{m.DocumentVectorsFile, m.QueryVectorsFile} {
		if !canonicalSHA256(m.Files[name].SHA256) {
			return m, errors.New("invalid corpus checksum")
		}
	}
	return m, nil
}

func loadCorpus(dir string, m manifest) ([]vectorpartition.Vector, [][]float64, error) {
	df := m.Files[m.DocumentVectorsFile]
	qf := m.Files[m.QueryVectorsFile]
	docs, e := readF32(filepath.Join(dir, m.DocumentVectorsFile), df, m.Docs, m.Dimensions)
	if e != nil {
		return nil, nil, e
	}
	if err := validateNormalized(docs, m.Docs, m.Dimensions); err != nil {
		return nil, nil, err
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
		q, e := readQueryF32(filepath.Join(dir, m.QueryVectorsFile), qf, m.Queries, m.Dimensions, qcount)
		if e != nil {
			return nil, nil, e
		}
		for i := 0; i < qcount; i++ {
			row := make([]float64, m.Dimensions)
			for j := range row {
				row[j] = float64(q[i*m.Dimensions+j])
			}
			qs = append(qs, row)
		}
	}
	return vs, qs, nil
}
func safeCorpusName(name string) bool {
	return name != "" && name != "." && name != ".." && !filepath.IsAbs(name) && filepath.Base(name) == name && !strings.Contains(name, "\\")
}
func readF32(path string, fi fileInfo, rows, dims int) ([]float32, error) {
	want, ok := checkedByteCount(rows, dims)
	if !ok || want > maxCorpusBytes || fi.Bytes != want || !canonicalSHA256(fi.SHA256) {
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

// readQueryF32 verifies every declared query row while retaining only the
// bounded quality prefix. Its fixed row buffer prevents query-file allocation
// from scaling with m.Queries.
func readQueryF32(path string, fi fileInfo, rows, dims, retainRows int) ([]float32, error) {
	want, ok := checkedByteCount(rows, dims)
	if !ok || want > maxCorpusBytes || fi.Bytes != want || !canonicalSHA256(fi.SHA256) || retainRows < 0 || retainRows > rows {
		return nil, errors.New("invalid corpus file bounds")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(f, want+1), h)
	rowBytes := make([]byte, dims*4)
	row := make([]float32, dims)
	retained := make([]float32, retainRows*dims)
	for i := 0; i < rows; i++ {
		if _, err := io.ReadFull(r, rowBytes); err != nil {
			return nil, errors.New("corpus truncated or oversized")
		}
		for j := range row {
			row[j] = math.Float32frombits(binary.LittleEndian.Uint32(rowBytes[4*j:]))
		}
		if err := validateNormalizedRow(row); err != nil {
			return nil, err
		}
		if i < retainRows {
			copy(retained[i*dims:(i+1)*dims], row)
		}
	}
	var extra [1]byte
	if n, err := r.Read(extra[:]); err != io.EOF || n != 0 {
		return nil, errors.New("corpus truncated or oversized")
	}
	if hex.EncodeToString(h.Sum(nil)) != fi.SHA256 {
		return nil, errors.New("corpus checksum mismatch")
	}
	return retained, nil
}

// canonicalSHA256 accepts only the lower-case, fixed-width hex form used by
// exporter manifests. Decode and re-encode so length/casing checks cannot
// admit malformed non-hex metadata before corpus I/O.
func canonicalSHA256(s string) bool {
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == s
}

func checkedByteCount(rows, dims int) (int64, bool) {
	if rows < 1 || dims < 1 || int64(rows) > math.MaxInt64/int64(dims) || int64(rows)*int64(dims) > math.MaxInt64/4 {
		return 0, false
	}
	return int64(rows) * int64(dims) * 4, true
}
func validateNormalized(values []float32, rows, dims int) error {
	if len(values) != rows*dims {
		return errors.New("corpus shape mismatch")
	}
	for row := 0; row < rows; row++ {
		if err := validateNormalizedRow(values[row*dims : (row+1)*dims]); err != nil {
			return err
		}
	}
	return nil
}

func validateNormalizedRow(row []float32) error {
	var norm float64
	for _, value := range row {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return errors.New("corpus contains non-finite vector")
		}
		norm = math.FMA(float64(value), float64(value), norm)
	}
	if math.Abs(math.Sqrt(norm)-1) > normalizedTolerance {
		return errors.New("corpus vector is not unit-normalized")
	}
	return nil
}
func validateQualityWork(docs, queries, dims, samples int) error {
	// graph recall scans one corpus per sample; each oracle query performs the
	// global truth scan plus graph and hash selected-partition scans.
	if exceedsProduct(maxQualityWork, int64(docs), int64(dims), int64(samples+3*queries)) {
		return errors.New("quality scalar-work bound exceeded")
	}
	return nil
}
func exceedsProduct(limit int64, values ...int64) bool {
	p := int64(1)
	for _, value := range values {
		if value < 0 || value != 0 && p > limit/value {
			return true
		}
		p *= value
	}
	return p > limit
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
	a := make(candidateMaxHeap, 0, k)
	for i, x := range v {
		if include(i) {
			c := candidate{i, 1 - dot(x.Values, q)}
			if len(a) < k {
				heap.Push(&a, c)
			} else if candidateLess(c, a[0]) {
				a[0] = c
				heap.Fix(&a, 0)
			}
		}
	}
	sort.Slice(a, func(i, j int) bool { return candidateLess(a[i], a[j]) })
	o := make([]int, len(a))
	for i := range a {
		o[i] = a[i].i
	}
	return o
}

type candidate struct {
	i int
	d float64
}
type candidateMaxHeap []candidate

func (h candidateMaxHeap) Len() int           { return len(h) }
func (h candidateMaxHeap) Less(i, j int) bool { return candidateLess(h[j], h[i]) }
func (h candidateMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *candidateMaxHeap) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *candidateMaxHeap) Pop() any          { o := *h; x := o[len(o)-1]; *h = o[:len(o)-1]; return x }
func candidateLess(a, b candidate) bool       { return a.d < b.d || a.d == b.d && a.i < b.i }
func graphRecall(v []vectorpartition.Vector, a vectorpartition.Artifact, n int) float64 {
	if n == 0 {
		return 0
	}
	sum := 0.0
	valid := 0
	for i := 0; i < n; i++ {
		want := nearest(v, v[i].Values, min(10, len(v)-1), func(j int) bool { return j != i })
		if len(want) == 0 {
			continue
		}
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
		valid++
	}
	if valid == 0 {
		return 0
	}
	return sum / float64(valid)
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
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
