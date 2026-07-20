// Package vectorpartition builds a bounded, deterministic offline vector graph
// and a disjoint balanced partition artifact. It is deliberately independent
// of TreeDB collection and Raft lifecycle code.
package vectorpartition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
)

const (
	SchemaVersion   = 1
	maxVectors      = 1_000_000
	maxDimensions   = 4096
	maxEdges        = 64_000_000
	maxRepetitions  = 32
	maxPivots       = 1024
	maxLeafBucket   = 65536
	maxDegree       = 1024
	maxPartitions   = 16384
	maxIDBytes      = 1 << 20
	maxDistanceWork = int64(1_000_000_000)
)

// Vector IDs must be unique and are canonically ordered before construction.
type Vector struct {
	ID     string
	Values []float64
}

// Config makes the clean-room sketch controls explicit. The reference backend
// is intentionally in-process and deterministic, so CI never needs a native
// partitioner or runtime FFI.
type Config struct {
	Metric        string  `json:"metric"`
	Seed          int64   `json:"seed"`
	Repetitions   int     `json:"repetitions"`
	Pivots        int     `json:"pivots"`
	MaxLeafBucket int     `json:"max_leaf_bucket"`
	Degree        int     `json:"degree"`
	Partitions    int     `json:"partitions"`
	Imbalance     float64 `json:"imbalance"`
	Symmetric     bool    `json:"symmetric"`
	MaxVectors    int     `json:"max_vectors"`
	MaxEdges      int     `json:"max_edges"`
}

func DefaultConfig() Config {
	return Config{Metric: "cosine", Seed: 1, Repetitions: 4, Pivots: 8, MaxLeafBucket: 128, Degree: 16, Partitions: 16, Imbalance: .05, Symmetric: false, MaxVectors: maxVectors, MaxEdges: maxEdges}
}

type Graph struct {
	Neighbors [][]int `json:"neighbors"`
}
type Metrics struct {
	GraphEdges          int `json:"graph_edges"`
	MaxDegree           int `json:"max_degree"`
	EdgeCut             int `json:"edge_cut"`
	StableIDHashEdgeCut int `json:"stable_id_hash_edge_cut"`
	MaxPartitionSize    int `json:"max_partition_size"`
	Cap                 int `json:"cap"`
}

// Source binds an artifact to the exact immutable input snapshot. SourceID is
// caller supplied (for example exporter manifest digest); Checksum is computed
// over canonical stable-ID/vector bits and never accepted on trust.
type Source struct {
	SourceID   string `json:"source_id"`
	Checksum   string `json:"checksum"`
	Vectors    int    `json:"vectors"`
	Dimensions int    `json:"dimensions"`
	Metric     string `json:"metric"`
}
type Artifact struct {
	SchemaVersion  int      `json:"schema_version"`
	Backend        string   `json:"backend"`
	BackendLicense string   `json:"backend_license"`
	Source         Source   `json:"source"`
	Config         Config   `json:"config"`
	IDs            []string `json:"ids"`
	Graph          Graph    `json:"graph"`
	Assignment     []int    `json:"assignment"`
	Metrics        Metrics  `json:"metrics"`
}

// Partitioner is the deliberately narrow backend seam. Implementations return
// ordinals only; ValidateArtifact independently verifies every invariant.
type Partitioner interface {
	Name() string
	Partition(Graph, int, int) ([]int, error)
}
type ReferencePartitioner struct{}

func (ReferencePartitioner) Name() string { return "treedb_reference_greedy_v1" }

func Build(vectors []Vector, cfg Config) (Artifact, error) {
	return BuildWithPartitioner(vectors, cfg, Source{SourceID: "inline_vectors_v1"}, ReferencePartitioner{})
}

// BuildWithPartitioner is the usable offline backend seam. The supplied
// partitioner's result is never trusted: graph, assignment, source and metrics
// are independently validated before the artifact is returned.
func BuildWithPartitioner(vectors []Vector, cfg Config, source Source, backend Partitioner) (Artifact, error) {
	if backend == nil || backend.Name() == "" {
		return Artifact{}, errors.New("partition backend identity is required")
	}
	if err := validateInput(vectors, cfg); err != nil {
		return Artifact{}, err
	}
	v := append([]Vector(nil), vectors...)
	sort.Slice(v, func(i, j int) bool { return v[i].ID < v[j].ID })
	for i := 1; i < len(v); i++ {
		if v[i].ID == v[i-1].ID {
			return Artifact{}, fmt.Errorf("duplicate vector ID %q", v[i].ID)
		}
	}
	g, err := buildGraph(v, cfg)
	if err != nil {
		return Artifact{}, err
	}
	cap := partitionCap(len(v), cfg.Partitions, cfg.Imbalance)
	a, err := backend.Partition(g, cfg.Partitions, cap)
	if err != nil {
		return Artifact{}, err
	}
	ids := make([]string, len(v))
	for i := range v {
		ids[i] = v[i].ID
	}
	computed := sourceFor(v, cfg.Metric, source.SourceID)
	if source.Checksum != "" && source != computed {
		return Artifact{}, errors.New("source identity does not match input snapshot")
	}
	art := Artifact{SchemaVersion: SchemaVersion, Backend: backend.Name(), BackendLicense: "TreeDB clean-room reference implementation (repository license)", Source: computed, Config: cfg, IDs: ids, Graph: g, Assignment: a}
	art.Metrics = metrics(art)
	if err := ValidateArtifact(art); err != nil {
		return Artifact{}, err
	}
	return art, nil
}

func sourceFor(v []Vector, metric, id string) Source {
	h := sha256.New()
	var b [8]byte
	for _, x := range v {
		h.Write([]byte(x.ID))
		h.Write([]byte{0})
		for _, f := range x.Values {
			binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
			h.Write(b[:])
		}
	}
	return Source{SourceID: id, Checksum: hex.EncodeToString(h.Sum(nil)), Vectors: len(v), Dimensions: len(v[0].Values), Metric: metric}
}

func validateInput(v []Vector, c Config) error {
	if c.Metric != "cosine" || c.Repetitions < 1 || c.Repetitions > maxRepetitions || c.Pivots < 2 || c.Pivots > maxPivots || c.MaxLeafBucket < 2 || c.MaxLeafBucket > maxLeafBucket || c.Degree < 1 || c.Degree > maxDegree || c.Partitions < 1 || c.Partitions > maxPartitions || !finite(c.Imbalance) || c.Imbalance < 0 || c.Imbalance > 1 || c.MaxVectors < 1 || c.MaxVectors > maxVectors || c.MaxEdges < 1 || c.MaxEdges > maxEdges {
		return errors.New("invalid vector partition configuration")
	}
	if len(v) < c.Partitions || len(v) > c.MaxVectors {
		return errors.New("vector count outside configured bounds")
	}
	dims := 0
	for _, x := range v {
		if x.ID == "" || len(x.ID) > maxIDBytes {
			return errors.New("empty vector ID")
		}
		if dims == 0 {
			dims = len(x.Values)
			if dims < 1 || dims > maxDimensions {
				return errors.New("invalid vector dimensions")
			}
		}
		if len(x.Values) != dims {
			return errors.New("wrong vector dimension")
		}
		for _, n := range x.Values {
			if !finite(n) {
				return errors.New("non-finite vector value")
			}
		}
	}
	if int64(len(v))*int64(c.Degree) > int64(c.MaxEdges) || int64(len(v))*int64(c.Degree) > int64(c.MaxEdges)/int64(c.Repetitions) || int64(len(v))*int64(c.MaxLeafBucket)*int64(c.Repetitions) > maxDistanceWork {
		return errors.New("configured graph edge bound exceeded before allocation")
	}
	return nil
}
func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

func buildGraph(v []Vector, c Config) (Graph, error) {
	n := len(v)
	sets := make([]map[int]float64, n)
	for i := range sets {
		sets[i] = make(map[int]float64, c.Degree)
	}
	for rep := 0; rep < c.Repetitions; rep++ {
		r := rand.New(rand.NewSource(c.Seed + int64(rep)*0x9e3779b))
		order := r.Perm(n)
		if err := carveDepth(v, order, c, sets, 0); err != nil {
			return Graph{}, err
		}
	}
	g := Graph{Neighbors: make([][]int, n)}
	for i, s := range sets {
		for j := range s {
			if i != j {
				g.Neighbors[i] = append(g.Neighbors[i], j)
			}
		}
		sort.Ints(g.Neighbors[i])
		if len(g.Neighbors[i]) > c.Degree {
			g.Neighbors[i] = g.Neighbors[i][:c.Degree]
		}
	}
	if c.Symmetric {
		return Graph{}, errors.New("symmetric graph policy is not supported by bounded reference backend")
	}
	return g, nil
}
func carveDepth(v []Vector, ids []int, c Config, sets []map[int]float64, depth int) error {
	if len(ids) <= c.MaxLeafBucket {
		for _, i := range ids {
			nearest := nearest(v, ids, i, c.Degree)
			for _, candidate := range nearest {
				addCandidateBounded(sets[i], candidate.i, candidate.x, c.Degree)
			}
		}
		return nil
	}
	k := min(c.Pivots, len(ids))
	pivots := append([]int(nil), ids[:k]...)
	buckets := make([][]int, k)
	for _, i := range ids {
		best, second := 0, -1
		bestD := distance(v[i].Values, v[pivots[0]].Values)
		for p := 1; p < k; p++ {
			d := distance(v[i].Values, v[pivots[p]].Values)
			if d < bestD || d == bestD && pivots[p] < pivots[best] {
				second, best, bestD = best, p, d
			} else if second < 0 || d < distance(v[i].Values, v[pivots[second]].Values) || d == distance(v[i].Values, v[pivots[second]].Values) && pivots[p] < pivots[second] {
				second = p
			}
		}
		buckets[best] = append(buckets[best], i)
		// The paper sketch allows one-or-more nearest top-level pivots. The
		// reference intentionally adds exactly one bounded extra membership at
		// depth zero; deeper levels use one pivot to keep work bounded.
		if depth == 0 && second >= 0 {
			buckets[second] = append(buckets[second], i)
		}
	}
	for _, b := range buckets {
		if len(b) == len(ids) { // duplicate/skew progress guarantee: deterministic chunking.
			for start := 0; start < len(b); start += c.MaxLeafBucket {
				end := min(start+c.MaxLeafBucket, len(b))
				if err := carveDepth(v, b[start:end], c, sets, depth+1); err != nil {
					return err
				}
			}
			continue
		}
		if len(b) > 0 {
			if err := carveDepth(v, b, c, sets, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
func nearest(v []Vector, ids []int, target, k int) []candidate {
	type d struct {
		i int
		x float64
	}
	a := make([]d, 0, len(ids)-1)
	for _, j := range ids {
		if j != target {
			a = append(a, d{j, distance(v[target].Values, v[j].Values)})
		}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].x < a[j].x || a[i].x == a[j].x && a[i].i < a[j].i })
	if len(a) > k {
		a = a[:k]
	}
	out := make([]candidate, len(a))
	for i := range a {
		out[i] = candidate{a[i].i, a[i].x}
	}
	return out
}
func distance(a, b []float64) float64 {
	var dot, na, nb float64
	for i := range a {
		dot = math.FMA(a[i], b[i], dot)
		na = math.FMA(a[i], a[i], na)
		nb = math.FMA(b[i], b[i], nb)
	}
	if na == 0 || nb == 0 {
		return 1
	}
	return 1 - dot/math.Sqrt(na*nb)
}

type candidate struct {
	i int
	x float64
}

func addCandidateBounded(s map[int]float64, x int, d float64, cap int) {
	if old, ok := s[x]; ok {
		if d < old {
			s[x] = d
		}
		return
	}
	if len(s) < cap {
		s[x] = d
		return
	}
	worst := -1
	worstD := -1.0
	for i, v := range s {
		if v > worstD || v == worstD && i > worst {
			worst, worstD = i, v
		}
	}
	if d < worstD || d == worstD && x < worst {
		delete(s, worst)
		s[x] = d
	}
}

func (ReferencePartitioner) Partition(g Graph, parts, cap int) ([]int, error) {
	n := len(g.Neighbors)
	if parts < 1 || cap < 1 || n < parts {
		return nil, errors.New("invalid partition request")
	}
	if err := validateGraph(g, n, maxVectors); err != nil {
		return nil, err
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		return len(g.Neighbors[a]) > len(g.Neighbors[b]) || len(g.Neighbors[a]) == len(g.Neighbors[b]) && a < b
	})
	out := make([]int, n)
	for i := range out {
		out[i] = -1
	}
	loads := make([]int, parts)
	for _, node := range order {
		best, bscore := -1, -1
		for p := 0; p < parts; p++ {
			if loads[p] >= cap {
				continue
			}
			score := 0
			for _, to := range g.Neighbors[node] {
				if out[to] == p {
					score++
				}
			}
			if score > bscore || score == bscore && (best < 0 || loads[p] < loads[best] || loads[p] == loads[best] && p < best) {
				best, bscore = p, score
			}
		}
		if best < 0 {
			return nil, errors.New("partition cap prevents exact coverage")
		}
		out[node] = best
		loads[best]++
	}
	return out, nil
}
func partitionCap(n, p int, e float64) int { return int(math.Ceil((1 + e) * float64(n) / float64(p))) }
func metrics(a Artifact) Metrics {
	m := Metrics{Cap: partitionCap(len(a.IDs), a.Config.Partitions, a.Config.Imbalance)}
	for i, ns := range a.Graph.Neighbors {
		m.GraphEdges += len(ns)
		if len(ns) > m.MaxDegree {
			m.MaxDegree = len(ns)
		}
		for _, j := range ns {
			if a.Assignment[i] != a.Assignment[j] {
				m.EdgeCut++
			}
			if stableIDPartition(a.IDs[i], a.Config.Partitions) != stableIDPartition(a.IDs[j], a.Config.Partitions) {
				m.StableIDHashEdgeCut++
			}
		}
	}
	for p := 0; p < a.Config.Partitions; p++ {
		n := 0
		for _, x := range a.Assignment {
			if x == p {
				n++
			}
		}
		if n > m.MaxPartitionSize {
			m.MaxPartitionSize = n
		}
	}
	return m
}
func stableIDPartition(id string, partitions int) int {
	sum := sha256.Sum256([]byte(id))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(partitions))
}

func ValidateArtifact(a Artifact) error {
	if a.SchemaVersion != SchemaVersion || a.Backend == "" || len(a.Backend) > 256 || a.BackendLicense == "" || len(a.BackendLicense) > 1024 || a.Source.SourceID == "" || len(a.Source.SourceID) > 1024 || len(a.Source.Checksum) != 64 || a.Source.Vectors != len(a.IDs) || a.Source.Dimensions < 1 || a.Source.Metric != a.Config.Metric || len(a.IDs) == 0 || len(a.IDs) != len(a.Graph.Neighbors) || len(a.IDs) != len(a.Assignment) {
		return errors.New("malformed partition artifact")
	}
	if err := validateInput(makeVectors(a.IDs, a.Graph.Neighbors), a.Config); err != nil {
		return fmt.Errorf("artifact config: %w", err)
	}
	if err := validateGraph(a.Graph, len(a.IDs), a.Config.Degree); err != nil {
		return err
	}
	cap := partitionCap(len(a.IDs), a.Config.Partitions, a.Config.Imbalance)
	loads := make([]int, a.Config.Partitions)
	for i, id := range a.IDs {
		if id == "" || i > 0 && id <= a.IDs[i-1] {
			return errors.New("IDs not unique canonical order")
		}
		p := a.Assignment[i]
		if p < 0 || p >= a.Config.Partitions {
			return errors.New("assignment out of range")
		}
		loads[p]++
	}
	if _, err := hex.DecodeString(a.Source.Checksum); err != nil {
		return errors.New("invalid source checksum")
	}
	for _, n := range loads {
		if n > cap {
			return errors.New("partition cap exceeded")
		}
	}
	m := metrics(a)
	if a.Metrics.GraphEdges != m.GraphEdges || a.Metrics.MaxDegree != m.MaxDegree || a.Metrics.EdgeCut != m.EdgeCut || a.Metrics.StableIDHashEdgeCut != m.StableIDHashEdgeCut || a.Metrics.MaxPartitionSize != m.MaxPartitionSize || a.Metrics.Cap != m.Cap {
		return errors.New("artifact metrics do not match graph and assignment")
	}
	return nil
}
func validateGraph(g Graph, n, degree int) error {
	if len(g.Neighbors) != n {
		return errors.New("graph node count mismatch")
	}
	for i, ns := range g.Neighbors {
		if len(ns) > degree {
			return errors.New("degree cap exceeded")
		}
		previous := -1
		for _, j := range ns {
			if j < 0 || j >= n || j == i || j <= previous {
				return errors.New("invalid graph edge")
			}
			previous = j
		}
	}
	return nil
}

// makeVectors supplies structural validation without allocating payload-sized copies.
func makeVectors(ids []string, g [][]int) []Vector {
	v := make([]Vector, len(ids))
	for i := range v {
		v[i] = Vector{ID: ids[i], Values: []float64{0}}
	}
	return v
}
func CanonicalJSON(a Artifact) ([]byte, error) {
	if err := ValidateArtifact(a); err != nil {
		return nil, err
	}
	return json.Marshal(a)
}

// DecodeArtifact is a strict bounded decoder for offline backend output. It
// rejects unknown fields, trailing bytes, non-canonical encodings, and output
// whose independently recomputed metrics do not match its graph/assignment.
func DecodeArtifact(raw []byte, maxBytes int) (Artifact, error) {
	if maxBytes < 1 || len(raw) > maxBytes {
		return Artifact{}, errors.New("partition artifact exceeds byte cap")
	}
	var a Artifact
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&a); err != nil {
		return Artifact{}, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Artifact{}, errors.New("partition artifact has trailing JSON")
	}
	if err := ValidateArtifact(a); err != nil {
		return Artifact{}, err
	}
	canonical, err := CanonicalJSON(a)
	if err != nil {
		return Artifact{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return Artifact{}, errors.New("partition artifact is not canonical JSON")
	}
	return a, nil
}

// RunExternalJSON is an optional offline adapter seam. It never becomes a
// TreeDB runtime dependency. Its output is written to a private temp path and
// removed on cancellation, timeout, command failure, or invalid output.
func RunExternalJSON(ctx context.Context, command []string, input []byte, maxOutput int) (Artifact, error) {
	if ctx == nil {
		return Artifact{}, errors.New("external backend requires context deadline")
	}
	if _, ok := ctx.Deadline(); !ok {
		return Artifact{}, errors.New("external backend requires context deadline")
	}
	if len(command) == 0 || maxOutput < 1 || len(input) > maxOutput {
		return Artifact{}, errors.New("invalid external backend command")
	}
	dir, err := os.MkdirTemp("", "treedb-vectorpartition-backend-*")
	if err != nil {
		return Artifact{}, err
	}
	defer os.RemoveAll(dir)
	in := filepath.Join(dir, "input.json")
	out := filepath.Join(dir, "output.json")
	if err = os.WriteFile(in, input, 0600); err != nil {
		return Artifact{}, err
	}
	cmd := exec.CommandContext(ctx, command[0], append(command[1:], in, out)...)
	stderr := &cappedBuffer{limit: maxOutput}
	cmd.Stderr = stderr
	if e := cmd.Run(); e != nil {
		return Artifact{}, fmt.Errorf("external partition backend: %w: %s", e, bytes.TrimSpace(stderr.Bytes()))
	}
	if stderr.exceeded {
		return Artifact{}, errors.New("external partition backend stderr exceeds cap")
	}
	f, e := os.Open(out)
	if e != nil {
		return Artifact{}, e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, int64(maxOutput)+1))
	if e != nil {
		return Artifact{}, e
	}
	if len(raw) > maxOutput {
		return Artifact{}, errors.New("external partition backend output exceeds cap")
	}
	return DecodeArtifact(raw, maxOutput)
}

type cappedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		b.exceeded = true
		return len(p), nil
	}
	return b.Buffer.Write(p)
}
func Digest(a Artifact) (string, error) {
	b, e := CanonicalJSON(a)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
