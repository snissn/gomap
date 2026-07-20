// Package vectorpartition builds a bounded, deterministic offline vector graph
// and a disjoint balanced partition artifact. It is deliberately independent
// of TreeDB collection and Raft lifecycle code.
package vectorpartition

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

const (
	SchemaVersion = 1
	maxVectors    = 1_000_000
	maxDimensions = 4096
	maxEdges      = 64_000_000
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
	return Config{Metric: "cosine", Seed: 1, Repetitions: 4, Pivots: 8, MaxLeafBucket: 128, Degree: 16, Partitions: 16, Imbalance: .05, Symmetric: true, MaxVectors: maxVectors, MaxEdges: maxEdges}
}

type Graph struct {
	Neighbors [][]int `json:"neighbors"`
}
type Metrics struct {
	BuildNanos       int64 `json:"build_nanos"`
	GraphEdges       int   `json:"graph_edges"`
	MaxDegree        int   `json:"max_degree"`
	EdgeCut          int   `json:"edge_cut"`
	MaxPartitionSize int   `json:"max_partition_size"`
	Cap              int   `json:"cap"`
}
type Artifact struct {
	SchemaVersion  int      `json:"schema_version"`
	Backend        string   `json:"backend"`
	BackendLicense string   `json:"backend_license"`
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
	start := time.Now()
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
	a, err := ReferencePartitioner{}.Partition(g, cfg.Partitions, cap)
	if err != nil {
		return Artifact{}, err
	}
	ids := make([]string, len(v))
	for i := range v {
		ids[i] = v[i].ID
	}
	art := Artifact{SchemaVersion: SchemaVersion, Backend: ReferencePartitioner{}.Name(), BackendLicense: "TreeDB clean-room reference implementation (repository license)", Config: cfg, IDs: ids, Graph: g, Assignment: a}
	art.Metrics = metrics(art)
	// Timings are run-report data rather than immutable artifact data. Keeping
	// this field zero makes CanonicalJSON byte-identical for identical input.
	_ = start
	if err := ValidateArtifact(art); err != nil {
		return Artifact{}, err
	}
	return art, nil
}

func validateInput(v []Vector, c Config) error {
	if c.Metric != "cosine" || c.Repetitions < 1 || c.Pivots < 2 || c.MaxLeafBucket < 2 || c.Degree < 1 || c.Partitions < 1 || !finite(c.Imbalance) || c.Imbalance < 0 || c.Imbalance > 1 || c.MaxVectors < 1 || c.MaxVectors > maxVectors || c.MaxEdges < 1 || c.MaxEdges > maxEdges {
		return errors.New("invalid vector partition configuration")
	}
	if len(v) < c.Partitions || len(v) > c.MaxVectors {
		return errors.New("vector count outside configured bounds")
	}
	dims := 0
	for _, x := range v {
		if x.ID == "" {
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
	if int64(len(v))*int64(c.Degree) > int64(c.MaxEdges) {
		return errors.New("configured graph edge bound exceeded before allocation")
	}
	return nil
}
func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

func buildGraph(v []Vector, c Config) (Graph, error) {
	n := len(v)
	sets := make([]map[int]struct{}, n)
	for i := range sets {
		sets[i] = make(map[int]struct{}, c.Degree*c.Repetitions)
	}
	for rep := 0; rep < c.Repetitions; rep++ {
		r := rand.New(rand.NewSource(c.Seed + int64(rep)*0x9e3779b))
		order := r.Perm(n)
		if err := carve(v, order, c, sets); err != nil {
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
	if c.Symmetric { // symmetric policy is bounded by retaining mutual/lowest ordinal candidates deterministically.
		for i := range g.Neighbors {
			for _, j := range g.Neighbors[i] {
				addBounded(&g.Neighbors[j], i, c.Degree)
			}
		}
		for i := range g.Neighbors {
			sort.Ints(g.Neighbors[i])
			if len(g.Neighbors[i]) > c.Degree {
				g.Neighbors[i] = g.Neighbors[i][:c.Degree]
			}
		}
	}
	return g, nil
}
func carve(v []Vector, ids []int, c Config, sets []map[int]struct{}) error {
	if len(ids) <= c.MaxLeafBucket {
		for _, i := range ids {
			nearest := nearest(v, ids, i, c.Degree)
			for _, j := range nearest {
				sets[i][j] = struct{}{}
			}
		}
		return nil
	}
	k := min(c.Pivots, len(ids))
	pivots := append([]int(nil), ids[:k]...)
	buckets := make([][]int, k)
	for _, i := range ids {
		best := 0
		bestD := distance(v[i].Values, v[pivots[0]].Values)
		for p := 1; p < k; p++ {
			d := distance(v[i].Values, v[pivots[p]].Values)
			if d < bestD || d == bestD && pivots[p] < pivots[best] {
				best, bestD = p, d
			}
		}
		buckets[best] = append(buckets[best], i)
	}
	for _, b := range buckets {
		if len(b) == len(ids) { // duplicate/skew progress guarantee: deterministic chunking.
			for start := 0; start < len(b); start += c.MaxLeafBucket {
				end := min(start+c.MaxLeafBucket, len(b))
				if err := carve(v, b[start:end], c, sets); err != nil {
					return err
				}
			}
			continue
		}
		if len(b) > 0 {
			if err := carve(v, b, c, sets); err != nil {
				return err
			}
		}
	}
	return nil
}
func nearest(v []Vector, ids []int, target, k int) []int {
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
	out := make([]int, len(a))
	for i := range a {
		out[i] = a[i].i
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
func addBounded(a *[]int, x, cap int) {
	for _, v := range *a {
		if v == x {
			return
		}
	}
	*a = append(*a, x)
	sort.Ints(*a)
	if len(*a) > cap {
		*a = (*a)[:cap]
	}
}

func (ReferencePartitioner) Partition(g Graph, parts, cap int) ([]int, error) {
	n := len(g.Neighbors)
	if parts < 1 || cap < 1 || n < parts {
		return nil, errors.New("invalid partition request")
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

func ValidateArtifact(a Artifact) error {
	if a.SchemaVersion != SchemaVersion || a.Backend == "" || len(a.IDs) == 0 || len(a.IDs) != len(a.Graph.Neighbors) || len(a.IDs) != len(a.Assignment) {
		return errors.New("malformed partition artifact")
	}
	if err := validateInput(makeVectors(a.IDs, a.Graph.Neighbors), a.Config); err != nil {
		return fmt.Errorf("artifact config: %w", err)
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
		seen := map[int]bool{}
		for _, j := range a.Graph.Neighbors[i] {
			if j < 0 || j >= len(a.IDs) || j == i || seen[j] {
				return errors.New("invalid graph edge")
			}
			seen[j] = true
		}
		if len(a.Graph.Neighbors[i]) > a.Config.Degree {
			return errors.New("degree cap exceeded")
		}
	}
	for _, n := range loads {
		if n > cap {
			return errors.New("partition cap exceeded")
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
