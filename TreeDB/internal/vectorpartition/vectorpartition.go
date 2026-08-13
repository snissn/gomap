// Package vectorpartition builds a bounded, deterministic offline vector graph
// and a disjoint balanced partition artifact. It is deliberately independent
// of TreeDB collection and Raft lifecycle code.
package vectorpartition

import (
	"bytes"
	"container/heap"
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
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	SchemaVersion                   = 1
	maxVectors                      = 1_000_000
	maxDimensions                   = 4096
	maxEdges                        = 64_000_000
	maxRepetitions                  = 32
	maxPivots                       = 1024
	maxLeafBucket                   = 65536
	maxDegree                       = 1024
	maxPartitions                   = 16384
	maxIDBytes                      = 1 << 20
	maxTotalIDBytes                 = 64 << 20
	maxDistanceWork                 = int64(20_000_000_000) // scalar cosine dimensions
	maxPartitionWork                = int64(250_000_000)
	maxCarveDepth                   = 64
	maxDuplicateFingerprintVariants = 64
)

const externalJSONFixedSyntaxBytes = 1 << 20

var maxExternalJSONBytes = mustExternalJSONByteBound(maxTotalIDBytes, maxEdges, maxVectors)

// externalJSONByteBound conservatively bounds a serialized M2 graph request or
// artifact. JSON control bytes may escape to six bytes, so ID, source ID,
// backend name, and backend license all receive that multiplier. Graph
// ordinals, per-vector syntax, and a fixed field/syntax reserve are charged
// separately with checked integer arithmetic.
func externalJSONByteBound(totalIDBytes, edges, vectors int) (int, bool) {
	if totalIDBytes < 0 || edges < 0 || vectors < 0 {
		return 0, false
	}
	total := 0
	add := func(n int) bool {
		if n < 0 || total > math.MaxInt-n {
			return false
		}
		total += n
		return true
	}
	mul := func(a, b int) (int, bool) {
		if a < 0 || b < 0 || a != 0 && b > math.MaxInt/a {
			return 0, false
		}
		return a * b, true
	}
	escapedIDs, ok := mul(totalIDBytes, 6)
	if !ok || !add(escapedIDs) {
		return 0, false
	}
	// The largest caller-controlled non-vector identity fields are SourceID,
	// backend Name, and backend License.
	escapedIdentities, ok := mul(1024+256+1024, 6)
	if !ok || !add(escapedIdentities) {
		return 0, false
	}
	graph, ok := mul(edges, 9) // ordinal, comma, and conservative syntax slack
	if !ok || !add(graph) {
		return 0, false
	}
	perVector, ok := mul(vectors, 32) // IDs/arrays/assignment/metric syntax
	if !ok || !add(perVector) || !add(externalJSONFixedSyntaxBytes) {
		return 0, false
	}
	return total, true
}

func mustExternalJSONByteBound(totalIDBytes, edges, vectors int) int {
	bound, ok := externalJSONByteBound(totalIDBytes, edges, vectors)
	if !ok {
		panic("external JSON byte bound overflow")
	}
	return bound
}

// Vector IDs must be unique and are canonically ordered before construction.
type Vector struct {
	ID     string
	Values []float64
}

// Config makes the clean-room sketch controls explicit. The reference backend
// is intentionally in-process and deterministic, so CI never needs a native
// partitioner or runtime FFI.
type Config struct {
	Metric           string  `json:"metric"`
	Seed             int64   `json:"seed"`
	Repetitions      int     `json:"repetitions"`
	Pivots           int     `json:"pivots"`
	MaxLeafBucket    int     `json:"max_leaf_bucket"`
	Degree           int     `json:"degree"`
	Partitions       int     `json:"partitions"`
	Imbalance        float64 `json:"imbalance"`
	Symmetric        bool    `json:"symmetric"`
	MaxVectors       int     `json:"max_vectors"`
	MaxEdges         int     `json:"max_edges"`
	MaxDistanceWork  int64   `json:"max_distance_work"`
	MaxPartitionWork int64   `json:"max_partition_work"`
}

func DefaultConfig() Config {
	return Config{Metric: "cosine", Seed: 1, Repetitions: 4, Pivots: 8, MaxLeafBucket: 128, Degree: 16, Partitions: 16, Imbalance: .05, Symmetric: false, MaxVectors: maxVectors, MaxEdges: maxEdges, MaxDistanceWork: maxDistanceWork, MaxPartitionWork: maxPartitionWork}
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
type PhaseMetrics struct {
	GraphBuildNanos       int64
	BackendPartitionNanos int64
	ValidationNanos       int64
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

// ExternalJSONLimits independently bounds the private request file and the
// backend response/stderr. Both must remain within the M2 serialization cap.
type ExternalJSONLimits struct {
	MaxInput  int
	MaxOutput int
}

// Partitioner is the deliberately narrow backend seam. Implementations return
// ordinals only; ValidateArtifact independently verifies every invariant.
type Partitioner interface {
	Name() string
	License() string
	Partition(Graph, int, int) ([]int, error)
}
type ReferencePartitioner struct{ maxPartitionWork int64 }

func (ReferencePartitioner) Name() string    { return "treedb_reference_greedy_v1" }
func (ReferencePartitioner) License() string { return "TreeDB repository license" }

func Build(vectors []Vector, cfg Config) (Artifact, error) {
	return BuildWithPartitioner(vectors, cfg, Source{SourceID: "inline_vectors_v1"}, ReferencePartitioner{})
}

// BuildWithPartitioner is the usable offline backend seam. The supplied
// partitioner's result is never trusted: graph, assignment, source and metrics
// are independently validated before the artifact is returned.
func BuildWithPartitioner(vectors []Vector, cfg Config, source Source, backend Partitioner) (Artifact, error) {
	a, _, err := BuildWithPartitionerPhased(vectors, cfg, source, backend)
	return a, err
}

// RepartitionArtifact reuses a validated canonical graph and stable-ID order
// while replacing only its partition assignment.  It is an offline evidence
// seam: unlike BuildWithPartitioner it never reads vectors or rebuilds graph
// topology.  The replacement assignment and all derived metrics are validated
// before the resulting artifact is returned.
func RepartitionArtifact(in Artifact, partitions int, backend Partitioner) (Artifact, error) {
	if err := ValidateArtifact(in); err != nil {
		return Artifact{}, fmt.Errorf("input artifact: %w", err)
	}
	if backend == nil {
		return Artifact{}, errors.New("partition backend identity is required")
	}
	cfg := in.Config
	cfg.Partitions = partitions
	if err := validateArtifactConfig(in.IDs, cfg); err != nil {
		return Artifact{}, err
	}
	if reference, ok := backend.(ReferencePartitioner); ok {
		reference.maxPartitionWork = cfg.MaxPartitionWork
		backend = reference
	}
	if reference, ok := backend.(*ReferencePartitioner); ok {
		copy := *reference
		copy.maxPartitionWork = cfg.MaxPartitionWork
		backend = copy
	}
	name, license := backend.Name(), backend.License()
	if name == "" || !utf8.ValidString(name) || len(name) > 256 || license == "" || !utf8.ValidString(license) || len(license) > 1024 {
		return Artifact{}, errors.New("partition backend identity is required")
	}
	backendGraph, err := cloneGraphBounded(in.Graph, cfg.MaxEdges)
	if err != nil {
		return Artifact{}, err
	}
	assignment, err := backend.Partition(backendGraph, partitions, partitionCap(len(in.IDs), partitions, cfg.Imbalance))
	if err != nil {
		return Artifact{}, err
	}
	if _, err := validateAssignment(assignment, len(in.IDs), cfg); err != nil {
		return Artifact{}, fmt.Errorf("partition backend assignment: %w", err)
	}
	outGraph, err := cloneGraphBounded(in.Graph, cfg.MaxEdges)
	if err != nil {
		return Artifact{}, err
	}
	out := Artifact{SchemaVersion: SchemaVersion, Backend: name, BackendLicense: license, Source: in.Source, Config: cfg, IDs: append([]string(nil), in.IDs...), Graph: outGraph, Assignment: assignment}
	out.Metrics = metrics(out)
	if err := ValidateArtifact(out); err != nil {
		return Artifact{}, err
	}
	return out, nil
}
func BuildWithPartitionerPhased(vectors []Vector, cfg Config, source Source, backend Partitioner) (Artifact, PhaseMetrics, error) {
	var phases PhaseMetrics
	if backend == nil {
		return Artifact{}, phases, errors.New("partition backend identity is required")
	}
	if reference, ok := backend.(ReferencePartitioner); ok {
		reference.maxPartitionWork = cfg.MaxPartitionWork
		backend = reference
	}
	if reference, ok := backend.(*ReferencePartitioner); ok {
		copy := *reference // never mutate a caller-owned backend through preflight.
		copy.maxPartitionWork = cfg.MaxPartitionWork
		backend = copy
	}
	backendName, backendLicense := backend.Name(), backend.License()
	if backendName == "" || !utf8.ValidString(backendName) || len(backendName) > 256 || backendLicense == "" || !utf8.ValidString(backendLicense) || len(backendLicense) > 1024 {
		return Artifact{}, phases, errors.New("partition backend identity is required")
	}
	if err := validateRequestedSource(source); err != nil {
		return Artifact{}, phases, err
	}
	dimensions, err := preflightInputShape(vectors, cfg, backend)
	if err != nil {
		return Artifact{}, phases, err
	}
	if err := validateInput(vectors, dimensions); err != nil {
		return Artifact{}, phases, err
	}
	v := append([]Vector(nil), vectors...)
	sort.Slice(v, func(i, j int) bool { return v[i].ID < v[j].ID })
	for i := 1; i < len(v); i++ {
		if v[i].ID == v[i-1].ID {
			return Artifact{}, phases, fmt.Errorf("duplicate vector ID %q", v[i].ID)
		}
	}
	started := time.Now()
	g, err := buildGraph(v, cfg)
	phases.GraphBuildNanos = time.Since(started).Nanoseconds()
	if err != nil {
		return Artifact{}, phases, err
	}
	cap := partitionCap(len(v), cfg.Partitions, cfg.Imbalance)
	started = time.Now()
	backendGraph := g
	if !isReferencePartitioner(backend) {
		backendGraph, err = cloneGraphBounded(g, cfg.MaxEdges)
		if err != nil {
			return Artifact{}, phases, err
		}
	}
	a, err := backend.Partition(backendGraph, cfg.Partitions, cap)
	phases.BackendPartitionNanos = time.Since(started).Nanoseconds()
	if err != nil {
		return Artifact{}, phases, err
	}
	if _, err := validateAssignment(a, len(v), cfg); err != nil {
		return Artifact{}, phases, fmt.Errorf("partition backend assignment: %w", err)
	}
	ids := make([]string, len(v))
	for i := range v {
		ids[i] = v[i].ID
	}
	computed := sourceFor(v, cfg.Metric, source.SourceID)
	if source != (Source{SourceID: source.SourceID}) && source != computed {
		return Artifact{}, phases, errors.New("source identity does not match input snapshot")
	}
	art := Artifact{SchemaVersion: SchemaVersion, Backend: backendName, BackendLicense: backendLicense, Source: computed, Config: cfg, IDs: ids, Graph: g, Assignment: a}
	art.Metrics = metrics(art)
	started = time.Now()
	if err := ValidateArtifact(art); err != nil {
		phases.ValidationNanos = time.Since(started).Nanoseconds()
		return Artifact{}, phases, err
	}
	phases.ValidationNanos = time.Since(started).Nanoseconds()
	return art, phases, nil
}

// Non-reference in-process backends receive an isolated graph because Graph
// contains mutable nested slices. The canonical graph remains the one built
// from vectors and recorded in the artifact. The clone has exact backing
// storage and is bounded by the already-validated MaxVectors/MaxEdges limits.
func cloneGraphBounded(g Graph, maxEdges int) (Graph, error) {
	if len(g.Neighbors) > maxVectors || maxEdges < 0 {
		return Graph{}, errors.New("backend graph copy exceeds bounds")
	}
	total := 0
	for _, neighbors := range g.Neighbors {
		if len(neighbors) > maxEdges-total {
			return Graph{}, errors.New("backend graph copy exceeds edge bound")
		}
		total += len(neighbors)
	}
	clone := Graph{Neighbors: make([][]int, len(g.Neighbors))}
	backing := make([]int, total)
	at := 0
	for i, neighbors := range g.Neighbors {
		if len(neighbors) == 0 {
			// Keep the canonical JSON graph shape stable: a zero-degree row is
			// an empty array, never a nil slice serialized as null.
			clone.Neighbors[i] = make([]int, 0)
			continue
		}
		clone.Neighbors[i] = backing[at : at+len(neighbors) : at+len(neighbors)]
		copy(clone.Neighbors[i], neighbors)
		at += len(neighbors)
	}
	return clone, nil
}

func isReferencePartitioner(backend Partitioner) bool {
	switch backend.(type) {
	case ReferencePartitioner, *ReferencePartitioner:
		return true
	default:
		return false
	}
}
func validateRequestedSource(s Source) error {
	if s.SourceID == "" || !utf8.ValidString(s.SourceID) || len(s.SourceID) > 1024 {
		return errors.New("source ID is required")
	}
	if s == (Source{SourceID: s.SourceID}) {
		return nil
	}
	if len(s.Checksum) != 64 || strings.ToLower(s.Checksum) != s.Checksum || s.Vectors < 1 || s.Vectors > maxVectors || s.Dimensions < 1 || s.Dimensions > maxDimensions || s.Metric != "cosine" {
		return errors.New("partial source identity is not allowed")
	}
	if _, err := hex.DecodeString(s.Checksum); err != nil {
		return errors.New("partial source identity is not allowed")
	}
	return nil
}

func sourceFor(v []Vector, metric, id string) Source {
	h := sha256.New()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(v)))
	h.Write(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(len(v[0].Values)))
	h.Write(b[:])
	h.Write([]byte(metric))
	h.Write([]byte{0})
	for _, x := range v {
		binary.BigEndian.PutUint64(b[:], uint64(len(x.ID)))
		h.Write(b[:])
		h.Write([]byte(x.ID))
		for _, f := range x.Values {
			binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
			h.Write(b[:])
		}
	}
	return Source{SourceID: id, Checksum: hex.EncodeToString(h.Sum(nil)), Vectors: len(v), Dimensions: len(v[0].Values), Metric: metric}
}

// preflightInputShape derives the declared dimension from the first vector only
// after count validation, then runs every selected-backend shape gate before
// inspecting caller-owned IDs or scalar values. Later vectors are checked for
// exactly this dimension before their values are read by validateInput.
func preflightInputShape(v []Vector, c Config, backend Partitioner) (int, error) {
	if err := ValidateConfig(c); err != nil {
		return 0, err
	}
	if len(v) < c.Partitions || len(v) > c.MaxVectors {
		return 0, errors.New("vector count outside configured bounds")
	}
	dimensions := len(v[0].Values)
	if err := ValidateInputShape(c, len(v), dimensions); err != nil {
		return 0, err
	}
	if isReferencePartitioner(backend) {
		if err := ValidateReferenceInputShape(c, len(v), dimensions); err != nil {
			return 0, err
		}
	}
	return dimensions, nil
}

// validateInput validates every vector after preflightInputShape has bounded
// the aggregate shape. Dimension equality is intentionally checked before ID
// or scalar inspection for each vector, preventing a heterogeneous later
// vector from evading the declared-dimension bound.
func validateInput(v []Vector, dimensions int) error {
	totalIDBytes := 0
	for _, x := range v {
		if len(x.Values) != dimensions {
			return errors.New("wrong vector dimension")
		}
		if x.ID == "" {
			return errors.New("empty vector ID")
		}
		if !utf8.ValidString(x.ID) {
			return errors.New("invalid UTF-8 vector ID")
		}
		if len(x.ID) > maxIDBytes {
			return errors.New("vector ID exceeds per-ID byte cap")
		}
		if totalIDBytes > maxTotalIDBytes-len(x.ID) {
			return errors.New("vector ID aggregate bytes exceed cap")
		}
		totalIDBytes += len(x.ID)
		for _, n := range x.Values {
			if !finite(n) {
				return errors.New("non-finite vector value")
			}
		}
		if !nonZeroFiniteNorm(x.Values) {
			return errors.New("zero-norm vector")
		}
	}
	return nil
}

// ValidateConfig validates bounded offline builder options before corpus I/O.
func ValidateConfig(c Config) error {
	if c.Symmetric {
		return errors.New("symmetric graph policy is not supported")
	}
	if c.Metric != "cosine" || c.Repetitions < 1 || c.Repetitions > maxRepetitions || c.Pivots < 2 || c.Pivots > maxPivots || c.MaxLeafBucket < 2 || c.MaxLeafBucket > maxLeafBucket || c.Degree < 1 || c.Degree > maxDegree || c.Partitions < 1 || c.Partitions > maxPartitions || !finite(c.Imbalance) || c.Imbalance < 0 || c.Imbalance > 1 || c.MaxVectors < 1 || c.MaxVectors > maxVectors || c.MaxEdges < 1 || c.MaxEdges > maxEdges || c.MaxDistanceWork < 0 || c.MaxPartitionWork < 0 {
		return errors.New("invalid vector partition configuration")
	}
	return nil
}

// ValidateInputShape checks count-derived graph bounds before vector allocation.
func ValidateInputShape(c Config, vectors, dimensions int) error {
	if err := ValidateConfig(c); err != nil {
		return err
	}
	if vectors < c.Partitions || vectors > c.MaxVectors {
		return errors.New("vector count outside configured bounds")
	}
	if dimensions < 1 || dimensions > maxDimensions {
		return errors.New("invalid vector dimensions")
	}
	if int64(vectors)*int64(c.Degree) > int64(c.MaxEdges) || int64(vectors)*int64(c.Degree) > int64(c.MaxEdges)/int64(c.Repetitions) {
		return errors.New("configured graph edge bound exceeded before allocation")
	}
	if graphDistanceWorkExceeds(c, vectors, dimensions) {
		return errors.New("configured graph scalar-work bound exceeded before allocation")
	}
	return nil
}

// ValidateReferenceInputShape adds the repository reference partitioner's
// deterministic work cap to the graph/input shape checks. Other backends may
// have distinct work contracts and are intentionally not charged this bound.
func ValidateReferenceInputShape(c Config, vectors, dimensions int) error {
	if err := ValidateInputShape(c, vectors, dimensions); err != nil {
		return err
	}
	if partitionWorkExceededWithCap(vectors, c.Partitions, c.Degree, c.MaxPartitionWork) {
		return errors.New("partition work bound exceeded before allocation")
	}
	return nil
}

// graphDistanceWorkExceeds accounts for every shape-fixed graph phase: the
// first-carve pivot comparisons, plus the bounded leaf comparisons. At depth
// zero carveDepth deliberately puts each vector in both its closest and
// second-closest bucket, so two leaf memberships are possible whenever there
// are at least two top-level pivots. Recursive carve work is
// geometry-dependent and remains guarded by distanceBudget at runtime.
func graphDistanceWorkExceeds(c Config, vectors, dimensions int) bool {
	limit := c.MaxDistanceWork
	if limit == 0 {
		limit = maxDistanceWork
	}
	topLevelPivots := 0
	if vectors > c.MaxLeafBucket {
		topLevelPivots = min(c.Pivots, vectors)
	}
	if exceedsProduct(limit, int64(vectors), int64(topLevelPivots), int64(c.Repetitions), int64(dimensions)) {
		return true
	}
	topLevelPivotWork := int64(vectors) * int64(topLevelPivots) * int64(c.Repetitions) * int64(dimensions)
	topLevelMemberships := 1
	if topLevelPivots >= 2 {
		// carveDepth adds the closest and second-closest top-level bucket.
		topLevelMemberships = 2
	}
	leafComparisons := max(0, min(c.MaxLeafBucket, vectors)-1)
	return exceedsProduct(limit-topLevelPivotWork, int64(vectors), int64(topLevelMemberships), int64(leafComparisons), int64(c.Repetitions), int64(dimensions))
}
func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

func buildGraph(v []Vector, c Config) (Graph, error) {
	n := len(v)
	sets := make([]map[int]float64, n)
	for i := range sets {
		sets[i] = make(map[int]float64, c.Degree)
	}
	limit := c.MaxDistanceWork
	if limit == 0 {
		limit = maxDistanceWork
	}
	budget := distanceBudget{remaining: limit}
	for rep := 0; rep < c.Repetitions; rep++ {
		r := rand.New(rand.NewSource(c.Seed + int64(rep)*0x9e3779b))
		order := r.Perm(n)
		if err := carveDepth(v, order, c, sets, 0, &budget); err != nil {
			return Graph{}, err
		}
	}
	// Exact duplicate vectors can be separated by the bounded pivot leaves even
	// though their cosine distance is zero. Link each canonical fingerprint
	// class into an ordinal chain before materializing the degree-bounded graph.
	// This is corpus-only, deterministic, and adds at most two candidates per
	// row; zero-distance links displace farther sketch candidates when needed.
	type duplicateClass struct {
		values  []float64
		members []int
	}
	classes := make(map[[32]byte][]duplicateClass)
	duplicateLinks := make([]map[int]struct{}, n)
	for i := range v {
		fingerprint := VectorBitsFingerprintV1(v[i].Values)
		bucket := classes[fingerprint]
		matched := -1
		for class := range bucket {
			if vectorBitsEqualV1(bucket[class].values, v[i].Values) {
				matched = class
				break
			}
		}
		if matched < 0 {
			if len(bucket) >= maxDuplicateFingerprintVariants {
				return Graph{}, fmt.Errorf("duplicate fingerprint collision variants exceed bound: bound=%d observed=%d fingerprint=%x", maxDuplicateFingerprintVariants, len(bucket)+1, fingerprint)
			}
			bucket = append(bucket, duplicateClass{values: v[i].Values})
			matched = len(bucket) - 1
		}
		bucket[matched].members = append(bucket[matched].members, i)
		classes[fingerprint] = bucket
	}
	for _, bucket := range classes {
		for _, class := range bucket {
			if len(class.members) < 2 {
				continue
			}
			if c.Degree == 1 {
				// A directed ordinal cycle is the only way to keep a class larger
				// than two strongly connected with one outgoing edge per row.
				for i, from := range class.members {
					to := class.members[(i+1)%len(class.members)]
					duplicateLinks[from] = map[int]struct{}{to: {}}
					sets[from][to] = 0
				}
				continue
			}
			for i := 1; i < len(class.members); i++ {
				left, right := class.members[i-1], class.members[i]
				if duplicateLinks[left] == nil {
					duplicateLinks[left] = make(map[int]struct{}, 2)
				}
				if duplicateLinks[right] == nil {
					duplicateLinks[right] = make(map[int]struct{}, 2)
				}
				duplicateLinks[left][right] = struct{}{}
				duplicateLinks[right][left] = struct{}{}
				sets[left][right] = 0
				sets[right][left] = 0
			}
		}
	}
	g := Graph{Neighbors: make([][]int, n)}
	for i := range g.Neighbors {
		// Canonical artifacts represent every zero-degree row as [] rather
		// than null, including isolated vectors.
		g.Neighbors[i] = make([]int, 0)
	}
	for i, s := range sets {
		if err := pruneCandidatesPreservingRequired(s, duplicateLinks[i], c.Degree); err != nil {
			return Graph{}, err
		}
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
	return g, nil
}

func pruneCandidatesPreservingRequired(s map[int]float64, required map[int]struct{}, cap int) error {
	if len(required) > cap {
		return errors.New("required duplicate links exceed graph degree")
	}
	for len(s) > cap {
		worst := -1
		worstD := 0.0
		for ordinal, distance := range s {
			if _, keep := required[ordinal]; keep {
				continue
			}
			if worst < 0 || distance > worstD || distance == worstD && ordinal > worst {
				worst, worstD = ordinal, distance
			}
		}
		if worst < 0 {
			return errors.New("cannot prune graph candidates without removing required duplicate links")
		}
		delete(s, worst)
	}
	return nil
}

func vectorBitsEqualV1(left, right []float64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if math.Float64bits(left[i]) != math.Float64bits(right[i]) {
			return false
		}
	}
	return true
}

// VectorBitsFingerprintV1 returns the canonical SHA-256 fingerprint used to
// narrow exact float-bit vector classes. Callers must still compare vector
// bits before treating equal fingerprints as equal vectors.
func VectorBitsFingerprintV1(values []float64) [32]byte {
	h := sha256.New()
	var raw [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(raw[:], math.Float64bits(value))
		_, _ = h.Write(raw[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// distanceBudget bounds scalar operations in the pivot and leaf phases.
// It is runtime-enforced because input geometry, rather than configuration
// alone, determines recursive bucket shape.
type distanceBudget struct{ remaining int64 }

func (b *distanceBudget) take(n int64) error {
	if n < 0 || n > b.remaining {
		return errors.New("graph distance-work budget exceeded")
	}
	b.remaining -= n
	return nil
}

func carveDepth(v []Vector, ids []int, c Config, sets []map[int]float64, depth int, budget *distanceBudget) error {
	if len(ids) <= c.MaxLeafBucket {
		for _, i := range ids {
			nearest, err := nearest(v, ids, i, c.Degree, budget)
			if err != nil {
				return err
			}
			for _, candidate := range nearest {
				addCandidateBounded(sets[i], candidate.i, candidate.x, c.Degree)
			}
		}
		return nil
	}
	k := min(c.Pivots, len(ids))
	if depth >= maxCarveDepth {
		return carveChunks(v, ids, c, sets, depth, budget)
	}
	pivots := append([]int(nil), ids[:k]...)
	buckets := make([][]int, k)
	for _, i := range ids {
		if err := budget.take(int64(k) * int64(len(v[i].Values))); err != nil {
			return err
		}
		var distances [maxPivots]float64
		for p := range pivots {
			distances[p] = distance(v[i].Values, v[pivots[p]].Values)
		}
		best, second := 0, -1
		bestD := distances[0]
		for p := 1; p < k; p++ {
			d := distances[p]
			if d < bestD || d == bestD && pivots[p] < pivots[best] {
				second, best, bestD = best, p, d
			} else if second < 0 || d < distances[second] || d == distances[second] && pivots[p] < pivots[second] {
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
		// A n-1 bucket can otherwise recurse one ordinal at a time on skewed
		// geometry. Chunk the current membership deterministically instead.
		if len(b) >= len(ids)-1 {
			if err := carveChunks(v, b, c, sets, depth, budget); err != nil {
				return err
			}
			continue
		}
		if len(b) > 0 {
			if err := carveDepth(v, b, c, sets, depth+1, budget); err != nil {
				return err
			}
		}
	}
	return nil
}
func carveChunks(v []Vector, ids []int, c Config, sets []map[int]float64, depth int, budget *distanceBudget) error {
	for start := 0; start < len(ids); start += c.MaxLeafBucket {
		end := min(start+c.MaxLeafBucket, len(ids))
		if err := carveDepth(v, ids[start:end], c, sets, depth+1, budget); err != nil {
			return err
		}
	}
	return nil
}
func nearest(v []Vector, ids []int, target, k int, budget *distanceBudget) ([]candidate, error) {
	a := make(candidateMaxHeap, 0, min(k, len(ids)-1))
	for _, j := range ids {
		if j != target {
			if err := budget.take(int64(len(v[target].Values))); err != nil {
				return nil, err
			}
			c := candidate{j, distance(v[target].Values, v[j].Values)}
			if len(a) < k {
				heap.Push(&a, c)
				continue
			}
			if candidateBetter(c, a[0]) {
				a[0] = c
				heap.Fix(&a, 0)
			}
		}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].x < a[j].x || a[i].x == a[j].x && a[i].i < a[j].i })
	return []candidate(a), nil
}
func distance(a, b []float64) float64 {
	var scaleA, scaleB float64
	for i := range a {
		scaleA = math.Max(scaleA, math.Abs(a[i]))
		scaleB = math.Max(scaleB, math.Abs(b[i]))
	}
	if scaleA == 0 || scaleB == 0 {
		return 1
	}
	var dot, na, nb float64
	for i := range a {
		av, bv := a[i]/scaleA, b[i]/scaleB
		dot = math.FMA(av, bv, dot)
		na = math.FMA(av, av, na)
		nb = math.FMA(bv, bv, nb)
	}
	cosine := dot / (math.Sqrt(na) * math.Sqrt(nb))
	if cosine > 1 {
		cosine = 1
	}
	if cosine < -1 {
		cosine = -1
	}
	return 1 - cosine
}
func nonZeroFiniteNorm(values []float64) bool {
	var scale float64
	for _, value := range values {
		scale = math.Max(scale, math.Abs(value))
	}
	return scale > 0 && finite(scale)
}

type candidate struct {
	i int
	x float64
}
type candidateMaxHeap []candidate

func (h candidateMaxHeap) Len() int           { return len(h) }
func (h candidateMaxHeap) Less(i, j int) bool { return candidateBetter(h[j], h[i]) }
func (h candidateMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *candidateMaxHeap) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *candidateMaxHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
func candidateBetter(a, b candidate) bool { return a.x < b.x || a.x == b.x && a.i < b.i }

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

func (r ReferencePartitioner) Partition(g Graph, parts, cap int) ([]int, error) {
	n := len(g.Neighbors)
	if parts < 1 || cap < 1 || n < parts {
		return nil, errors.New("invalid partition request")
	}
	if n > maxVectors {
		return nil, errors.New("partition vector bound exceeded before allocation")
	}
	degree, err := validateGraphAndDegree(g, n, maxDegree)
	if err != nil {
		return nil, err
	}
	if partitionWorkExceededWithCap(n, parts, degree, r.maxPartitionWork) {
		return nil, errors.New("partition work bound exceeded before allocation")
	}
	// Degree is bounded, so bucket ordering is a deterministic O(n+degree)
	// replacement for comparison sorting: each ordinal enters exactly one bucket
	// and ascending insertion order supplies the ordinal tie-break. Count first
	// so every bucket has exact backing capacity: this avoids Go slice-growth
	// copies and makes the pre-allocation work contract structural.
	degreeCounts := make([]int, degree+1)
	for _, ns := range g.Neighbors {
		degreeCounts[len(ns)]++
	}
	buckets := make([][]int, degree+1)
	for d, count := range degreeCounts {
		buckets[d] = make([]int, 0, count)
	}
	for node, ns := range g.Neighbors {
		buckets[len(ns)] = append(buckets[len(ns)], node)
	}
	order := make([]int, 0, n)
	for d := degree; d >= 0; d-- {
		order = append(order, buckets[d]...)
	}
	out := make([]int, n)
	for i := range out {
		out[i] = -1
	}
	loads := make([]int, parts)
	marks := make([]int, parts)
	candidates := make([]int, 0, degree+1)
	// Reserve one ordinal for each partition before affinity scoring. This
	// matches ValidateArtifact's exact-coverage invariant even on graphs whose
	// edges all prefer a small subset of partitions.
	for partition := 0; partition < parts; partition++ {
		out[order[partition]] = partition
		loads[partition] = 1
	}
	for _, node := range order[parts:] {
		// Only partitions already represented by a neighbor can have a positive
		// affinity. Add the globally least-loaded admissible partition so a
		// zero-affinity choice remains deterministic and balanced without a
		// partitions-by-degree scan for every node.
		candidates = candidates[:0]
		stamp := node + 1
		least := -1
		for p := 0; p < parts; p++ {
			if loads[p] < cap && (least < 0 || loads[p] < loads[least] || loads[p] == loads[least] && p < least) {
				least = p
			}
		}
		if least >= 0 {
			candidates = append(candidates, least)
			marks[least] = stamp
		}
		for _, to := range g.Neighbors[node] {
			p := out[to]
			if p < 0 || loads[p] >= cap {
				continue
			}
			if marks[p] != stamp {
				marks[p] = stamp
				candidates = append(candidates, p)
			}
		}
		best, bscore := -1, -1
		for _, p := range candidates {
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
func partitionWorkExceededWithCap(n, parts, degree int, cap int64) bool {
	work, overflow := partitionWorkUnits(n, parts, degree)
	if cap == 0 {
		cap = maxPartitionWork
	}
	return overflow || cap < 1 || work > cap
}
func partitionWorkExceeded(n, parts, degree int) bool {
	return partitionWorkExceededWithCap(n, parts, degree, maxPartitionWork)
}
func partitionWorkUnits(n, parts, degree int) (int64, bool) {
	if n < 0 || parts < 0 || degree < 0 {
		return 0, true
	}
	// Deliberately overcount every loop/allocation family. Per-node work charges
	// validation (degree+1); the degree-frequency pass (1); exact bucket backing
	// zeroing and fill (2); order backing zeroing plus append (2); output backing
	// zeroing plus -1 initialization (2); candidate reset (1); assignment/load
	// update (1); the partition scan; candidate construction including the
	// least-loaded choice (degree+1); its outer scoring iteration (degree+1);
	// and affinity rescans. Global setup covers loads/marks zeroing, reserve,
	// and degree-count/bucket/order/candidate slice initialization or iteration.
	// Exact bucket capacities eliminate runtime-dependent slice-growth copies.
	perNode := int64(degree+1) + 9 + int64(parts) + 2*int64(degree+1) + int64(degree)*int64(degree+1)
	if int64(n) != 0 && perNode > math.MaxInt64/int64(n) {
		return 0, true
	}
	work := int64(n) * perNode
	global := 3*int64(parts) + 5*int64(degree+1)
	if global < 0 || work > math.MaxInt64-global {
		return 0, true
	}
	return work + global, false
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
func partitionCap(n, p int, e float64) int { return int(math.Ceil((1 + e) * float64(n) / float64(p))) }
func metrics(a Artifact) Metrics {
	return metricsWithLoads(a, nil)
}

// metricsWithLoads accepts a validated assignment histogram when available so
// validation does not need a second partitions-sized scan to compute the same
// maximum load.
func metricsWithLoads(a Artifact, loads []int) Metrics {
	m := Metrics{Cap: partitionCap(len(a.IDs), a.Config.Partitions, a.Config.Imbalance)}
	if loads == nil {
		loads = make([]int, a.Config.Partitions)
		for _, p := range a.Assignment {
			loads[p]++
		}
	}
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
	for _, n := range loads {
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

// BuildStableIDHashBaseline preserves an artifact's immutable source graph and
// config while replacing only its assignment with the deterministic stable-ID
// hash baseline. The result must satisfy the same partition cap as the graph
// assignment or it is rejected rather than silently rebalanced.
func BuildStableIDHashBaseline(a Artifact) (Artifact, error) {
	if err := ValidateArtifact(a); err != nil {
		return Artifact{}, fmt.Errorf("stable-ID hash baseline source: %w", err)
	}
	out := a
	out.Backend = "stable_id_hash_baseline_v1"
	out.BackendLicense = "gomap deterministic SHA-256 stable-ID baseline"
	out.IDs = append([]string(nil), a.IDs...)
	out.Graph.Neighbors = make([][]int, len(a.Graph.Neighbors))
	for i := range a.Graph.Neighbors {
		out.Graph.Neighbors[i] = make([]int, len(a.Graph.Neighbors[i]))
		copy(out.Graph.Neighbors[i], a.Graph.Neighbors[i])
	}
	out.Assignment = make([]int, len(a.IDs))
	for i, id := range a.IDs {
		out.Assignment[i] = stableIDPartition(id, a.Config.Partitions)
	}
	out.Metrics = metrics(out)
	if err := ValidateArtifact(out); err != nil {
		return Artifact{}, fmt.Errorf("stable-ID hash baseline: %w", err)
	}
	return out, nil
}

func ValidateArtifact(a Artifact) error {
	if a.SchemaVersion != SchemaVersion || a.Backend == "" || !utf8.ValidString(a.Backend) || len(a.Backend) > 256 || a.BackendLicense == "" || !utf8.ValidString(a.BackendLicense) || len(a.BackendLicense) > 1024 || a.Source.SourceID == "" || !utf8.ValidString(a.Source.SourceID) || len(a.Source.SourceID) > 1024 || len(a.Source.Checksum) != 64 || strings.ToLower(a.Source.Checksum) != a.Source.Checksum || a.Source.Vectors != len(a.IDs) || a.Source.Dimensions < 1 || a.Source.Dimensions > maxDimensions || a.Source.Metric != a.Config.Metric || len(a.IDs) == 0 || len(a.IDs) != len(a.Graph.Neighbors) || len(a.IDs) != len(a.Assignment) {
		return errors.New("malformed partition artifact")
	}
	if err := validateArtifactConfig(a.IDs, a.Config); err != nil {
		return fmt.Errorf("artifact config: %w", err)
	}
	if err := validateGraph(a.Graph, len(a.IDs), a.Config.Degree); err != nil {
		return err
	}
	var totalIDBytes int
	for i, id := range a.IDs {
		if id == "" || !utf8.ValidString(id) || i > 0 && id <= a.IDs[i-1] {
			return errors.New("IDs not unique canonical order")
		}
		if len(id) > maxIDBytes || totalIDBytes > maxTotalIDBytes-len(id) {
			return errors.New("artifact ID bytes exceed cap")
		}
		totalIDBytes += len(id)
	}
	if _, err := hex.DecodeString(a.Source.Checksum); err != nil {
		return errors.New("invalid source checksum")
	}
	loads, err := validateAssignment(a.Assignment, len(a.IDs), a.Config)
	if err != nil {
		return err
	}
	m := metricsWithLoads(a, loads)
	if a.Metrics.GraphEdges != m.GraphEdges || a.Metrics.MaxDegree != m.MaxDegree || a.Metrics.EdgeCut != m.EdgeCut || a.Metrics.StableIDHashEdgeCut != m.StableIDHashEdgeCut || a.Metrics.MaxPartitionSize != m.MaxPartitionSize || a.Metrics.Cap != m.Cap {
		return errors.New("artifact metrics do not match graph and assignment")
	}
	return nil
}
func validateAssignment(assignment []int, n int, c Config) ([]int, error) {
	if len(assignment) != n {
		return nil, errors.New("assignment length mismatch")
	}
	cap := partitionCap(n, c.Partitions, c.Imbalance)
	loads := make([]int, c.Partitions)
	for _, p := range assignment {
		if p < 0 || p >= c.Partitions {
			return nil, errors.New("assignment out of range")
		}
		loads[p]++
	}
	for _, n := range loads {
		if n == 0 || n > cap {
			return nil, errors.New("partition cap exceeded")
		}
	}
	return loads, nil
}
func validateArtifactConfig(ids []string, c Config) error {
	if c.Symmetric {
		return errors.New("symmetric graph policy is not supported")
	}
	if c.Metric != "cosine" || c.Repetitions < 1 || c.Repetitions > maxRepetitions || c.Pivots < 2 || c.Pivots > maxPivots || c.MaxLeafBucket < 2 || c.MaxLeafBucket > maxLeafBucket || c.Degree < 1 || c.Degree > maxDegree || c.Partitions < 1 || c.Partitions > maxPartitions || !finite(c.Imbalance) || c.Imbalance < 0 || c.Imbalance > 1 || c.MaxVectors < 1 || c.MaxVectors > maxVectors || c.MaxEdges < 1 || c.MaxEdges > maxEdges || c.MaxDistanceWork < 0 || c.MaxPartitionWork < 0 || len(ids) < c.Partitions || len(ids) > c.MaxVectors {
		return errors.New("invalid artifact configuration")
	}
	if int64(len(ids))*int64(c.Degree) > int64(c.MaxEdges) || int64(len(ids))*int64(c.Degree) > int64(c.MaxEdges)/int64(c.Repetitions) {
		return errors.New("artifact graph bound exceeded")
	}
	return nil
}
func validateGraph(g Graph, n, degree int) error {
	_, err := validateGraphAndDegree(g, n, degree)
	return err
}
func validateGraphAndDegree(g Graph, n, degree int) (int, error) {
	if len(g.Neighbors) != n {
		return 0, errors.New("graph node count mismatch")
	}
	maxObservedDegree := 0
	for i, ns := range g.Neighbors {
		if ns == nil {
			return 0, errors.New("graph neighbor row must be an array")
		}
		if len(ns) > degree {
			return 0, errors.New("degree cap exceeded")
		}
		if len(ns) > maxObservedDegree {
			maxObservedDegree = len(ns)
		}
		previous := -1
		for _, j := range ns {
			if j < 0 || j >= n || j == i || j <= previous {
				return 0, errors.New("invalid graph edge")
			}
			previous = j
		}
	}
	return maxObservedDegree, nil
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
	if !utf8.Valid(raw) {
		return Artifact{}, errors.New("partition artifact contains invalid UTF-8")
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

// DecodeArtifactForSource additionally binds untrusted backend output to the
// source snapshot supplied by the caller.
func DecodeArtifactForSource(raw []byte, maxBytes int, expected Source) (Artifact, error) {
	if err := validateExpectedSource(expected); err != nil {
		return Artifact{}, err
	}
	a, err := DecodeArtifact(raw, maxBytes)
	if err != nil {
		return Artifact{}, err
	}
	if a.Source != expected {
		return Artifact{}, errors.New("backend artifact source does not match expected snapshot")
	}
	return a, nil
}

// DecodeArtifactForRequest binds an external backend response to the exact
// deterministic corpus, IDs, configuration, and graph supplied to it. The
// assignment intentionally remains backend output, but must satisfy the
// independent artifact validator.
func DecodeArtifactForRequest(raw []byte, maxBytes int, request Artifact) (Artifact, error) {
	if err := ValidateArtifact(request); err != nil {
		return Artifact{}, fmt.Errorf("invalid external backend request: %w", err)
	}
	a, err := DecodeArtifactForSource(raw, maxBytes, request.Source)
	if err != nil {
		return Artifact{}, err
	}
	if !reflect.DeepEqual(a.IDs, request.IDs) || a.Config != request.Config || !reflect.DeepEqual(a.Graph, request.Graph) {
		return Artifact{}, errors.New("backend artifact does not match requested graph/configuration")
	}
	return a, nil
}

// RunExternalJSON is an optional offline adapter seam. It never becomes a
// TreeDB runtime dependency. Its output is written to a private temp path and
// removed on cancellation, timeout, command failure, or invalid output.
func RunExternalJSON(ctx context.Context, command []string, input []byte, maxOutput int) (Artifact, error) {
	return Artifact{}, errors.New("external backend requires expected source binding")
}
func RunExternalJSONForSource(ctx context.Context, command []string, input []byte, maxOutput int, expected Source) (Artifact, error) {
	if err := validateExpectedSource(expected); err != nil {
		return Artifact{}, err
	}
	return Artifact{}, errors.New("external backend requires requested artifact binding")
}

// RunExternalJSONForSourceWithLimits fails closed without running a backend:
// source-only binding cannot prove an output used the requested graph. Use
// RunExternalJSONForRequestWithLimits with the exact requested artifact.
func RunExternalJSONForSourceWithLimits(ctx context.Context, command []string, input []byte, limits ExternalJSONLimits, expected Source) (Artifact, error) {
	if err := validateExpectedSource(expected); err != nil {
		return Artifact{}, err
	}
	return Artifact{}, errors.New("external backend requires requested artifact binding")
}

// RunExternalJSONForRequestWithLimits runs an optional backend with separately
// bounded request/response files and binds its response to request's exact
// deterministic graph and configuration.
func RunExternalJSONForRequestWithLimits(ctx context.Context, command []string, input []byte, limits ExternalJSONLimits, request Artifact) (Artifact, error) {
	canonicalRequest, err := CanonicalJSON(request)
	if err != nil {
		return Artifact{}, fmt.Errorf("invalid external backend request: %w", err)
	}
	expected := request.Source
	if err := validateExpectedSource(expected); err != nil {
		return Artifact{}, err
	}
	if ctx == nil {
		return Artifact{}, errors.New("external backend requires context deadline")
	}
	if _, ok := ctx.Deadline(); !ok {
		return Artifact{}, errors.New("external backend requires context deadline")
	}
	if len(command) == 0 || limits.MaxInput < 1 || limits.MaxInput > maxExternalJSONBytes || limits.MaxOutput < 1 || limits.MaxOutput > maxExternalJSONBytes {
		return Artifact{}, errors.New("invalid external backend command")
	}
	if len(input) > limits.MaxInput {
		return Artifact{}, errors.New("external partition backend input exceeds cap")
	}
	if !bytes.Equal(input, canonicalRequest) {
		return Artifact{}, errors.New("external backend input does not match requested artifact")
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
	configureExternalCommand(cmd)
	stderr := &cappedBuffer{limit: limits.MaxOutput}
	cmd.Stderr = stderr
	e := cmd.Run()
	// CommandContext only invokes Cancel while the root process is running.
	// Always clear the private process group after Wait as well, so a root that
	// exits normally cannot leave a pipe-holding child behind.
	cleanupExternalCommand(cmd)
	if e != nil {
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
	raw, e := io.ReadAll(io.LimitReader(f, int64(limits.MaxOutput)+1))
	if e != nil {
		return Artifact{}, e
	}
	if len(raw) > limits.MaxOutput {
		return Artifact{}, errors.New("external partition backend output exceeds cap")
	}
	return DecodeArtifactForRequest(raw, limits.MaxOutput, request)
}

func validateExpectedSource(s Source) error {
	if s.SourceID == "" || !utf8.ValidString(s.SourceID) || len(s.SourceID) > 1024 || len(s.Checksum) != 64 || strings.ToLower(s.Checksum) != s.Checksum || s.Vectors < 1 || s.Vectors > maxVectors || s.Dimensions < 1 || s.Dimensions > maxDimensions || s.Metric != "cosine" {
		return errors.New("invalid expected source binding")
	}
	if _, err := hex.DecodeString(s.Checksum); err != nil {
		return errors.New("invalid expected source binding")
	}
	return nil
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
