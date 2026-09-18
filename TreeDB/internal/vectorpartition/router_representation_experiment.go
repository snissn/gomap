package vectorpartition

// A deliberately separate offline model. These records must never be encoded
// as a V1 router or silently normalized by its native search-pack encoder.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sort"
)

const (
	RouterRepresentationFormatV1       = "treedb_router_representation_experiment_v1"
	RouterRepresentationLeafV1         = "leaf_only"
	RouterRepresentationMultilevelV1   = "budget_safe_multilevel"
	RouterRepresentationLegacySphereV1 = "legacy_spherical_f32"
	RouterRepresentationMeanV1         = "arithmetic_mean_fp64"
	RouterRepresentationUnitMeanV1     = "normalized_mean_fp64"
)

type RouterRepresentationQuotaV1 struct {
	Domain uint32 `json:"domain"`
	Tokens int    `json:"tokens"`
}
type RouterRepresentationOptionsV1 struct {
	Shape    string                        `json:"shape"`
	Geometry string                        `json:"geometry"`
	Config   RouterConfigV1                `json:"clustering_config"`
	Quotas   []RouterRepresentationQuotaV1 `json:"frozen_domain_quotas"`
}
type RouterRepresentationPlanV1 struct {
	Memberships, Dimensions, Tokens, NodeBound int
	ScalarWork                                 int64
	PeakBytes                                  uint64
}
type RouterRepresentationNodeV1 struct {
	RouterHierarchyNodeV1
	MembershipSHA256       string  `json:"membership_sha256"`
	SourceAnchor           uint64  `json:"source_anchor"`
	LegacyCosineDistortion float64 `json:"legacy_cosine_distortion"`
	Radius                 float64 `json:"center_chord_radius"`
	// Only leaves retain IDs. Internal coverage is reconstructed from children.
	Members []uint64 `json:"leaf_members,omitempty"`
}
type RouterRepresentationCentroidV1 struct {
	Domain       uint32    `json:"domain"`
	NodeID       uint32    `json:"represented_node_id"`
	SourceAnchor uint64    `json:"source_anchor"`
	Values       []float64 `json:"center_values"`
}
type RouterRepresentationMetricsV1 struct {
	RequestedBudget      int    `json:"requested_budget"`
	RealizedCount        int    `json:"realized_count"`
	UnusedQuota          []int  `json:"unused_quota"`
	RetainedInternal     int    `json:"retained_internal"`
	RetainedLeaves       int    `json:"retained_leaves"`
	CentroidPayloadBytes uint64 `json:"centroid_payload_bytes"`
	BuildWorkBound       int64  `json:"build_work_bound"`
	BuildPeakBytesBound  uint64 `json:"build_peak_bytes_bound"`
}
type RouterRepresentationModelV1 struct {
	Format               string                           `json:"format"`
	Options              RouterRepresentationOptionsV1    `json:"options"`
	Dimensions           int                              `json:"dimensions"`
	SourceSHA256         string                           `json:"source_sha256"`
	MembershipSHA256     string                           `json:"membership_sha256"`
	LeafMembershipSHA256 string                           `json:"leaf_membership_sha256"`
	Nodes                []RouterRepresentationNodeV1     `json:"nodes"`
	Representatives      []RouterRepresentationCentroidV1 `json:"representatives"`
	Metrics              RouterRepresentationMetricsV1    `json:"metrics"`
}

type RouterRepresentationSourceV1 struct {
	domains                        []uint32
	vectors                        [][]routerBuildVectorV1
	sourceDigest, membershipDigest string
	config                         RouterConfigV1
	quotas                         []RouterRepresentationQuotaV1
	plan                           RouterRepresentationPlanV1
}

func routerBuildContextErrV1(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// Stable bottom-up mergesort with bounded cancellation latency; unlike a
// comparator-panic trick it returns no success after a partial ordering.
func routerRepresentationSortV1[T any](ctx context.Context, a []T, less func(T, T) bool) error {
	if err := routerBuildContextErrV1(ctx); err != nil {
		return err
	}
	if len(a) < 2 {
		return nil
	}
	tmp := make([]T, len(a))
	src, dst := a, tmp
	for width := 1; width < len(a); {
		for start := 0; start < len(a); start += 2 * width {
			if err := routerBuildContextErrV1(ctx); err != nil {
				return err
			}
			mid, end := min(start+width, len(a)), min(start+2*width, len(a))
			i, j := start, mid
			for pos := start; pos < end; pos++ {
				if pos&255 == 0 {
					if err := routerBuildContextErrV1(ctx); err != nil {
						return err
					}
				}
				if i < mid && (j >= end || !less(src[j], src[i])) {
					dst[pos] = src[i]
					i++
				} else {
					dst[pos] = src[j]
					j++
				}
			}
		}
		src, dst = dst, src
		if width > len(a)/2 {
			break
		}
		width *= 2
	}
	copy(a, src)
	return routerBuildContextErrV1(ctx)
}
func representationMulV1(values ...uint64) (uint64, error) {
	v := uint64(1)
	for _, x := range values {
		if x != 0 && v > math.MaxUint64/x {
			return 0, errors.New("representation arithmetic overflow")
		}
		v *= x
	}
	return v, nil
}
func representationAddV1(values ...uint64) (uint64, error) {
	var v uint64
	for _, x := range values {
		if v > math.MaxUint64-x {
			return 0, errors.New("representation arithmetic overflow")
		}
		v += x
	}
	return v, nil
}
func representationSHA256V1(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func representationMembersDigestV1(ctx context.Context, domain uint32, ids []uint64) (string, error) {
	h := sha256.New()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(domain))
	_, _ = h.Write(b[:])
	for i, id := range ids {
		if i&255 == 0 {
			if err := routerBuildContextErrV1(ctx); err != nil {
				return "", err
			}
		}
		binary.LittleEndian.PutUint64(b[:], id)
		_, _ = h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil)), routerBuildContextErrV1(ctx)
}

// Plan uses shape only and runs before source normalization or corpus-sized
// allocations. ScalarWork bounds clustering distance/centroid work plus sorting,
// metadata and hashing; it is not a CPU-time or instruction-count estimate.
func PlanRouterRepresentationV1(populations []int, dimensions int, o RouterRepresentationOptionsV1) (p RouterRepresentationPlanV1, err error) {
	if err = ValidateRouterConfigV1(o.Config); err != nil {
		return p, err
	}
	if o.Shape != RouterRepresentationLeafV1 && o.Shape != RouterRepresentationMultilevelV1 {
		return p, errors.New("unknown representation shape")
	}
	if o.Geometry != RouterRepresentationLegacySphereV1 && o.Geometry != RouterRepresentationMeanV1 && o.Geometry != RouterRepresentationUnitMeanV1 {
		return p, errors.New("unknown centroid geometry")
	}
	if len(populations) == 0 || len(populations) > routerMaxPartitions || len(populations) != len(o.Quotas) || dimensions < 1 || dimensions > o.Config.MaxDimensions {
		return p, errors.New("invalid representation shape")
	}
	p.Dimensions = dimensions
	var work, peak uint64
	for i, n := range populations {
		q := o.Quotas[i]
		if n < 1 || n > o.Config.MaxVectors || q.Tokens < 1 || q.Tokens > n || q.Tokens > o.Config.MaxRepresentatives || q.Domain != uint32(i) {
			return p, errors.New("invalid canonical domain quota/population")
		}
		if p.Memberships > o.Config.MaxVectors-n || p.Tokens > o.Config.MaxRepresentatives-q.Tokens {
			return p, errors.New("representation source/token cap")
		}
		p.Memberships += n
		p.Tokens += q.Tokens
		levels := min(o.Config.MaxDepth, q.Tokens-1)
		k := min(o.Config.BranchFactor, min(q.Tokens, n))
		factor := uint64(levels)*(uint64(k)*uint64(k-1)/2+uint64(o.Config.MaxIterations)*(2*uint64(k)+2)+16) + 16
		v, e := representationMulV1(uint64(n), uint64(dimensions), factor)
		if e != nil {
			return p, e
		}
		// Canonical input sorting and subtree validation at every covering depth.
		order, e := representationMulV1(uint64(n), uint64(levels+2), uint64(bits.Len(uint(n))+1), 32)
		if e != nil {
			return p, e
		}
		selection, e := representationMulV1(uint64(q.Tokens), uint64(q.Tokens), 8)
		if e != nil {
			return p, e
		}
		work, e = representationAddV1(work, v, order, selection)
		if e != nil {
			return p, e
		}
	}
	p.NodeBound = 2*p.Tokens - len(populations)
	if o.Shape == RouterRepresentationMultilevelV1 {
		p.NodeBound = p.Tokens
	}
	// Includes input owned snapshot, normalized shared values, current membership
	// frontier, leaf IDs, validation scratch, maps, centroids, and JSON copies.
	sources, e := representationMulV1(uint64(p.Memberships), uint64(dimensions)*16+1024)
	if e != nil {
		return p, e
	}
	nodes, e := representationMulV1(uint64(p.NodeBound), uint64(dimensions)*128+2048)
	if e != nil {
		return p, e
	}
	peak, e = representationAddV1(sources, nodes, uint64(len(populations))*1024)
	if e != nil {
		return p, e
	}
	if work > uint64(o.Config.MaxScalarWork) || work > math.MaxInt64 || peak > o.Config.MaxRouterBytes {
		return p, fmt.Errorf("representation preflight exceeds caps: work=%d bytes=%d", work, peak)
	}
	p.ScalarWork = int64(work)
	p.PeakBytes = peak
	return p, nil
}

// Prepare canonicalizes inputs, checks duplicate source bits across overlapping
// domains and normalizes each distinct source ordinal exactly once. The returned
// source is private/immutable and can build multiple scoring arms without a new
// normalized corpus. Domain IDs are canonical contiguous logical IDs.
func PrepareRouterRepresentationSourceV1(ctx context.Context, input []RouterPartitionV1, o RouterRepresentationOptionsV1) (*RouterRepresentationSourceV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(input) == 0 || len(input) > routerMaxPartitions {
		return nil, errors.New("invalid representation domains")
	}
	// A small quota/domain array is the only allocation preceding the full plan.
	populations := make([]int, len(input))
	dimensions := 0
	for _, p := range input {
		if p.PartitionID >= uint32(len(input)) || populations[p.PartitionID] != 0 || len(p.Vectors) == 0 {
			return nil, errors.New("invalid/duplicate domain")
		}
		populations[p.PartitionID] = len(p.Vectors)
		if dimensions == 0 {
			dimensions = len(p.Vectors[0].Values)
		}
	}
	plan, err := PlanRouterRepresentationV1(populations, dimensions, o)
	if err != nil {
		return nil, err
	}
	s := &RouterRepresentationSourceV1{domains: make([]uint32, len(input)), vectors: make([][]routerBuildVectorV1, len(input)), config: o.Config, quotas: append([]RouterRepresentationQuotaV1(nil), o.Quotas...), plan: plan}
	type shared struct {
		raw  []float32
		norm []float32
	}
	unique := make(map[uint64]shared, plan.Memberships)
	memberships := sha256.New()
	var b [8]byte
	for _, p := range input {
		vectors := append([]RouterVectorV1(nil), p.Vectors...)
		if err := routerRepresentationSortV1(ctx, vectors, func(a, b RouterVectorV1) bool { return a.Ordinal < b.Ordinal }); err != nil {
			return nil, err
		}
		out := make([]routerBuildVectorV1, len(vectors))
		ids := make([]uint64, len(vectors))
		for i, v := range vectors {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if len(v.Values) != dimensions || (i > 0 && v.Ordinal == vectors[i-1].Ordinal) {
				return nil, errors.New("invalid source dimension/duplicate")
			}
			shared, ok := unique[v.Ordinal]
			if ok {
				for j, x := range v.Values {
					if math.Float32bits(x) != math.Float32bits(shared.raw[j]) {
						return nil, errors.New("overlapping source ordinal has conflicting bits")
					}
				}
			} else {
				norm, err := normalizeRouterVectorV1(v.Values)
				if err != nil {
					return nil, err
				}
				shared.raw = append([]float32(nil), v.Values...)
				shared.norm = norm
				unique[v.Ordinal] = shared
			}
			out[i] = routerBuildVectorV1{ordinal: v.Ordinal, values: shared.norm}
			ids[i] = v.Ordinal
		}
		s.domains[p.PartitionID] = p.PartitionID
		s.vectors[p.PartitionID] = out
	}
	for domain, vs := range s.vectors {
		binary.LittleEndian.PutUint64(b[:], uint64(domain))
		_, _ = memberships.Write(b[:])
		binary.LittleEndian.PutUint64(b[:], uint64(len(vs)))
		_, _ = memberships.Write(b[:])
		for i, v := range vs {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			binary.LittleEndian.PutUint64(b[:], v.ordinal)
			_, _ = memberships.Write(b[:])
		}
	}
	s.membershipDigest = hex.EncodeToString(memberships.Sum(nil))
	ordinals := make([]uint64, 0, len(unique))
	for id := range unique {
		if len(ordinals)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		ordinals = append(ordinals, id)
	}
	if err := routerRepresentationSortV1(ctx, ordinals, func(a, b uint64) bool { return a < b }); err != nil {
		return nil, err
	}
	h := sha256.New()
	binary.LittleEndian.PutUint64(b[:], uint64(dimensions))
	_, _ = h.Write(b[:])
	for i, id := range ordinals {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		binary.LittleEndian.PutUint64(b[:], id)
		_, _ = h.Write(b[:])
		for _, x := range unique[id].raw {
			binary.LittleEndian.PutUint32(b[:4], math.Float32bits(x))
			_, _ = h.Write(b[:4])
		}
	}
	s.sourceDigest = hex.EncodeToString(h.Sum(nil))
	return s, nil
}
func BuildRouterRepresentationForDiagnosticsV1(ctx context.Context, input []RouterPartitionV1, o RouterRepresentationOptionsV1) (RouterRepresentationModelV1, error) {
	s, err := PrepareRouterRepresentationSourceV1(ctx, input, o)
	if err != nil {
		return RouterRepresentationModelV1{}, err
	}
	return s.BuildWithContextV1(ctx, o)
}

func representationValuesV1(ctx context.Context, vectors []routerBuildVectorV1, node *routerBuildNodeV1, geometry string) ([]float64, uint64, float64, error) {
	values := make([]float64, len(node.center))
	anchor := node.members[0]
	best := math.Inf(1)
	for pos, id := range node.members {
		if pos&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, 0, err
			}
		}
		v := vectors[id]
		d := routerCosineDistanceNormalizedV1(v.values, node.center)
		if d < best || d == best && v.ordinal < vectors[anchor].ordinal {
			anchor, best = id, d
		}
		for j, x := range v.values {
			values[j] += float64(x)
		}
	}
	for j := range values {
		values[j] /= float64(len(node.members))
	}
	switch geometry {
	case RouterRepresentationLegacySphereV1:
		for j, x := range node.center {
			values[j] = float64(x)
		}
	case RouterRepresentationUnitMeanV1:
		var norm float64
		for _, x := range values {
			norm += x * x
		}
		if norm == 0 {
			for j, x := range vectors[node.members[0]].values {
				values[j] = float64(x)
			}
		} else {
			inv := 1 / math.Sqrt(norm)
			for j := range values {
				values[j] *= inv
			}
		}
	}
	var radius2 float64
	for pos, id := range node.members {
		if pos&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, 0, err
			}
		}
		var d float64
		for j, x := range vectors[id].values {
			delta := float64(x) - values[j]
			d += delta * delta
		}
		radius2 = max(radius2, d)
	}
	return values, vectors[anchor].ordinal, math.Sqrt(radius2), nil
}

func (s *RouterRepresentationSourceV1) BuildWithContextV1(ctx context.Context, o RouterRepresentationOptionsV1) (RouterRepresentationModelV1, error) {
	var m RouterRepresentationModelV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return m, err
	}
	if s == nil || !reflect.DeepEqual(s.config, o.Config) || !reflect.DeepEqual(s.quotas, o.Quotas) {
		return m, errors.New("representation source/config/quota mismatch")
	}
	pops := make([]int, len(s.vectors))
	for i := range pops {
		pops[i] = len(s.vectors[i])
	}
	plan, err := PlanRouterRepresentationV1(pops, s.plan.Dimensions, o)
	if err != nil {
		return m, err
	}
	o.Quotas = append([]RouterRepresentationQuotaV1(nil), o.Quotas...)
	m = RouterRepresentationModelV1{Format: RouterRepresentationFormatV1, Options: o, Dimensions: plan.Dimensions, SourceSHA256: s.sourceDigest, MembershipSHA256: s.membershipDigest, Metrics: RouterRepresentationMetricsV1{RequestedBudget: plan.Tokens, UnusedQuota: make([]int, len(pops)), BuildWorkBound: plan.ScalarWork, BuildPeakBytesBound: plan.PeakBytes}}
	var next uint32 = 1
	for domain, vs := range s.vectors {
		if err := ctx.Err(); err != nil {
			return RouterRepresentationModelV1{}, err
		}
		members := make([]int, len(vs))
		for i := range members {
			members[i] = i
		}
		root := &routerBuildNodeV1{record: RouterHierarchyNodeV1{NodeID: next, PartitionID: uint32(domain), MemberCount: uint32(len(vs)), Leaf: true}, members: members, path: []uint32{next}}
		next++
		root.center, root.error, err = routerCenterAndErrorWithContextV1(ctx, vs, members)
		if err != nil {
			return RouterRepresentationModelV1{}, err
		}
		nodes := []*routerBuildNodeV1{root}
		leaves := []*routerBuildNodeV1{root}
		// Saved centers/metadata, not ancestor member arrays: live memberships remain O(N).
		records := map[uint32]RouterRepresentationNodeV1{}
		centers := map[uint32][]float64{}
		capture := func(node *routerBuildNodeV1) error {
			values, anchor, radius, err := representationValuesV1(ctx, vs, node, o.Geometry)
			if err != nil {
				return err
			}
			ids := make([]uint64, len(node.members))
			for i, id := range node.members {
				if i&255 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				ids[i] = vs[id].ordinal
			}
			digest, err := representationMembersDigestV1(ctx, uint32(domain), ids)
			if err != nil {
				return err
			}
			records[node.record.NodeID] = RouterRepresentationNodeV1{RouterHierarchyNodeV1: node.record, MembershipSHA256: digest, SourceAnchor: anchor, LegacyCosineDistortion: node.error, Radius: radius}
			centers[node.record.NodeID] = values
			return nil
		}
		if err := capture(root); err != nil {
			return RouterRepresentationModelV1{}, err
		}
		quota := o.Quotas[domain].Tokens
		for {
			used := len(leaves)
			if o.Shape == RouterRepresentationMultilevelV1 {
				used = len(nodes)
			}
			remaining := quota - used
			if remaining <= 0 {
				break
			}
			index, err := routerNextSplitWithContextV1(ctx, leaves, o.Config)
			if err != nil {
				return RouterRepresentationModelV1{}, err
			}
			if index < 0 {
				break
			}
			parent := leaves[index]
			k := min(o.Config.BranchFactor, len(parent.members))
			if o.Shape == RouterRepresentationLeafV1 {
				k = min(k, remaining+1)
			} else {
				k = min(k, remaining)
			}
			if k < 2 {
				break
			}
			children, _, _, err := routerSplitNodeWithContextV1(ctx, vs, parent, k, o.Config, &next)
			if err != nil {
				return RouterRepresentationModelV1{}, err
			}
			if len(children) < 2 {
				break
			}
			parent.record.Leaf = false
			rec := records[parent.record.NodeID]
			rec.Leaf = false
			records[parent.record.NodeID] = rec
			for _, child := range children {
				if err := capture(child); err != nil {
					return RouterRepresentationModelV1{}, err
				}
			}
			parent.members = nil
			parent.center = nil
			if o.Shape == RouterRepresentationLeafV1 {
				delete(centers, parent.record.NodeID)
			}
			leaves = append(leaves[:index], leaves[index+1:]...)
			leaves = append(leaves, children...)
			nodes = append(nodes, children...)
		}
		for _, leaf := range leaves {
			rec := records[leaf.record.NodeID]
			rec.Members = make([]uint64, len(leaf.members))
			for i, id := range leaf.members {
				if i&255 == 0 {
					if err := ctx.Err(); err != nil {
						return RouterRepresentationModelV1{}, err
					}
				}
				rec.Members[i] = vs[id].ordinal
			}
			records[leaf.record.NodeID] = rec
		}
		// Node IDs, not source anchors, are representative identity.
		if err := routerRepresentationSortV1(ctx, nodes, func(a, b *routerBuildNodeV1) bool { return a.record.NodeID < b.record.NodeID }); err != nil {
			return RouterRepresentationModelV1{}, err
		}
		count := 0
		for _, node := range nodes {
			rec := records[node.record.NodeID]
			m.Nodes = append(m.Nodes, rec)
			if values, ok := centers[node.record.NodeID]; ok {
				m.Representatives = append(m.Representatives, RouterRepresentationCentroidV1{Domain: uint32(domain), NodeID: node.record.NodeID, SourceAnchor: rec.SourceAnchor, Values: values})
				count++
				if rec.Leaf {
					m.Metrics.RetainedLeaves++
				} else {
					m.Metrics.RetainedInternal++
				}
			}
		}
		m.Metrics.UnusedQuota[domain] = quota - count
	}
	m.Metrics.RealizedCount = len(m.Representatives)
	m.Metrics.CentroidPayloadBytes = uint64(len(m.Representatives)) * uint64(m.Dimensions) * 8
	m.LeafMembershipSHA256, err = representationLeafDigestV1(ctx, m.Nodes)
	if err != nil {
		return RouterRepresentationModelV1{}, err
	}
	if err := ValidateRouterRepresentationWithContextV1(ctx, m); err != nil {
		return RouterRepresentationModelV1{}, err
	}
	return m, nil
}

func representationLeafDigestV1(ctx context.Context, nodes []RouterRepresentationNodeV1) (string, error) {
	h := sha256.New()
	var b [8]byte
	for i, n := range nodes {
		if i&255 == 0 {
			if err := routerBuildContextErrV1(ctx); err != nil {
				return "", err
			}
		}
		if n.Leaf {
			binary.LittleEndian.PutUint64(b[:], uint64(n.NodeID))
			_, _ = h.Write(b[:])
			_, _ = h.Write([]byte(n.MembershipSHA256))
		}
	}
	return hex.EncodeToString(h.Sum(nil)), routerBuildContextErrV1(ctx)
}

// Reconstruct complete covering subtrees from disjoint leaf memberships.
// Child work buffers are released at each parent; no ancestor-ID matrix persists.
func ValidateRouterRepresentationWithContextV1(ctx context.Context, m RouterRepresentationModelV1) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.Format != RouterRepresentationFormatV1 || !representationSHA256V1(m.SourceSHA256) || !representationSHA256V1(m.MembershipSHA256) || !representationSHA256V1(m.LeafMembershipSHA256) || len(m.Nodes) == 0 || len(m.Nodes) > 2*routerMaxRepresentatives {
		return errors.New("invalid representation format/identity")
	}
	if err := ValidateRouterConfigV1(m.Options.Config); err != nil {
		return err
	}
	d := len(m.Options.Quotas)
	if d < 1 || d > routerMaxPartitions {
		return errors.New("invalid representation quota count")
	}
	populations := make([]int, d)
	roots := make([]int, d)
	for i := range roots {
		roots[i] = -1
	}
	for i, n := range m.Nodes {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if n.NodeID != uint32(i+1) || n.PartitionID >= uint32(d) || n.MemberCount == 0 || n.Depth > uint16(m.Options.Config.MaxDepth) || !representationSHA256V1(n.MembershipSHA256) || math.IsNaN(n.Radius) || math.IsInf(n.Radius, 0) || n.Radius < 0 || math.IsNaN(n.LegacyCosineDistortion) || math.IsInf(n.LegacyCosineDistortion, 0) {
			return errors.New("invalid representation node")
		}
		if n.ParentNodeID == 0 {
			if n.Depth != 0 || roots[n.PartitionID] >= 0 {
				return errors.New("duplicate/invalid representation root")
			}
			roots[n.PartitionID] = i
			populations[n.PartitionID] = int(n.MemberCount)
		} else {
			if n.ParentNodeID >= n.NodeID {
				return errors.New("cyclic representation ancestry")
			}
			parent := m.Nodes[n.ParentNodeID-1]
			if parent.PartitionID != n.PartitionID || n.Depth != parent.Depth+1 || n.MemberCount > parent.MemberCount {
				return errors.New("inconsistent representation ancestry")
			}
		}
	}
	plan, err := PlanRouterRepresentationV1(populations, m.Dimensions, m.Options)
	if err != nil {
		return err
	}
	if len(m.Nodes) > plan.NodeBound || len(m.Representatives) > plan.Tokens {
		return errors.New("representation node/token overspend")
	}
	children := make([][]int, len(m.Nodes))
	for i, n := range m.Nodes {
		if n.ParentNodeID != 0 {
			children[n.ParentNodeID-1] = append(children[n.ParentNodeID-1], i)
		}
	}
	frontier := make([][]uint64, len(m.Nodes))
	membershipHash := sha256.New()
	var b [8]byte
	for i := len(m.Nodes) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := m.Nodes[i]
		var ids []uint64
		if n.Leaf {
			if len(children[i]) != 0 || len(n.Members) != int(n.MemberCount) {
				return errors.New("invalid representation leaf")
			}
			ids = n.Members
		} else {
			if len(children[i]) < 2 || len(n.Members) != 0 {
				return errors.New("internal representative mislabeled as leaf")
			}
			ids = make([]uint64, 0, int(n.MemberCount))
			for _, child := range children[i] {
				if len(ids) > int(n.MemberCount)-len(frontier[child]) {
					return errors.New("child membership count exceeds parent")
				}
				ids = append(ids, frontier[child]...)
				frontier[child] = nil
			}
			if err := routerRepresentationSortV1(ctx, ids, func(a, b uint64) bool { return a < b }); err != nil {
				return err
			}
		}
		if len(ids) != int(n.MemberCount) {
			return errors.New("incomplete covering subtree")
		}
		for j, id := range ids {
			if j&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if j > 0 && id <= ids[j-1] {
				return errors.New("duplicate/noncanonical leaf membership")
			}
		}
		anchor := sort.Search(len(ids), func(i int) bool { return ids[i] >= n.SourceAnchor })
		if anchor == len(ids) || ids[anchor] != n.SourceAnchor {
			return errors.New("source anchor outside represented subtree")
		}
		digest, err := representationMembersDigestV1(ctx, n.PartitionID, ids)
		if err != nil {
			return err
		}
		if digest != n.MembershipSHA256 {
			return errors.New("subtree membership digest mismatch")
		}
		frontier[i] = ids
	}
	for domain, root := range roots {
		if root < 0 {
			return errors.New("missing representation root")
		}
		ids := frontier[root]
		binary.LittleEndian.PutUint64(b[:], uint64(domain))
		_, _ = membershipHash.Write(b[:])
		binary.LittleEndian.PutUint64(b[:], uint64(len(ids)))
		_, _ = membershipHash.Write(b[:])
		for i, id := range ids {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			binary.LittleEndian.PutUint64(b[:], id)
			_, _ = membershipHash.Write(b[:])
		}
	}
	leafDigest, err := representationLeafDigestV1(ctx, m.Nodes)
	if err != nil {
		return err
	}
	if hex.EncodeToString(membershipHash.Sum(nil)) != m.MembershipSHA256 || leafDigest != m.LeafMembershipSHA256 {
		return errors.New("representation membership identity mismatch")
	}
	used := make([]int, d)
	seen := make([]bool, len(m.Nodes))
	internal, leaves := 0, 0
	for i, r := range m.Representatives {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if r.NodeID < 1 || int(r.NodeID) > len(m.Nodes) || seen[r.NodeID-1] || r.Domain >= uint32(d) || len(r.Values) != m.Dimensions {
			return errors.New("invalid representation centroid identity")
		}
		seen[r.NodeID-1] = true
		n := m.Nodes[r.NodeID-1]
		if n.PartitionID != r.Domain || n.SourceAnchor != r.SourceAnchor || (m.Options.Shape == RouterRepresentationLeafV1 && !n.Leaf) || i > 0 && r.NodeID <= m.Representatives[i-1].NodeID {
			return errors.New("inconsistent represented node")
		}
		var norm float64
		for _, x := range r.Values {
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return errors.New("nonfinite centroid")
			}
			norm += x * x
		}
		if norm > 1.0001 || (m.Options.Geometry != RouterRepresentationMeanV1 && (norm < .9999 || norm > 1.0001)) {
			return errors.New("centroid geometry norm invalid")
		}
		used[r.Domain]++
		if n.Leaf {
			leaves++
		} else {
			internal++
		}
	}
	for i, n := range m.Nodes {
		if (m.Options.Shape == RouterRepresentationMultilevelV1 || n.Leaf) != seen[i] {
			return errors.New("incomplete representation node selection")
		}
	}
	unused := make([]int, d)
	for i, q := range m.Options.Quotas {
		unused[i] = q.Tokens - used[i]
		if unused[i] < 0 {
			return errors.New("domain quota exceeded")
		}
	}
	expected := RouterRepresentationMetricsV1{RequestedBudget: plan.Tokens, RealizedCount: len(m.Representatives), UnusedQuota: unused, RetainedInternal: internal, RetainedLeaves: leaves, CentroidPayloadBytes: uint64(len(m.Representatives)) * uint64(m.Dimensions) * 8, BuildWorkBound: plan.ScalarWork, BuildPeakBytesBound: plan.PeakBytes}
	if !reflect.DeepEqual(expected, m.Metrics) {
		return errors.New("representation metrics not derived from model")
	}
	return ctx.Err()
}

func RouterRepresentationControlMatchesV1(m RouterRepresentationModelV1, want RouterModelV1) error {
	if m.Options.Shape != RouterRepresentationLeafV1 || m.Options.Geometry != RouterRepresentationLegacySphereV1 || m.Dimensions != want.Dimensions || len(m.Nodes) != len(want.Nodes) || len(m.Representatives) != len(want.Representatives) {
		return errors.New("leaf control shape differs from V1")
	}
	for i, n := range m.Nodes {
		if n.RouterHierarchyNodeV1 != want.Nodes[i] {
			return errors.New("leaf control hierarchy differs from V1")
		}
	}
	for i, r := range m.Representatives {
		w := want.Representatives[i]
		if r.Domain != w.PartitionID || r.NodeID != w.LeafNodeID || r.SourceAnchor != w.SourceOrdinal || len(r.Values) != len(w.Values) {
			return errors.New("leaf control representative differs from V1")
		}
		for j, x := range r.Values {
			if math.Float64bits(x) != math.Float64bits(float64(w.Values[j])) {
				return errors.New("leaf control centroid bits differ from V1")
			}
		}
	}
	return nil
}

func EncodeRouterRepresentationV1(ctx context.Context, m RouterRepresentationModelV1) ([]byte, string, error) {
	if err := ValidateRouterRepresentationWithContextV1(ctx, m); err != nil {
		return nil, "", err
	}
	return encodeRouterRepresentationJSONV1(ctx, m)
}

func DecodeRouterRepresentationV1(ctx context.Context, raw []byte, maxBytes uint64) (RouterRepresentationModelV1, error) {
	var m RouterRepresentationModelV1
	if maxBytes == 0 || maxBytes > routerMaxBytes || len(raw) == 0 || uint64(len(raw)) > maxBytes {
		return m, errors.New("invalid experimental model byte cap")
	}
	if err := routerBuildContextErrV1(ctx); err != nil {
		return m, err
	}
	dec := json.NewDecoder(&representationContextReaderV1{ctx: ctx, reader: bytes.NewReader(raw)})
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return RouterRepresentationModelV1{}, err
	}
	// InputOffset ends after the first JSON value even when the decoder has
	// read ahead. Inspect the original byte slice, never decode a trailing
	// value into an arbitrary object (which could allocate another model).
	if err := representationJSONTrailingV1(ctx, raw[dec.InputOffset():]); err != nil {
		return RouterRepresentationModelV1{}, err
	}
	if err := ValidateRouterRepresentationWithContextV1(ctx, m); err != nil {
		return RouterRepresentationModelV1{}, err
	}
	return m, nil
}

// Frozen exact-scoring owner retains only bounded centroids, not source rows or
// leaf memberships. Validation/encoding are performed once, not per query.
type PreparedRouterRepresentationV1 struct {
	reps    []RouterRepresentationCentroidV1
	summary RouterRepresentationSummaryV1
}

// Identity-aligned scalar node diagnostics; no borrowed membership or vector data.
type RouterRepresentationNodeSummaryV1 struct {
	Ordinal                int     `json:"representative_ordinal"`
	Domain                 uint32  `json:"domain"`
	NodeID                 uint32  `json:"node_id"`
	SourceAnchor           uint64  `json:"source_anchor"`
	Depth                  uint16  `json:"depth"`
	Members                uint32  `json:"member_count"`
	Leaf                   bool    `json:"leaf"`
	Radius                 float64 `json:"center_chord_radius"`
	LegacyCosineDistortion float64 `json:"legacy_cosine_distortion"`
	MembershipSHA256       string  `json:"membership_sha256"`
}
type RouterRepresentationSummaryV1 struct {
	Format               string                              `json:"format"`
	Shape                string                              `json:"shape"`
	Geometry             string                              `json:"geometry"`
	SourceSHA256         string                              `json:"source_sha256"`
	MembershipSHA256     string                              `json:"membership_sha256"`
	LeafMembershipSHA256 string                              `json:"leaf_membership_sha256"`
	ModelSHA256          string                              `json:"model_sha256"`
	Dimensions           int                                 `json:"dimensions"`
	Domains              int                                 `json:"domains"`
	Representatives      int                                 `json:"representatives"`
	Nodes                int                                 `json:"node_count"`
	EncodedBytes         uint64                              `json:"encoded_bytes"`
	Metrics              RouterRepresentationMetricsV1       `json:"metrics"`
	RepresentedNodes     []RouterRepresentationNodeSummaryV1 `json:"represented_nodes"`
}
type RouterRepresentationScoreV1 struct {
	Ordinal      int
	Domain       uint32
	SourceAnchor uint64
	Score        float64
}

func PrepareRouterRepresentationScoringV1(ctx context.Context, m RouterRepresentationModelV1) (*PreparedRouterRepresentationV1, error) {
	raw, digest, err := EncodeRouterRepresentationV1(ctx, m)
	if err != nil {
		return nil, err
	}
	p := &PreparedRouterRepresentationV1{summary: RouterRepresentationSummaryV1{Format: m.Format, Shape: m.Options.Shape, Geometry: m.Options.Geometry, SourceSHA256: m.SourceSHA256, MembershipSHA256: m.MembershipSHA256, LeafMembershipSHA256: m.LeafMembershipSHA256, ModelSHA256: digest, Dimensions: m.Dimensions, Domains: len(m.Options.Quotas), Representatives: len(m.Representatives), Nodes: len(m.Nodes), EncodedBytes: uint64(len(raw)), Metrics: m.Metrics}, reps: make([]RouterRepresentationCentroidV1, len(m.Representatives))}
	p.summary.Metrics.UnusedQuota = append([]int(nil), m.Metrics.UnusedQuota...)
	p.summary.RepresentedNodes = make([]RouterRepresentationNodeSummaryV1, len(m.Representatives))
	for i, r := range m.Representatives {
		if i&255 == 0 {
			if err := routerBuildContextErrV1(ctx); err != nil {
				return nil, err
			}
		}
		p.reps[i] = r
		n := m.Nodes[r.NodeID-1]
		p.summary.RepresentedNodes[i] = RouterRepresentationNodeSummaryV1{Ordinal: i, Domain: r.Domain, NodeID: r.NodeID, SourceAnchor: r.SourceAnchor, Depth: n.Depth, Members: n.MemberCount, Leaf: n.Leaf, Radius: n.Radius, LegacyCosineDistortion: n.LegacyCosineDistortion, MembershipSHA256: n.MembershipSHA256}
		p.reps[i].Values = append([]float64(nil), r.Values...)
	}
	return p, nil
}
func (p *PreparedRouterRepresentationV1) SummaryV1() RouterRepresentationSummaryV1 {
	if p == nil {
		return RouterRepresentationSummaryV1{}
	}
	s := p.summary
	s.Metrics.UnusedQuota = append([]int(nil), s.Metrics.UnusedQuota...)
	s.RepresentedNodes = append([]RouterRepresentationNodeSummaryV1(nil), s.RepresentedNodes...)
	return s
}
func (p *PreparedRouterRepresentationV1) ScoreExactWithContextV1(ctx context.Context, query []float32) ([]RouterRepresentationScoreV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || len(query) != p.summary.Dimensions {
		return nil, errors.New("invalid prepared representation/query")
	}
	q, err := normalizeRouterVectorV1(query)
	if err != nil {
		return nil, err
	}
	out := make([]RouterRepresentationScoreV1, len(p.reps))
	for i, r := range p.reps {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		var dot, norm float64
		for j, x := range r.Values {
			dot += float64(q[j]) * x
			norm += x * x
		}
		score := dot
		if p.summary.Geometry == RouterRepresentationMeanV1 {
			score = 2*dot - norm
		}
		out[i] = RouterRepresentationScoreV1{Ordinal: i, Domain: r.Domain, SourceAnchor: r.SourceAnchor, Score: score}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
