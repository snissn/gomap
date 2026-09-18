package collections

// Offline representation owners are deliberately distinct from native V1
// router owners. Raw arithmetic means must not be renormalized by a search pack.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

const VectorPartitionRouterRepresentationMethodV1 = "frozen_membership_representation_comparison_v1"
const VectorPartitionRouterRepresentationMaxBuildWorkV1 uint64 = 1_000_000_000_000_000
const VectorPartitionRouterRepresentationMaxBuildBytesV1 uint64 = 1 << 30

type VectorPartitionRouterRepresentationOptionsV1 struct {
	Arm           string `json:"arm"`
	ReturnedWidth int    `json:"returned_width"`
	MaxBuildWork  uint64 `json:"max_build_work"`
	MaxBuildBytes uint64 `json:"max_build_bytes"`
}

func ValidateVectorPartitionRouterRepresentationOptionsV1(o VectorPartitionRouterRepresentationOptionsV1) error {
	if (o.Arm != "multilevel" && o.Arm != "centroid_geometry") || o.ReturnedWidth < 1 || o.ReturnedWidth > 1_000_000 || o.MaxBuildWork < 1 || o.MaxBuildWork > VectorPartitionRouterRepresentationMaxBuildWorkV1 || o.MaxBuildBytes < 1 || o.MaxBuildBytes > VectorPartitionRouterRepresentationMaxBuildBytesV1 {
		return errors.New("invalid bounded representation experiment options")
	}
	return nil
}
func VectorPartitionRouterRepresentationIdentityV1(o VectorPartitionRouterRepresentationOptionsV1) string {
	raw, _ := json.Marshal(o)
	h := sha256.New()
	_, _ = h.Write([]byte(VectorPartitionRouterRepresentationMethodV1 + "\x00"))
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

type VectorPartitionRouterRepresentationInfoV1 struct {
	Method              string                                         `json:"method"`
	QuotaMethod         string                                         `json:"quota_method"`
	ClusteringConfig    internalrouter.RouterConfigV1                  `json:"clustering_config"`
	Options             VectorPartitionRouterRepresentationOptionsV1   `json:"options"`
	OptionsSHA256       string                                         `json:"options_sha256"`
	Generation          uint64                                         `json:"generation"`
	SourceGeneration    uint64                                         `json:"source_generation"`
	OriginalModelSHA256 string                                         `json:"original_model_sha256"`
	ManifestSHA256      string                                         `json:"manifest_sha256"`
	OriginalRouterBytes uint64                                         `json:"original_router_asset_bytes"`
	SourceChecksum      uint64                                         `json:"source_checksum"`
	SourceSchemaHash    uint64                                         `json:"source_schema_hash"`
	SourceRows          uint64                                         `json:"source_rows"`
	Quotas              []internalrouter.RouterRepresentationQuotaV1   `json:"quotas"`
	Models              []internalrouter.RouterRepresentationSummaryV1 `json:"models"`
	BuildWorkBound      uint64                                         `json:"build_work_bound"`
	BuildPeakBytesBound uint64                                         `json:"build_peak_bytes_bound"`
}
type VectorPartitionRouterRepresentationResultV1 struct {
	Status                    string                                 `json:"status"`
	ModelSHA256               string                                 `json:"model_sha256"`
	Geometry                  string                                 `json:"score_geometry"`
	QuerySHA256               string                                 `json:"query_sha256"`
	ReturnedWidth             int                                    `json:"returned_width"`
	Probes                    int                                    `json:"probes"`
	ScoreInvocations          uint64                                 `json:"score_invocations"`
	VectorBytes               uint64                                 `json:"centroid_value_bytes_read"`
	FullExactDomainPriorities []VectorPartitionRouterPolicyDomainV1  `json:"full_exact_domain_priorities,omitempty"`
	Policies                  *VectorPartitionRouterEffortPoliciesV1 `json:"policies,omitempty"`
}
type VectorPartitionRouterRepresentationV1 struct {
	info   VectorPartitionRouterRepresentationInfoV1
	models []*internalrouter.PreparedRouterRepresentationV1
}

func (e *VectorPartitionRouterRepresentationV1) InfoV1() VectorPartitionRouterRepresentationInfoV1 {
	if e == nil {
		return VectorPartitionRouterRepresentationInfoV1{}
	}
	out := e.info
	out.Quotas = append([]internalrouter.RouterRepresentationQuotaV1(nil), out.Quotas...)
	out.Models = append([]internalrouter.RouterRepresentationSummaryV1(nil), out.Models...)
	for i := range out.Models {
		out.Models[i].Metrics.UnusedQuota = append([]int(nil), out.Models[i].Metrics.UnusedQuota...)
		out.Models[i].RepresentedNodes = append([]internalrouter.RouterRepresentationNodeSummaryV1(nil), out.Models[i].RepresentedNodes...)
	}
	return out
}
func representationCollectionAddV1(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, errors.New("representation collection bound overflow")
	}
	return a + b, nil
}

// The normalized source and current model/codec scratch are shared or serial,
// not multiplied by the number of arms. Previously built centroid-only owners
// and their copied scalar summaries coexist, so they are charged additively.
func representationCollectionEnvelopeV1(populations []int, dimensions int, arms []internalrouter.RouterRepresentationOptionsV1) (work, peak uint64, err error) {
	var retained uint64
	for _, arm := range arms {
		p, e := internalrouter.PlanRouterRepresentationV1(populations, dimensions, arm)
		if e != nil {
			return 0, 0, e
		}
		work, e = representationCollectionAddV1(work, uint64(p.ScalarWork))
		if e != nil {
			return 0, 0, e
		}
		if p.PeakBytes > peak {
			peak = p.PeakBytes
		}
		r := uint64(p.Tokens)*(uint64(dimensions)*8+768) + uint64(len(populations))*128
		retained, e = representationCollectionAddV1(retained, r)
		if e != nil {
			return 0, 0, e
		}
	}
	peak, err = representationCollectionAddV1(peak, retained)
	return
}

// BuildRepresentationForDiagnosticsV1 captures one immutable source/model and
// releases every borrowed asset before returning owned centroid-only scorers.
// It neither changes the active generation nor updates ordinary search counters.
func (r *VectorPartitionRouterV1) BuildRepresentationForDiagnosticsV1(ctx context.Context, c *Collection, o VectorPartitionRouterRepresentationOptionsV1) (_ *VectorPartitionRouterRepresentationV1, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateVectorPartitionRouterRepresentationOptionsV1(o); err != nil {
		return nil, err
	}
	if r == nil || c == nil {
		return nil, errors.New("missing representation source owner")
	}
	r.closeMu.RLock()
	defer r.closeMu.RUnlock()
	if r.closed.Load() || r.view == nil {
		return nil, errors.New("closed representation source owner")
	}
	m := r.manifest
	d := r.model.Metrics.Partitions
	if c.name != m.Collection || d < 1 || d > int(m.PartitionCount) || m.PartitionCount > 16384 || r.model.Dimensions < 1 || o.ReturnedWidth > len(r.model.Representatives) {
		return nil, errors.New("representation source/model shape mismatch")
	}
	packDomains := make([]int, m.PartitionCount)
	for i := range packDomains {
		packDomains[i] = -1
	}
	if len(m.DomainPacks) == 0 {
		if d != int(m.PartitionCount) {
			return nil, errors.New("missing logical pack bindings")
		}
		for i := range packDomains {
			packDomains[i] = i
		}
	} else {
		for _, b := range m.DomainPacks {
			if int(b.PackID) >= len(packDomains) || int(b.DomainID) >= d || packDomains[b.PackID] >= 0 {
				return nil, errors.New("invalid representation pack binding")
			}
			packDomains[b.PackID] = int(b.DomainID)
		}
		for _, domain := range packDomains {
			if domain < 0 {
				return nil, errors.New("unbound representation pack")
			}
		}
	}
	quotas := make([]internalrouter.RouterRepresentationQuotaV1, d)
	for i := range quotas {
		quotas[i].Domain = uint32(i)
	}
	for _, rep := range r.model.Representatives {
		if int(rep.PartitionID) >= d {
			return nil, errors.New("invalid representative domain")
		}
		quotas[rep.PartitionID].Tokens++
	}
	// Conservative raw counts are available without copying source rows. They
	// include any same-domain pack duplicates; deduplication can only lower cost.
	populations := make([]int, d)
	for _, members := range [][]VectorPartitionMembershipV1{m.Memberships, m.OverlapMemberships} {
		for i, x := range members {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if int(x.PartitionID) >= len(packDomains) || x.VectorOrdinal >= m.SourceRowCount {
				return nil, errors.New("representation membership outside source")
			}
			populations[packDomains[x.PartitionID]]++
		}
	}
	base := internalrouter.RouterRepresentationOptionsV1{Shape: internalrouter.RouterRepresentationLeafV1, Geometry: internalrouter.RouterRepresentationLegacySphereV1, Config: r.model.Config, Quotas: quotas}
	arms := []internalrouter.RouterRepresentationOptionsV1{base}
	if o.Arm == "multilevel" {
		next := base
		next.Shape = internalrouter.RouterRepresentationMultilevelV1
		arms = append(arms, next)
	} else {
		unit, mean := base, base
		unit.Geometry = internalrouter.RouterRepresentationUnitMeanV1
		mean.Geometry = internalrouter.RouterRepresentationMeanV1
		arms = append(arms, unit, mean)
	}
	work, peak, err := representationCollectionEnvelopeV1(populations, r.model.Dimensions, arms)
	if err != nil {
		return nil, err
	}
	if work > o.MaxBuildWork || peak > o.MaxBuildBytes {
		return nil, fmt.Errorf("representation preflight: work=%d bytes=%d exceed work=%d bytes=%d", work, peak, o.MaxBuildWork, o.MaxBuildBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	def, err := c.vectorPartitionRouterDefinitionV1(m.IndexName)
	if err != nil {
		return nil, err
	}
	reader, err := c.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, reader.Close()) }()
	if reader.typedVectorSource == nil || reader.graph.RowCount < 1 || uint64(reader.graph.RowCount) != m.SourceRowCount || reader.graph.BaseManifestGeneration != m.SourceGeneration || reader.graph.BaseManifestChecksum != m.SourceChecksum || reader.graph.BaseSchemaHash != m.SourceSchemaHash || def.Dimensions != r.model.Dimensions {
		return nil, errors.New("representation source generation changed")
	}
	// No document materialization: source ordinal and the immutable manifest bind
	// membership. One vector copy per source ordinal, shared by overlapping domains.
	rows := make(map[uint64][]float32)
	parts := make([]internalrouter.RouterPartitionV1, d)
	seen := make([]map[uint64]bool, d)
	for i := range parts {
		parts[i].PartitionID = uint32(i)
		seen[i] = make(map[uint64]bool)
	}
	for _, members := range [][]VectorPartitionMembershipV1{m.Memberships, m.OverlapMemberships} {
		for i, x := range members {
			if i&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			domain := packDomains[x.PartitionID]
			if seen[domain][x.VectorOrdinal] {
				continue
			}
			seen[domain][x.VectorOrdinal] = true
			if _, present := rows[x.VectorOrdinal]; !present {
				values, _, _, ok := reader.typedVectorSource.vectorForOrdinal(int(x.VectorOrdinal))
				if !ok || len(values) != def.Dimensions {
					return nil, errors.New("unavailable representation vector")
				}
				rows[x.VectorOrdinal] = append([]float32(nil), values...)
			}
			parts[domain].Vectors = append(parts[domain].Vectors, internalrouter.RouterVectorV1{Ordinal: x.VectorOrdinal, Values: rows[x.VectorOrdinal]})
		}
	}
	source, err := internalrouter.PrepareRouterRepresentationSourceV1(ctx, parts, base)
	if err != nil {
		return nil, err
	}
	manifestSHA, err := m.integrityDigestWithContextV1(ctx)
	if err != nil {
		return nil, err
	}
	out := &VectorPartitionRouterRepresentationV1{info: VectorPartitionRouterRepresentationInfoV1{Method: VectorPartitionRouterRepresentationMethodV1, QuotaMethod: "frozen_realized_control_per_domain_v1", ClusteringConfig: r.model.Config, Options: o, OptionsSHA256: VectorPartitionRouterRepresentationIdentityV1(o), Generation: m.Generation, SourceGeneration: m.SourceGeneration, OriginalModelSHA256: r.modelDigest, ManifestSHA256: manifestSHA, OriginalRouterBytes: m.RouterAsset.Bytes, SourceChecksum: m.SourceChecksum, SourceSchemaHash: m.SourceSchemaHash, SourceRows: m.SourceRowCount, Quotas: append([]internalrouter.RouterRepresentationQuotaV1(nil), quotas...), BuildWorkBound: work, BuildPeakBytesBound: peak}}
	var leafHash string
	for i, arm := range arms {
		model, err := source.BuildWithContextV1(ctx, arm)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			if err := internalrouter.RouterRepresentationControlMatchesV1(model, r.model); err != nil {
				return nil, err
			}
			leafHash = model.LeafMembershipSHA256
		} else if o.Arm == "centroid_geometry" && model.LeafMembershipSHA256 != leafHash {
			return nil, errors.New("geometry comparison changed leaf membership")
		}
		prepared, err := internalrouter.PrepareRouterRepresentationScoringV1(ctx, model)
		if err != nil {
			return nil, err
		}
		out.models = append(out.models, prepared)
		out.info.Models = append(out.info.Models, prepared.SummaryV1())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *VectorPartitionRouterRepresentationV1) CompareWithContextV1(ctx context.Context, q []float32, probes int) ([]VectorPartitionRouterRepresentationResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e == nil || len(e.models) < 2 || probes < 1 || probes > len(e.info.Quotas) {
		return nil, errors.New("invalid representation comparison")
	}
	if _, err := normalizeVectorPartitionRouterQueryV1(q, e.info.Models[0].Dimensions); err != nil {
		return nil, err
	}
	out := make([]VectorPartitionRouterRepresentationResultV1, len(e.models))
	qh := vectorPartitionPolicyQueryDigestV1(q)
	for i, model := range e.models {
		info := e.info.Models[i]
		r := &out[i]
		*r = VectorPartitionRouterRepresentationResultV1{ModelSHA256: info.ModelSHA256, Geometry: info.Geometry, QuerySHA256: qh, ReturnedWidth: e.info.Options.ReturnedWidth, Probes: probes}
		if r.ReturnedWidth > info.Representatives {
			r.Status = "returned_width_exceeds_model"
			continue
		}
		scores, err := model.ScoreExactWithContextV1(ctx, q)
		if err != nil {
			return nil, err
		}
		r.ScoreInvocations = uint64(len(scores))
		r.VectorBytes = uint64(len(scores)) * uint64(info.Dimensions) * 8
		candidates := make([]vectorPartitionPolicyCandidateV1, len(scores))
		for j, s := range scores {
			if j&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			candidates[j] = vectorPartitionPolicyCandidateV1{Ordinal: s.Ordinal, Domain: s.Domain, SourceOrdinal: s.SourceAnchor, Score: s.Score}
		}
		// A separate full-scan distance priority explains coarsening misses. It is
		// not substituted for nearest-w voting or used as a fallback route.
		best := make([]VectorPartitionRouterPolicyDomainV1, len(e.info.Quotas))
		seen := make([]bool, len(best))
		for j, c := range candidates {
			if j&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			d := vectorPartitionPolicyDistanceV1(c.Score)
			old := best[c.Domain]
			if !seen[c.Domain] || d < old.Distance || (d == old.Distance && c.Ordinal < old.WinningRepresentative) {
				best[c.Domain] = VectorPartitionRouterPolicyDomainV1{Domain: c.Domain, Distance: d, WinningRepresentative: c.Ordinal, WinningSourceOrdinal: c.SourceOrdinal}
			}
			seen[c.Domain] = true
		}
		for _, ok := range seen {
			if !ok {
				return nil, errors.New("experimental model leaves domain unrepresented")
			}
		}
		if err := sortVectorPartitionSliceWithContextV1(ctx, best, func(a, b VectorPartitionRouterPolicyDomainV1) bool {
			if a.Distance != b.Distance {
				return a.Distance < b.Distance
			}
			return a.Domain < b.Domain
		}); err != nil {
			return nil, err
		}
		r.FullExactDomainPriorities = best
		meta := vectorPartitionPolicyContextV1{ModelDigest: info.ModelSHA256, QueryDigest: qh, Mode: "exact", RepresentativeCount: len(scores), DomainCount: len(e.info.Quotas), CandidateBudget: len(scores), ReturnedWidth: r.ReturnedWidth, Probes: probes, ScoreGeometry: info.Geometry, RetrievalDigest: e.info.OptionsSHA256}
		reduced, err := reduceVectorPartitionRouterPoliciesV1(ctx, meta, candidates)
		if err != nil && !errors.Is(err, errVectorPartitionPolicyCoverageV1) {
			return nil, err
		}
		r.Policies = &reduced
		r.Status = "pass"
		if err != nil {
			r.Status = "candidate_coverage_shortfall"
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// A small structural validator for evidence consumers. These claims must also
// be rebuilt from authoritative source assets during retained replay.
func ValidateVectorPartitionRouterRepresentationInfoV1(i VectorPartitionRouterRepresentationInfoV1) error {
	if i.QuotaMethod != "frozen_realized_control_per_domain_v1" {
		return errors.New("unknown representation quota method")
	}
	if err := internalrouter.ValidateRouterConfigV1(i.ClusteringConfig); err != nil {
		return err
	}
	if err := ValidateVectorPartitionRouterRepresentationOptionsV1(i.Options); err != nil {
		return err
	}
	n := 2
	if i.Options.Arm == "centroid_geometry" {
		n = 3
	}
	if i.Method != VectorPartitionRouterRepresentationMethodV1 || i.OptionsSHA256 != VectorPartitionRouterRepresentationIdentityV1(i.Options) || len(i.Models) != n || len(i.Quotas) == 0 || i.BuildWorkBound == 0 || i.BuildWorkBound > i.Options.MaxBuildWork || i.BuildPeakBytesBound == 0 || i.BuildPeakBytesBound > i.Options.MaxBuildBytes || i.Generation == 0 || i.SourceRows == 0 {
		return errors.New("invalid representation info")
	}
	digests := []string{i.OriginalModelSHA256, i.ManifestSHA256}
	budget := 0
	for j, q := range i.Quotas {
		if int(q.Domain) != j || q.Tokens < 1 || q.Tokens > 1_000_000 || budget > 1_000_000-q.Tokens {
			return errors.New("invalid representation quota")
		}
		budget += q.Tokens
	}
	for j, m := range i.Models {
		shape, geometry := internalrouter.RouterRepresentationLeafV1, internalrouter.RouterRepresentationLegacySphereV1
		if j > 0 {
			if i.Options.Arm == "multilevel" {
				shape = internalrouter.RouterRepresentationMultilevelV1
			} else if j == 1 {
				geometry = internalrouter.RouterRepresentationUnitMeanV1
			} else {
				geometry = internalrouter.RouterRepresentationMeanV1
			}
		}
		if m.Format != internalrouter.RouterRepresentationFormatV1 || m.Shape != shape || m.Geometry != geometry || m.Domains != len(i.Quotas) || m.Dimensions < 1 || m.Dimensions > 4096 || m.Representatives < len(i.Quotas) || m.Representatives > budget || m.Nodes < m.Representatives || m.Nodes > 2*budget || m.EncodedBytes == 0 || m.EncodedBytes > i.Options.MaxBuildBytes || m.Metrics.RequestedBudget != budget || m.Metrics.RealizedCount != m.Representatives || m.Metrics.CentroidPayloadBytes != uint64(m.Representatives)*uint64(m.Dimensions)*8 || len(m.Metrics.UnusedQuota) != len(i.Quotas) || m.Metrics.BuildWorkBound <= 0 || uint64(m.Metrics.BuildWorkBound) > i.BuildWorkBound || m.Metrics.BuildPeakBytesBound > i.BuildPeakBytesBound {
			return errors.New("invalid representation model summary")
		}
		unused := 0
		for k, v := range m.Metrics.UnusedQuota {
			if v < 0 || v >= i.Quotas[k].Tokens {
				return errors.New("invalid unused representation quota")
			}
			unused += v
		}
		if unused+m.Representatives != budget || m.Metrics.RetainedInternal+m.Metrics.RetainedLeaves != m.Representatives {
			return errors.New("representation count mismatch")
		}
		if j > 0 && (m.SourceSHA256 != i.Models[0].SourceSHA256 || m.MembershipSHA256 != i.Models[0].MembershipSHA256 || m.Dimensions != i.Models[0].Dimensions || (i.Options.Arm == "centroid_geometry" && m.LeafMembershipSHA256 != i.Models[0].LeafMembershipSHA256)) {
			return errors.New("representation source/membership comparison mismatch")
		}
		if len(m.RepresentedNodes) != m.Representatives {
			return errors.New("missing represented-node diagnostics")
		}
		for ordinal, node := range m.RepresentedNodes {
			if node.Ordinal != ordinal || int(node.Domain) >= m.Domains || node.NodeID < 1 || int(node.NodeID) > m.Nodes || node.Members == 0 || node.Members > 1_200_000 || node.Depth > 64 || math.IsNaN(node.Radius) || math.IsInf(node.Radius, 0) || node.Radius < 0 || math.IsNaN(node.LegacyCosineDistortion) || math.IsInf(node.LegacyCosineDistortion, 0) {
				return errors.New("invalid represented-node diagnostic")
			}
			if j == 0 && !node.Leaf {
				return errors.New("control includes internal representative")
			}
			digests = append(digests, node.MembershipSHA256)
		}
		digests = append(digests, m.ModelSHA256, m.SourceSHA256, m.MembershipSHA256, m.LeafMembershipSHA256)
	}
	for _, d := range digests {
		b, err := hex.DecodeString(d)
		if err != nil || len(b) != 32 || hex.EncodeToString(b) != d {
			return errors.New("invalid representation digest")
		}
	}
	// The control's realized count defines the common quota, never a nominal cap.
	if i.Models[0].Representatives != budget || !reflect.DeepEqual(i.Models[0].Metrics.UnusedQuota, make([]int, len(i.Quotas))) {
		return errors.New("representation control does not spend frozen realized quota")
	}
	return nil
}
