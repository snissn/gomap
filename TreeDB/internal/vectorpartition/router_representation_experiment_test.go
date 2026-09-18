package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func representationFixtureV1() ([]RouterPartitionV1, RouterRepresentationOptionsV1) {
	p := []RouterPartitionV1{{PartitionID: 0}, {PartitionID: 1}}
	for d := range p {
		for i := 0; i < 12; i++ {
			a := float64(i)*.23 + float64(d)
			p[d].Vectors = append(p[d].Vectors, RouterVectorV1{Ordinal: uint64(d*12 + i), Values: []float32{float32(math.Cos(a)), float32(math.Sin(a))}})
		}
	}
	cfg := DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.RepresentativesPerPartition = 4
	return p, RouterRepresentationOptionsV1{Shape: RouterRepresentationLeafV1, Geometry: RouterRepresentationLegacySphereV1, Config: cfg, Quotas: []RouterRepresentationQuotaV1{{Domain: 0, Tokens: 4}, {Domain: 1, Tokens: 4}}}
}

func TestRouterRepresentationControlReproducesV1(t *testing.T) {
	p, o := representationFixtureV1()
	legacy, err := BuildRouterV1(p, o.Config)
	if err != nil {
		t.Fatal(err)
	}
	m, err := BuildRouterRepresentationForDiagnosticsV1(context.Background(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := RouterRepresentationControlMatchesV1(m, legacy); err != nil {
		t.Fatal(err)
	}
}

func TestRouterRepresentationMultilevelBudgetChargesParentsAndChildren(t *testing.T) {
	p, o := representationFixtureV1()
	o.Shape = RouterRepresentationMultilevelV1
	for _, q := range []int{1, 2, 3, 4, 7} {
		o.Quotas = []RouterRepresentationQuotaV1{{Domain: 0, Tokens: q}, {Domain: 1, Tokens: q}}
		m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Representatives) > 2*q || len(m.Representatives) != len(m.Nodes) {
			t.Fatal("budget overspend", m.Metrics)
		}
		want := q
		if q%2 == 0 {
			want--
		}
		if len(m.Representatives) != 2*want {
			t.Fatalf("quota %d: count=%d", q, len(m.Representatives))
		}
	}
}

func TestRouterRepresentationControlGridAndDeterminism(t *testing.T) {
	for _, branch := range []int{2, 3, 4} {
		for quota := 1; quota <= 8; quota++ {
			for _, leaf := range []int{1, 4, 20} {
				p, o := representationFixtureV1()
				o.Config.BranchFactor = branch
				o.Config.LeafSize = leaf
				o.Config.RepresentativesPerPartition = quota
				o.Quotas = []RouterRepresentationQuotaV1{{Domain: 0, Tokens: quota}, {Domain: 1, Tokens: quota}}
				legacy, err := BuildRouterV1(p, o.Config)
				if err != nil {
					t.Fatal(err)
				}
				m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
				if err != nil {
					t.Fatal(err)
				}
				if err := RouterRepresentationControlMatchesV1(m, legacy); err != nil {
					t.Fatalf("branch=%d quota=%d leaf=%d: %v", branch, quota, leaf, err)
				}
				for i := range p {
					slices.Reverse(p[i].Vectors)
				}
				slices.Reverse(p)
				reordered, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(m, reordered) {
					t.Fatal("input enumeration changed model")
				}
			}
		}
	}
}

func TestRouterRepresentationCentroidGeometryOnFrozenMembership(t *testing.T) {
	angle := math.Pi / 3
	b := 20 * math.Pi / 180
	p := []RouterPartitionV1{{PartitionID: 0, Vectors: []RouterVectorV1{{Ordinal: 0, Values: []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}}, {Ordinal: 1, Values: []float32{float32(math.Cos(angle)), -float32(math.Sin(angle))}}}}, {PartitionID: 1, Vectors: []RouterVectorV1{{Ordinal: 2, Values: []float32{float32(math.Cos(b)), float32(math.Sin(b))}}}}}
	cfg := DefaultRouterConfigV1()
	o := RouterRepresentationOptionsV1{Shape: RouterRepresentationLeafV1, Geometry: RouterRepresentationLegacySphereV1, Config: cfg, Quotas: []RouterRepresentationQuotaV1{{Domain: 0, Tokens: 1}, {Domain: 1, Tokens: 1}}}
	source, err := PrepareRouterRepresentationSourceV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	var memberships string
	digests := map[string]bool{}
	for _, geometry := range []string{RouterRepresentationLegacySphereV1, RouterRepresentationUnitMeanV1, RouterRepresentationMeanV1} {
		o.Geometry = geometry
		m, err := source.BuildWithContextV1(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
		if memberships == "" {
			memberships = m.LeafMembershipSHA256
		} else if memberships != m.LeafMembershipSHA256 {
			t.Fatal("geometry reclustered membership")
		}
		prepared, err := PrepareRouterRepresentationScoringV1(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		summary := prepared.SummaryV1()
		if digests[summary.ModelSHA256] {
			t.Fatal("geometry not in identity")
		}
		digests[summary.ModelSHA256] = true
		scores, err := prepared.ScoreExactWithContextV1(t.Context(), []float32{1, 0})
		if err != nil {
			t.Fatal(err)
		}
		if geometry == RouterRepresentationMeanV1 {
			if math.Abs(scores[0].Score-.75) > 1e-6 || scores[1].Score <= scores[0].Score {
				t.Fatal("wrong arithmetic mean priority", scores)
			}
		} else if scores[0].Score < scores[1].Score {
			t.Fatal("sphere should choose domain A", scores)
		}
		raw, _, err := EncodeRouterRepresentationV1(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeRouterRepresentationV1(t.Context(), raw, 1<<20)
		if err != nil || !reflect.DeepEqual(m, decoded) {
			t.Fatal("roundtrip", err)
		}
		// Experimental JSON is not accepted as a production V1 model.
		var ordinary RouterModelV1
		if err := json.Unmarshal(raw, &ordinary); err != nil {
			t.Fatal(err)
		}
		if ValidateRouterModelV1(ordinary) == nil {
			t.Fatal("experimental format admitted as V1")
		}
	}
}

func TestRouterRepresentationZeroMeanAndDegenerateSource(t *testing.T) {
	p, o := representationFixtureV1()
	p = p[:1]
	p[0].Vectors = []RouterVectorV1{{Ordinal: 2, Values: []float32{1, 0}}, {Ordinal: 1, Values: []float32{-1, 0}}}
	o.Quotas = []RouterRepresentationQuotaV1{{Domain: 0, Tokens: 1}}
	o.Geometry = RouterRepresentationMeanV1
	m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Representatives[0].Values, []float64{0, 0}) {
		t.Fatal("zero mean changed")
	}
	o.Geometry = RouterRepresentationUnitMeanV1
	m, err = BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Representatives[0].Values, []float64{-1, 0}) {
		t.Fatal("zero sphere fallback not canonical source-first")
	}
	p[0].Vectors[0].Values = []float32{0, 0}
	if _, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o); err == nil {
		t.Fatal("zero source accepted")
	}
}

type representationPollContext struct {
	context.Context
	polls, limit int
}

func (c *representationPollContext) Err() error {
	c.polls++
	if c.polls >= c.limit {
		return context.Canceled
	}
	return nil
}
func TestRouterRepresentationPreflightAndCancellation(t *testing.T) {
	p, o := representationFixtureV1()
	bad := o
	bad.Config.MaxRouterBytes = 1
	p[0].Vectors[0].Values[0] = float32(math.NaN())
	if _, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, bad); err == nil || !strings.Contains(err.Error(), "preflight") {
		t.Fatal("allocated/scanned before shape preflight", err)
	}
	p, o = representationFixtureV1()
	for _, limit := range []int{1, 5, 12, 25, 50} {
		ctx := &representationPollContext{Context: context.Background(), limit: limit}
		if _, err := BuildRouterRepresentationForDiagnosticsV1(ctx, p, o); !errors.Is(err, context.Canceled) {
			t.Fatal("uncanceled partial build", limit, ctx.polls, err)
		}
	}
	if _, err := representationMulV1(math.MaxUint64, 2); err == nil {
		t.Fatal("overflow accepted")
	}
	// Same ordinal in two domains is legal only with identical source bits.
	p[1].Vectors[0].Ordinal = p[0].Vectors[0].Ordinal
	if _, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o); err == nil {
		t.Fatal("conflicting overlapping source accepted")
	}
}

func TestRouterRepresentationMalformedHierarchyAndIdentity(t *testing.T) {
	p, o := representationFixtureV1()
	o.Shape = RouterRepresentationMultilevelV1
	m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	raw, _, _ := EncodeRouterRepresentationV1(t.Context(), m)
	for name, mutate := range map[string]func(*RouterRepresentationModelV1){
		"cycle":           func(m *RouterRepresentationModelV1) { m.Nodes[1].ParentNodeID = m.Nodes[1].NodeID },
		"depth":           func(m *RouterRepresentationModelV1) { m.Nodes[1].Depth++ },
		"population":      func(m *RouterRepresentationModelV1) { m.Nodes[0].MemberCount++ },
		"internal-leaf":   func(m *RouterRepresentationModelV1) { m.Nodes[0].Leaf = true },
		"anchor":          func(m *RouterRepresentationModelV1) { m.Nodes[0].SourceAnchor = math.MaxUint64 },
		"quota":           func(m *RouterRepresentationModelV1) { m.Options.Quotas[0].Tokens = 1 },
		"nonfinite":       func(m *RouterRepresentationModelV1) { m.Representatives[0].Values[0] = math.NaN() },
		"duplicate-rep":   func(m *RouterRepresentationModelV1) { m.Representatives[1] = m.Representatives[0] },
		"missing-rep":     func(m *RouterRepresentationModelV1) { m.Representatives = m.Representatives[1:] },
		"membership-hash": func(m *RouterRepresentationModelV1) { m.Nodes[0].MembershipSHA256 = strings.Repeat("0", 64) },
		"metrics":         func(m *RouterRepresentationModelV1) { m.Metrics.RealizedCount++ },
	} {
		t.Run(name, func(t *testing.T) {
			var c RouterRepresentationModelV1
			if err := json.Unmarshal(raw, &c); err != nil {
				t.Fatal(err)
			}
			mutate(&c)
			if ValidateRouterRepresentationWithContextV1(t.Context(), c) == nil {
				t.Fatal("forged structure accepted")
			}
		})
	}
	if _, err := DecodeRouterRepresentationV1(t.Context(), append(raw, []byte(" {}")...), 1<<20); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if _, err := DecodeRouterRepresentationV1(t.Context(), raw, uint64(len(raw)-1)); err == nil {
		t.Fatal("byte cap ignored")
	}
}

func TestRouterRepresentationPreparedOwnsCentroids(t *testing.T) {
	p, o := representationFixtureV1()
	m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareRouterRepresentationScoringV1(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	before, err := prepared.ScoreExactWithContextV1(t.Context(), []float32{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	m.Representatives[0].Values[0] = 123
	m.Metrics.UnusedQuota[0] = 999
	summary := prepared.SummaryV1()
	summary.Metrics.UnusedQuota[0] = 999
	again, err := prepared.ScoreExactWithContextV1(t.Context(), []float32{1, 0})
	if err != nil || !reflect.DeepEqual(before, again) || prepared.SummaryV1().Metrics.UnusedQuota[0] == 999 {
		t.Fatal("mutable storage escaped prepared owner", err)
	}
}

func TestRouterRepresentationApportionmentExplicitAndBounded(t *testing.T) {
	cases := []struct {
		p    []uint64
		b    int
		want []int
	}{{[]uint64{1, 1, 1}, 5, nil}, {[]uint64{1, 1, 100}, 100, []int{1, 1, 98}}, {[]uint64{5, 5, 5}, 8, []int{3, 3, 2}}}
	for _, c := range cases {
		q, err := ApportionRouterRepresentationBudgetV1(t.Context(), c.p, c.b, 1<<20)
		if c.want == nil {
			if err == nil {
				t.Fatal("over-cap accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		sum := 0
		for i, r := range q {
			sum += r.Tokens
			if r.Tokens != c.want[i] || r.Tokens < 1 || uint64(r.Tokens) > c.p[i] {
				t.Fatal("wrong apportionment", q)
			}
		}
		if sum != c.b {
			t.Fatal("budget not conserved")
		}
	}
	if _, err := ApportionRouterRepresentationBudgetV1(t.Context(), []uint64{5, 5, 5}, 8, 1); err == nil {
		t.Fatal("work cap bypass")
	}
	if _, err := ApportionRouterRepresentationBudgetV1(t.Context(), []uint64{math.MaxUint64, 1}, 2, 1<<20); err == nil {
		t.Fatal("overflow population accepted")
	}
}

// Golden obtained by running the unchanged parent implementation at tree
// a00c4981ac382af85eca45304b989346266bf90c before the context-helper extraction.
func TestRouterRepresentationLegacyIndependentGolden(t *testing.T) {
	p, o := representationFixtureV1()
	m, err := BuildRouterV1(p, o.Config)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RouterDigestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if got != "015c583cfcf275fc42d8d656f2b84c16fe67bb9cb5b5a39c61d6fee0ee0d0586" {
		t.Fatal("ordinary builder bits/hierarchy changed", got)
	}
}
func TestRouterRepresentationCodecMatchesJSONAndCancels(t *testing.T) {
	p, o := representationFixtureV1()
	m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	streamed, _, err := EncodeRouterRepresentationV1(t.Context(), m)
	if err != nil || string(raw) != string(streamed) {
		t.Fatal("stream encoding changed canonical bytes", err)
	}
	ctx := &representationPollContext{Context: context.Background(), limit: 12}
	if raw, hash, err := encodeRouterRepresentationJSONV1(ctx, m); !errors.Is(err, context.Canceled) || raw != nil || hash != "" {
		t.Fatal("canceled model encoding returned partial artifact", err)
	}
}
func TestRouterRepresentationPreflightChargesLeafSelection(t *testing.T) {
	_, o := representationFixtureV1()
	o.Config.MaxDepth = 1
	o.Config.MaxIterations = 1
	o.Config.MaxScalarWork = 50_000_000_000
	o.Config.MaxRouterBytes = 1 << 30
	o.Quotas = []RouterRepresentationQuotaV1{{Domain: 0, Tokens: 10000}}
	p, err := PlanRouterRepresentationV1([]int{10000}, 1, o)
	if err != nil {
		t.Fatal(err)
	}
	if p.ScalarWork < 8*10000*10000 {
		t.Fatal("quadratic leaf-selection work uncharged")
	}
}

// Vary seeds, dimensions, skew and overlap without selecting an algorithm on
// these fixtures. Every control must reproduce V1 and input permutations must
// preserve the full experimental encoding, including source and node identity.
func TestRouterRepresentationRandomizedControlAndSourcePermutation(t *testing.T) {
	for seed := int64(1); seed <= 16; seed++ {
		cfg := DefaultRouterConfigV1()
		cfg.Seed = seed
		cfg.LeafSize = 2 + int(seed%4)
		cfg.BranchFactor = 2 + int(seed%3)
		cfg.RepresentativesPerPartition = 3 + int(seed%5)
		cfg.MaxScalarWork = 50_000_000_000
		const dims = 7
		parts := make([]RouterPartitionV1, 3)
		for d := range parts {
			parts[d].PartitionID = uint32(d)
			for j := 0; j < 17+d*11; j++ {
				// The first row is genuinely shared across domains.
				id := uint64(d*100 + j)
				if j == 0 {
					id = 0
				}
				v := make([]float32, dims)
				for k := range v {
					v[k] = float32(math.Sin(float64(id+uint64(k)*13) + float64(seed)/7))
				}
				parts[d].Vectors = append(parts[d].Vectors, RouterVectorV1{Ordinal: id, Values: v})
			}
		}
		want, err := BuildRouterV1(parts, cfg)
		if err != nil {
			t.Fatal(seed, err)
		}
		quotas := make([]RouterRepresentationQuotaV1, 3)
		for i := range quotas {
			quotas[i].Domain = uint32(i)
		}
		for _, r := range want.Representatives {
			quotas[r.PartitionID].Tokens++
		}
		opts := RouterRepresentationOptionsV1{Shape: RouterRepresentationLeafV1, Geometry: RouterRepresentationLegacySphereV1, Config: cfg, Quotas: quotas}
		model, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), parts, opts)
		if err != nil {
			t.Fatal(seed, err)
		}
		if err := RouterRepresentationControlMatchesV1(model, want); err != nil {
			t.Fatal(seed, err)
		}
		raw, digest, err := EncodeRouterRepresentationV1(t.Context(), model)
		if err != nil {
			t.Fatal(err)
		}
		for d := range parts {
			for l, r := 0, len(parts[d].Vectors)-1; l < r; l, r = l+1, r-1 {
				parts[d].Vectors[l], parts[d].Vectors[r] = parts[d].Vectors[r], parts[d].Vectors[l]
			}
		}
		parts[0], parts[2] = parts[2], parts[0]
		again, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), parts, opts)
		if err != nil {
			t.Fatal(seed, err)
		}
		other, otherDigest, err := EncodeRouterRepresentationV1(t.Context(), again)
		if err != nil || digest != otherDigest || !bytes.Equal(raw, other) {
			t.Fatal("permutation changed experimental model", seed, err)
		}
	}
}
