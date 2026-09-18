package collections

import (
	"context"
	"errors"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	"reflect"
	"sync"
	"testing"
)

func TestRouterRepresentationPersistedControlAndReopen(t *testing.T) {
	r, c, index := policyPersistedFixtureV1(t)
	o := VectorPartitionRouterRepresentationOptionsV1{Arm: "multilevel", ReturnedWidth: 2, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	experiment, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, o)
	if err != nil {
		t.Fatal(err)
	}
	before := r.Status()
	got, err := experiment.CompareWithContextV1(t.Context(), []float32{1, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	control, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), []float32{1, 0}, VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: VectorPartitionRouterModeExactV1, CandidateBudget: 4, ReturnedWidth: 2, PartitionProbes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[0].Policies.Distance, control.Distance) {
		t.Fatal("control does not reproduce actual router")
	}
	if r.Status().Searches != before.Searches {
		t.Fatal("experiment changed serving counters")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := experiment.CompareWithContextV1(t.Context(), []float32{1, 0}, 1)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("experiment borrowed closed owner", err)
	}
	second, _, err := c.OpenVectorPartitionRouterV1(index)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	reopened, err := second.BuildRepresentationForDiagnosticsV1(t.Context(), c, o)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(experiment.InfoV1(), reopened.InfoV1()) {
		t.Fatal("reopened model identity changed")
	}
}

func TestRouterRepresentationCanceledBuildAndLimits(t *testing.T) {
	r, c, _ := policyPersistedFixtureV1(t)
	o := VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 2, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.BuildRepresentationForDiagnosticsV1(ctx, c, o); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	bad := o
	bad.MaxBuildBytes = 1
	if _, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, bad); err == nil {
		t.Fatal("byte cap ignored")
	}
	bad = o
	bad.MaxBuildWork = 1
	if _, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, bad); err == nil {
		t.Fatal("work cap ignored")
	}
	if r.Status().ActiveHandles != 1 {
		t.Fatal("build leaked owner pin")
	}
}

func TestRouterRepresentationManifestMismatchAndOwnedMetadata(t *testing.T) {
	r, c, _ := policyPersistedFixtureV1(t)
	o := VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 2, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	source := r.manifest.SourceChecksum
	r.manifest.SourceChecksum++
	if _, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, o); err == nil {
		t.Fatal("stale source manifest accepted")
	}
	r.manifest.SourceChecksum = source
	e, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, o)
	if err != nil {
		t.Fatal(err)
	}
	info := e.InfoV1()
	if err := ValidateVectorPartitionRouterRepresentationInfoV1(info); err != nil {
		t.Fatal(err)
	}
	info.Quotas[0].Tokens++
	info.Models[0].Metrics.UnusedQuota[0]++
	info.Models[0].RepresentedNodes[0].Radius = 100
	if reflect.DeepEqual(info, e.InfoV1()) {
		t.Fatal("mutable info aliases prepared owner")
	}
	if err := ValidateVectorPartitionRouterRepresentationInfoV1(e.InfoV1()); err != nil {
		t.Fatal("caller mutation escaped", err)
	}
	for _, poll := range []int{2, 8, 24} {
		ctx := &vectorPartitionRouterDeadlineAfterErrContextV1{Context: context.Background(), deadlineAfter: poll}
		if got, err := r.BuildRepresentationForDiagnosticsV1(ctx, c, o); !errors.Is(err, context.DeadlineExceeded) || got != nil {
			t.Fatal("canceled build returned owner", poll, err)
		}
	}
	if r.Status().ActiveHandles != 1 {
		t.Fatal("canceled build leaked source owner")
	}
}
func TestRouterRepresentationManyPacksDeduplicateLogicalMembership(t *testing.T) {
	r, c, _ := policyPersistedFixtureV1(t)
	o := VectorPartitionRouterRepresentationOptionsV1{Arm: "multilevel", ReturnedWidth: 2, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	first, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, o)
	if err != nil {
		t.Fatal(err)
	}
	// Unit-test the physical-to-logical adapter on actual source vectors. The
	// duplicate pack relationship is not a claim of a new persisted four-pack DB.
	m := r.manifest
	r.manifest.PartitionCount = 4
	r.manifest.DomainCount = 2
	r.manifest.DomainPacks = []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}, {DomainID: 0, PackID: 2}, {DomainID: 1, PackID: 3}}
	r.manifest.Memberships = append([]VectorPartitionMembershipV1(nil), m.Memberships...)
	for _, x := range m.Memberships {
		x.PartitionID += 2
		r.manifest.Memberships = append(r.manifest.Memberships, x)
	}
	defer func() { r.manifest = m }()
	second, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, o)
	if err != nil {
		t.Fatal(err)
	}
	a, b := first.InfoV1(), second.InfoV1()
	if !reflect.DeepEqual(a.Models, b.Models) || a.ManifestSHA256 == b.ManifestSHA256 {
		t.Fatal("physical copies changed geometry or escaped layout identity")
	}
}

func TestRouterRepresentationAncestorVotesUseDistinctNodeIdentity(t *testing.T) {
	parts := []internalrouter.RouterPartitionV1{{PartitionID: 0}, {PartitionID: 1}}
	for d := range parts {
		for i := 0; i < 4; i++ {
			parts[d].Vectors = append(parts[d].Vectors, internalrouter.RouterVectorV1{Ordinal: uint64(d*4 + i), Values: []float32{1, float32(d) + float32(i)*.1}})
		}
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.RepresentativesPerPartition = 3
	o := internalrouter.RouterRepresentationOptionsV1{Shape: internalrouter.RouterRepresentationMultilevelV1, Geometry: internalrouter.RouterRepresentationLegacySphereV1, Config: cfg, Quotas: []internalrouter.RouterRepresentationQuotaV1{{Domain: 0, Tokens: 3}, {Domain: 1, Tokens: 3}}}
	m, err := internalrouter.BuildRouterRepresentationForDiagnosticsV1(t.Context(), parts, o)
	if err != nil {
		t.Fatal(err)
	}
	p, err := internalrouter.PrepareRouterRepresentationScoringV1(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	repeated := false
	anchors := make(map[uint64]bool)
	for _, r := range m.Representatives {
		repeated = repeated || anchors[r.SourceAnchor]
		anchors[r.SourceAnchor] = true
	}
	if !repeated {
		t.Fatal("fixture did not reuse an ancestor/descendant anchor")
	}
	opts := VectorPartitionRouterRepresentationOptionsV1{Arm: "multilevel", ReturnedWidth: 6, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	e := &VectorPartitionRouterRepresentationV1{info: VectorPartitionRouterRepresentationInfoV1{Options: opts, OptionsSHA256: VectorPartitionRouterRepresentationIdentityV1(opts), Quotas: o.Quotas, Models: []internalrouter.RouterRepresentationSummaryV1{p.SummaryV1(), p.SummaryV1()}}, models: []*internalrouter.PreparedRouterRepresentationV1{p, p}}
	rs, err := e.CompareWithContextV1(t.Context(), []float32{1, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if r.Policies.UniqueReturned != 6 || r.Policies.Frequency[0].Frequency != 3 || r.Policies.Frequency[1].Frequency != 3 {
			t.Fatal("source-anchor deduplication lost distinct node votes")
		}
	}
}
func TestRouterRepresentationPreparedConcurrentSourceClose(t *testing.T) {
	r, c, _ := policyPersistedFixtureV1(t)
	o := VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 2, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	e, err := r.BuildRepresentationForDiagnosticsV1(t.Context(), c, o)
	if err != nil {
		t.Fatal(err)
	}
	want, err := e.CompareWithContextV1(t.Context(), []float32{1, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				got, err := e.CompareWithContextV1(t.Context(), []float32{1, 0}, 1)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Error("prepared concurrent result changed", err)
					return
				}
			}
		}()
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if r.Status().ActiveHandles != 0 {
		t.Fatal("source owner leaked")
	}
}
func TestRouterRepresentationEnvelopeSharesSourceAcrossArms(t *testing.T) {
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.MaxScalarWork = 50_000_000_000
	pop := make([]int, 16)
	quotas := make([]internalrouter.RouterRepresentationQuotaV1, 16)
	for i := range pop {
		pop[i] = 15625
		quotas[i] = internalrouter.RouterRepresentationQuotaV1{Domain: uint32(i), Tokens: 16}
	}
	base := internalrouter.RouterRepresentationOptionsV1{Shape: internalrouter.RouterRepresentationLeafV1, Geometry: internalrouter.RouterRepresentationLegacySphereV1, Config: cfg, Quotas: quotas}
	unit, mean := base, base
	unit.Geometry = internalrouter.RouterRepresentationUnitMeanV1
	mean.Geometry = internalrouter.RouterRepresentationMeanV1
	work, peak, err := representationCollectionEnvelopeV1(pop, 128, []internalrouter.RouterRepresentationOptionsV1{base, unit, mean})
	if err != nil {
		t.Fatal(err)
	}
	p, err := internalrouter.PlanRouterRepresentationV1(pop, 128, base)
	if err != nil {
		t.Fatal(err)
	}
	if work != 3*uint64(p.ScalarWork) || peak <= p.PeakBytes || peak >= 2*p.PeakBytes || peak > 1<<30 {
		t.Fatal("source was tripled or centroid owners uncharged", work, peak, p.PeakBytes)
	}
}
