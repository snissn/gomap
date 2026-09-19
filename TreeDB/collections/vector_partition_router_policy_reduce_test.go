package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func policyTestContext() vectorPartitionPolicyContextV1 {
	return vectorPartitionPolicyContextV1{strings.Repeat("a", 64), strings.Repeat("b", 64), "approximate", 16, 4, 16, 16, 4}
}
func policyGolden() []vectorPartitionPolicyCandidateV1 {
	return []vectorPartitionPolicyCandidateV1{{0, 0, 0, .99}, {1, 2, 1, .985}, {2, 1, 2, .98}, {3, 1, 3, .97}, {4, 1, 4, .96}, {5, 3, 5, .95}}
}
func policyDomains(rows []vectorPartitionPolicyDomainV1) []uint32 {
	out := make([]uint32, len(rows))
	for i, r := range rows {
		out[i] = r.Domain
	}
	return out
}
func TestVectorPartitionRouterHybridKeepsDistanceOrderAfterWinner(t *testing.T) {
	input := policyGolden()
	meta := policyTestContext()
	seen := 0
	var digest string
	var visit func(int)
	visit = func(i int) {
		if i == len(input) {
			got, err := reduceVectorPartitionRouterPoliciesV1(context.Background(), meta, input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(policyDomains(got.Distance), []uint32{0, 2, 1, 3}) || !reflect.DeepEqual(policyDomains(got.Hybrid), []uint32{1, 0, 2, 3}) {
				t.Fatalf("bad golden %+v", got)
			}
			if seen == 0 {
				digest = got.CandidateSetSHA256
			} else if digest != got.CandidateSetSHA256 {
				t.Fatal("set digest depends on order")
			}
			seen++
			return
		}
		for j := i; j < len(input); j++ {
			input[i], input[j] = input[j], input[i]
			visit(i + 1)
			input[i], input[j] = input[j], input[i]
		}
	}
	visit(0)
	if seen != 720 {
		t.Fatal(seen)
	}
	for probes := 1; probes <= 4; probes++ {
		meta.Probes = probes
		got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(policyDomains(got.Hybrid), []uint32{1, 0, 2, 3}[:probes]) {
			t.Fatal("prefix changed")
		}
	}
}
func TestVectorPartitionRouterFrequencyCountsDistinctReturnedRepresentatives(t *testing.T) {
	input := policyGolden()
	meta := policyTestContext()
	base, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	input = append(input, input[0], input[0], input[0], input[0])
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hybrid[0].Domain != 1 || got.CandidateSetSHA256 != base.CandidateSetSHA256 || got.UniqueReturned != 6 {
		t.Fatal("duplicate added votes")
	}
	input = append(input, vectorPartitionPolicyCandidateV1{6, 0, 0, .99})
	got, err = reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Distance[0].Frequency != 2 {
		t.Fatal("different identity with same anchor was deduplicated")
	}
}
func TestVectorPartitionRouterFrequencyRejectsConflictingDuplicateScores(t *testing.T) {
	for _, mutate := range []func(*vectorPartitionPolicyCandidateV1){func(c *vectorPartitionPolicyCandidateV1) { c.Score = .1 }, func(c *vectorPartitionPolicyCandidateV1) { c.Domain = 1 }, func(c *vectorPartitionPolicyCandidateV1) { c.SourceOrdinal = 99 }} {
		input := policyGolden()
		dup := input[0]
		mutate(&dup)
		input = append(input, dup)
		got, err := reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), input)
		if err == nil || got.Hybrid != nil {
			t.Fatal("conflicting identity returned a route")
		}
	}
	for _, score := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		input := policyGolden()
		input[0].Score = score
		if _, err := reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), input); err == nil {
			t.Fatal("nonfinite accepted")
		}
	}
	input := policyGolden()
	input[0].Score = 0
	dup := input[0]
	dup.Score = math.Copysign(0, -1)
	input = append(input, dup)
	if _, err := reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), input); err == nil {
		t.Fatal("inconsistent signed-zero receipt")
	}
}
func TestVectorPartitionRouterCandidateSetAndOrderDigestsAreDistinct(t *testing.T) {
	input := policyGolden()
	before := append([]vectorPartitionPolicyCandidateV1(nil), input...)
	base, err := reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, input) {
		t.Fatal("input mutated")
	}
	input[0], input[1] = input[1], input[0]
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), input)
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateSetSHA256 != base.CandidateSetSHA256 || got.CandidateSequenceSHA256 == base.CandidateSequenceSHA256 {
		t.Fatal("set/sequence identities conflated")
	}
	input[0].Score -= .01
	got, err = reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), input)
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateSetSHA256 == base.CandidateSetSHA256 {
		t.Fatal("score change not bound")
	}
	meta := policyTestContext()
	meta.ModelDigest = strings.Repeat("c", 64)
	got, err = reduceVectorPartitionRouterPoliciesV1(nil, meta, before)
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateSetSHA256 == base.CandidateSetSHA256 {
		t.Fatal("model not bound")
	}
}
func TestVectorPartitionRouterExactVotingTruncatesToReturnedWidth(t *testing.T) {
	meta := policyTestContext()
	meta.Mode = "exact"
	meta.ReturnedWidth = 4
	meta.Probes = 3
	input := policyGolden()
	if _, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input); err == nil {
		t.Fatal("incomplete exact scan accepted")
	}
	for i := 6; i < 16; i++ {
		input = append(input, vectorPartitionPolicyCandidateV1{i, 0, uint64(i), -.5})
	}
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	// Full exact coverage remains present, but move all far representatives
	// to another domain. Nearest-w voting must be unaffected.
	for i := 6; i < 16; i++ {
		input[i].Domain = 3
	}
	more, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if more.UniqueReturned != 4 || more.CandidateSetSHA256 != got.CandidateSetSHA256 || !reflect.DeepEqual(got.Hybrid, more.Hybrid) {
		t.Fatal("far candidates changed nearest-w votes")
	}
}
func TestVectorPartitionRouterHybridDeterministicTies(t *testing.T) {
	meta := policyTestContext()
	meta.Probes = 3
	input := []vectorPartitionPolicyCandidateV1{{3, 2, 3, .9}, {2, 1, 2, .9}, {1, 0, 1, .9}}
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(policyDomains(got.Hybrid), []uint32{0, 1, 2}) {
		t.Fatal("domain ties unstable")
	}
	input = append(input, vectorPartitionPolicyCandidateV1{0, 0, 0, .9})
	got, err = reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hybrid[0].WinningRepresentative != 0 {
		t.Fatal("representative tie unstable")
	}
}
func TestVectorPartitionRouterPolicyCoverageRefusal(t *testing.T) {
	meta := policyTestContext()
	input := policyGolden()[:1]
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if !errors.Is(err, errVectorPartitionPolicyCoverageV1) || got.Distance != nil {
		t.Fatalf("%+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reduceVectorPartitionRouterPoliciesV1(ctx, meta, policyGolden()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*vectorPartitionPolicyContextV1){func(m *vectorPartitionPolicyContextV1) { m.Mode = "other" }, func(m *vectorPartitionPolicyContextV1) { m.ModelDigest = "bad" }, func(m *vectorPartitionPolicyContextV1) { m.ReturnedWidth = 17 }, func(m *vectorPartitionPolicyContextV1) { m.Probes = 0 }, func(m *vectorPartitionPolicyContextV1) { m.CandidateBudget = 2 }} {
		m := meta
		mutate(&m)
		if _, err := reduceVectorPartitionRouterPoliciesV1(nil, m, policyGolden()); err == nil {
			t.Fatalf("accepted %+v", m)
		}
	}
}
func TestRouterRepresentationFrequencyDepthInteraction(t *testing.T) {
	meta := policyTestContext()
	meta.Probes = 2
	// Ordinals 0/1 represent distinct ancestor/descendant nodes sharing the
	// same source anchor, not two independent populations of documents.
	input := []vectorPartitionPolicyCandidateV1{{0, 0, 7, .9}, {1, 0, 7, .9}, {2, 1, 9, .99}}
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hybrid[0].Domain != 0 || got.Hybrid[0].Frequency != 2 {
		t.Fatal("distinct hierarchy identities lost")
	}
	input = append(input, input[2], input[2])
	got, err = reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hybrid[0].Domain != 0 {
		t.Fatal("duplicate retrieval gained votes")
	}
}
func BenchmarkVectorPartitionRouterRankingPolicyV1(b *testing.B) {
	for _, representatives := range []int{16, 256, 4096} {
		for _, width := range []int{16, 64, 256} {
			if width > representatives {
				continue
			}
			b.Run(fmt.Sprintf("representatives=%d/returned=%d", representatives, width), func(b *testing.B) {
				meta := policyTestContext()
				meta.RepresentativeCount = representatives
				meta.DomainCount = 16
				meta.CandidateBudget = width
				meta.ReturnedWidth = width
				input := make([]vectorPartitionPolicyCandidateV1, width)
				for i := range input {
					input[i] = vectorPartitionPolicyCandidateV1{i, uint32(i % 16), uint64(i), 1 - float64(i)/float64(width+1)}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := reduceVectorPartitionRouterPoliciesV1(context.Background(), meta, input); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestVectorPartitionRouterPolicyCoverageRefusalCarriesCandidateIdentity(t *testing.T) {
	meta := policyTestContext()
	meta.Probes = 2
	input := policyGolden()[:2]
	input[1].Domain = input[0].Domain // Two distinct representatives cannot cover two domains.
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if !errors.Is(err, errVectorPartitionPolicyCoverageV1) || len(got.CandidateSetSHA256) != 64 || len(got.CandidateSequenceSHA256) != 64 || got.UniqueReturned != 2 || len(got.Distance)+len(got.Hybrid)+len(got.Frequency) != 0 {
		t.Fatalf("refused route lost reproducible candidate identity: %+v %v", got, err)
	}
	input[0].Score -= .01
	changed, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, input)
	if !errors.Is(err, errVectorPartitionPolicyCoverageV1) || changed.CandidateSetSHA256 == got.CandidateSetSHA256 {
		t.Fatal("different failed candidate set is not identifiable")
	}
}

func TestVectorPartitionRouterPolicyDigestV1GoldenEncoding(t *testing.T) {
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, policyTestContext(), policyGolden())
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateSetSHA256 != "b1fe473339ac5edcdd63f022bb2d19acbc998e8ad13c137c24c5ba14bb55fe7c" || got.CandidateSequenceSHA256 != "1bfc7cef627d0b785c62f62f11c7a61c673e1e4bcd7039d2c14d17caadc6ec0d" {
		t.Fatal("candidate digest byte encoding changed")
	}
}
func TestVectorPartitionRouterPolicyResultPrefixesHaveBoundedOwnership(t *testing.T) {
	meta := policyTestContext()
	meta.Probes = 1
	got, err := reduceVectorPartitionRouterPoliciesV1(nil, meta, policyGolden())
	if err != nil {
		t.Fatal(err)
	}
	if cap(got.Distance) != 1 || cap(got.Frequency) != 1 || cap(got.Hybrid) != 1 {
		t.Fatal("prefix retains extra domain array or append alias")
	}
	before := got.Hybrid[0]
	got.Distance = append(got.Distance, vectorPartitionPolicyDomainV1{Domain: 99})
	if got.Hybrid[0] != before {
		t.Fatal("append crossed owned route boundary")
	}
}
