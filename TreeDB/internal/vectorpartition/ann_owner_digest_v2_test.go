package vectorpartition

import (
	"crypto/sha256"
	"testing"
)

func TestANNOwnerCommitmentV2BindsExactCanonicalIntent(t *testing.T) {
	domain := ANNDomainV2{DomainID: 7, LogicalPackID: "pack-7", MembershipCount: 2}
	first := ANNMemberV2{Kind: "home", Source: ANNSourceRowIdentityV2{SourceOwner: "source-a", ShardID: "shard", SnapshotRevision: 4, SnapshotDigest: sha256.Sum256([]byte("snapshot")), Ordinal: 0, DocumentRevision: 11}}
	second := first
	second.Source.Ordinal = 1
	second.Kind = "overlap"
	build := func(d ANNDomainV2, a, b ANNMemberV2) (ANNOwnerCommitmentV2, error) {
		h, _ := NewANNOwnerAccumulatorV2("ann-b")
		if err := h.BeginDomain(d); err != nil {
			return ANNOwnerCommitmentV2{}, err
		}
		if err := h.AddMember(a); err != nil {
			return ANNOwnerCommitmentV2{}, err
		}
		if err := h.AddMember(b); err != nil {
			return ANNOwnerCommitmentV2{}, err
		}
		return h.Commitment()
	}
	base, err := build(domain, first, second)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ANNDomainV2, *ANNMemberV2){
		"domain":   func(d *ANNDomainV2, _ *ANNMemberV2) { d.DomainID++ },
		"pack":     func(d *ANNDomainV2, _ *ANNMemberV2) { d.LogicalPackID = "other" },
		"kind":     func(_ *ANNDomainV2, m *ANNMemberV2) { m.Kind = "overlap" },
		"snapshot": func(_ *ANNDomainV2, m *ANNMemberV2) { m.Source.SnapshotDigest[0] ^= 1 },
		"revision": func(_ *ANNDomainV2, m *ANNMemberV2) { m.Source.DocumentRevision++ },
	} {
		t.Run(name, func(t *testing.T) {
			d, m := domain, first
			mutate(&d, &m)
			got, err := build(d, m, second)
			if err != nil || got.MembershipDigest == base.MembershipDigest {
				t.Fatalf("not bound: %v", err)
			}
		})
	}
	if _, err := build(domain, second, first); err == nil {
		t.Fatal("reordered members")
	}
	if _, err := build(domain, first, first); err == nil {
		t.Fatal("duplicate members")
	}
	domain.MembershipCount = 3
	if _, err := build(domain, first, second); err == nil {
		t.Fatal("missing member")
	}
	domain.MembershipCount = 1
	if _, err := build(domain, first, second); err == nil {
		t.Fatal("extra member")
	}
	if _, err := ANNOwnerSetDigestV2([]ANNOwnerCommitmentV2{base, base}); err == nil {
		t.Fatal("duplicate owners")
	}
	if _, err := ANNOwnerSetDigestV2(make([]ANNOwnerCommitmentV2, 129)); err == nil {
		t.Fatal("unbounded owners")
	}
}
