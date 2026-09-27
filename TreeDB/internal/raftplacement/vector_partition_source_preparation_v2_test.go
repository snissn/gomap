package raftplacement

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func sourcePreparationFixtureV2(t *testing.T, catalog CatalogMetaRecordV1) (VectorPartitionLifecycleIdentityV1, []VectorPartitionSourceOwnerPreparationV2) {
	t.Helper()
	owners := []VectorPartitionSourceOwnerPreparationV2{{GroupID: "group-a", ShardCount: 2, SnapshotSetDigest: strings.Repeat("a", 64), CompletionEvidenceDigest: strings.Repeat("b", 64)}, {GroupID: "group-b", ShardCount: 3, SnapshotSetDigest: strings.Repeat("c", 64), CompletionEvidenceDigest: strings.Repeat("d", 64)}}
	root, err := VectorPartitionSourceOwnerSetDigestV2(owners)
	if err != nil {
		t.Fatal(err)
	}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 11, 1)
	identity.Source = VectorPartitionLifecycleSourceIdentityV1{}
	identity.SourceFormat = 2
	identity.SourceV2 = VectorPartitionLifecycleSourceIdentityV2{SourceMapEpoch: 7, SourceMapDigest: strings.Repeat("e", 64), SnapshotSetDigest: root, GraphProfileDigest: strings.Repeat("f", 64), PlacementDigest: strings.Repeat("1", 64)}
	identity.SourceV2.PlacementDigest, err = VectorPartitionANNOwnerSetDigestV2(annPreparationFixtureV2("group-a", "group-b"))
	if err != nil {
		t.Fatal(err)
	}
	return identity, owners
}

func TestVectorPartitionSourcePreparationV2AdmissionRetryAndRestore(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity, owners := sourcePreparationFixtureV2(t, catalog)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	c := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	groups := []raftcluster.GroupID{"group-b", "group-a"}
	if _, err := c.BeginBuildV1(t.Context(), identity, groups, 0, 1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("missing preparer: %v", err)
	}
	calls := 0
	c.PrepareSourceV2 = func(context.Context, VectorPartitionLifecycleIdentityV1) ([]VectorPartitionSourceOwnerPreparationV2, error) {
		calls++
		return slices.Clone(owners), nil
	}
	c.PrepareANNV2 = func(context.Context, VectorPartitionLifecycleIdentityV1) ([]VectorPartitionANNOwnerPreparationV2, error) {
		return annPreparationFixtureV2("group-a", "group-b"), nil
	}
	record, err := c.BeginBuildV1(t.Context(), identity, groups, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.SourceOwners) != 2 || record.Identity != identity {
		t.Fatalf("record: %+v", record)
	}
	// Reordered arrivals and different replica-local completion evidence preserve
	// semantic identity and the already committed preparation record.
	slices.Reverse(owners)
	owners[0].CompletionEvidenceDigest = strings.Repeat("2", 64)
	retry, err := c.BeginBuildV1(t.Context(), identity, groups, 0, 1)
	if err != nil || !reflect.DeepEqual(retry, record) || calls != 2 {
		t.Fatalf("retry=%+v calls=%d err=%v", retry, calls, err)
	}
	// Even an exact identity retry must validate the complete BEGIN shape.
	if _, err := c.Submit(t.Context(), VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1, ExpectedRevision: 1, Identity: identity, RequiredGroups: groups, MutationEpoch: 1}); err == nil {
		t.Fatal("malformed exact retry admitted")
	}
	raw, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(raw); err != nil {
		t.Fatal(err)
	}
	got, ok := restored.VectorPartitionLifecycleRecordV1(identity)
	if !ok || !reflect.DeepEqual(got, record) {
		t.Fatal("snapshot lost prepared source commitments")
	}
	forged := identity
	forged.SourceV2.SnapshotSetDigest = strings.Repeat("3", 64)
	if _, err := c.BeginBuildV1(t.Context(), forged, groups, 0, 1); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("forged root: %v", err)
	}
	forgedPlacement := identity
	forgedPlacement.SourceV2.PlacementDigest = strings.Repeat("3", 64)
	if _, err := c.BeginBuildV1(t.Context(), forgedPlacement, groups, 0, 1); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("forged placement root: %v", err)
	}
	owners[0].SnapshotSetDigest = strings.Repeat("4", 64)
	changed := identity
	changed.SourceV2.SnapshotSetDigest = ""
	if _, err := c.BeginBuildV1(t.Context(), changed, groups, 0, 1); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("same generation changed source: %v", err)
	}
}

func TestVectorPartitionSourcePreparationV2CodecAndServingRefusal(t *testing.T) {
	_, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity, owners := sourcePreparationFixtureV2(t, catalog)
	begin := VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1, Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a", "group-b"}, SourceOwners: owners, ANNOwners: annPreparationFixtureV2("group-a", "group-b"), MutationEpoch: 1}
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, begin)
	for _, owner := range owners {
		record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
			c.GroupReady = vectorPartitionLifecycleTestReadyV1(owner.GroupID, "5")
		}))
	}
	digest, err := VectorPartitionLifecycleReadySetDigestV1(identity, record.RequiredGroups, record.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(c *VectorPartitionLifecycleCommandV1) { c.ReadySetDigest = digest }))
	activate := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = 1 })
	if _, err := ApplyVectorPartitionLifecycleCommandV1(record, activate); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("activate: %v", err)
	}
	record.State = VectorPartitionLifecycleActiveV1
	if err := record.CanSearch(VectorPartitionLifecycleSearchProofV1{Identity: identity, ReadySetDigest: digest}); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("search: %v", err)
	}
	if _, err := EncodeVectorPartitionLifecycleRecordV1(record); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("forged active snapshot: %v", err)
	}
	for name, mutate := range map[string]func(*VectorPartitionLifecycleCommandV1){
		"ANN missing":   func(c *VectorPartitionLifecycleCommandV1) { c.ANNOwners = nil },
		"ANN duplicate": func(c *VectorPartitionLifecycleCommandV1) { c.ANNOwners[1] = c.ANNOwners[0] },
		"ANN foreign":   func(c *VectorPartitionLifecycleCommandV1) { c.ANNOwners[1].GroupID = "foreign" },
		"ANN count":     func(c *VectorPartitionLifecycleCommandV1) { c.ANNOwners[0].MembershipCount++ },
		"mixed":         func(c *VectorPartitionLifecycleCommandV1) { c.Identity.Source.Generation = 1 },
		"duplicate":     func(c *VectorPartitionLifecycleCommandV1) { c.SourceOwners[1] = c.SourceOwners[0] },
		"omitted":       func(c *VectorPartitionLifecycleCommandV1) { c.SourceOwners = c.SourceOwners[:1] },
		"extra": func(c *VectorPartitionLifecycleCommandV1) {
			c.SourceOwners = append(c.SourceOwners, VectorPartitionSourceOwnerPreparationV2{GroupID: "group-c", ShardCount: 1, SnapshotSetDigest: strings.Repeat("7", 64), CompletionEvidenceDigest: strings.Repeat("8", 64)})
		},
		"wrong root": func(c *VectorPartitionLifecycleCommandV1) {
			c.Identity.SourceV2.SnapshotSetDigest = strings.Repeat("6", 64)
		},
		"empty evidence": func(c *VectorPartitionLifecycleCommandV1) { c.SourceOwners[0].CompletionEvidenceDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			c := begin
			c.SourceOwners = slices.Clone(begin.SourceOwners)
			c.ANNOwners = slices.Clone(begin.ANNOwners)
			mutate(&c)
			if _, err := EncodeVectorPartitionLifecycleCommandV1(c); err == nil {
				t.Fatal("accepted invalid preparation")
			}
		})
	}
}

func TestVectorPartitionSourcePreparationV2SourceAndANNGroupsIndependent(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity, owners := sourcePreparationFixtureV2(t, catalog)
	owners = owners[:1]
	root, err := VectorPartitionSourceOwnerSetDigestV2(owners)
	if err != nil {
		t.Fatal(err)
	}
	identity.SourceV2.SnapshotSetDigest = root
	c := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}, PrepareSourceV2: func(context.Context, VectorPartitionLifecycleIdentityV1) ([]VectorPartitionSourceOwnerPreparationV2, error) {
		return owners, nil
	}}
	identity.SourceV2.PlacementDigest, _ = VectorPartitionANNOwnerSetDigestV2(annPreparationFixtureV2("group-b"))
	c.PrepareANNV2 = func(context.Context, VectorPartitionLifecycleIdentityV1) ([]VectorPartitionANNOwnerPreparationV2, error) {
		return annPreparationFixtureV2("group-b"), nil
	}
	record, err := c.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-b"}, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if record.SourceOwners[0].GroupID != "group-a" || record.RequiredGroups[0] != "group-b" {
		t.Fatalf("conflated authorities: %+v", record)
	}
}

func TestVectorPartitionSourcePreparationV2SnapshotRejectsUnknownOwner(t *testing.T) {
	_, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity, owners := sourcePreparationFixtureV2(t, catalog)
	owners[0].GroupID = "foreign"
	root, err := VectorPartitionSourceOwnerSetDigestV2(owners)
	if err != nil {
		t.Fatal(err)
	}
	identity.SourceV2.SnapshotSetDigest = root
	identity.SourceV2.PlacementDigest, _ = VectorPartitionANNOwnerSetDigestV2(annPreparationFixtureV2("group-b"))
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleCommandV1{Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1, Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-b"}, SourceOwners: owners, ANNOwners: annPreparationFixtureV2("group-b"), MutationEpoch: 1})
	raw, err := encodeVectorPartitionLifecycleSnapshotV1(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{identity: record}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, err := decodeVectorPartitionLifecycleSnapshotV1(raw, catalog); !errors.Is(err, ErrUnknownGroup) {
		t.Fatalf("foreign owner restore: %v", err)
	}
}

func TestVectorPartitionLocalPreparationV2ExactHostedScope(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity, owners := sourcePreparationFixtureV2(t, catalog)
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}, PrepareSourceV2: func(context.Context, VectorPartitionLifecycleIdentityV1) ([]VectorPartitionSourceOwnerPreparationV2, error) {
		return slices.Clone(owners), nil
	}}
	identity.SourceV2.PlacementDigest, _ = VectorPartitionANNOwnerSetDigestV2(annPreparationFixtureV2("group-b"))
	coordinator.PrepareANNV2 = func(context.Context, VectorPartitionLifecycleIdentityV1) ([]VectorPartitionANNOwnerPreparationV2, error) {
		return annPreparationFixtureV2("group-b"), nil
	}
	if _, err := coordinator.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-b"}, 0, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		node        raftcluster.NodeID
		source, ann []string
	}{
		{"node-a", []string{"group-a"}, nil},
		{"node-b", []string{"group-b"}, []string{"group-b"}},
		{"node-c", []string{"group-a", "group-b"}, []string{"group-b"}},
	} {
		record, source, ann, err := authority.VectorPartitionLocalPreparationV2(t.Context(), identity, tc.node)
		if err != nil || !slices.Equal(source, tc.source) || !slices.Equal(ann, tc.ann) || record.Identity != identity {
			t.Fatalf("node=%s source=%v ann=%v err=%v", tc.node, source, ann, err)
		}
		record.SourceOwners[0].GroupID = "mutated-copy"
	}
	stale := identity
	stale.Index.CatalogEpoch++
	if _, _, _, err := authority.VectorPartitionLocalPreparationV2(t.Context(), stale, "node-a"); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale proof: %v", err)
	}
	if _, _, _, err := authority.VectorPartitionLocalPreparationV2(t.Context(), identity, "foreign"); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("foreign node: %v", err)
	}
}

func annPreparationFixtureV2(groups ...string) []VectorPartitionANNOwnerPreparationV2 {
	out := make([]VectorPartitionANNOwnerPreparationV2, len(groups))
	for i, g := range groups {
		out[i] = VectorPartitionANNOwnerPreparationV2{GroupID: g, DomainCount: 1, MembershipCount: 2, MembershipDigest: strings.Repeat("9", 64)}
	}
	return out
}
