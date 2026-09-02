package raftplacement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestCatalogMetaCanonicalCommandAndExactRetry(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatalf("NewCatalogMetaRecordV1: %v", err)
	}
	command, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1: %v", err)
	}
	again, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1 again: %v", err)
	}
	if string(command) != string(again) {
		t.Fatalf("command bytes are not deterministic")
	}

	a := NewCatalogMetaAuthorityV1()
	first, err := a.applyCommittedCatalogMetaV1(command, 11)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if first.Epoch != 1 || first.AppliedIndex != 11 || first.Digest != record.Digest {
		t.Fatalf("first status=%+v", first)
	}
	retry, err := a.applyCommittedCatalogMetaV1(command, 12)
	if err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if retry.Epoch != 1 || retry.AppliedIndex != 11 {
		t.Fatalf("retry status=%+v", retry)
	}
}

func TestCatalogMetaCanonicalizesDefaultsBeforeDigest(t *testing.T) {
	ref := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
	implicit := validCatalog()
	implicit.Placements = append(implicit.Placements, tokenPlacement(ref, PlacementModeTokenV1))

	explicit := cloneCatalog(implicit)
	explicit.Features = DefaultFeatureSet()
	for i := range explicit.Placements {
		if explicit.Placements[i].Mode == "" {
			explicit.Placements[i].Mode = PlacementModeCollectionV1
		}
		if explicit.Placements[i].Mode == PlacementModeTokenV1 && explicit.Placements[i].RouteKey == "" {
			explicit.Placements[i].RouteKey = RouteKeyDocumentIDV1
		}
	}

	implicitRecord, err := NewCatalogMetaRecordV1(1, implicit)
	if err != nil {
		t.Fatalf("implicit defaults: %v", err)
	}
	explicitRecord, err := NewCatalogMetaRecordV1(1, explicit)
	if err != nil {
		t.Fatalf("explicit defaults: %v", err)
	}
	implicitBytes, err := encodeCatalogMetaRecordV1(implicitRecord)
	if err != nil {
		t.Fatalf("encode implicit: %v", err)
	}
	explicitBytes, err := encodeCatalogMetaRecordV1(explicitRecord)
	if err != nil {
		t.Fatalf("encode explicit: %v", err)
	}
	if implicitRecord.Digest != explicitRecord.Digest || !bytes.Equal(implicitBytes, explicitBytes) {
		t.Fatalf("semantic defaults diverged: implicit=%s explicit=%s", implicitRecord.Digest, explicitRecord.Digest)
	}
	if implicitRecord.Catalog.Features.ConfigVersion != SupportedCatalogVersion ||
		len(implicitRecord.Catalog.Features.Required) != 1 ||
		implicitRecord.Catalog.Features.Required[0].Name != FeatureCollectionGroups {
		t.Fatalf("canonical features=%+v", implicitRecord.Catalog.Features)
	}
	for _, placement := range implicitRecord.Catalog.Placements {
		if placement.Mode == "" {
			t.Fatalf("canonical placement retained empty mode: %+v", placement)
		}
		if placement.Mode == PlacementModeTokenV1 && placement.RouteKey != RouteKeyDocumentIDV1 {
			t.Fatalf("canonical token route key=%q", placement.RouteKey)
		}
	}

	authority := NewCatalogMetaAuthorityV1()
	command, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: implicitRecord})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	if _, err := authority.applyCommittedCatalogMetaV1(command, 1); err != nil {
		t.Fatalf("apply command: %v", err)
	}
	status, ok := authority.Status()
	if !ok || status.Features.ConfigVersion != SupportedCatalogVersion ||
		len(status.Features.Required) != 1 ||
		status.Features.Required[0].Name != FeatureCollectionGroups {
		t.Fatalf("status=%+v available=%v", status, ok)
	}
}

func TestCatalogMetaDigestExcludesDigestField(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(struct {
		Format  uint16    `json:"format"`
		Epoch   uint64    `json:"epoch"`
		Catalog CatalogV1 `json:"catalog"`
	}{
		Format:  record.Format,
		Epoch:   record.Epoch,
		Catalog: record.Catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"digest"`)) {
		t.Fatalf("independent digest payload contains digest field: %s", payload)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(payload))
	if record.Digest != want {
		t.Fatalf("record digest=%s want independent payload digest=%s", record.Digest, want)
	}
}

func TestCatalogMetaRejectsStaleSkippedAndConflictingEpoch(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	first := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := a.applyCommittedCatalogMetaV1(first, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 2, validCatalog()), 2); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale err=%v", err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 3, validCatalog()), 3); !errors.Is(err, ErrCatalogMetaSkippedEpoch) {
		t.Fatalf("skipped err=%v", err)
	}
	c := validCatalog()
	c.Groups[0].LeaderHint = ""
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, c), 3); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("conflict err=%v", err)
	}
}

func TestCatalogMetaTopologyTransitionsFailClosedWithoutMigration(t *testing.T) {
	tokenRef := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "events"}
	base := validCatalog()
	base.Features = DefaultFeatureSet()
	base.Placements = append(base.Placements, tokenPlacement(tokenRef, PlacementModeTokenV1))

	tests := []struct {
		name   string
		mutate func(*CatalogV1)
		want   error
		check  func(*testing.T, *CatalogMetaAuthorityV1, CatalogMetaStatusV1)
	}{
		{
			name: "leader hint metadata update",
			mutate: func(c *CatalogV1) {
				c.Groups[0].LeaderHint = "node-c"
			},
			check: func(t *testing.T, authority *CatalogMetaAuthorityV1, status CatalogMetaStatusV1) {
				decision := routeCatalogMetaCollectionV1(t, authority, status, CollectionRefV1{
					Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users",
				})
				if decision.LeaderHint() != "node-c" {
					t.Fatalf("leader hint=%q want node-c", decision.LeaderHint())
				}
			},
		},
		{
			name: "canonical feature defaults",
			mutate: func(c *CatalogV1) {
				c.Features = raftcluster.FeatureSet{}
			},
		},
		{
			name: "add group and placement",
			mutate: func(c *CatalogV1) {
				c.Groups = append(c.Groups, GroupV1{
					ID: "group-c", Members: []raftcluster.NodeID{"node-c", "node-d"}, LeaderHint: "node-c",
				})
				c.Placements = append(c.Placements, CollectionPlacementV1{
					Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "audit"},
					GroupID:    "group-c",
				})
			},
			check: func(t *testing.T, authority *CatalogMetaAuthorityV1, status CatalogMetaStatusV1) {
				decision := routeCatalogMetaCollectionV1(t, authority, status, CollectionRefV1{
					Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "audit",
				})
				if decision.GroupID() != "group-c" {
					t.Fatalf("new placement group=%q want group-c", decision.GroupID())
				}
			},
		},
		{
			name: "change collection owner",
			mutate: func(c *CatalogV1) {
				c.Placements[0].GroupID = "group-b"
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "change group members",
			mutate: func(c *CatalogV1) {
				c.Groups[0].Members = append(c.Groups[0].Members, "node-d")
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "remove group and its placements",
			mutate: func(c *CatalogV1) {
				c.Groups = c.Groups[:1]
				c.Placements = c.Placements[:1]
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "remove placement",
			mutate: func(c *CatalogV1) {
				c.Placements = c.Placements[1:]
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "change placement mode",
			mutate: func(c *CatalogV1) {
				c.Placements[2].Mode = PlacementModeRingV1
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "change partition owner",
			mutate: func(c *CatalogV1) {
				c.Placements[2].TokenPartitions[0].GroupID = "group-b"
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "change partition boundary",
			mutate: func(c *CatalogV1) {
				c.Placements[2].TokenPartitions[0].End--
				c.Placements[2].TokenPartitions[1].Start--
			},
			want: ErrCatalogMetaTopologyChange,
		},
		{
			name: "change partition identity",
			mutate: func(c *CatalogV1) {
				c.Placements[2].TokenPartitions[0].ID = "token-replacement"
			},
			want: ErrCatalogMetaTopologyChange,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authority := NewCatalogMetaAuthorityV1()
			if _, err := authority.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, base), 1); err != nil {
				t.Fatalf("apply base: %v", err)
			}
			before, _ := authority.Status()
			next := cloneCatalog(base)
			tc.mutate(&next)
			status, err := authority.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 2, next), 2)
			if !errors.Is(err, tc.want) {
				t.Fatalf("apply transition error=%v want errors.Is(%v)", err, tc.want)
			}
			if tc.want != nil {
				after, _ := authority.Status()
				if after.Epoch != before.Epoch || after.Digest != before.Digest || after.AppliedIndex != before.AppliedIndex {
					t.Fatalf("refused transition published state: before=%+v after=%+v", before, after)
				}
				decision := routeCatalogMetaCollectionV1(t, authority, after, CollectionRefV1{
					Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users",
				})
				if decision.GroupID() != "group-a" {
					t.Fatalf("refused transition route group=%q want group-a", decision.GroupID())
				}
				return
			}
			if status.Epoch != 2 || status.AppliedIndex != 2 {
				t.Fatalf("allowed transition status=%+v", status)
			}
			if tc.check != nil {
				tc.check(t, authority, status)
			}
		})
	}
}

func TestCatalogMetaForwardSnapshotRejectsTopologyChangeOnLiveAuthority(t *testing.T) {
	target := NewCatalogMetaAuthorityV1()
	if _, err := target.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, validCatalog()), 1); err != nil {
		t.Fatalf("apply target base: %v", err)
	}
	before, _ := target.Status()

	next := validCatalog()
	next.Placements[0].GroupID = "group-b"
	record, err := NewCatalogMetaRecordV1(2, next)
	if err != nil {
		t.Fatalf("build snapshot record: %v", err)
	}
	recordBytes, err := encodeCatalogMetaRecordV1(record)
	if err != nil {
		t.Fatalf("encode snapshot record: %v", err)
	}
	command, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 1, Record: record})
	if err != nil {
		t.Fatalf("encode snapshot command: %v", err)
	}
	_, err = target.installCatalogMetaSnapshotV1(CatalogMetaSnapshotV1{
		Format:       CatalogMetaFormatV1,
		AppliedIndex: 2,
		Record:       recordBytes,
		LastCommand:  command,
	})
	if !errors.Is(err, ErrCatalogMetaTopologyChange) {
		t.Fatalf("install topology-changing snapshot error=%v want ErrCatalogMetaTopologyChange", err)
	}
	after, _ := target.Status()
	if after.Epoch != before.Epoch || after.Digest != before.Digest || after.AppliedIndex != before.AppliedIndex {
		t.Fatalf("refused snapshot published state: before=%+v after=%+v", before, after)
	}
}

func routeCatalogMetaCollectionV1(t *testing.T, authority *CatalogMetaAuthorityV1, status CatalogMetaStatusV1, ref CollectionRefV1) RouteDecisionV1 {
	t.Helper()
	decision, err := authority.Route(context.Background(), CatalogProofV1{
		Epoch: status.Epoch, Digest: status.Digest,
	}, RouteRequestV1{Collection: ref, Shape: RouteShapeCollectionV1})
	if err != nil {
		t.Fatalf("route %s/%s/%s: %v", ref.Database, ref.Catalog, ref.Collection, err)
	}
	return decision
}

func TestCatalogMetaRouteAdmissionFailsClosed(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := a.applyCommittedCatalogMetaV1(command, 7); err != nil {
		t.Fatal(err)
	}
	status, ok := a.Status()
	if !ok {
		t.Fatal("missing status")
	}
	request := RouteRequestV1{Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"}, Shape: RouteShapeCollectionV1}
	if _, err := a.Route(context.Background(), CatalogProofV1{}, request); !errors.Is(err, ErrCatalogMetaProofMissing) {
		t.Fatalf("missing proof err=%v", err)
	}
	if _, err := a.Route(context.Background(), CatalogProofV1{Epoch: 2, Digest: status.Digest}, request); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale proof err=%v", err)
	}
	if _, err := a.Route(context.Background(), CatalogProofV1{Epoch: 1, Digest: "00"}, request); !errors.Is(err, ErrCatalogMetaDigestMismatch) {
		t.Fatalf("digest proof err=%v", err)
	}
	decision, err := a.Route(context.Background(), CatalogProofV1{Epoch: 1, Digest: status.Digest}, request)
	if err != nil || decision.GroupID() != "group-a" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestCatalogMetaOwnerDispatchRevalidatesCompleteRoute(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, validCatalog()), 7); err != nil {
		t.Fatalf("apply catalog: %v", err)
	}
	status, ok := a.Status()
	if !ok {
		t.Fatal("catalog status unavailable")
	}
	metadata := raftentry.RequestMetadataV1{
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      "default",
		ClusterRouteCatalog:       "default",
		ClusterRouteCollection:    "users",
		ClusterRouteShape:         string(RouteShapeCollectionV1),
		ClusterRouteGroupID:       "group-a",
		ClusterRouteMembers:       []string{"node-a", "node-c"},
		ClusterRouteLeaderHint:    "node-a",
		ClusterRoutePlacementMode: string(PlacementModeCollectionV1),
		CatalogMetaEpoch:          status.Epoch,
		CatalogMetaDigest:         status.Digest,
	}
	if err := a.ValidateCatalogRouteMetadata(context.Background(), metadata); err != nil {
		t.Fatalf("validate authoritative route: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*raftentry.RequestMetadataV1)
	}{
		{name: "missing proof", mutate: func(m *raftentry.RequestMetadataV1) { m.CatalogMetaDigest = "" }},
		{name: "wrong collection", mutate: func(m *raftentry.RequestMetadataV1) { m.ClusterRouteCollection = "orders" }},
		{name: "wrong group", mutate: func(m *raftentry.RequestMetadataV1) { m.ClusterRouteGroupID = "group-b" }},
		{name: "wrong members", mutate: func(m *raftentry.RequestMetadataV1) { m.ClusterRouteMembers = []string{"node-a"} }},
		{name: "wrong leader hint", mutate: func(m *raftentry.RequestMetadataV1) { m.ClusterRouteLeaderHint = "node-b" }},
		{name: "wrong placement", mutate: func(m *raftentry.RequestMetadataV1) { m.ClusterRoutePlacementMode = string(PlacementModeTokenV1) }},
		{name: "invented token", mutate: func(m *raftentry.RequestMetadataV1) { m.ClusterRouteTokenKnown = true; m.ClusterRouteToken = 9 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := metadata
			candidate.ClusterRouteMembers = append([]string(nil), metadata.ClusterRouteMembers...)
			tc.mutate(&candidate)
			if err := a.ValidateCatalogRouteMetadata(context.Background(), candidate); err == nil {
				t.Fatal("tampered route unexpectedly validated")
			}
		})
	}
}

func TestCatalogMetaSnapshotInstallNeverMovesBackward(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	if _, err := source.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, validCatalog()), 9); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	target := NewCatalogMetaAuthorityV1()
	if _, err := target.installCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := target.installCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatalf("same snapshot: %v", err)
	}
	if _, err := target.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 2, validCatalog()), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := target.installCatalogMetaSnapshotV1(snapshot); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("rollback snapshot err=%v", err)
	}
}

func TestCatalogMetaSnapshotRestorePreservesExactRetryIdentity(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := source.applyCommittedCatalogMetaV1(command, 9); err != nil {
		t.Fatalf("apply source: %v", err)
	}
	snapshot, err := source.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}
	target := NewCatalogMetaAuthorityV1()
	if _, err := target.installCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	retry, err := target.applyCommittedCatalogMetaV1(command, 10)
	if err != nil {
		t.Fatalf("exact retry after restore: %v", err)
	}
	if retry.Epoch != 1 || retry.AppliedIndex != 9 {
		t.Fatalf("retry status=%+v want restored epoch/index 1/9", retry)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*CatalogMetaSnapshotV1)
	}{
		{name: "missing command", mutate: func(s *CatalogMetaSnapshotV1) { s.LastCommand = nil }},
		{name: "different command", mutate: func(s *CatalogMetaSnapshotV1) {
			s.LastCommand = mustCatalogMetaCommand(t, 1, 2, validCatalog())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := snapshot
			tc.mutate(&candidate)
			if _, err := target.installCatalogMetaSnapshotV1(candidate); !errors.Is(err, ErrCatalogMetaConflict) {
				t.Fatalf("install same-epoch snapshot error=%v want ErrCatalogMetaConflict", err)
			}
		})
	}
}

func TestCatalogMetaBackupArchiveRestorePreservesIdentityPlacementAndFeatures(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	sourceStatus, err := source.applyCommittedCatalogMetaV1(command, 9)
	if err != nil {
		t.Fatalf("apply source: %v", err)
	}
	archive, err := source.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatalf("export backup archive: %v", err)
	}

	// Backup transport treats the catalog snapshot as an opaque byte payload.
	// Production restore feeds this payload through the Raft FSM restore
	// capability; the package-private install models that final state-machine
	// step without creating a follower-local activation API.
	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(bytes.Clone(archive)); err != nil {
		t.Fatalf("restore backup archive: %v", err)
	}
	restoredStatus, ok := restored.Status()
	if !ok {
		t.Fatal("restored backup catalog unavailable")
	}
	if restoredStatus.Epoch != sourceStatus.Epoch ||
		restoredStatus.Digest != sourceStatus.Digest ||
		restoredStatus.AppliedIndex != sourceStatus.AppliedIndex ||
		restoredStatus.RetainedWireBytes != sourceStatus.RetainedWireBytes {
		t.Fatalf("restored status=%+v want %+v", restoredStatus, sourceStatus)
	}
	if restoredStatus.Features.ConfigVersion != sourceStatus.Features.ConfigVersion ||
		!slices.Equal(restoredStatus.Features.Required, sourceStatus.Features.Required) {
		t.Fatalf("restored features=%+v want %+v", restoredStatus.Features, sourceStatus.Features)
	}
	proof, err := restored.CurrentCatalogProof(context.Background())
	if err != nil {
		t.Fatalf("restored proof: %v", err)
	}
	decision, err := restored.Route(context.Background(), proof, RouteRequestV1{
		Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
		Shape:      RouteShapeCollectionV1,
	})
	if err != nil {
		t.Fatalf("restored placement route: %v", err)
	}
	if decision.GroupID() != "group-a" ||
		decision.PlacementMode != PlacementModeCollectionV1 {
		t.Fatalf("restored placement decision=%+v proof=%+v", decision, proof)
	}
}

func TestCatalogMetaSnapshotRestoreRequiresRaftCapability(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	if _, err := source.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, validCatalog()), 9); err != nil {
		t.Fatalf("apply source: %v", err)
	}
	raw, err := source.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatalf("export snapshot bytes: %v", err)
	}
	target := NewCatalogMetaAuthorityV1()
	if err := target.InstallCatalogMetaSnapshotBytesV1(raftcluster.CatalogMetaRestoreCapabilityV1{}, raw); !errors.Is(err, ErrCatalogMetaUnavailable) {
		t.Fatalf("unauthorized restore error=%v want ErrCatalogMetaUnavailable", err)
	}
	if _, ok := target.Status(); ok {
		t.Fatal("unauthorized restore activated catalog state")
	}
}

func TestCatalogMetaReplayCrashRestoreModelNeverMovesBackward(t *testing.T) {
	const seeds = 64
	for seed := int64(0); seed < seeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		authority := NewCatalogMetaAuthorityV1()
		var epoch uint64
		var lastCommand []byte
		var snapshot CatalogMetaSnapshotV1
		for step := 0; step < 64; step++ {
			switch rng.Intn(4) {
			case 0, 1:
				next := epoch + 1
				command := mustCatalogMetaCommand(t, epoch, next, validCatalog())
				if _, err := authority.applyCommittedCatalogMetaV1(command, uint64(step+1)); err != nil {
					t.Fatalf("seed=%d step=%d apply epoch %d: %v", seed, step, next, err)
				}
				epoch, lastCommand = next, command
			case 2:
				if epoch == 0 {
					continue
				}
				if _, err := authority.applyCommittedCatalogMetaV1(lastCommand, uint64(step+1)); err != nil {
					t.Fatalf("seed=%d step=%d exact retry: %v", seed, step, err)
				}
			case 3:
				if epoch == 0 {
					continue
				}
				var err error
				snapshot, err = authority.ExportCatalogMetaSnapshotV1()
				if err != nil {
					t.Fatalf("seed=%d step=%d export: %v", seed, step, err)
				}
				restarted := NewCatalogMetaAuthorityV1()
				if _, err := restarted.installCatalogMetaSnapshotV1(snapshot); err != nil {
					t.Fatalf("seed=%d step=%d restore: %v", seed, step, err)
				}
				authority = restarted
			}
			status, ok := authority.Status()
			if epoch == 0 {
				if ok {
					t.Fatalf("seed=%d step=%d empty model published status=%+v", seed, step, status)
				}
				continue
			}
			if !ok || status.Epoch != epoch {
				t.Fatalf("seed=%d step=%d status=%+v/%t want epoch %d", seed, step, status, ok, epoch)
			}
			if epoch > 1 {
				stale := mustCatalogMetaCommand(t, epoch-2, epoch-1, validCatalog())
				if _, err := authority.applyCommittedCatalogMetaV1(stale, uint64(step+1000)); err == nil {
					t.Fatalf("seed=%d step=%d stale command unexpectedly applied", seed, step)
				}
				after, _ := authority.Status()
				if after.Epoch != epoch || after.Digest != status.Digest {
					t.Fatalf("seed=%d step=%d stale command changed state from %+v to %+v", seed, step, status, after)
				}
			}
		}
	}
}

func TestCatalogMetaConcurrentReadersObserveOnlyCompleteGenerations(t *testing.T) {
	authority := NewCatalogMetaAuthorityV1()
	type generation struct {
		digest string
		leader raftcluster.NodeID
	}
	generations := make(map[uint64]generation)
	var generationsMu sync.RWMutex
	makeCatalog := func(epoch uint64) CatalogV1 {
		catalog := validCatalog()
		if epoch%2 == 0 {
			catalog.Groups[0].LeaderHint = "node-c"
		}
		return catalog
	}
	first := mustCatalogMetaCommand(t, 0, 1, makeCatalog(1))
	if _, err := authority.applyCommittedCatalogMetaV1(first, 1); err != nil {
		t.Fatalf("apply first generation: %v", err)
	}
	firstStatus, _ := authority.Status()
	generations[1] = generation{digest: firstStatus.Digest, leader: "node-a"}

	const finalEpoch = 64
	done := make(chan struct{})
	errs := make(chan error, 16)
	var readers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				status, ok := authority.Status()
				if !ok {
					errs <- fmt.Errorf("authority became unavailable")
					return
				}
				generationsMu.RLock()
				want, known := generations[status.Epoch]
				generationsMu.RUnlock()
				if !known {
					continue
				}
				if status.Digest != want.digest {
					errs <- fmt.Errorf("epoch %d digest=%s want %s", status.Epoch, status.Digest, want.digest)
					return
				}
				decision, err := authority.Route(context.Background(), CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest}, RouteRequestV1{
					Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
					Shape:      RouteShapeCollectionV1,
				})
				if err != nil {
					if errors.Is(err, ErrCatalogMetaStaleEpoch) {
						continue
					}
					errs <- err
					return
				}
				if decision.GroupID() != "group-a" {
					errs <- fmt.Errorf("epoch %d route group=%s want group-a", status.Epoch, decision.GroupID())
					return
				}
				if decision.LeaderHint() != want.leader {
					errs <- fmt.Errorf("epoch %d leader hint=%s want %s", status.Epoch, decision.LeaderHint(), want.leader)
					return
				}
			}
		}()
	}
	for epoch := uint64(2); epoch <= finalEpoch; epoch++ {
		command := mustCatalogMetaCommand(t, epoch-1, epoch, makeCatalog(epoch))
		status, err := authority.applyCommittedCatalogMetaV1(command, epoch)
		if err != nil {
			close(done)
			readers.Wait()
			t.Fatalf("apply epoch %d: %v", epoch, err)
		}
		leader := raftcluster.NodeID("node-a")
		if epoch%2 == 0 {
			leader = "node-c"
		}
		generationsMu.Lock()
		generations[epoch] = generation{digest: status.Digest, leader: leader}
		generationsMu.Unlock()
	}
	close(done)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestCatalogMetaDecodeRejectsUnknownAndOversized(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCatalogMetaCommandV1(append(append([]byte{}, b[:len(b)-1]...), []byte(`,"unknown":true}`)...)); !errors.Is(err, ErrInvalidCatalogMeta) {
		t.Fatalf("unknown err=%v", err)
	}
	if _, err := DecodeCatalogMetaCommandV1(make([]byte, MaxCatalogMetaCommandBytesV1+1)); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("limit err=%v", err)
	}
}

func TestCatalogMetaDecodePreflightRejectsMalformedBeforeTypedDecode(t *testing.T) {
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	recordBytes, err := encodeCatalogMetaRecordV1(record)
	if err != nil {
		t.Fatal(err)
	}
	duplicateGroups := bytes.Replace(command, []byte(`"ID":"group-b"`), []byte(`"ID":"group-a"`), 1)
	duplicateMembers := bytes.Replace(command, []byte(`"Members":["node-a","node-c"]`), []byte(`"Members":["node-a","node-a"]`), 1)
	duplicatePlacements := bytes.Replace(command, []byte(`"Collection":"orders"`), []byte(`"Collection":"users"`), 1)
	duplicatePartitions := bytes.Replace(catalogMetaPartitionsShapeV1(2), []byte(`"ID":"p1"`), []byte(`"ID":"p0"`), 1)
	feature := `{"Name":"` + string(FeatureCollectionGroups) + `","Version":{"Major":1,"Minor":0}}`
	duplicateFeatures := bytes.Replace(
		catalogMetaCommandShapeV1(`null`, `null`),
		[]byte(`"Required":null`),
		[]byte(`"Required":[`+feature+`,`+feature+`]`),
		1,
	)
	badDigest := bytes.Clone(command)
	digestAt := bytes.Index(badDigest, []byte(`"digest":"`))
	if digestAt < 0 {
		t.Fatal("missing digest field")
	}
	digestAt += len(`"digest":"`)
	if badDigest[digestAt] == '0' {
		badDigest[digestAt] = '1'
	} else {
		badDigest[digestAt] = '0'
	}
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "truncated", raw: command[:len(command)-1], want: ErrInvalidCatalogMeta},
		{name: "duplicate command key", raw: bytes.Replace(command, []byte(`{"format":1`), []byte(`{"format":1,"format":1`), 1), want: ErrInvalidCatalogMeta},
		{name: "duplicate nested key", raw: bytes.Replace(command, []byte(`"Groups":`), []byte(`"Groups":null,"Groups":`), 1), want: ErrInvalidCatalogMeta},
		{name: "unknown exact-case field", raw: bytes.Replace(command, []byte(`"expected_epoch":0`), []byte(`"Expected_Epoch":0`), 1), want: ErrInvalidCatalogMeta},
		{name: "unknown format", raw: bytes.Replace(command, []byte(`{"format":1`), []byte(`{"format":2`), 1), want: ErrUnsupportedVersion},
		{name: "uint64 overflow", raw: bytes.Replace(command, []byte(`"expected_epoch":0`), []byte(`"expected_epoch":18446744073709551616`), 1), want: ErrCatalogMetaLimit},
		{name: "non-integer numeric", raw: bytes.Replace(command, []byte(`"expected_epoch":0`), []byte(`"expected_epoch":0.0`), 1), want: ErrInvalidCatalogMeta},
		{name: "overlong string", raw: bytes.Replace(command, []byte(`"ID":"group-a"`), []byte(`"ID":"`+strings.Repeat("a", MaxCatalogMetaStringBytesV1+1)+`"`), 1), want: ErrCatalogMetaLimit},
		{name: "digest mismatch", raw: badDigest, want: ErrCatalogMetaDigestMismatch},
		{name: "duplicate group identity", raw: duplicateGroups, want: ErrDuplicateGroup},
		{name: "duplicate member identity", raw: duplicateMembers, want: ErrDuplicateMember},
		{name: "duplicate placement identity", raw: duplicatePlacements, want: ErrDuplicatePlacement},
		{name: "duplicate partition identity", raw: duplicatePartitions, want: ErrDuplicateTokenPartition},
		{name: "duplicate feature identity", raw: duplicateFeatures, want: ErrUnsupportedFeature},
		{name: "record duplicate key", raw: bytes.Replace(recordBytes, []byte(`{"format":1`), []byte(`{"format":1,"format":1`), 1), want: ErrInvalidCatalogMeta},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got error
			if strings.HasPrefix(tc.name, "record ") {
				_, got = decodeCatalogMetaRecordV1(tc.raw)
			} else {
				_, got = DecodeCatalogMetaCommandV1(tc.raw)
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("error=%v want errors.Is(%v)", got, tc.want)
			}
		})
	}
}

func TestCatalogMetaRecordConstructionEnforcesWireStringLimit(t *testing.T) {
	catalog := validCatalog()
	longID := raftcluster.GroupID(strings.Repeat("g", MaxCatalogMetaStringBytesV1+1))
	catalog.Groups[0].ID = longID
	catalog.Placements[0].GroupID = longID
	if _, err := NewCatalogMetaRecordV1(1, catalog); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("NewCatalogMetaRecordV1 error=%v want ErrCatalogMetaLimit", err)
	}
}

func TestCatalogMetaDecodePreflightElementLimits(t *testing.T) {
	tests := []struct {
		name  string
		count int
		build func(int) []byte
		want  error
	}{
		{name: "groups minimum", count: 1, build: catalogMetaGroupsShapeV1},
		{name: "groups maximum", count: MaxCatalogMetaGroupsV1, build: catalogMetaGroupsShapeV1},
		{name: "groups one over", count: MaxCatalogMetaGroupsV1 + 1, build: catalogMetaGroupsShapeV1, want: ErrCatalogMetaLimit},
		{name: "members minimum", count: 1, build: catalogMetaMembersShapeV1},
		{name: "members maximum", count: MaxCatalogMetaMembersPerGroupV1, build: catalogMetaMembersShapeV1},
		{name: "members one over", count: MaxCatalogMetaMembersPerGroupV1 + 1, build: catalogMetaMembersShapeV1, want: ErrCatalogMetaLimit},
		{name: "placements minimum", count: 1, build: catalogMetaPlacementsShapeV1},
		{name: "placements maximum", count: MaxCatalogMetaPlacementsV1, build: catalogMetaPlacementsShapeV1},
		{name: "placements one over", count: MaxCatalogMetaPlacementsV1 + 1, build: catalogMetaPlacementsShapeV1, want: ErrCatalogMetaLimit},
		{name: "partitions minimum", count: 1, build: catalogMetaPartitionsShapeV1},
		{name: "partitions maximum", count: MaxCatalogMetaPartitionsPerPlacementV1, build: catalogMetaPartitionsShapeV1},
		{name: "partitions one over", count: MaxCatalogMetaPartitionsPerPlacementV1 + 1, build: catalogMetaPartitionsShapeV1, want: ErrCatalogMetaLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.build(tc.count)
			if tc.want == nil && len(raw) > MaxCatalogMetaCommandBytesV1 {
				t.Fatalf("test shape is %d bytes, beyond command cap", len(raw))
			}
			err := preflightCatalogMetaCommandJSONV1(raw)
			if !errors.Is(err, tc.want) {
				t.Fatalf("preflight error=%v want errors.Is(%v); bytes=%d", err, tc.want, len(raw))
			}
		})
	}
}

func TestCatalogMetaDecodePreflightAggregatePartitionLimit(t *testing.T) {
	raw := catalogMetaCommandShapeV1(`null`, catalogMetaPlacementArrayV1([]int{
		MaxCatalogMetaPartitionsV1 / 2,
		MaxCatalogMetaPartitionsV1 - MaxCatalogMetaPartitionsV1/2 + 1,
	}))
	if len(raw) > MaxCatalogMetaCommandBytesV1 {
		t.Fatalf("aggregate test shape is %d bytes, beyond command cap", len(raw))
	}
	if err := preflightCatalogMetaCommandJSONV1(raw); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("preflight error=%v want ErrCatalogMetaLimit", err)
	}
}

func TestCatalogMetaMaximumPlacementShapeRoundTrip(t *testing.T) {
	catalog := CatalogV1{
		Groups: []GroupV1{{ID: "g", Members: []raftcluster.NodeID{"n"}, LeaderHint: "n"}},
	}
	catalog.Placements = make([]CollectionPlacementV1, MaxCatalogMetaPlacementsV1)
	for i := range catalog.Placements {
		catalog.Placements[i] = CollectionPlacementV1{
			Collection: CollectionRefV1{Database: "d", Catalog: "c", Collection: fmt.Sprintf("x%04d", i)},
			GroupID:    "g",
		}
	}
	command := mustCatalogMetaCommand(t, 0, 1, catalog)
	if len(command) > MaxCatalogMetaCommandBytesV1 {
		t.Fatalf("maximum placement command is %d bytes", len(command))
	}
	decoded, err := DecodeCatalogMetaCommandV1(command)
	if err != nil {
		t.Fatalf("DecodeCatalogMetaCommandV1: %v", err)
	}
	if len(decoded.Record.Catalog.Placements) != MaxCatalogMetaPlacementsV1 {
		t.Fatalf("placements=%d want %d", len(decoded.Record.Catalog.Placements), MaxCatalogMetaPlacementsV1)
	}
	again, err := EncodeCatalogMetaCommandV1(decoded)
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1: %v", err)
	}
	if !bytes.Equal(command, again) {
		t.Fatal("maximum placement command did not round trip exactly")
	}
}

func TestCatalogMetaSnapshotBytesPreflightAndCanonicalRestore(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := source.applyCommittedCatalogMetaV1(command, 42); err != nil {
		t.Fatal(err)
	}
	raw, err := source.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	target := NewCatalogMetaAuthorityV1()
	if err := target.installCatalogMetaSnapshotBytesV1(raw); err != nil {
		t.Fatalf("canonical restore: %v", err)
	}
	status, ok := target.Status()
	if !ok || status.Epoch != 1 || status.AppliedIndex != 42 {
		t.Fatalf("restored status=%+v ok=%t", status, ok)
	}

	var snapshot CatalogMetaSnapshotV1
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Record = []byte(`{"format":1,"format":1}`)
	badNested, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	tooLong := []byte(`{"format":1,"applied_index":1,"record":"` + strings.Repeat("A", maxCatalogMetaSnapshotFieldBytesV1+1) + `","last_command":null}`)
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "duplicate wrapper key", raw: bytes.Replace(raw, []byte(`{"format":1`), []byte(`{"format":1,"format":1`), 1), want: ErrInvalidCatalogMeta},
		{name: "non canonical whitespace", raw: append(append([]byte{}, raw...), '\n'), want: ErrInvalidCatalogMeta},
		{name: "nested record duplicate key", raw: badNested, want: ErrInvalidCatalogMeta},
		{name: "encoded record over limit", raw: tooLong, want: ErrCatalogMetaLimit},
		{name: "total bytes over limit", raw: make([]byte, MaxCatalogMetaSnapshotBytesV1+1), want: ErrCatalogMetaLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(tc.raw)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want errors.Is(%v)", err, tc.want)
			}
		})
	}
}

func FuzzDecodeCatalogMetaCommandV1(f *testing.F) {
	valid := mustCatalogMetaCommand(f, 0, 1, validCatalog())
	f.Add(valid)
	f.Add(valid[:len(valid)-1])
	f.Add([]byte(`{"format":1,"expected_epoch":18446744073709551616}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxCatalogMetaCommandBytesV1 {
			return
		}
		command, err := DecodeCatalogMetaCommandV1(raw)
		if err != nil {
			return
		}
		again, err := EncodeCatalogMetaCommandV1(command)
		if err != nil {
			t.Fatalf("accepted command failed re-encode: %v", err)
		}
		if !bytes.Equal(raw, again) {
			t.Fatal("accepted command was not canonical")
		}
	})
}

func catalogMetaCommandShapeV1(groups, placements string) []byte {
	return []byte(`{"format":1,"expected_epoch":0,"record":{"format":1,"epoch":1,"catalog":{"Features":{"ConfigVersion":{"Major":0,"Minor":0},"Required":null},"Groups":` + groups + `,"Placements":` + placements + `},"digest":"` + strings.Repeat("0", MaxCatalogMetaDigestBytesV1) + `"}}`)
}

func catalogMetaGroupsShapeV1(count int) []byte {
	var groups strings.Builder
	groups.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			groups.WriteByte(',')
		}
		fmt.Fprintf(&groups, `{"ID":"g%d","Members":["n"],"LeaderHint":"n"}`, i)
	}
	groups.WriteByte(']')
	return catalogMetaCommandShapeV1(groups.String(), `null`)
}

func catalogMetaMembersShapeV1(count int) []byte {
	var members strings.Builder
	members.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			members.WriteByte(',')
		}
		fmt.Fprintf(&members, `"n%d"`, i)
	}
	members.WriteByte(']')
	groups := `[{"ID":"g","Members":` + members.String() + `,"LeaderHint":"n0"}]`
	return catalogMetaCommandShapeV1(groups, `null`)
}

func catalogMetaPlacementsShapeV1(count int) []byte {
	counts := make([]int, count)
	return catalogMetaCommandShapeV1(`null`, catalogMetaPlacementArrayV1(counts))
}

func catalogMetaPartitionsShapeV1(count int) []byte {
	return catalogMetaCommandShapeV1(`null`, catalogMetaPlacementArrayV1([]int{count}))
}

func catalogMetaPlacementArrayV1(partitionCounts []int) string {
	var placements strings.Builder
	placements.WriteByte('[')
	for i, count := range partitionCounts {
		if i > 0 {
			placements.WriteByte(',')
		}
		placements.WriteString(`{"Collection":{"Database":"d","Catalog":"c","Collection":"x`)
		placements.WriteString(strconv.Itoa(i))
		placements.WriteString(`"},"GroupID":"g","Mode":"","RouteKey":"","TokenPartitions":`)
		if count == 0 {
			placements.WriteString(`null}`)
			continue
		}
		placements.WriteByte('[')
		for j := 0; j < count; j++ {
			if j > 0 {
				placements.WriteByte(',')
			}
			fmt.Fprintf(&placements, `{"ID":"p%d","GroupID":"g","Start":0,"End":0}`, j)
		}
		placements.WriteString(`]}`)
	}
	placements.WriteByte(']')
	return placements.String()
}

func mustCatalogMetaCommand(t testing.TB, expected, epoch uint64, catalog CatalogV1) []byte {
	t.Helper()
	record, err := NewCatalogMetaRecordV1(epoch, catalog)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	b, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: expected, Record: record})
	if err != nil {
		t.Fatalf("command: %v", err)
	}
	return b
}
