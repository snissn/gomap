package raftplacement

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func BenchmarkCatalogMetaRoute(b *testing.B) {
	a := NewCatalogMetaAuthorityV1()
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(b, 0, 1, validCatalog()), 1); err != nil {
		b.Fatal(err)
	}
	status, ok := a.Status()
	if !ok {
		b.Fatal("missing status")
	}
	proof := CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest}
	req := RouteRequestV1{Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"}, Shape: RouteShapeCollectionV1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.Route(b.Context(), proof, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogMetaDecode(b *testing.B) {
	command := mustCatalogMetaCommand(b, 0, 1, validCatalog())
	b.ReportAllocs()
	b.SetBytes(int64(len(command)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeCatalogMetaCommandV1(command); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCatalogMetaStatusRouteAdmissionMatrix(b *testing.B) {
	benchmarkCatalogMetaShapes(b, func(b *testing.B, fixture catalogMetaBenchmarkFixture) {
		b.Run("status", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(fixture.status.RetainedWireBytes), "retained_wire_B")
			for i := 0; i < b.N; i++ {
				if _, ok := fixture.authority.Status(); !ok {
					b.Fatal("missing status")
				}
			}
		})
		b.Run("route", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(fixture.status.RetainedWireBytes), "retained_wire_B")
			for i := 0; i < b.N; i++ {
				if _, err := fixture.authority.Route(b.Context(), fixture.proof, fixture.request); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("owner_admission", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(fixture.status.RetainedWireBytes), "retained_wire_B")
			for i := 0; i < b.N; i++ {
				if err := fixture.authority.ValidateCatalogRouteMetadata(b.Context(), fixture.metadata); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkCatalogMetaEncodeDecodeApplyMatrix(b *testing.B) {
	benchmarkCatalogMetaShapes(b, func(b *testing.B, fixture catalogMetaBenchmarkFixture) {
		decoded, err := DecodeCatalogMetaCommandV1(fixture.command)
		if err != nil {
			b.Fatal(err)
		}
		b.Run("encode", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.command)))
			b.ReportMetric(float64(len(fixture.command)), "command_B")
			for i := 0; i < b.N; i++ {
				if _, err := EncodeCatalogMetaCommandV1(decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("decode", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.command)))
			b.ReportMetric(float64(len(fixture.command)), "command_B")
			for i := 0; i < b.N; i++ {
				if _, err := DecodeCatalogMetaCommandV1(fixture.command); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("apply_fresh", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.command)))
			b.ReportMetric(float64(len(fixture.command)), "command_B")
			for i := 0; i < b.N; i++ {
				if _, err := NewCatalogMetaAuthorityV1().applyCommittedCatalogMetaV1(fixture.command, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkCatalogMetaSnapshotArchiveInstallStatusRoute(b *testing.B) {
	benchmarkCatalogMetaShapes(b, func(b *testing.B, fixture catalogMetaBenchmarkFixture) {
		snapshot, err := fixture.authority.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			b.Fatal(err)
		}
		b.Run("archive", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(snapshot)))
			b.ReportMetric(float64(len(snapshot)), "snapshot_B")
			for i := 0; i < b.N; i++ {
				if _, err := fixture.authority.ExportCatalogMetaSnapshotBytesV1(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("install", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(snapshot)))
			b.ReportMetric(float64(len(snapshot)), "snapshot_B")
			for i := 0; i < b.N; i++ {
				if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("install_status_route", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(snapshot)))
			b.ReportMetric(float64(len(snapshot)), "snapshot_B")
			for i := 0; i < b.N; i++ {
				installed := NewCatalogMetaAuthorityV1()
				if err := installed.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
					b.Fatal(err)
				}
				status, ok := installed.Status()
				if !ok {
					b.Fatal("installed status unavailable")
				}
				if _, err := installed.Route(b.Context(), CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest}, fixture.request); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func benchmarkCatalogMetaShapes(b *testing.B, run func(*testing.B, catalogMetaBenchmarkFixture)) {
	b.Helper()
	for _, shape := range []struct {
		name    string
		catalog CatalogV1
	}{
		{name: "small", catalog: validCatalog()},
		{name: "maximum_placements", catalog: catalogMetaMaximumPlacementBenchmarkCatalog()},
	} {
		b.Run(shape.name, func(b *testing.B) {
			run(b, newCatalogMetaBenchmarkFixture(b, shape.catalog))
		})
	}
}

type catalogMetaBenchmarkFixture struct {
	authority *CatalogMetaAuthorityV1
	command   []byte
	status    CatalogMetaStatusV1
	proof     CatalogProofV1
	request   RouteRequestV1
	metadata  raftentry.RequestMetadataV1
}

func newCatalogMetaBenchmarkFixture(b *testing.B, catalog CatalogV1) catalogMetaBenchmarkFixture {
	b.Helper()
	command := mustCatalogMetaCommand(b, 0, 1, catalog)
	authority := NewCatalogMetaAuthorityV1()
	status, err := authority.applyCommittedCatalogMetaV1(command, 1)
	if err != nil {
		b.Fatal(err)
	}
	ref := catalog.Placements[0].Collection
	request := RouteRequestV1{Collection: ref, Shape: RouteShapeCollectionV1}
	decision, err := authority.Route(b.Context(), CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest}, request)
	if err != nil {
		b.Fatal(err)
	}
	members := make([]string, len(decision.Group.Members))
	for i := range decision.Group.Members {
		members[i] = string(decision.Group.Members[i])
	}
	return catalogMetaBenchmarkFixture{
		authority: authority,
		command:   command,
		status:    status,
		proof:     CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest},
		request:   request,
		metadata: raftentry.RequestMetadataV1{
			ClusterRouteKnown:         true,
			ClusterRouteDatabase:      ref.Database,
			ClusterRouteCatalog:       ref.Catalog,
			ClusterRouteCollection:    ref.Collection,
			ClusterRouteShape:         string(decision.Shape),
			ClusterRouteGroupID:       string(decision.GroupID()),
			ClusterRouteMembers:       members,
			ClusterRouteLeaderHint:    string(decision.LeaderHint()),
			ClusterRoutePlacementMode: string(decision.PlacementMode),
			ClusterRouteKey:           string(decision.RouteKey),
			CatalogMetaEpoch:          status.Epoch,
			CatalogMetaDigest:         status.Digest,
		},
	}
}

func catalogMetaMaximumPlacementBenchmarkCatalog() CatalogV1 {
	placements := make([]CollectionPlacementV1, MaxCatalogMetaPlacementsV1)
	for i := range placements {
		placements[i] = CollectionPlacementV1{
			Collection: CollectionRefV1{
				Database:   DefaultDatabase,
				Catalog:    DefaultCatalog,
				Collection: fmt.Sprintf("c%04d", i),
			},
			Mode:    PlacementModeCollectionV1,
			GroupID: "group-a",
		}
	}
	return CatalogV1{
		Features: DefaultFeatureSet(),
		Groups: []GroupV1{{
			ID:         "group-a",
			Members:    []raftcluster.NodeID{"node-a", "node-b", "node-c"},
			LeaderHint: "node-a",
		}},
		Placements: placements,
	}
}
