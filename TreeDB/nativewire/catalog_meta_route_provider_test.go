package nativewire

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestCatalogMetaClusterRouteProviderRejectsStaleProof(t *testing.T) {
	authority := raftplacement.NewCatalogMetaAuthorityV1()
	record, err := raftplacement.NewCatalogMetaRecordV1(1, raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: raftplacement.DefaultDatabase, Catalog: raftplacement.DefaultCatalog, Collection: "users"}, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.ApplyCommittedCatalogMetaV1(command, 1); err != nil {
		t.Fatal(err)
	}
	p, err := NewCatalogMetaClusterRouteProvider(authority, func(context.Context) (raftplacement.CatalogProofV1, error) {
		return raftplacement.CatalogProofV1{Epoch: 2, Digest: record.Digest}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.ClusterRoute(context.Background(), ClusterRouteRequest{Database: raftplacement.DefaultDatabase, Catalog: raftplacement.DefaultCatalog, Collection: "users", Shape: ClusterRouteShapeCollection})
	if !errors.Is(err, raftplacement.ErrCatalogMetaStaleEpoch) {
		t.Fatalf("route err=%v", err)
	}
}
