package raftplacement

import "testing"

func BenchmarkCatalogMetaStatusAndRoute(b *testing.B) {
	a := NewCatalogMetaAuthorityV1()
	if _, err := a.ApplyCommittedCatalogMetaV1(mustCatalogMetaCommand(b, 0, 1, validCatalog()), 1); err != nil {
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

func BenchmarkCatalogMetaEncodeDecode(b *testing.B) {
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
