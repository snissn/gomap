package raftplacement

import (
	"errors"
	"strings"
	"testing"
)

func sourceShardMapFixtureV2(t testing.TB) (ResolvedCatalogV1, SourceShardMapV2) {
	t.Helper()
	catalog, err := Validate(validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	m, err := catalog.CanonicalSourceShardMapV2(SourceShardMapV2{
		Format: SourceShardMapFormatV2,
		Collection: CollectionRefV1{Database: "default", Catalog: "default", Collection: "documents"},
		Epoch: 7,
		TokenAlgorithm: DocumentIDTokenAlgorithmV2,
		Shards: []SourceShardV2{
			{ShardID: "source-a", GroupID: "group-a", Start: 0, End: (1 << 63) - 1},
			{ShardID: "source-b", GroupID: "group-b", Start: 1 << 63, End: ^uint64(0)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog, m
}

func TestSourceShardMapDocumentTokenIdentityV2(t *testing.T) {
	for _, tc := range []struct { id []byte; want uint64 }{
		{[]byte("a"), 0x50bef51bd7c90063},
		{[]byte("document-123"), 0xe02774b2a304fc4a},
		{[]byte{0, 255}, 0xe04f161b78878bbb},
	} {
		got, err := DocumentIDTokenV2(tc.id)
		if err != nil || got != tc.want {
			t.Fatalf("token(%x)=%x, %v; want %x", tc.id, got, err, tc.want)
		}
	}
	for _, id := range [][]byte{nil, make([]byte, MaxSourceDocumentIDBytesV2+1)} {
		if _, err := DocumentIDTokenV2(id); !errors.Is(err, ErrInvalidSourceShardMapV2) {
			t.Fatalf("ID length %d accepted: %v", len(id), err)
		}
	}
}

func TestSourceShardMapBoundImmutableLookupV2(t *testing.T) {
	catalog, input := sourceShardMapFixtureV2(t)
	m, err := catalog.ValidateSourceShardMapV2(input)
	if err != nil {
		t.Fatal(err)
	}
	digest := input.Digest
	input.Epoch++
	input.Shards[0].GroupID = "group-b"
	input.Shards[0].ShardID = "changed"
	owner, err := m.ResolveDocumentID([]byte("a"))
	if err != nil || owner.ShardID != "source-a" || owner.GroupID != "group-a" || m.Epoch() != 7 || m.Digest() != digest || m.Collection().Collection != "documents" {
		t.Fatalf("caller changed resolved authority: owner=%+v epoch=%d digest=%s err=%v", owner, m.Epoch(), m.Digest(), err)
	}
	if err := m.ValidateImportIDs("source-a", [][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateImportIDs("source-a", [][]byte{[]byte("document-123")}); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("wrong source shard accepted: %v", err)
	}
	if err := m.ValidateImportIDs("source-a", [][]byte{[]byte("a"), []byte("a")}); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("duplicate exact ID accepted: %v", err)
	}
	if err := (ResolvedSourceShardMapV2{}).ValidateImportIDs("source-a", [][]byte{[]byte("a")}); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("zero map accepted: %v", err)
	}
}

func TestSourceShardMapRefusesIdentityCoverageDriftV2(t *testing.T) {
	for _, tc := range []struct { name string; mutate func(*SourceShardMapV2) }{
		{"epoch", func(m *SourceShardMapV2) { m.Epoch = 0 }},
		{"changed epoch", func(m *SourceShardMapV2) { m.Epoch++ }},
		{"algorithm", func(m *SourceShardMapV2) { m.TokenAlgorithm = "ann_partition" }},
		{"format", func(m *SourceShardMapV2) { m.Format = "source_v1" }},
		{"collection", func(m *SourceShardMapV2) { m.Collection.Collection = "other" }},
		{"digest", func(m *SourceShardMapV2) { m.Digest = strings.Repeat("0", 64) }},
		{"gap", func(m *SourceShardMapV2) { m.Shards[1].Start++ }},
		{"overlap", func(m *SourceShardMapV2) { m.Shards[1].Start-- }},
		{"missing first", func(m *SourceShardMapV2) { m.Shards[0].Start++ }},
		{"missing last", func(m *SourceShardMapV2) { m.Shards[1].End-- }},
		{"duplicate shard", func(m *SourceShardMapV2) { m.Shards[1].ShardID = m.Shards[0].ShardID }},
		{"unknown group", func(m *SourceShardMapV2) { m.Shards[0].GroupID = "missing" }},
		{"reordered", func(m *SourceShardMapV2) { m.Shards[0], m.Shards[1] = m.Shards[1], m.Shards[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, input := sourceShardMapFixtureV2(t)
			tc.mutate(&input)
			if _, err := catalog.ValidateSourceShardMapV2(input); !errors.Is(err, ErrInvalidSourceShardMapV2) {
				t.Fatalf("invalid map accepted: %v", err)
			}
		})
	}
}

func TestSourceShardMapSameTokenRangeDoesNotAliasIDsV2(t *testing.T) {
	catalog, input := sourceShardMapFixtureV2(t)
	input.Shards = []SourceShardV2{{ShardID: "all", GroupID: "group-a", Start: 0, End: ^uint64(0)}}
	input, err := catalog.CanonicalSourceShardMapV2(input)
	if err != nil {
		t.Fatal(err)
	}
	m, err := catalog.ValidateSourceShardMapV2(input)
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("b"), []byte("a"), []byte("document-123")}
	if err := m.ValidateImportIDs("all", ids); err != nil {
		t.Fatal(err)
	}
	if string(ids[0]) != "b" || string(ids[1]) != "a" {
		t.Fatal("import validation reordered caller input")
	}
	ids[2] = []byte("b")
	if err := m.ValidateImportIDs("all", ids); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("nonadjacent duplicate accepted: %v", err)
	}
}

func BenchmarkSourceShardMapResolveDocumentIDV2(b *testing.B) {
	catalog, input := sourceShardMapFixtureV2(b)
	m, err := catalog.ValidateSourceShardMapV2(input)
	if err != nil {
		b.Fatal(err)
	}
	id := []byte("document-123")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.ResolveDocumentID(id); err != nil {
			b.Fatal(err)
		}
	}
}
