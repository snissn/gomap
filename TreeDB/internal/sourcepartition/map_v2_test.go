package sourcepartition

import (
	"bytes"
	"errors"
	"testing"
)

func sourceMapFixtureV2(t *testing.T) SourceShardMapV2 {
	t.Helper()
	m, err := CanonicalSourceShardMapV2(SourceShardMapV2{Format: SourceShardMapFormatV2, Collection: CollectionRefV2{Database: "default", Catalog: "default", Collection: "documents"}, Epoch: 7, TokenAlgorithm: DocumentIDTokenAlgorithmV2, Shards: []SourceShardV2{{ShardID: "source-a", GroupID: "group-a", Start: 0, End: (1 << 63) - 1}, {ShardID: "source-b", GroupID: "group-b", Start: 1 << 63, End: ^uint64(0)}}})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSourceShardMapCanonicalCodecAndPriorDigestV2(t *testing.T) {
	m := sourceMapFixtureV2(t)
	// Frozen before extraction from raftplacement; exact prior V2 encoding.
	const want = "394447277b9a94741458c9678959e505cc6da38dbf84705ff9fbc1677a5e3caf"
	if m.Digest != want {
		t.Fatalf("changed prior V2 digest: %s", m.Digest)
	}
	raw, err := EncodeSourceShardMapV2(m)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := DecodeSourceShardMapV2(raw)
	if err != nil || resolved.Digest() != want {
		t.Fatalf("canonical map decode: %v", err)
	}
	copy := resolved.Copy()
	copy.Shards[0].GroupID = "changed"
	owner, err := resolved.ResolveDocumentID([]byte("a"))
	if err != nil || owner.GroupID != "group-a" {
		t.Fatalf("copy changed lookup: %+v %v", owner, err)
	}
	if err := resolved.ValidateImportIDs("source-a", [][]byte{[]byte("document-123")}); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("nonowner import: %v", err)
	}
	if err := resolved.ValidateImportIDs("source-a", [][]byte{[]byte("a"), []byte("a")}); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("duplicate exact ID: %v", err)
	}
	for _, changed := range [][]byte{append(bytes.Clone(raw), '\n'), append(bytes.Clone(raw), raw...), bytes.Replace(raw, []byte(`"Epoch":7`), []byte(`"Epoch":7,"Epoch":7`), 1), bytes.Replace(raw, []byte(`"Epoch":7`), []byte(`"Epoch":7,"unknown":1`), 1), bytes.Replace(raw, []byte(`"Epoch":7`), []byte(`"Epoch":8`), 1)} {
		if _, err := DecodeSourceShardMapV2(changed); !errors.Is(err, ErrInvalidSourceShardMapV2) {
			t.Fatalf("noncanonical or changed map accepted: %v", err)
		}
	}
}

func TestSourceShardMapPureCoverageAndBoundsV2(t *testing.T) {
	for _, change := range []func(*SourceShardMapV2){
		func(m *SourceShardMapV2) { m.Shards[0].Start = 1 },
		func(m *SourceShardMapV2) { m.Shards[1].End-- },
		func(m *SourceShardMapV2) { m.Shards[1].Start++ },
		func(m *SourceShardMapV2) { m.Shards[1].Start-- },
		func(m *SourceShardMapV2) { m.Shards[1].ShardID = m.Shards[0].ShardID },
		func(m *SourceShardMapV2) { m.Shards[1].GroupID = "invalid/group" },
		func(m *SourceShardMapV2) { m.Collection.Catalog = "invalid/catalog" },
		func(m *SourceShardMapV2) { m.Epoch = 0 },
	} {
		m := sourceMapFixtureV2(t)
		change(&m)
		if _, err := CanonicalSourceShardMapV2(m); !errors.Is(err, ErrInvalidSourceShardMapV2) {
			t.Fatalf("invalid complete map accepted: %+v %v", m, err)
		}
	}
	if _, err := DecodeSourceShardMapV2(make([]byte, MaxSourceShardMapBytesV2+1)); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("encoded cap: %v", err)
	}
	if _, err := (ResolvedSourceShardMapV2{}).ResolveDocumentID([]byte("a")); !errors.Is(err, ErrInvalidSourceShardMapV2) {
		t.Fatalf("zero lookup: %v", err)
	}
}
