package vectorpartition

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"testing"
)

func sourceSnapshotFixtureV2(rows uint64) SourceSnapshotV2 {
	return SourceSnapshotV2{
		Version: SourceSnapshotVersionV2,
		CollectionScope: "default/default/documents",
		ShardID: "source-a",
		SnapshotRevision: 12,
		OrdinalNamespace: "source-a-native-v2",
		SourceMapEpoch: 9,
		SourceMapDigest: sha256.Sum256([]byte("source-map")),
		SchemaDigest: sha256.Sum256([]byte("source-schema")),
		IndexDefinitionDigest: sha256.Sum256([]byte("embedding-definition")),
		Encoding: SourceSnapshotEncodingV2,
		Dimensions: 3,
		RowCount: rows,
		RowsPerChunk: 2,
	}
}

func sourceChunkFixtureV2(s SourceSnapshotV2, index uint64) SourceChunkV2 {
	start := index * uint64(s.RowsPerChunk)
	rows := min(uint64(s.RowsPerChunk), s.RowCount-start)
	chunk := SourceChunkV2{Index: index, Rows: make([]SourceRowV2, rows)}
	for i := range chunk.Rows {
		ordinal := start+uint64(i)
		chunk.Rows[i] = SourceRowV2{LocalOrdinal: ordinal, DocumentID: []byte(fmt.Sprintf("document-%d", ordinal)), DocumentRevision: 100+ordinal, Values: []float32{float32(ordinal), 1, -2}}
	}
	return chunk
}

// Test-only complete tree provides an independent reference for the bounded
// streaming accumulator and generates proof paths for small fixtures.
func sourceSnapshotReferenceTreeV2(t testing.TB, s SourceSnapshotV2) (SourceSnapshotV2, [][][sha256.Size]byte) {
	t.Helper()
	header, err := s.headerDigestV2()
	if err != nil {
		t.Fatal(err)
	}
	size := 1
	for uint64(size) < s.ChunkCount() {
		size *= 2
	}
	leaves := make([][sha256.Size]byte, size)
	for i := range leaves {
		if uint64(i) < s.ChunkCount() {
			leaves[i], err = SourceChunkDigestV2(s, sourceChunkFixtureV2(s, uint64(i)))
			if err != nil {
				t.Fatal(err)
			}
		} else {
			leaves[i] = sourceSnapshotEmptyDigestV2(header)
		}
	}
	levels := [][][sha256.Size]byte{leaves}
	for len(levels[len(levels)-1]) > 1 {
		previous := levels[len(levels)-1]
		next := make([][sha256.Size]byte, len(previous)/2)
		for i := range next {
			next[i] = sourceSnapshotNodeDigestV2(previous[2*i], previous[2*i+1])
		}
		levels = append(levels, next)
	}
	sealed, err := SealSourceSnapshotV2(s, levels[len(levels)-1][0])
	if err != nil {
		t.Fatal(err)
	}
	return sealed, levels
}

func sourceSnapshotReferenceProofV2(levels [][][sha256.Size]byte, index uint64) SourceChunkProofV2 {
	proof := SourceChunkProofV2{Siblings: make([][sha256.Size]byte, len(levels)-1)}
	for level := range proof.Siblings {
		proof.Siblings[level] = levels[level][index^1]
		index /= 2
	}
	return proof
}

func TestSourceSnapshotBoundedAccumulatorAndProofV2(t *testing.T) {
	for _, rows := range []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 31, 32, 33, 127} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			s := sourceSnapshotFixtureV2(rows)
			want, levels := sourceSnapshotReferenceTreeV2(t, s)
			acc, err := NewSourceSnapshotAccumulatorV2(s)
			if err != nil {
				t.Fatal(err)
			}
			for index := uint64(0); index < s.ChunkCount(); index++ {
				chunk := sourceChunkFixtureV2(s, index)
				if err := acc.Append(chunk); err != nil {
					t.Fatal(err)
				}
				if err := VerifySourceChunkV2(want, want.Digest, chunk, sourceSnapshotReferenceProofV2(levels, index)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := acc.Finish()
			if err != nil || got != want {
				t.Fatalf("streaming tree differs: got=%x want=%x err=%v", got.Digest, want.Digest, err)
			}
		})
	}
}

func TestSourceSnapshotRefusesCorruptOrIncompleteChunkV2(t *testing.T) {
	s, levels := sourceSnapshotReferenceTreeV2(t, sourceSnapshotFixtureV2(7))
	for _, tc := range []struct { name string; change func(*SourceSnapshotV2, *SourceChunkV2, *SourceChunkProofV2) }{
		{"snapshot revision", func(s *SourceSnapshotV2, _ *SourceChunkV2, _ *SourceChunkProofV2) { s.SnapshotRevision++ }},
		{"shard", func(s *SourceSnapshotV2, _ *SourceChunkV2, _ *SourceChunkProofV2) { s.ShardID = "source-b" }},
		{"namespace", func(s *SourceSnapshotV2, _ *SourceChunkV2, _ *SourceChunkProofV2) { s.OrdinalNamespace = "wrong" }},
		{"schema", func(s *SourceSnapshotV2, _ *SourceChunkV2, _ *SourceChunkProofV2) { s.SchemaDigest[0] ^= 1 }},
		{"encoding", func(s *SourceSnapshotV2, _ *SourceChunkV2, _ *SourceChunkProofV2) { s.Encoding = "other" }},
		{"row revision", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[0].DocumentRevision++ }},
		{"zero row revision", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[0].DocumentRevision = 0 }},
		{"ID", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[0].DocumentID = []byte("changed") }},
		{"duplicate ID", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[1].DocumentID = c.Rows[0].DocumentID }},
		{"vector", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[0].Values[0]++ }},
		{"nonfinite", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[0].Values[0] = float32(math.NaN()) }},
		{"missing", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows = c.Rows[:1] }},
		{"reordered", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Rows[0], c.Rows[1] = c.Rows[1], c.Rows[0] }},
		{"wrong chunk", func(_ *SourceSnapshotV2, c *SourceChunkV2, _ *SourceChunkProofV2) { c.Index++ }},
		{"proof height", func(_ *SourceSnapshotV2, _ *SourceChunkV2, p *SourceChunkProofV2) { p.Siblings = p.Siblings[:1] }},
		{"proof root", func(_ *SourceSnapshotV2, _ *SourceChunkV2, p *SourceChunkProofV2) { p.Siblings[0][0] ^= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := s
			chunk := sourceChunkFixtureV2(s, 0)
			proof := sourceSnapshotReferenceProofV2(levels, 0)
			tc.change(&changed, &chunk, &proof)
			if err := VerifySourceChunkV2(changed, s.Digest, chunk, proof); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
				t.Fatalf("corrupt/incomplete chunk accepted: %v", err)
			}
		})
	}
	if err := VerifySourceChunkV2(s, [sha256.Size]byte{}, sourceChunkFixtureV2(s, 0), sourceSnapshotReferenceProofV2(levels, 0)); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("unbound root accepted: %v", err)
	}
}

func TestSourceSnapshotAccumulatorRefusesReplayGapAndChangedInputV2(t *testing.T) {
	s := sourceSnapshotFixtureV2(5)
	acc, err := NewSourceSnapshotAccumulatorV2(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := acc.Append(sourceChunkFixtureV2(s, 1)); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("gap accepted: %v", err)
	}
	if err := acc.Append(sourceChunkFixtureV2(s, 0)); err != nil {
		t.Fatal(err)
	}
	before := acc
	if err := acc.Append(sourceChunkFixtureV2(s, 0)); !errors.Is(err, ErrInvalidSourceSnapshotV2) || acc != before {
		t.Fatalf("duplicate changed accumulator: %v", err)
	}
	if _, err := acc.Finish(); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("incomplete source sealed: %v", err)
	}
	sealed, _ := sourceSnapshotReferenceTreeV2(t, s)
	acc, err = NewSourceSnapshotAccumulatorV2(sealed)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(0); index < sealed.ChunkCount(); index++ {
		chunk := sourceChunkFixtureV2(sealed, index)
		chunk.Rows[0].DocumentRevision++
		if err := acc.Append(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := acc.Finish(); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("changed immutable input sealed: %v", err)
	}
}

func TestSourceSnapshotLegacyOrdinalProvenanceV2(t *testing.T) {
	s := sourceSnapshotFixtureV2(1)
	chunk := sourceChunkFixtureV2(s, 0)
	chunk.Rows[0].LegacyOrigin = &SourceOrdinalOriginV2{CollectionScope: "original/catalog/documents", IndexDefinitionDigest: s.IndexDefinitionDigest, Generation: 33, Checksum: 44, SchemaHash: 55, RowCount: 99, Ordinal: 87}
	digest, err := SourceChunkDigestV2(s, chunk)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := SealSourceSnapshotV2(s, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySourceChunkV2(sealed, sealed.Digest, chunk, SourceChunkProofV2{}); err != nil {
		t.Fatal(err)
	}
	chunk.Rows[0].LegacyOrigin.CollectionScope = "another/catalog/documents"
	if err := VerifySourceChunkV2(sealed, sealed.Digest, chunk, SourceChunkProofV2{}); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("same bare ordinal rebound to another source: %v", err)
	}
}

func BenchmarkSourceSnapshotChunkVerificationV2(b *testing.B) {
	s, levels := sourceSnapshotReferenceTreeV2(b, sourceSnapshotFixtureV2(127))
	chunk := sourceChunkFixtureV2(s, 1)
	proof := sourceSnapshotReferenceProofV2(levels, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := VerifySourceChunkV2(s, s.Digest, chunk, proof); err != nil {
			b.Fatal(err)
		}
	}
}
