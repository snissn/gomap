package vectorpartition

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"testing"
)

func TestSourceSnapshotCheckpointResumeV2(t *testing.T) {
	s, _ := sourceSnapshotReferenceTreeV2(t, sourceSnapshotFixtureV2(33))
	acc, err := NewSourceSnapshotAccumulatorV2(s)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(0); index <= s.ChunkCount(); index++ {
		raw, err := acc.MarshalCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != sourceSnapshotCheckpointBytesV2 {
			t.Fatalf("unbounded checkpoint: %d", len(raw))
		}
		restored, err := RestoreSourceSnapshotAccumulatorV2(s, raw)
		if err != nil || restored != acc || restored.ImportedChunks() != index {
			t.Fatalf("resume at %d: %v", index, err)
		}
		acc = restored
		if index < s.ChunkCount() {
			if err := acc.Append(sourceChunkFixtureV2(s, index)); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, err := acc.Finish()
	if err != nil || got != s {
		t.Fatalf("resumed source differs: %v", err)
	}
	raw, err := acc.MarshalCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		changed := bytes.Clone(raw)
		changed[i] ^= 1
		if _, err := RestoreSourceSnapshotAccumulatorV2(s, changed); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
			t.Fatalf("corrupt byte %d accepted: %v", i, err)
		}
	}
	for _, length := range []int{0, 3, 4, len(raw) - 1} {
		if _, err := RestoreSourceSnapshotAccumulatorV2(s, raw[:length]); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
			t.Fatalf("truncated checkpoint %d: %v", length, err)
		}
	}
	s.SnapshotRevision++
	if _, err := RestoreSourceSnapshotAccumulatorV2(s, raw); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("different source revision resumed: %v", err)
	}
}

func TestSourceSnapshotChunkCodecV2(t *testing.T) {
	s, levels := sourceSnapshotReferenceTreeV2(t, sourceSnapshotFixtureV2(7))
	for index := uint64(0); index < s.ChunkCount(); index++ {
		chunk, proof := sourceChunkFixtureV2(s, index), sourceSnapshotReferenceProofV2(levels, index)
		raw, err := EncodeSourceChunkV2(s, chunk, proof)
		if err != nil {
			t.Fatal(err)
		}
		got, gotProof, err := DecodeSourceChunkV2(s, s.Digest, raw)
		if err != nil || !reflect.DeepEqual(got, chunk) || !reflect.DeepEqual(gotProof, proof) {
			t.Fatalf("chunk %d round trip: %v", index, err)
		}
		reencoded, err := EncodeSourceChunkV2(s, got, gotProof)
		if err != nil || !bytes.Equal(raw, reencoded) {
			t.Fatalf("noncanonical reencoding: %v", err)
		}
		for length := 0; length < len(raw); length++ {
			if _, _, err := DecodeSourceChunkV2(s, s.Digest, raw[:length]); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
				t.Fatalf("chunk %d truncated %d accepted: %v", index, length, err)
			}
		}
		for offset := range raw {
			changed := bytes.Clone(raw)
			changed[offset] ^= 1
			if _, _, err := DecodeSourceChunkV2(s, s.Digest, changed); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
				t.Fatalf("chunk %d corrupt %d accepted: %v", index, offset, err)
			}
		}
		if _, _, err := DecodeSourceChunkV2(s, s.Digest, append(bytes.Clone(raw), 0)); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
			t.Fatalf("trailing bytes accepted: %v", err)
		}
		clear(raw)
		if !reflect.DeepEqual(got, chunk) || !reflect.DeepEqual(gotProof, proof) {
			t.Fatal("decoded rows borrow mutable encoded storage")
		}
	}
}

func TestSourceSnapshotChunkCodecLegacyOriginV2(t *testing.T) {
	s := sourceSnapshotFixtureV2(1)
	chunk := sourceChunkFixtureV2(s, 0)
	chunk.Rows[0].LegacyOrigin = &SourceOrdinalOriginV2{CollectionScope: "original/source", IndexDefinitionDigest: s.IndexDefinitionDigest, Generation: 3, Checksum: 4, SchemaHash: 5, RowCount: 6, Ordinal: 2}
	digest, err := SourceChunkDigestV2(s, chunk)
	if err != nil {
		t.Fatal(err)
	}
	s, err = SealSourceSnapshotV2(s, digest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeSourceChunkV2(s, chunk, SourceChunkProofV2{})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := DecodeSourceChunkV2(s, s.Digest, raw)
	if err != nil || !reflect.DeepEqual(got, chunk) {
		t.Fatalf("legacy provenance round trip: %v", err)
	}
	for offset := range raw {
		changed := bytes.Clone(raw)
		changed[offset] ^= 1
		if _, _, err := DecodeSourceChunkV2(s, s.Digest, changed); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
			t.Fatalf("corrupt legacy provenance byte %d accepted: %v", offset, err)
		}
	}
}

// Construct one valid proof fixture without allocating other snapshot rows.
// This creates a test identity, not a source-authority admission certificate.
func sourceSnapshotSparseProofFixtureV2(t testing.TB, rows uint64) (SourceSnapshotV2, SourceChunkV2, SourceChunkProofV2) {
	t.Helper()
	s := sourceSnapshotFixtureV2(rows)
	chunk := sourceChunkFixtureV2(s, 0)
	digest, err := SourceChunkDigestV2(s, chunk)
	if err != nil {
		t.Fatal(err)
	}
	proof := SourceChunkProofV2{Siblings: make([][sha256.Size]byte, bits.Len64(s.ChunkCount()-1))}
	for level := range proof.Siblings {
		proof.Siblings[level] = sha256.Sum256([]byte(fmt.Sprintf("test sibling %d", level)))
		digest = sourceSnapshotNodeDigestV2(digest, proof.Siblings[level])
	}
	s, err = SealSourceSnapshotV2(s, digest)
	if err != nil {
		t.Fatal(err)
	}
	return s, chunk, proof
}

func TestSourceSnapshotChunkDecodeBoundsV2(t *testing.T) {
	for _, rows := range []uint64{4, math.MaxUint64} {
		s, chunk, proof := sourceSnapshotSparseProofFixtureV2(t, rows)
		raw, err := EncodeSourceChunkV2(s, chunk, proof)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := DecodeSourceChunkV2(s, s.Digest, raw); err != nil {
			t.Fatalf("global row count %d: %v", rows, err)
		}
		changed := bytes.Clone(raw)
		binary.BigEndian.PutUint32(changed[4+sha256.Size+8:], math.MaxUint32)
		if _, _, err := DecodeSourceChunkV2(s, s.Digest, changed); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
			t.Fatalf("unbounded chunk row count: %v", err)
		}
		changed = bytes.Clone(raw)
		binary.BigEndian.PutUint32(changed[4+sha256.Size+8+4+8+8:], math.MaxUint32)
		if _, _, err := DecodeSourceChunkV2(s, s.Digest, changed); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
			t.Fatalf("unbounded ID length: %v", err)
		}
	}
	if _, _, err := DecodeSourceChunkV2(SourceSnapshotV2{}, [sha256.Size]byte{}, make([]byte, MaxSourceChunkBytesV2+1)); !errors.Is(err, ErrInvalidSourceSnapshotV2) {
		t.Fatalf("unbounded encoded bytes: %v", err)
	}
}

func BenchmarkSourceSnapshotChunkDecodeV2(b *testing.B) {
	for _, rows := range []uint64{4, math.MaxUint64} {
		b.Run(fmt.Sprintf("global_rows_%d", rows), func(b *testing.B) {
			s, chunk, proof := sourceSnapshotSparseProofFixtureV2(b, rows)
			raw, err := EncodeSourceChunkV2(s, chunk, proof)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := DecodeSourceChunkV2(s, s.Digest, raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
