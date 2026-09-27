package vectorpartition

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
)

func TestSourceSnapshotPersistentNodesAndLeafV2(t *testing.T) {
	for rows := uint64(1); rows <= 257; rows++ {
		s := sourceSnapshotFixtureV2(rows)
		want, levels := sourceSnapshotReferenceTreeV2(t, s)
		acc, err := NewSourceSnapshotAccumulatorV2(s)
		if err != nil {
			t.Fatal(err)
		}
		nodes := make(map[[2]uint64][sha256.Size]byte)
		put := func(batch []SourceSnapshotNodeV2) {
			if len(batch) > 65 {
				t.Fatal("unbounded node batch")
			}
			for _, node := range batch {
				key := [2]uint64{uint64(node.Level), node.Index}
				if old, exists := nodes[key]; exists && old != node.Digest {
					t.Fatal("changed immutable node")
				}
				nodes[key] = node.Digest
			}
		}
		for index := uint64(0); index < s.ChunkCount(); index++ {
			chunk := sourceChunkFixtureV2(s, index)
			leaf, err := EncodeSourceLeafV2(s, chunk)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := SourceChunkDigestV2(s, chunk)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeSourceLeafV2(s, digest, leaf)
			if err != nil || !reflect.DeepEqual(got, chunk) {
				t.Fatalf("immutable leaf: %v", err)
			}
			if _, _, err := DecodeSourceChunkV2(want, want.Digest, leaf); err == nil {
				t.Fatal("local leaf accepted as independently proved serving chunk")
			}
			batch, err := acc.AppendNodesV2(chunk)
			if err != nil {
				t.Fatal(err)
			}
			put(batch)
			checkpoint, err := acc.MarshalCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			acc, err = RestoreSourceSnapshotAccumulatorV2(s, checkpoint)
			if err != nil {
				t.Fatal(err)
			}
		}
		batch, err := acc.FinalProofNodesV2()
		if err != nil {
			t.Fatal(err)
		}
		put(batch)
		for index := uint64(0); index < s.ChunkCount(); index++ {
			reads := 0
			proof, err := ReadSourceChunkProofV2(want, index, func(level uint8, index uint64) ([sha256.Size]byte, error) {
				reads++
				v, ok := nodes[[2]uint64{uint64(level), index}]
				if !ok {
					return v, fmt.Errorf("missing node %d/%d", level, index)
				}
				return v, nil
			})
			if err != nil || !reflect.DeepEqual(proof, sourceSnapshotReferenceProofV2(levels, index)) || reads > 64 {
				t.Fatalf("rows %d chunk %d proof: %v", rows, index, err)
			}
			if err := VerifySourceChunkV2(want, want.Digest, sourceChunkFixtureV2(s, index), proof); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSourceSnapshotLeafRefusesMutationAndTruncationV2(t *testing.T) {
	s := sourceSnapshotFixtureV2(2)
	chunk := sourceChunkFixtureV2(s, 0)
	chunk.Rows[0].LegacyOrigin = &SourceOrdinalOriginV2{CollectionScope: "old/source", IndexDefinitionDigest: s.IndexDefinitionDigest, Generation: 7, Checksum: 8, SchemaHash: 9, RowCount: 10, Ordinal: 6}
	digest, err := SourceChunkDigestV2(s, chunk)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeSourceLeafV2(s, chunk)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		if _, err := DecodeSourceLeafV2(s, digest, raw[:i]); err == nil {
			t.Fatalf("truncation %d", i)
		}
		bad := bytes.Clone(raw)
		bad[i] ^= 1
		if _, err := DecodeSourceLeafV2(s, digest, bad); err == nil {
			t.Fatalf("changed byte %d", i)
		}
	}
	changed := s
	changed.SnapshotRevision++
	if _, err := DecodeSourceLeafV2(changed, digest, raw); err == nil {
		t.Fatal("changed revision accepted")
	}
	got, err := DecodeSourceLeafV2(s, digest, raw)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	if !reflect.DeepEqual(got, chunk) {
		t.Fatal("leaf borrows mutable input")
	}
}
