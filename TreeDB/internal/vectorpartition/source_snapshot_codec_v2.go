package vectorpartition

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

const sourceSnapshotCheckpointBytesV2 = 4 + sha256.Size + 8 + 64*sha256.Size + sha256.Size

// MarshalCheckpoint returns bounded import progress. It is not a certificate:
// the importing collection must commit these bytes with the corresponding rows
// in one durable publication. A caller-supplied checkpoint grants no authority.
func (a SourceSnapshotAccumulatorV2) MarshalCheckpoint() ([]byte, error) {
	if a.header == ([sha256.Size]byte{}) || a.count > a.snapshot.ChunkCount() {
		return nil, fmt.Errorf("%w: invalid accumulator checkpoint", ErrInvalidSourceSnapshotV2)
	}
	out := make([]byte, sourceSnapshotCheckpointBytesV2)
	copy(out, "SAC2")
	copy(out[4:], a.header[:])
	binary.BigEndian.PutUint64(out[4+sha256.Size:], a.count)
	for i, digest := range a.levels {
		if (a.count&(uint64(1)<<i) == 0) != (digest == ([sha256.Size]byte{})) {
			return nil, fmt.Errorf("%w: invalid accumulator occupancy", ErrInvalidSourceSnapshotV2)
		}
		copy(out[4+sha256.Size+8+i*sha256.Size:], digest[:])
	}
	digest := sourceSnapshotCheckpointDigestV2(out[:len(out)-sha256.Size])
	copy(out[len(out)-sha256.Size:], digest[:])
	return out, nil
}

// RestoreSourceSnapshotAccumulatorV2 restores only a checkpoint read from the
// importing collection's durable system root. Its checksum detects corruption,
// not authority; never accept progress independently supplied by an importer.
func RestoreSourceSnapshotAccumulatorV2(s SourceSnapshotV2, checkpoint []byte) (SourceSnapshotAccumulatorV2, error) {
	a, err := NewSourceSnapshotAccumulatorV2(s)
	if err != nil {
		return a, err
	}
	if len(checkpoint) != sourceSnapshotCheckpointBytesV2 || string(checkpoint[:4]) != "SAC2" || !bytes.Equal(checkpoint[4:4+sha256.Size], a.header[:]) {
		return SourceSnapshotAccumulatorV2{}, fmt.Errorf("%w: checkpoint identity or length", ErrInvalidSourceSnapshotV2)
	}
	digest := sourceSnapshotCheckpointDigestV2(checkpoint[:len(checkpoint)-sha256.Size])
	if !bytes.Equal(checkpoint[len(checkpoint)-sha256.Size:], digest[:]) {
		return SourceSnapshotAccumulatorV2{}, fmt.Errorf("%w: checkpoint checksum", ErrInvalidSourceSnapshotV2)
	}
	a.count = binary.BigEndian.Uint64(checkpoint[4+sha256.Size:])
	if a.count > s.ChunkCount() {
		return SourceSnapshotAccumulatorV2{}, fmt.Errorf("%w: checkpoint progress", ErrInvalidSourceSnapshotV2)
	}
	for i := range a.levels {
		copy(a.levels[i][:], checkpoint[4+sha256.Size+8+i*sha256.Size:])
		if (a.count&(uint64(1)<<i) == 0) != (a.levels[i] == ([sha256.Size]byte{})) {
			return SourceSnapshotAccumulatorV2{}, fmt.Errorf("%w: checkpoint occupancy", ErrInvalidSourceSnapshotV2)
		}
	}
	return a, nil
}

func sourceSnapshotCheckpointDigestV2(raw []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-snapshot/checkpoint/v2\x00"))
	_, _ = h.Write(raw)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

// ImportedChunks is progress within this immutable snapshot, not a document
// revision and not a count that may be used to allocate global metadata.
func (a SourceSnapshotAccumulatorV2) ImportedChunks() uint64 { return a.count }

// EncodeSourceChunkV2 serializes a complete bounded chunk and its proof. The
// expected digest must be independently admitted before these bytes are served.
func EncodeSourceChunkV2(s SourceSnapshotV2, chunk SourceChunkV2, proof SourceChunkProofV2) ([]byte, error) {
	if err := VerifySourceChunkV2(s, s.Digest, chunk, proof); err != nil {
		return nil, err
	}
	size := 4 + sha256.Size + 8 + 4 + 1 + sha256.Size*len(proof.Siblings)
	for _, row := range chunk.Rows {
		size += 8 + 8 + 4 + len(row.DocumentID) + 1 + 4*len(row.Values)
		if row.LegacyOrigin != nil {
			size += 4 + len(row.LegacyOrigin.CollectionScope) + sha256.Size + 5*8
		}
	}
	if size > MaxSourceChunkBytesV2 {
		return nil, fmt.Errorf("%w: encoded chunk bytes cap", ErrInvalidSourceSnapshotV2)
	}
	out := make([]byte, 0, size)
	out = append(out, "SCK2"...)
	out = append(out, s.Digest[:]...)
	out = binary.BigEndian.AppendUint64(out, chunk.Index)
	out = binary.BigEndian.AppendUint32(out, uint32(len(chunk.Rows)))
	for _, row := range chunk.Rows {
		out = binary.BigEndian.AppendUint64(out, row.LocalOrdinal)
		out = binary.BigEndian.AppendUint64(out, row.DocumentRevision)
		out = binary.BigEndian.AppendUint32(out, uint32(len(row.DocumentID)))
		out = append(out, row.DocumentID...)
		if row.LegacyOrigin == nil {
			out = append(out, 0)
		} else {
			origin := row.LegacyOrigin
			out = append(out, 1)
			out = binary.BigEndian.AppendUint32(out, uint32(len(origin.CollectionScope)))
			out = append(out, origin.CollectionScope...)
			out = append(out, origin.IndexDefinitionDigest[:]...)
			for _, value := range []uint64{origin.Generation, origin.Checksum, origin.SchemaHash, origin.RowCount, origin.Ordinal} {
				out = binary.BigEndian.AppendUint64(out, value)
			}
		}
		for _, value := range row.Values {
			out = binary.LittleEndian.AppendUint32(out, math.Float32bits(value))
		}
	}
	out = append(out, byte(len(proof.Siblings)))
	for _, digest := range proof.Siblings {
		out = append(out, digest[:]...)
	}
	return out, nil
}

// DecodeSourceChunkV2 owns its returned IDs, vectors and proof. Allocation is
// bounded by the encoded chunk and explicit row/dimension limits, never by the
// canonical shard's row count. It verifies actual bytes against expectedDigest
// before returning a chunk; admission of that digest belongs to the caller.
func DecodeSourceChunkV2(s SourceSnapshotV2, expectedDigest [sha256.Size]byte, raw []byte) (SourceChunkV2, SourceChunkProofV2, error) {
	invalid := func(reason string) (SourceChunkV2, SourceChunkProofV2, error) {
		return SourceChunkV2{}, SourceChunkProofV2{}, fmt.Errorf("%w: %s", ErrInvalidSourceSnapshotV2, reason)
	}
	if len(raw) > MaxSourceChunkBytesV2 || len(raw) < 4+sha256.Size+8+4+1 || string(raw[:4]) != "SCK2" || expectedDigest == ([sha256.Size]byte{}) || s.Digest != expectedDigest || !bytes.Equal(raw[4:4+sha256.Size], expectedDigest[:]) {
		return invalid("chunk encoding, cap or expected identity")
	}
	sealed, err := SealSourceSnapshotV2(s, s.MerkleRoot)
	if err != nil || sealed.Digest != expectedDigest {
		return invalid("snapshot identity")
	}
	index := binary.BigEndian.Uint64(raw[4+sha256.Size:])
	rows := binary.BigEndian.Uint32(raw[4+sha256.Size+8:])
	if index >= s.ChunkCount() || uint64(rows) != min(uint64(s.RowsPerChunk), s.RowCount-index*uint64(s.RowsPerChunk)) {
		return invalid("chunk index or row count")
	}
	r := sourceChunkReaderV2{raw: raw[4+sha256.Size+8+4:]}
	vectorBytes := uint64(s.Dimensions)*4
	if uint64(rows)*(8+8+4+1+1+vectorBytes)+1 > uint64(len(r.raw)) {
		return invalid("truncated row payload")
	}
	chunk := SourceChunkV2{Index: index, Rows: make([]SourceRowV2, int(rows))}
	for i := range chunk.Rows {
		row := &chunk.Rows[i]
		row.LocalOrdinal = r.u64()
		row.DocumentRevision = r.u64()
		idLength := r.u32()
		if idLength == 0 || idLength > MaxSourceChunkIDBytesV2 {
			return invalid("ID bytes cap")
		}
		row.DocumentID = bytes.Clone(r.take(int(idLength)))
		flag := r.take(1)
		if len(flag) != 1 || flag[0] > 1 {
			return invalid("legacy source discriminant")
		}
		if flag[0] == 1 {
			length := r.u32()
			if length == 0 || length > 4096 {
				return invalid("legacy source scope cap")
			}
			origin := &SourceOrdinalOriginV2{CollectionScope: string(r.take(int(length)))}
			copy(origin.IndexDefinitionDigest[:], r.take(sha256.Size))
			origin.Generation = r.u64()
			origin.Checksum = r.u64()
			origin.SchemaHash = r.u64()
			origin.RowCount = r.u64()
			origin.Ordinal = r.u64()
			row.LegacyOrigin = origin
		}
		values := r.take(int(vectorBytes))
		if r.failed {
			return invalid("truncated row")
		}
		row.Values = make([]float32, int(s.Dimensions))
		for j := range row.Values {
			row.Values[j] = math.Float32frombits(binary.LittleEndian.Uint32(values[j*4:]))
		}
	}
	count := r.take(1)
	if len(count) != 1 || count[0] > 64 || len(r.raw) != int(count[0])*sha256.Size {
		return invalid("proof length or trailing bytes")
	}
	proof := SourceChunkProofV2{Siblings: make([][sha256.Size]byte, int(count[0]))}
	for i := range proof.Siblings {
		copy(proof.Siblings[i][:], r.take(sha256.Size))
	}
	if err := VerifySourceChunkV2(s, expectedDigest, chunk, proof); err != nil {
		return SourceChunkV2{}, SourceChunkProofV2{}, err
	}
	return chunk, proof, nil
}

type sourceChunkReaderV2 struct {
	raw []byte
	failed bool
}

func (r *sourceChunkReaderV2) take(n int) []byte {
	if r.failed || n < 0 || n > len(r.raw) {
		r.failed = true
		return nil
	}
	out := r.raw[:n]
	r.raw = r.raw[n:]
	return out
}

func (r *sourceChunkReaderV2) u32() uint32 {
	raw := r.take(4)
	if len(raw) != 4 { return 0 }
	return binary.BigEndian.Uint32(raw)
}

func (r *sourceChunkReaderV2) u64() uint64 {
	raw := r.take(8)
	if len(raw) != 8 { return 0 }
	return binary.BigEndian.Uint64(raw)
}
