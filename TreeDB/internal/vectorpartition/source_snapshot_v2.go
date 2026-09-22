package vectorpartition

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"math/bits"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	SourceSnapshotVersionV2 uint32 = 2
	SourceSnapshotEncodingV2 = "float32_le_v2"
	MaxSourceChunkRowsV2 = 256
	MaxSourceChunkBytesV2 = 8 << 20
	MaxSourceChunkIDBytesV2 = 4096
)

var ErrInvalidSourceSnapshotV2 = errors.New("vectorpartition: invalid source snapshot")

// SourceSnapshotV2 describes an immutable canonical source-shard snapshot.
// This value and its self-consistent digest do not grant source authority:
// serving must obtain ExpectedDigest from the canonical source authority and
// bind it to the committed generation before accepting any chunk proof.
type SourceSnapshotV2 struct {
	Version uint32
	CollectionScope string
	ShardID string
	SnapshotRevision uint64
	OrdinalNamespace string
	SourceMapEpoch uint64
	SourceMapDigest [sha256.Size]byte
	SchemaDigest [sha256.Size]byte
	IndexDefinitionDigest [sha256.Size]byte
	Encoding string
	Dimensions uint32
	RowCount uint64
	RowsPerChunk uint32
	MerkleRoot [sha256.Size]byte
	Digest [sha256.Size]byte
}

// SourceOrdinalOriginV2 gives a retained legacy ordinal its original source
// identity. A bare legacy ordinal is never a globally meaningful ID.
type SourceOrdinalOriginV2 struct {
	CollectionScope string
	IndexDefinitionDigest [sha256.Size]byte
	Generation uint64
	Checksum uint64
	SchemaHash uint64
	RowCount uint64
	Ordinal uint64
}

type SourceRowV2 struct {
	LocalOrdinal uint64
	DocumentID []byte
	DocumentRevision uint64
	Values []float32
	LegacyOrigin *SourceOrdinalOriginV2
}

type SourceChunkV2 struct {
	Index uint64
	Rows []SourceRowV2
}

type SourceChunkProofV2 struct {
	Siblings [][sha256.Size]byte
}

func sourceSnapshotTextV2(value string, max int) bool {
	return value != "" && len(value) <= max && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

func (s SourceSnapshotV2) ChunkCount() uint64 {
	if s.RowCount == 0 || s.RowsPerChunk == 0 {
		return 0
	}
	return 1 + (s.RowCount-1)/uint64(s.RowsPerChunk)
}

func (s SourceSnapshotV2) headerDigestV2() ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if s.Version != SourceSnapshotVersionV2 || !sourceSnapshotTextV2(s.CollectionScope, 4096) || !sourceSnapshotTextV2(s.ShardID, 128) || !sourceSnapshotTextV2(s.OrdinalNamespace, 128) || s.SnapshotRevision == 0 || s.SourceMapEpoch == 0 || s.SourceMapDigest == zero || s.SchemaDigest == zero || s.IndexDefinitionDigest == zero || s.Encoding != SourceSnapshotEncodingV2 || s.RowCount == 0 || s.RowsPerChunk == 0 || s.RowsPerChunk > MaxSourceChunkRowsV2 || s.Dimensions == 0 || uint64(s.RowsPerChunk)*uint64(s.Dimensions)*4 > MaxSourceChunkBytesV2 {
		return zero, fmt.Errorf("%w: header or chunk bound", ErrInvalidSourceSnapshotV2)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-snapshot/header/v2\x00"))
	writeSourceSnapshotUintV2(h, uint64(s.Version))
	writeSourceSnapshotStringV2(h, s.CollectionScope)
	writeSourceSnapshotStringV2(h, s.ShardID)
	writeSourceSnapshotUintV2(h, s.SnapshotRevision)
	writeSourceSnapshotStringV2(h, s.OrdinalNamespace)
	writeSourceSnapshotUintV2(h, s.SourceMapEpoch)
	_, _ = h.Write(s.SourceMapDigest[:])
	_, _ = h.Write(s.SchemaDigest[:])
	_, _ = h.Write(s.IndexDefinitionDigest[:])
	writeSourceSnapshotStringV2(h, s.Encoding)
	writeSourceSnapshotUintV2(h, uint64(s.Dimensions))
	writeSourceSnapshotUintV2(h, s.RowCount)
	writeSourceSnapshotUintV2(h, uint64(s.RowsPerChunk))
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(digest[:0]))
	return digest, nil
}

// SealSourceSnapshotV2 creates the content identity after a complete bounded
// import/build. The caller still must commit it through canonical authority.
func SealSourceSnapshotV2(s SourceSnapshotV2, merkleRoot [sha256.Size]byte) (SourceSnapshotV2, error) {
	header, err := s.headerDigestV2()
	if err != nil {
		return SourceSnapshotV2{}, err
	}
	if merkleRoot == ([sha256.Size]byte{}) {
		return SourceSnapshotV2{}, fmt.Errorf("%w: empty Merkle root", ErrInvalidSourceSnapshotV2)
	}
	s.MerkleRoot = merkleRoot
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-snapshot/root/v2\x00"))
	_, _ = h.Write(header[:])
	_, _ = h.Write(merkleRoot[:])
	copy(s.Digest[:], h.Sum(s.Digest[:0]))
	return s, nil
}

// SourceChunkDigestV2 checks actual local IDs/revisions/vector bytes and exact
// chunk completeness before hashing. It never allocates from snapshot row
// count and never treats a document token or local ordinal as document ID.
func SourceChunkDigestV2(s SourceSnapshotV2, chunk SourceChunkV2) ([sha256.Size]byte, error) {
	header, err := s.headerDigestV2()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sourceChunkDigestForHeaderV2(s, header, chunk)
}

func sourceChunkDigestForHeaderV2(s SourceSnapshotV2, header [sha256.Size]byte, chunk SourceChunkV2) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if chunk.Index >= s.ChunkCount() {
		return zero, fmt.Errorf("%w: chunk index", ErrInvalidSourceSnapshotV2)
	}
	start := chunk.Index * uint64(s.RowsPerChunk)
	rows := min(uint64(s.RowsPerChunk), s.RowCount-start)
	if uint64(len(chunk.Rows)) != rows {
		return zero, fmt.Errorf("%w: missing or extra chunk rows", ErrInvalidSourceSnapshotV2)
	}
	var order [MaxSourceChunkRowsV2]int
	bytesUsed := uint64(0)
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-snapshot/chunk/v2\x00"))
	_, _ = h.Write(header[:])
	writeSourceSnapshotUintV2(h, chunk.Index)
	writeSourceSnapshotUintV2(h, rows)
	for i, row := range chunk.Rows {
		if row.LocalOrdinal != start+uint64(i) || row.DocumentRevision == 0 || len(row.DocumentID) == 0 || len(row.DocumentID) > MaxSourceChunkIDBytesV2 || uint64(len(row.Values)) != uint64(s.Dimensions) {
			return zero, fmt.Errorf("%w: row ordinal, revision, ID or dimensions", ErrInvalidSourceSnapshotV2)
		}
		bytesUsed += 32 + uint64(len(row.DocumentID)) + uint64(len(row.Values))*4
		if bytesUsed > MaxSourceChunkBytesV2 {
			return zero, fmt.Errorf("%w: chunk bytes cap", ErrInvalidSourceSnapshotV2)
		}
		order[i] = i
		writeSourceSnapshotUintV2(h, row.LocalOrdinal)
		writeSourceSnapshotUintV2(h, row.DocumentRevision)
		writeSourceSnapshotUintV2(h, uint64(len(row.DocumentID)))
		_, _ = h.Write(row.DocumentID)
		if row.LegacyOrigin == nil {
			writeSourceSnapshotUintV2(h, 0)
		} else {
			origin := row.LegacyOrigin
			if !sourceSnapshotTextV2(origin.CollectionScope, 4096) || origin.IndexDefinitionDigest == zero || origin.Generation == 0 || origin.RowCount == 0 || origin.Ordinal >= origin.RowCount {
				return zero, fmt.Errorf("%w: unbound legacy source ordinal", ErrInvalidSourceSnapshotV2)
			}
			bytesUsed += uint64(len(origin.CollectionScope)) + 80
			if bytesUsed > MaxSourceChunkBytesV2 {
				return zero, fmt.Errorf("%w: source provenance bytes cap", ErrInvalidSourceSnapshotV2)
			}
			writeSourceSnapshotUintV2(h, 1)
			writeSourceSnapshotStringV2(h, origin.CollectionScope)
			_, _ = h.Write(origin.IndexDefinitionDigest[:])
			for _, value := range []uint64{origin.Generation, origin.Checksum, origin.SchemaHash, origin.RowCount, origin.Ordinal} {
				writeSourceSnapshotUintV2(h, value)
			}
		}
		var valueBytes [4]byte
		for _, value := range row.Values {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return zero, fmt.Errorf("%w: nonfinite vector value", ErrInvalidSourceSnapshotV2)
			}
			binary.LittleEndian.PutUint32(valueBytes[:], math.Float32bits(value))
			_, _ = h.Write(valueBytes[:])
		}
	}
	indices := order[:len(chunk.Rows)]
	slices.SortFunc(indices, func(a, b int) int { return bytes.Compare(chunk.Rows[a].DocumentID, chunk.Rows[b].DocumentID) })
	for i := 1; i < len(indices); i++ {
		if bytes.Equal(chunk.Rows[indices[i-1]].DocumentID, chunk.Rows[indices[i]].DocumentID) {
			return zero, fmt.Errorf("%w: duplicate exact document ID", ErrInvalidSourceSnapshotV2)
		}
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(digest[:0]))
	return digest, nil
}

// VerifySourceChunkV2 verifies a bounded proof against an independently
// admitted snapshot digest. It does not retain caller memory. The serving
// reader must keep verified asset bytes immutable/pinned for their lifetime.
// Separately verify complete domain membership: chunk inclusion does not prove
// that the caller supplied every row required by an ANN domain.
func VerifySourceChunkV2(s SourceSnapshotV2, expectedDigest [sha256.Size]byte, chunk SourceChunkV2, proof SourceChunkProofV2) error {
	if expectedDigest == ([sha256.Size]byte{}) || s.Digest != expectedDigest {
		return fmt.Errorf("%w: unbound snapshot authority", ErrInvalidSourceSnapshotV2)
	}
	sealed, err := SealSourceSnapshotV2(s, s.MerkleRoot)
	if err != nil || sealed.Digest != expectedDigest {
		return fmt.Errorf("%w: snapshot identity changed", ErrInvalidSourceSnapshotV2)
	}
	height := bits.Len64(s.ChunkCount()-1)
	if len(proof.Siblings) != height {
		return fmt.Errorf("%w: proof height", ErrInvalidSourceSnapshotV2)
	}
	digest, err := SourceChunkDigestV2(s, chunk)
	if err != nil {
		return err
	}
	for level, sibling := range proof.Siblings {
		if chunk.Index&(uint64(1)<<level) == 0 {
			digest = sourceSnapshotNodeDigestV2(digest, sibling)
		} else {
			digest = sourceSnapshotNodeDigestV2(sibling, digest)
		}
	}
	if digest != s.MerkleRoot {
		return fmt.Errorf("%w: chunk bytes, identity or proof root", ErrInvalidSourceSnapshotV2)
	}
	return nil
}

// SourceSnapshotAccumulatorV2 retains at most 64 hashes, regardless of the
// number of source chunks. Copy the value before an import attempt and persist
// its state atomically with rows/progress before accepting the next chunk.
// The durable importer must enforce exact-ID uniqueness across chunks; this
// accumulator deliberately does not retain a global ID map or grant authority.
type SourceSnapshotAccumulatorV2 struct {
	snapshot SourceSnapshotV2
	header [sha256.Size]byte
	count uint64
	levels [64][sha256.Size]byte
}

func NewSourceSnapshotAccumulatorV2(s SourceSnapshotV2) (SourceSnapshotAccumulatorV2, error) {
	header, err := s.headerDigestV2()
	if err != nil {
		return SourceSnapshotAccumulatorV2{}, err
	}
	return SourceSnapshotAccumulatorV2{snapshot: s, header: header}, nil
}

func (a *SourceSnapshotAccumulatorV2) Append(chunk SourceChunkV2) error {
	if a == nil || a.header == ([sha256.Size]byte{}) || chunk.Index != a.count {
		return fmt.Errorf("%w: missing, duplicate or reordered import chunk", ErrInvalidSourceSnapshotV2)
	}
	digest, err := sourceChunkDigestForHeaderV2(a.snapshot, a.header, chunk)
	if err != nil {
		return err
	}
	level := 0
	for n := a.count; n&1 != 0; n >>= 1 {
		digest = sourceSnapshotNodeDigestV2(a.levels[level], digest)
		a.levels[level] = [sha256.Size]byte{}
		level++
	}
	a.levels[level] = digest
	a.count++
	return nil
}

func (a SourceSnapshotAccumulatorV2) Finish() (SourceSnapshotV2, error) {
	if a.header == ([sha256.Size]byte{}) || a.count == 0 || a.count != a.snapshot.ChunkCount() {
		return SourceSnapshotV2{}, fmt.Errorf("%w: incomplete source snapshot", ErrInvalidSourceSnapshotV2)
	}
	height := bits.Len64(a.count-1)
	var root [sha256.Size]byte
	if a.count&(a.count-1) == 0 {
		root = a.levels[height]
	} else {
		empty := sourceSnapshotEmptyDigestV2(a.header)
		root = empty
		for level := 0; level < height; level++ {
			if a.count&(uint64(1)<<level) != 0 {
				root = sourceSnapshotNodeDigestV2(a.levels[level], root)
			} else {
				root = sourceSnapshotNodeDigestV2(root, empty)
			}
			empty = sourceSnapshotNodeDigestV2(empty, empty)
		}
	}
	sealed, err := SealSourceSnapshotV2(a.snapshot, root)
	if err != nil {
		return SourceSnapshotV2{}, err
	}
	if a.snapshot.Digest != ([sha256.Size]byte{}) && sealed.Digest != a.snapshot.Digest {
		return SourceSnapshotV2{}, fmt.Errorf("%w: completed snapshot differs from input identity", ErrInvalidSourceSnapshotV2)
	}
	return sealed, nil
}

func sourceSnapshotNodeDigestV2(left, right [sha256.Size]byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-snapshot/node/v2\x00"))
	_, _ = h.Write(left[:])
	_, _ = h.Write(right[:])
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

func sourceSnapshotEmptyDigestV2(header [sha256.Size]byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-snapshot/empty/v2\x00"))
	_, _ = h.Write(header[:])
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

func writeSourceSnapshotStringV2(h hash.Hash, value string) {
	writeSourceSnapshotUintV2(h, uint64(len(value)))
	_, _ = h.Write([]byte(value))
}

func writeSourceSnapshotUintV2(h hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = h.Write(encoded[:])
}
