package vectorpartition

import (
	"crypto/sha256"
	"fmt"
	"math/bits"
)

// SourceSnapshotNodeV2 identifies one immutable subtree in the canonical
// source tree. Level zero is a chunk leaf; Index is local to that level.
// Nodes and source bytes must share the durable import publication boundary.
type SourceSnapshotNodeV2 struct {
	Level  uint8
	Index  uint64
	Digest [sha256.Size]byte
}

// AppendNodesV2 advances the same bounded accumulator as Append and returns
// only newly completed nodes. Failure leaves the accumulator unchanged.
func (a *SourceSnapshotAccumulatorV2) AppendNodesV2(chunk SourceChunkV2) ([]SourceSnapshotNodeV2, error) {
	if a == nil || a.header == ([sha256.Size]byte{}) || chunk.Index != a.count {
		return nil, fmt.Errorf("%w: missing, duplicate or reordered import chunk", ErrInvalidSourceSnapshotV2)
	}
	digest, err := sourceChunkDigestForHeaderV2(a.snapshot, a.header, chunk)
	if err != nil {
		return nil, err
	}
	nodes := make([]SourceSnapshotNodeV2, 0, 1+bits.TrailingZeros64(^a.count))
	nodes = append(nodes, SourceSnapshotNodeV2{Index: a.count, Digest: digest})
	next := *a
	level := 0
	for n := next.count; n&1 != 0; n >>= 1 {
		digest = sourceSnapshotNodeDigestV2(next.levels[level], digest)
		next.levels[level] = [sha256.Size]byte{}
		level++
		nodes = append(nodes, SourceSnapshotNodeV2{Level: uint8(level), Index: next.count >> level, Digest: digest})
	}
	next.levels[level] = digest
	next.count++
	*a = next
	return nodes, nil
}

// FinalProofNodesV2 returns the bounded right-edge nodes needed when the
// complete snapshot has a non-power-of-two chunk count. Empty subtrees need
// no records; readers derive their canonical digest from the source header.
func (a SourceSnapshotAccumulatorV2) FinalProofNodesV2() ([]SourceSnapshotNodeV2, error) {
	if _, err := a.Finish(); err != nil {
		return nil, err
	}
	if a.count&(a.count-1) == 0 {
		return nil, nil
	}
	height := bits.Len64(a.count - 1)
	empty := sourceSnapshotEmptyDigestV2(a.header)
	root := empty
	nodes := make([]SourceSnapshotNodeV2, 0, height)
	for level := 0; level < height; level++ {
		if a.count&(uint64(1)<<level) != 0 {
			root = sourceSnapshotNodeDigestV2(a.levels[level], root)
		} else {
			root = sourceSnapshotNodeDigestV2(root, empty)
		}
		index := a.count >> (level + 1)
		// If this right-edge subtree is wholly empty, its digest is derived.
		if index<<(level+1) < a.count {
			nodes = append(nodes, SourceSnapshotNodeV2{Level: uint8(level + 1), Index: index, Digest: root})
		}
		empty = sourceSnapshotNodeDigestV2(empty, empty)
	}
	return nodes, nil
}

// ReadSourceChunkProofV2 performs at most 64 direct subtree reads. The reader
// supplies immutable nodes scoped to this source snapshot, never a global
// source-row map. VerifySourceChunkV2 must validate the resulting proof against
// independently admitted authority and actual local rows before serving.
func ReadSourceChunkProofV2(s SourceSnapshotV2, index uint64, read func(level uint8, index uint64) ([sha256.Size]byte, error)) (SourceChunkProofV2, error) {
	header, err := s.headerDigestV2()
	if err != nil || index >= s.ChunkCount() || read == nil {
		return SourceChunkProofV2{}, fmt.Errorf("%w: proof request", ErrInvalidSourceSnapshotV2)
	}
	sealed, err := SealSourceSnapshotV2(s, s.MerkleRoot)
	if err != nil || sealed.Digest != s.Digest {
		return SourceChunkProofV2{}, fmt.Errorf("%w: unsealed proof request", ErrInvalidSourceSnapshotV2)
	}
	height := bits.Len64(s.ChunkCount() - 1)
	proof := SourceChunkProofV2{Siblings: make([][sha256.Size]byte, height)}
	empty := sourceSnapshotEmptyDigestV2(header)
	for level := 0; level < height; level++ {
		sibling := (index >> level) ^ 1
		if sibling<<level >= s.ChunkCount() {
			proof.Siblings[level] = empty
		} else {
			digest, err := read(uint8(level), sibling)
			if err != nil {
				return SourceChunkProofV2{}, err
			}
			if digest == ([sha256.Size]byte{}) {
				return SourceChunkProofV2{}, fmt.Errorf("%w: missing proof node", ErrInvalidSourceSnapshotV2)
			}
			proof.Siblings[level] = digest
		}
		empty = sourceSnapshotNodeDigestV2(empty, empty)
	}
	return proof, nil
}
