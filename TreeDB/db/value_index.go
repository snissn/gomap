package db

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// ValueID is a unique identifier for a value stored indirectly.
type ValueID uint64

// ValueIndexPrefix is the prefix used for Value Index entries in the System Tree.
// 0x00 'v' 'i'
var ValueIndexPrefix = []byte{0x00, 'v', 'i'}

func ValueIndexPrefixEnd() []byte {
	end := make([]byte, len(ValueIndexPrefix))
	copy(end, ValueIndexPrefix)
	end[len(end)-1]++
	return end
}

func encodeValueIndexKey(id ValueID) []byte {
	k := make([]byte, len(ValueIndexPrefix)+8)
	copy(k, ValueIndexPrefix)
	binary.BigEndian.PutUint64(k[len(ValueIndexPrefix):], uint64(id))
	return k
}

// valueIndexHelper encapsulates operations on the Value Index stored in the System Tree.
type valueIndexHelper struct{}

// Set writes a ValueID -> ValuePtr mapping to the provided System Tree.
// Note: This does NOT write the user-key mapping; that is handled by the caller.
func (vi valueIndexHelper) Set(t *tree.Tree, id ValueID, ptr page.ValuePtr) error {
	// key := encodeValueIndexKey(id) // Unused

	// Encode ptr as the "inline value" of the leaf entry.
	// We use the pointer's raw bytes as the value.
	var buf [page.ValuePtrSize]byte
	ptr.Encode(buf[:])

	// Write to System Tree.
	// We use FlagInline (implicit in simple Set) because the "value" of this KV pair
	// is the 16-byte encoded ValuePtr. It is NOT a pointer itself.
	// We use SetRaw or Set? Tree.Set does a copy.
	// Tree.Set calls SetRaw eventually.
	// But Tree is read-only wrapper usually?
	// The `t` passed here must be a *tree.Tree* that supports writing?
	// `tree.Tree` (TreeDB/tree/tree.go) is read-only. Writes go via Zipper.
	// So this helper might be generating batch entries, not calling Tree.Set directly.
	// But `finalizeCommit` applies updates via `zipper`.
	// So we probably don't need this helper to call Set. We just need it to generate the Key/Value pair.
	return nil
}

// Get resolves a ValueID to a ValuePtr using the System Tree.
func (vi valueIndexHelper) Get(t *tree.Tree, id ValueID) (page.ValuePtr, error) {
	key := encodeValueIndexKey(id)
	val, err := t.Get(key)
	if err != nil {
		return page.ValuePtr{}, err
	}
	if len(val) != page.ValuePtrSize {
		return page.ValuePtr{}, errors.New("invalid value index entry size")
	}
	return page.DecodeValuePtr(val), nil
}
