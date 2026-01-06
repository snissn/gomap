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

// Set is intentionally not implemented for direct writes.
// Value Index entries are written via batch operations (see batch.go) rather than
// through direct tree mutations. Calling this method will always return an error to
// make this design explicit and to avoid silent no-op writes.
func (vi valueIndexHelper) Set(t *tree.Tree, id ValueID, ptr page.ValuePtr) error {
	return errors.New("valueIndexHelper.Set: direct Value Index writes are not supported; use batch operations instead")
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
