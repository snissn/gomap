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
