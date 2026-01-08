package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafEntry_FlagIntegrity(t *testing.T) {
	testCases := []struct {
		name              string
		prefixCompression bool
	}{
		{"Standard", false},
		{"PrefixCompressed", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, page.PageSize)
			n := NewNode(data)
			n.SetType(page.PageTypeLeaf)
			if tc.prefixCompression {
				n.setLeafPrefixCompressed(true)
			}

			entries := []struct {
				key   []byte
				val   []byte
				ptr   page.ValuePtr
				flags byte
			}{
				{[]byte("key-inline"), []byte("value-inline"), page.ValuePtr{}, FlagInline},
				{[]byte("key-ptr"), nil, page.ValuePtr{FileID: 1, Offset: 100, Length: 50}, FlagPointer},
				{[]byte("key-tomb"), nil, page.ValuePtr{}, FlagTombstone},
				{[]byte("key-vid"), []byte("12345678"), page.ValuePtr{}, FlagValueID},
				{[]byte("key-ptr-vid"), []byte("87654321"), page.ValuePtr{FileID: 2, Offset: 200, Length: 60}, FlagPointer | FlagValueID},
			}

			for _, e := range entries {
				err := n.AddLeafEntry(e.key, e.val, e.flags, e.ptr)
				if err != nil {
					t.Fatalf("AddLeafEntry(%s): %v", e.key, err)
				}
			}

			// Verify each entry
			for _, e := range entries {
				k, v, ptr, flags, err := n.GetLeafEntryView(n.mustSearchLeaf(e.key))
				if err != nil {
					t.Fatalf("GetLeafEntryView(%s): %v", e.key, err)
				}

				if !bytes.Equal(k, e.key) {
					t.Errorf("%s: key mismatch", e.key)
				}
				if flags != e.flags {
					t.Errorf("%s: flags mismatch: got %x, want %x", e.key, flags, e.flags)
				}

				if flags&FlagPointer != 0 {
					if ptr.FileID != e.ptr.FileID || ptr.Offset != e.ptr.Offset {
						t.Errorf("%s: ptr mismatch", e.key)
					}
				} else if flags&FlagTombstone == 0 {
					if !bytes.Equal(v, e.val) {
						t.Errorf("%s: value mismatch: got %x, want %x", e.key, v, e.val)
					}
				}
			}
		})
	}
}

func (n *Node) mustSearchLeaf(key []byte) uint16 {
	idx, found, err := n.SearchLeaf(key)
	if err != nil || !found {
		panic("key not found in test")
	}
	return idx
}
