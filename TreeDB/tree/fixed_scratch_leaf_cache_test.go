package tree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func buildMultiLeafTestTree(t *testing.T, count int, keyPrefix string, prefixCompress, columnar bool) (*Tree, *pager.Pager, [][]byte, [][]byte) {
	t.Helper()
	p, err := pager.Open(t.TempDir()+"/index.db", 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Allocate an initial empty leaf root.
	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	rootData, err := p.GetForWrite(rootID)
	if err != nil {
		t.Fatal(err)
	}
	rootNode := node.NewNode(rootData)
	rootNode.SetType(page.PageTypeLeaf)
	rootNode.SetPageID(rootID)
	rootNode.UpdateChecksum()

	// Build a multi-level tree with sorted even keys (leaving odd keys as inter-leaf & intra-leaf gaps).
	keys := make([][]byte, count)
	vals := make([][]byte, count)
	for i := 0; i < count; i++ {
		if keyPrefix != "" {
			keys[i] = []byte(fmt.Sprintf("%s-%08d", keyPrefix, i*2))
		} else {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i*2+10))
			keys[i] = k
		}
		vals[i] = bytes.Repeat([]byte{byte('a' + (i % 26))}, 64)
	}

	// Build leaves packed with ~24 entries each so we have many leaves and internal levels.
	const perLeaf = 24
	var leafIDs []uint64
	var leafFirstKeys [][]byte
	for start := 0; start < count; start += perLeaf {
		end := min(start+perLeaf, count)
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatal(err)
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: prefixCompress,
			LeafColumnar:          columnar,
		})
		b.SetPageID(id)
		for i := start; i < end; i++ {
			if err := b.AddLeafEntry(keys[i], vals[i], node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("AddLeafEntry(%d): %v", i, err)
			}
		}
		b.Finish()
		leafIDs = append(leafIDs, id)
		leafFirstKeys = append(leafFirstKeys, keys[start])
	}

	// Build internal root (or two levels if needed).
	currIDs := leafIDs
	currKeys := leafFirstKeys
	for len(currIDs) > 1 {
		const perInternal = 32
		var nextIDs []uint64
		var nextKeys [][]byte
		for start := 0; start < len(currIDs); start += perInternal {
			end := min(start+perInternal, len(currIDs))
			id, err := p.Alloc(1)
			if err != nil {
				t.Fatal(err)
			}
			data, err := p.GetForWrite(id)
			if err != nil {
				t.Fatal(err)
			}
			b := node.NewBuilder(data, page.PageTypeInternal)
			b.SetPageID(id)
			for i := start; i < end; i++ {
				if err := b.AddInternalChild(currKeys[i], currIDs[i]); err != nil {
					t.Fatalf("AddInternalChild(%d): %v", i, err)
				}
			}
			b.Finish()
			nextIDs = append(nextIDs, id)
			nextKeys = append(nextKeys, currKeys[start])
		}
		currIDs = nextIDs
		currKeys = nextKeys
	}
	rootID = currIDs[0]
	return New(p, nil, rootID), p, keys, vals
}

func TestFixedScratchLeafCacheParityAndInterLeafGaps(t *testing.T) {
	modes := []struct {
		name           string
		prefixCompress bool
		columnar       bool
		keyPrefix      string
	}{
		{name: "plain", prefixCompress: false, columnar: false},
		{name: "prefix_compressed", prefixCompress: true, columnar: false},
		{name: "columnar_prefix", prefixCompress: true, columnar: true},
		{name: "oversize_separator_fallback", prefixCompress: true, columnar: true, keyPrefix: string(bytes.Repeat([]byte{'x'}, 140))},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			tr, _, keys, vals := buildMultiLeafTestTree(t, 400, tc.keyPrefix, tc.prefixCompress, tc.columnar)
			var keyScratch, leafScratch [page.PageSize]byte
			var cache FixedScratchLeafCache

			// Sorted scan covering hits, intra-leaf gaps, inter-leaf gaps, leftmost (<keys[0]) and rightmost (>keys[last]).
			probe := func(k []byte, wantVal []byte, wantFound bool) {
				t.Helper()
				entry, err := tr.GetEntryWithFixedScratchLeafCache(k, keyScratch[:], leafScratch[:], &cache, nil)
				if !wantFound {
					if !errors.Is(err, ErrKeyNotFound) {
						t.Fatalf("key %x: err=%v want ErrKeyNotFound", k, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("key %x: unexpected err=%v", k, err)
				}
				if !bytes.Equal(entry.Value, wantVal) {
					t.Fatalf("key %x: value mismatch", k)
				}
			}

			// Probe below leftmost key.
			if tc.keyPrefix == "" {
				var low [8]byte
				binary.BigEndian.PutUint64(low[:], 1)
				probe(low[:], nil, false)
			} else {
				probe([]byte("0000"), nil, false)
			}

			for i, k := range keys {
				probe(k, vals[i], true)
				// Inter-key / inter-leaf gap right after k.
				gap := append([]byte(nil), k...)
				gap[len(gap)-1]++
				probe(gap, nil, false)
			}

			// Probe beyond rightmost key.
			if tc.keyPrefix == "" {
				var high [8]byte
				binary.BigEndian.PutUint64(high[:], ^uint64(0))
				probe(high[:], nil, false)
			} else {
				probe([]byte("zzzz"), nil, false)
			}

			// Verify oversize separator did not populate the cache.
			if len(tc.keyPrefix) > fixedScratchLeafCacheMaxSepLen && cache.valid {
				t.Fatal("oversize separator populated FixedScratchLeafCache")
			}
			// Verify normal modes populated the cache and reused it with zero heap allocations.
			if len(tc.keyPrefix) == 0 {
				if !cache.valid {
					t.Fatal("expected FixedScratchLeafCache to be valid after normal lookup")
				}
				// Verify rootPageID change invalidates cache safely: probe keys[len(keys)-1],
				// which falls inside the cached rightmost leaf's [low, high) interval.
				otherTree := New(tr.pager, nil, tr.rootPageID+999)
				_, err := otherTree.GetEntryWithFixedScratchLeafCache(keys[len(keys)-1], keyScratch[:], leafScratch[:], &cache, nil)
				if err == nil {
					t.Fatal("expected invalid root lookup to miss cache and fail")
				}
				if cache.valid {
					t.Fatal("cache remained valid after root mismatch error")
				}
				// Verify VerifyOnRead bypasses cache reuse and population.
				tr.pager.SetVerifyOnRead(true)
				got, err := tr.GetEntryWithFixedScratchLeafCache(keys[len(keys)-1], keyScratch[:], leafScratch[:], &cache, nil)
				tr.pager.SetVerifyOnRead(false)
				if err != nil {
					t.Fatalf("VerifyOnRead lookup failed: %v", err)
				}
				if !bytes.Equal(got.Value, vals[len(keys)-1]) {
					t.Fatalf("VerifyOnRead value mismatch: got %q want %q", got.Value, vals[len(keys)-1])
				}
				if cache.valid {
					t.Fatal("VerifyOnRead populated FixedScratchLeafCache")
				}
			}
		})
	}
}
