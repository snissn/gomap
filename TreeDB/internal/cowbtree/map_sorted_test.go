package cowbtree

import (
	"fmt"
	"sort"
	"testing"
)

func verifyBTreeInvariants[K ordered, V any](t *testing.T, tr *Map[K, V]) {
	t.Helper()
	if tr.root == nil {
		if tr.count != 0 {
			t.Fatalf("nil root with count=%d", tr.count)
		}
		return
	}
	var leafDepth int
	var check func(n *mapNode[K, V], depth int, isRoot bool, hasMin bool, minKey K, hasMax bool, maxKey K) int
	check = func(n *mapNode[K, V], depth int, isRoot bool, hasMin bool, minKey K, hasMax bool, maxKey K) int {
		if len(n.items) > tr.max {
			t.Fatalf("node items=%d > max=%d", len(n.items), tr.max)
		}
		if !isRoot && len(n.items) < tr.min {
			t.Fatalf("non-root node items=%d < min=%d", len(n.items), tr.min)
		}
		if isRoot && len(n.items) == 0 {
			t.Fatalf("root node has 0 items")
		}
		for i := 0; i < len(n.items); i++ {
			k := n.items[i].key
			if hasMin && !(minKey < k) {
				t.Fatalf("key %v not > minKey %v", k, minKey)
			}
			if hasMax && !(k < maxKey) {
				t.Fatalf("key %v not < maxKey %v", k, maxKey)
			}
			if i > 0 && !(n.items[i-1].key < k) {
				t.Fatalf("unsorted keys in node: %v >= %v", n.items[i-1].key, k)
			}
		}
		if n.leaf() {
			if leafDepth == 0 {
				leafDepth = depth
			} else if leafDepth != depth {
				t.Fatalf("unbalanced leaf depth: got %d want %d", depth, leafDepth)
			}
			if n.count != len(n.items) {
				t.Fatalf("leaf count=%d != len(items)=%d", n.count, len(n.items))
			}
			return n.count
		}
		children := *n.children
		if len(children) != len(n.items)+1 {
			t.Fatalf("internal children=%d != len(items)+1=%d", len(children), len(n.items)+1)
		}
		total := len(n.items)
		for i, ch := range children {
			cHasMin, cMin := hasMin, minKey
			if i > 0 {
				cHasMin, cMin = true, n.items[i-1].key
			}
			cHasMax, cMax := hasMax, maxKey
			if i < len(n.items) {
				cHasMax, cMax = true, n.items[i].key
			}
			total += check(ch, depth+1, false, cHasMin, cMin, cHasMax, cMax)
		}
		if n.count != total {
			t.Fatalf("internal count=%d != subtree total=%d", n.count, total)
		}
		return total
	}
	gotCount := check(tr.root, 1, true, false, tr.empty.key, false, tr.empty.key)
	if gotCount != tr.count {
		t.Fatalf("tree count=%d != actual=%d", tr.count, gotCount)
	}
}

func TestSetSortedBytesMatchesReference(t *testing.T) {
	sizes := []int{1, 2, 31, 32, 63, 64, 65, 127, 128, 129, 500, 1000, 5000}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("empty_bulk_%d", n), func(t *testing.T) {
			tr := NewMap[string, int](32)
			keys := make([][]byte, n)
			for i := 0; i < n; i++ {
				keys[i] = []byte(fmt.Sprintf("k-%06d", i))
			}
			SetSortedBytes(tr, n,
				func(i int) []byte { return keys[i] },
				func(i int) int { return i + 1 },
				func(i int) string { return string(keys[i]) },
				func(i int, prev int) { t.Fatalf("unexpected replace at %d: %d", i, prev) },
			)
			verifyBTreeInvariants(t, tr)
			if tr.Len() != n {
				t.Fatalf("Len=%d want %d", tr.Len(), n)
			}
			for i := 0; i < n; i++ {
				got, ok := tr.Get(string(keys[i]))
				if !ok || got != i+1 {
					t.Fatalf("Get(%q)=%d,%v want %d,true", keys[i], got, ok, i+1)
				}
			}
		})
	}

	t.Run("forked_interleaved_append_and_replace", func(t *testing.T) {
		ref := make(map[string]int)
		base := NewMap[string, int](32).SealOwned()

		// Round 1: seed 2000 keys across 64 buckets.
		var batch1 [][]byte
		for b := 0; b < 64; b++ {
			for j := 0; j < 32; j++ {
				k := fmt.Sprintf("b%02d-k%04d", b, j*2)
				batch1 = append(batch1, []byte(k))
				ref[k] = b*1000 + j
			}
		}
		sort.Slice(batch1, func(i, j int) bool { return string(batch1[i]) < string(batch1[j]) })

		tr1 := base.ForkImmutable()
		SetSortedBytes(tr1, len(batch1),
			func(i int) []byte { return batch1[i] },
			func(i int) int { return ref[string(batch1[i])] },
			func(i int) string { return string(batch1[i]) },
			nil,
		)
		tr1.SealOwned()
		verifyBTreeInvariants(t, tr1)

		// Snapshot ref before Round 2 to verify tr1 immutability.
		ref1 := make(map[string]int, len(ref))
		for k, v := range ref {
			ref1[k] = v
		}

		// Round 2: interleaved new keys + replacements (including internal separators) + right-edge append.
		var batch2 [][]byte
		batch2Vals := make(map[string]int)
		expectedReplaces := make(map[string]int)
		for b := 0; b < 64; b++ {
			for j := 0; j < 40; j++ {
				k := fmt.Sprintf("b%02d-k%04d", b, j) // even=replace, odd=insert, >=64=insert
				v := 100000 + b*1000 + j
				if old, ok := ref[k]; ok {
					expectedReplaces[k] = old
				}
				batch2 = append(batch2, []byte(k))
				batch2Vals[k] = v
				ref[k] = v
			}
		}
		for r := 0; r < 1000; r++ {
			k := fmt.Sprintf("z-tail-%06d", r)
			batch2 = append(batch2, []byte(k))
			batch2Vals[k] = 900000 + r
			ref[k] = 900000 + r
		}
		sort.Slice(batch2, func(i, j int) bool { return string(batch2[i]) < string(batch2[j]) })

		tr2 := tr1.ForkImmutable()
		gotReplaces := make(map[string]int)
		SetSortedBytes(tr2, len(batch2),
			func(i int) []byte { return batch2[i] },
			func(i int) int { return batch2Vals[string(batch2[i])] },
			func(i int) string { return string(batch2[i]) },
			func(i int, prev int) { gotReplaces[string(batch2[i])] = prev },
		)
		tr2.SealOwned()
		verifyBTreeInvariants(t, tr2)

		if len(gotReplaces) != len(expectedReplaces) {
			t.Fatalf("replaces=%d want %d", len(gotReplaces), len(expectedReplaces))
		}
		for k, wantPrev := range expectedReplaces {
			if gotPrev, ok := gotReplaces[k]; !ok || gotPrev != wantPrev {
				t.Fatalf("replace(%q)=%d,%v want %d,true", k, gotPrev, ok, wantPrev)
			}
		}
		// Verify tr1 was untouched (immutable COW).
		if tr1.Len() != len(ref1) {
			t.Fatalf("tr1 Len mutated: %d != %d", tr1.Len(), len(ref1))
		}
		for k, want := range ref1 {
			if got, ok := tr1.Get(k); !ok || got != want {
				t.Fatalf("tr1.Get(%q)=%d,%v want %d,true", k, got, ok, want)
			}
		}
		// Verify tr2 matches full ref.
		if tr2.Len() != len(ref) {
			t.Fatalf("tr2 Len=%d want %d", tr2.Len(), len(ref))
		}
		for k, want := range ref {
			if got, ok := tr2.Get(k); !ok || got != want {
				t.Fatalf("tr2.Get(%q)=%d,%v want %d,true", k, got, ok, want)
			}
		}

		// Round 3: pure right-edge append of 2000 keys onto tr2.
		var batch3 [][]byte
		for r := 0; r < 2000; r++ {
			k := fmt.Sprintf("zz-append-%06d", r)
			batch3 = append(batch3, []byte(k))
			ref[k] = 2000000 + r
		}
		tr3 := tr2.ForkImmutable()
		SetSortedBytes(tr3, len(batch3),
			func(i int) []byte { return batch3[i] },
			func(i int) int { return 2000000 + i },
			func(i int) string { return string(batch3[i]) },
			func(i int, prev int) { t.Fatalf("unexpected replace in round 3") },
		)
		tr3.SealOwned()
		verifyBTreeInvariants(t, tr3)
		if tr3.Len() != len(ref) {
			t.Fatalf("tr3 Len=%d want %d", tr3.Len(), len(ref))
		}
		for k, want := range ref {
			if got, ok := tr3.Get(k); !ok || got != want {
				t.Fatalf("tr3.Get(%q)=%d,%v want %d,true", k, got, ok, want)
			}
		}
	})
}
