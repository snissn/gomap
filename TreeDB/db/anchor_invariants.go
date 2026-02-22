package db

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/snissn/gomap/TreeDB/page"
)

const envDebugAnchorInvariants = "TREEDB_DEBUG_ANCHOR_INVARIANTS"

type v1LeafLogAnchor struct {
	Key []byte
	Ptr page.ValuePtr
}

func debugAnchorInvariantsEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(envDebugAnchorInvariants)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// validateV1LeafLogAnchors performs a lightweight ordering/disjointness check
// for future routing-only anchor paths. It is intentionally side-effect free.
func validateV1LeafLogAnchors(anchors []v1LeafLogAnchor) error {
	var (
		prevKey []byte
	)
	seenPtrs := make(map[page.ValuePtr]int, len(anchors))
	for i := range anchors {
		a := anchors[i]
		if len(a.Key) == 0 {
			return fmt.Errorf("v1_leaflog anchor invariant: empty key at index=%d", i)
		}
		if i > 0 {
			if bytes.Compare(a.Key, prevKey) <= 0 {
				return fmt.Errorf("v1_leaflog anchor invariant: non-increasing key at index=%d", i)
			}
		}
		if firstIdx, exists := seenPtrs[a.Ptr]; exists {
			return fmt.Errorf("v1_leaflog anchor invariant: duplicate pointer at index=%d first_index=%d", i, firstIdx)
		}
		seenPtrs[a.Ptr] = i
		prevKey = a.Key
	}
	return nil
}

func debugFailFastOnV1LeafLogAnchorInvariant(err error) {
	if err == nil || !debugAnchorInvariantsEnabled() {
		return
	}
	panic(err)
}
