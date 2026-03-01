package db

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/snissn/gomap/TreeDB/page"
)

const envDebugAnchorInvariants = "TREEDB_DEBUG_ANCHOR_INVARIANTS"

func debugAnchorInvariantsEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(envDebugAnchorInvariants)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

type valueLogAnchor struct {
	Key []byte
	Ptr page.ValuePtr
}

// validateValueLogAnchors performs a lightweight ordering/disjointness check for
// persisted value-log anchors. It is intentionally side-effect free.
func validateValueLogAnchors(anchors []valueLogAnchor) error {
	var (
		prevKey []byte
	)
	seenPtrs := make(map[page.ValuePtr]int, len(anchors))
	for i := range anchors {
		a := anchors[i]
		if len(a.Key) == 0 {
			return fmt.Errorf("anchor invariant: empty key at index=%d", i)
		}
		if i > 0 {
			if bytes.Compare(a.Key, prevKey) <= 0 {
				return fmt.Errorf("anchor invariant: non-increasing key at index=%d", i)
			}
		}
		if firstIdx, exists := seenPtrs[a.Ptr]; exists {
			return fmt.Errorf("anchor invariant: duplicate pointer at index=%d first_index=%d", i, firstIdx)
		}
		seenPtrs[a.Ptr] = i
		prevKey = a.Key
	}
	return nil
}

func debugFailFastOnValueLogAnchorInvariant(err error) {
	if err == nil || !debugAnchorInvariantsEnabled() {
		return
	}
	panic(err)
}
