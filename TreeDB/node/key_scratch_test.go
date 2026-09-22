package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestEnsureKeyScratch_GeometricReuse(t *testing.T) {
	n := NewNode(make([]byte, page.PageSize))

	first := n.ensureKeyScratch(129)
	firstCap := cap(first)
	if firstCap < 129 {
		t.Fatalf("initial scratch cap=%d want >= 129", firstCap)
	}

	second := n.ensureKeyScratch(130)
	if cap(second) != firstCap {
		t.Fatalf("scratch cap changed on small growth: before=%d after=%d", firstCap, cap(second))
	}

	third := n.ensureKeyScratch(firstCap + 1)
	if cap(third) <= firstCap {
		t.Fatalf("scratch cap did not grow: before=%d after=%d", firstCap, cap(third))
	}
}
