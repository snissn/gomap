package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func requireLeafGenerationPackPromotionSupport(t *testing.T) {
	t.Helper()
	if rootpublication.StableRelativeNamespaceSupported() && rootpublication.StableCrossParentMoveNoReplaceSupported() {
		return
	}
	t.Skip("leaf generation pack promotion requires exact retained-parent namespace support")
}
