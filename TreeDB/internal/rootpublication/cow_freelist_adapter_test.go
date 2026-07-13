package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"testing"
)

func TestCowFreelistExtensionUsesReservedSlotWithoutActivation(t *testing.T) {
	g := freelist.MustNewFreelistGenerationV1(3, 8, []uint64{1}, nil)
	c, err := newPreparedRootCandidateWithCowFreelist(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, g)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := c.extensions.cowFreelist.(cowFreelistExtension)
	if !ok || e.generationID != 3 || e.highWater != 8 {
		t.Fatalf("slot=%#v", c.extensions.cowFreelist)
	}
}
