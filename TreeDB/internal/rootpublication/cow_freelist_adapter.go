package rootpublication

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/freelist"
)

// cowFreelistExtension retains the ordered ownership chain. #3679 validates
// exact ancestry and transfers reservation ownership while activating the
// format; choosing the largest numeric generation is not a safety rule.
type cowFreelistExtension struct {
	chain []freelist.GenerationRefV1
}

func (e cowFreelistExtension) union(other immutableExtension) (immutableExtension, error) {
	o, ok := other.(cowFreelistExtension)
	if !ok {
		return nil, fmt.Errorf("%w: COW freelist extension has type %T", ErrResourceConflict, other)
	}
	chain := make([]freelist.GenerationRefV1, 0, len(e.chain)+len(o.chain))
	chain = append(chain, e.chain...)
	chain = append(chain, o.chain...)
	return cowFreelistExtension{chain: chain}, nil
}

func newPreparedRootCandidateWithCowFreelist(spec CandidateSpec, g *freelist.FreelistGenerationV1) (*PreparedRootCandidate, error) {
	if g == nil || g.GenerationRef().HeaderPageID == 0 {
		return nil, ErrInvalidCandidate
	}
	return newPreparedRootCandidateWithExtensions(spec, extensionSlots{cowFreelist: cowFreelistExtension{chain: []freelist.GenerationRefV1{g.GenerationRef()}}})
}
