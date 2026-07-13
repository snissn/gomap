package rootpublication

import "github.com/snissn/gomap/TreeDB/freelist"

// cowFreelistExtension is a package-private carrier for #3679. It claims the
// existing extension slot but deliberately does not change a root/meta format.
type cowFreelistExtension struct{ generationID, highWater uint64 }

func (e cowFreelistExtension) union(other immutableExtension) immutableExtension {
	o, ok := other.(cowFreelistExtension)
	if !ok || o.generationID < e.generationID {
		return e
	}
	return o
}

func newPreparedRootCandidateWithCowFreelist(spec CandidateSpec, g *freelist.FreelistGenerationV1) (*PreparedRootCandidate, error) {
	if g == nil {
		return nil, ErrInvalidCandidate
	}
	return newPreparedRootCandidateWithExtensions(spec, extensionSlots{cowFreelist: cowFreelistExtension{generationID: g.GenerationID(), highWater: g.HighWater()}})
}
