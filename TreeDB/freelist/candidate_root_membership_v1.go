package freelist

import "github.com/snissn/gomap/TreeDB/internal/iterator"

// CandidateRootMembershipV1 binds live DATA and SYSTEM operands to the exact
// original allocator candidate that incorporated them. It grants no retention,
// reuse or publication authority. The caller retains the original candidate
// and physical index operands throughout use.
type CandidateRootMembershipV1 struct {
	candidate    *PreparedCOWCandidateV1
	generation   *FreelistGenerationV1
	user, system uint64
}

func (m CandidateRootMembershipV1) Matches(c *PreparedCOWCandidateV1, g *FreelistGenerationV1, user, system uint64) bool {
	return c != nil && (c.activated || c.published || c.rollbackTxn != nil || c.privatePrepared) && m.candidate == c && m.generation == g && c.candidate != nil && c.candidate.generation == g && m.user == user && m.system == system
}

// CertifyRootMembershipV1 inspects the materialized candidate's own immutable
// allocation state. Unlike a pinned export, ANY retired root is inadmissible.
// A privately prepared descriptor permits this proof before slot adoption; it
// still grants no activation authority and cancellation revokes its eligibility.
// Under-admission returns no proof and changes no candidate state.
func (c *PreparedCOWCandidateV1) CertifyRootMembershipV1(user, system uint64, w *iterator.OrdinalScanWork) (CandidateRootMembershipV1, bool, error) {
	if c == nil || c.candidate == nil || c.candidate.generation == nil || (!c.activated && !c.published && c.rollbackTxn == nil && !c.privatePrepared) {
		return CandidateRootMembershipV1{}, false, ErrGenerationFormat
	}
	g := c.candidate.generation
	for _, id := range [2]uint64{user, system} {
		unavailable, ready, err := g.pageUnavailableWithWorkV1(id, 1, true, w)
		if err != nil || !ready {
			return CandidateRootMembershipV1{}, ready, err
		}
		if unavailable {
			return CandidateRootMembershipV1{}, false, ErrGenerationFormat
		}
	}
	return CandidateRootMembershipV1{candidate: c, generation: g, user: user, system: system}, true, nil
}
