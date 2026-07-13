package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// NewStableDBResourceToken registers an exact already-open index handle for a
// DB/meta/root field. Publication integration consumes the returned token; the
// producer never reopens DiagnosticPath to establish identity.
func NewStableDBResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	classification := ""
	switch spec.Reachability {
	case rootpublication.ReachabilityIndexFile, rootpublication.ReachabilityMetaPage:
		classification = "authoritative"
	case rootpublication.ReachabilityUserRoot, rootpublication.ReachabilitySystemRoot, rootpublication.ReachabilityFreelist:
		classification = "authoritative-root-backed"
	default:
		return nil, fmt.Errorf("%w: db producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerDB, spec, classification)
}

// NewStableOuterLeafResourceToken registers an exact packed segment or
// generation-manifest handle captured before its rename result is exposed.
func NewStableOuterLeafResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	switch spec.Reachability {
	case rootpublication.ReachabilityOuterLeafPackedPointer, rootpublication.ReachabilityOuterLeafGeneration:
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerOuterLeaf, spec, "authoritative")
	default:
		return nil, fmt.Errorf("%w: outer-leaf pack producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
}
