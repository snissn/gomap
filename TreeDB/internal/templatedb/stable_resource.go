package templatedb

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// NewStableTemplateResourceToken registers one exact child resource in a
// template/catalog generation's transitive set.
func NewStableTemplateResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if spec.Reachability != rootpublication.ReachabilityTemplateGeneration {
		return nil, fmt.Errorf("%w: template producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerTemplate, spec, "authoritative-transitive")
}
