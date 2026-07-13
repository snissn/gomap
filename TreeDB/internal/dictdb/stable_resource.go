package dictdb

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// NewStableDictionaryResourceToken registers one exact child resource in a
// dictionary generation's transitive set.
func NewStableDictionaryResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if spec.Reachability != rootpublication.ReachabilityDictionaryGeneration {
		return nil, fmt.Errorf("%w: dictionary producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerDictionary, spec, "authoritative-transitive")
}
