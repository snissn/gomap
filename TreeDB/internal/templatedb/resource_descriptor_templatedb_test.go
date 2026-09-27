package templatedb

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

func mustStableResourceDescriptors(t testing.TB, resources *rootpublication.StableResourceSet) []rootpublication.StableResourceDescriptor {
	t.Helper()
	descriptors, err := resources.Descriptors()
	if err != nil {
		t.Fatal(err)
	}
	return descriptors
}
