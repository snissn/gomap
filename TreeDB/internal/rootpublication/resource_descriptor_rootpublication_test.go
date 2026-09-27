package rootpublication

import (
	"testing"
)

func mustStableResourceDescriptors(t testing.TB, resources *StableResourceSet) []StableResourceDescriptor {
	t.Helper()
	descriptors, err := resources.Descriptors()
	if err != nil {
		t.Fatal(err)
	}
	return descriptors
}
