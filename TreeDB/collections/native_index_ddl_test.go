package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestNativeIndexMutationMetadataBoundary(t *testing.T) {
	before, err := nativeJSONMeta(CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatal(err)
	}
	after, _, err := addIndexToCollectionMeta(before, IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString})
	if err != nil {
		t.Fatal(err)
	}
	encode := func(meta CollectionMeta) []byte {
		t.Helper()
		b, err := encodeNormalizedCollectionMeta(meta)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	payload := func(meta CollectionMeta) []byte {
		t.Helper()
		b, err := commitlog.EncodeCatalogIndexMutationPayload(commitlog.CatalogIndexMutationPayload{Collection: before.Name, Incarnation: 1, BeforeMetadata: encode(before), Metadata: encode(meta)})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// Creating the first index intentionally changes normalized buffered-index
	// defaults, while the transition still changes only the index declaration.
	if _, _, _, err := decodeNativeIndexMutation(payload(after)); err != nil {
		t.Fatal(err)
	}
	changed := after
	changed.Options.DataRootStoragePolicy = RootStorageCompressed
	if _, _, _, err := decodeNativeIndexMutation(payload(changed)); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("storage-policy mutation accepted: %v", err)
	}
	changed = after
	changed.Name = "other"
	if _, _, _, err := decodeNativeIndexMutation(payload(changed)); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("collection rename accepted: %v", err)
	}
}
