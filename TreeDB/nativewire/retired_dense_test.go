package nativewire

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestRetiredDenseResponseTagAndMetadataStrategy(t *testing.T) {
	for _, version := range []uint64{iwire.DenseVectorSearchTypedVersion, iwire.DenseVectorSearchTypedQuantizedVersion, iwire.DenseVectorSearchOrdinaryVersion} {
		_, _, _, _, err := decodeVersionedDenseVectorSearchResponse(version, nil, nil, []byte{1}, 1, iwire.Limits{}, nil, nil, nil)
		if nativeCodeOf(err) != iwire.ErrMalformedFrame {
			t.Fatalf("version %d accepted retired tag: %v", version, err)
		}
	}
	if _, err := decodeVectorIndexStrategyStrict(1); nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
		t.Fatalf("retired strategy: %v", err)
	}
	if _, err := decodeVectorIndexStrategyStrict(0); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("empty strategy: %v", err)
	}
	if got, err := decodeVectorIndexStrategyStrict(2); err != nil || got != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("modern strategy %q: %v", got, err)
	}
	for _, strategy := range []collections.VectorIndexStrategy{"", "native_runtime", "unknown"} {
		if _, err := encodeCollectionMeta(collections.CollectionMeta{Name: "x", VectorIndexes: []collections.VectorIndexDefinition{{Name: "v", Strategy: strategy}}}); err == nil {
			t.Fatalf("encoded invalid strategy %q", strategy)
		}
	}
}
