package nativewire

import "testing"

func TestRetiredDenseVersionAndStrategy(t *testing.T) {
	registry := MustV1Registry()
	if _, ok := registry.LookupCommand(CommandDenseVectorSearch, 1); ok {
		t.Fatal("retired dense command version 1 is advertised")
	}
	for _, version := range []uint64{DenseVectorSearchTypedVersion, DenseVectorSearchTypedQuantizedVersion, DenseVectorSearchNormalizedVersion, DenseVectorSearchOrdinaryVersion} {
		if _, ok := registry.LookupCommand(CommandDenseVectorSearch, version); !ok {
			t.Fatalf("modern dense version %d is missing", version)
		}
	}
	if err := validateDeterministicVectorIndexStrategyEnum(1); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("retired strategy: %v", err)
	}
	if err := validateDeterministicVectorIndexStrategyEnum(0); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing strategy: %v", err)
	}
	if err := validateDeterministicVectorIndexStrategyEnum(2); err != nil {
		t.Fatalf("modern strategy: %v", err)
	}
}
