package db

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type testStableDictionaryProvider struct {
	file         *os.File
	dictID       uint64
	dictionary   []byte
	captureCalls atomic.Int32
	releaseCalls atomic.Int32
}

type canceledContextStableDictionaryProvider struct {
	sawCanceled atomic.Bool
}

func (provider *canceledContextStableDictionaryProvider) CaptureDictionaryResources(ctx context.Context, _ uint64) (*rootpublication.StableResourceSet, error) {
	if ctx.Err() == context.Canceled {
		provider.sawCanceled.Store(true)
		return nil, ctx.Err()
	}
	return nil, fmt.Errorf("dictionary capture received uncanceled context")
}

func TestLeafGenerationPackPromotionAuthorityPropagatesCaptureContext(t *testing.T) {
	database := &DB{}
	provider := &canceledContextStableDictionaryProvider{}
	database.SetStableDictionaryResourceProvider(provider)
	authority := &leafGenerationPackPromotionAuthority{db: database}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := authority.captureDictionary(ctx, 7400, []byte("canceled packed dictionary authority"))
	if err != context.Canceled {
		t.Fatalf("capture error=%v want context.Canceled", err)
	}
	if !provider.sawCanceled.Load() {
		t.Fatal("packed dictionary provider did not receive operation cancellation")
	}
}

func newTestStableDictionaryProvider(t *testing.T, dictID uint64, dictionary []byte) *testStableDictionaryProvider {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "dictionary-authority-")
	if err != nil {
		t.Fatalf("create dictionary authority: %v", err)
	}
	if _, err := file.Write(dictionary); err != nil {
		_ = file.Close()
		t.Fatalf("write dictionary authority: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync dictionary authority: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return &testStableDictionaryProvider{file: file, dictID: dictID, dictionary: append([]byte(nil), dictionary...)}
}

func (provider *testStableDictionaryProvider) CaptureDictionaryResources(_ context.Context, dictID uint64) (*rootpublication.StableResourceSet, error) {
	provider.captureCalls.Add(1)
	if provider == nil || provider.file == nil || dictID != provider.dictID {
		return nil, fmt.Errorf("test dictionary %d unavailable", dictID)
	}
	logicalDigest := sha256.Sum256(provider.dictionary)
	logical := rootpublication.StableLogicalObligation{
		Class: "dictionary-generation", Kind: "dictionary", Namespace: "test",
		Generation: dictID, FileID: dictID, Offset: 0, Length: int64(len(provider.dictionary)),
		Reachability: rootpublication.ReachabilityDictionaryGeneration, Digest: logicalDigest,
	}
	token, err := rootpublication.NewStableProducerResourceTokenForDomain(
		rootpublication.StableProducerDictionary,
		rootpublication.StableResourceSpec{
			Kind: rootpublication.ResourceDictionary, LogicalLane: "test/dictionary", ResourceID: "dictionary",
			Generation: dictID, DiagnosticPath: "dictionary.test", File: provider.file,
			Frontier:           rootpublication.DurableFrontier{Bytes: uint64(len(provider.dictionary))},
			Digest:             sha256.Sum256([]byte("test-dictionary-physical-v1")),
			Reachability:       rootpublication.ReachabilityDictionaryGeneration,
			LogicalObligations: []rootpublication.StableLogicalObligation{logical}, ContentSynced: true,
			OnRelease: func() { provider.releaseCalls.Add(1) },
		},
		"authoritative-transitive",
	)
	if err != nil {
		return nil, err
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityDictionaryGeneration)
	if err := builder.Add(token); err != nil {
		token.Release()
		builder.Abandon()
		return nil, err
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
	}
	return resources, err
}
