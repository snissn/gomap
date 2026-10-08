//go:build !windows

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Supported same-ID reinstall/provider replacement produces overlapping physical
// authority with distinct initial snapshot fences. Coalescing retains one
// generation fence, rather than every historical snapshot/read-state owner.
func TestApplyLeafDictionaryCaptureOverlappingSnapshotFences(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		for _, finish := range []string{"release", "abandon", "freeze-failure"} {
			t.Run(fmt.Sprintf("replacement=%v/%s", replacement, finish), func(t *testing.T) {
				database, err := Open(Options{Dir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				const id = uint64(7412)
				dictionary := []byte("dictionary with overlapping real index snapshot leases")
				original := newTestStableDictionaryProvider(t, id, dictionary)
				provider := &snapshotDictionaryProvider{testStableDictionaryProvider: original, database: database}
				writer := &rewriteWriter{}
				capture, err := newApplyLeafResourceLog(&stableContractTestLeafLog{})
				if err != nil {
					t.Fatal(err)
				}
				defer capture.abandon()
				for i := 0; i < 2; i++ {
					writer.SetLeafDictMode(id, dictionary, false)
					authority := provider
					if i == 1 && replacement {
						authority = &snapshotDictionaryProvider{testStableDictionaryProvider: original, database: database}
					}
					resources, err := capture.capture.dictionaries.capture(context.Background(), writer, authority, id, writer.leafDict)
					if err != nil || resources != nil {
						t.Fatalf("capture %d resources=%v err=%v", i, resources != nil, err)
					}
				}
				if database.stableIndexCaptures.Load() != 2 {
					t.Fatal("initial snapshots not independently fenced")
				}
				switch finish {
				case "abandon":
					capture.abandon()
				case "freeze-failure":
					capture.capture.builder.Abandon()
					capture.capture.builder = rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityColumnManifest)
					resources, err := capture.freeze()
					resources.Release()
					if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
						t.Fatalf("freeze error=%v", err)
					}
				case "release":
					resources, err := capture.freeze()
					if err != nil {
						t.Fatal(err)
					}
					defer resources.Release()
					if got := database.stableIndexCaptures.Load(); got != 1 {
						t.Fatalf("coalesced fences=%d want one generation fence", got)
					}
					if err := database.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
						t.Fatalf("vacuum crossed generation fence: %v", err)
					}
					cloned, err := rootpublication.CloneStableResourceSetExcludingKinds(resources)
					if err != nil {
						t.Fatal(err)
					}
					defer cloned.Release()
					resources.Release()
					capture.abandon()
					if database.stableIndexCaptures.Load() != 1 {
						t.Fatal("candidate clone lost generation fence")
					}
					if err := ValidateStableDictionaryResourceClosure(cloned, id, dictionary); err != nil {
						t.Fatal(err)
					}
					cloned.Release()
				}
				if database.stableIndexCaptures.Load() != 0 || original.releaseCalls.Load() != 2 {
					t.Fatalf("final fences=%d physical releases=%d", database.stableIndexCaptures.Load(), original.releaseCalls.Load())
				}
			})
		}
	}
}

type unknownComparableDictionaryProvider struct{ provider *testStableDictionaryProvider }

func (p unknownComparableDictionaryProvider) CaptureDictionaryResources(ctx context.Context, id uint64) (*rootpublication.StableResourceSet, error) {
	return p.provider.CaptureDictionaryResources(ctx, id)
}

func TestApplyLeafDictionaryCaptureUnknownGenerationLifetimeUsesFullCapture(t *testing.T) {
	const id = uint64(7414)
	dictionary := []byte("unknown provider cannot certify physical generation callbacks")
	provider := newTestStableDictionaryProvider(t, id, dictionary)
	unknown := unknownComparableDictionaryProvider{provider: provider}
	writer := &rewriteWriter{}
	writer.SetLeafDictMode(id, dictionary, false)
	var scope applyLeafDictionaryCapture
	defer scope.release()
	for i := 0; i < 2; i++ {
		resources, err := scope.capture(context.Background(), writer, unknown, id, writer.leafDict)
		if err != nil || resources == nil {
			t.Fatalf("unknown capture resources=%v err=%v", resources != nil, err)
		}
		resources.Release()
	}
	if !scope.empty() || provider.captureCalls.Load() != 2 || provider.releaseCalls.Load() != 2 {
		t.Fatal("unknown authority entered reusable scope")
	}
}
