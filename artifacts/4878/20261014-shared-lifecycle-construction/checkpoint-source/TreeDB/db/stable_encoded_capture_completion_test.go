package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"testing"
	"unsafe"
)

type partialEncodedTemplateProvider struct {
	*testStableTemplateProvider
	resources *rootpublication.StableResourceSet
	err       error
}

func (p *partialEncodedTemplateProvider) CaptureTemplateResources(context.Context, uint64) (*rootpublication.StableResourceSet, error) {
	return p.resources, p.err
}

type partialStableLeafProvider struct {
	LeafPageLog
	resources *rootpublication.StableResourceSet
	err       error
}

func (p *partialStableLeafProvider) AppendLeafPageWithStableResources([]byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return page.LeafLogPtr{}, p.resources, p.err
}
func (p *partialStableLeafProvider) AppendPreparedLeafPageChildRefsWithStableResources([][]byte, [][]byte, []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error) {
	return nil, p.resources, p.err
}
func TestEncodedProviderErrorDisposesReturnedOriginal(t *testing.T) {
	for _, route := range []string{"encoded-provider", "production-leaf-wrapper", "production-child-ref-wrapper", "nil-result-cleanup-receipt", "encoded-provider-nil-receipt"} {
		t.Run(route, func(t *testing.T) {
			database, path := openDurableRootClosureDB3928(t)
			owner := database.valueLogIdentityPins.MetadataOwner()
			provider := newTestStableTemplateProvider(t, []byte("template-partial-original"))
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := rootpublication.StableIdentityFromFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if err = database.valueLogIdentityPins.Observe(identity); err != nil {
				t.Fatal(err)
			}
			charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootCaptureCloseOperations{})))
			fileCharge, err := rootpublication.StableFileMetadataCharge(uint64(len(path)))
			if err != nil {
				t.Fatal(err)
			}
			charge += fileCharge
			if err = owner.AddPending(charge); err != nil {
				t.Fatal(err)
			}
			operations := &rootCaptureCloseOperations{owner: owner, file: file, charge: charge, registry: database.valueLogIdentityPins, identity: identity}
			operations.refs.Store(1)
			token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
				MetadataOwner: owner, PinRegistry: database.valueLogIdentityPins, OwnedOperations: operations,
				Kind: rootpublication.ResourceTemplate, LogicalLane: "templates", ResourceID: "partial-original", Generation: provider.templateID, DiagnosticPath: "original-template",
				File: file, ContentSynced: true, Frontier: rootpublication.DurableFrontier{Bytes: 4096}, Reachability: rootpublication.ReachabilityTemplateGeneration,
				LogicalObligations: []rootpublication.StableLogicalObligation{{Class: "template-generation", Kind: "template", Namespace: "test", Generation: provider.templateID, FileID: provider.templateID, Length: int64(len(provider.definition)), Reachability: rootpublication.ReachabilityTemplateGeneration, Digest: sha256.Sum256(provider.definition)}},
			})
			if err != nil {
				t.Fatal(err)
			}
			builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(owner)
			if err != nil {
				t.Fatal(err)
			}
			if err = builder.Add(token); err != nil {
				t.Fatal(err)
			}
			original, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			if consumed, err := operations.Release(); !consumed || err != nil {
				t.Fatalf("initial edge %t %v", consumed, err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			first := errors.New("provider returned original with failure")
			partial := &partialEncodedTemplateProvider{testStableTemplateProvider: provider, resources: original, err: first}
			prior := errors.New("preexisting unrelated background error")
			database.retainOriginalCleanupError(prior)
			var result *rootpublication.StableResourceSet
			if route == "encoded-provider" || route == "encoded-provider-nil-receipt" {
				if route == "encoded-provider-nil-receipt" {
					partial.resources = nil
					partial.err = errors.Join(first, original.ReleaseWithError())
				}
				result, err = captureStableTemplateResourcesForDB(database, partial, partial, provider.templateID, owner)
			} else {
				returned, cause := original, first
				if route == "nil-result-cleanup-receipt" {
					returned = nil
					cause = errors.Join(first, original.ReleaseWithError())
				}
				log := &leafPageLogWithRecordLengthHints{db: database, inner: &partialStableLeafProvider{resources: returned, err: cause}}
				if route == "production-child-ref-wrapper" {
					_, result, err = log.AppendPreparedLeafPageChildRefsWithStableResources([][]byte{{1}}, [][]byte{{1}}, nil)
				} else {
					_, result, err = log.AppendLeafPageWithStableResources(nil)
				}
			}
			if result != nil || !errors.Is(err, first) || !errors.Is(err, os.ErrClosed) || operations.closeCalls.Load() != 1 {
				t.Fatalf("lost original result=%p err=%v actual Close attempts=%d", result, err, operations.closeCalls.Load())
			}
			// The returned original receipt is retained through the existing DB graph.

			if !errors.Is(database.backgroundError(), prior) || !errors.Is(database.backgroundError(), os.ErrClosed) {
				t.Fatal("DB lost original cleanup custody")
			}
			if operations.file != file || operations.charge == 0 || owner.PhysicalClosed() {
				t.Fatal("failed physical payload refunded")
			}
			if repeated := original.ReleaseWithError(); !errors.Is(repeated, os.ErrClosed) || operations.closeCalls.Load() != 1 {
				t.Fatal("physical cleanup replayed")
			}
		})
	}
}

func TestSelectedEncodedClosureAdmitsAndPreservesExactBytes(t *testing.T) {
	provider := newTestStableTemplateProvider(t, []byte("template-selected-original"))
	original, err := provider.CaptureTemplateResources(context.Background(), provider.templateID)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	selected, err := rootpublication.ImportStableResourceSetMetadata(&owner, original)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Release()
	budget := &refCountsBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.cap = budget.bytes
	if err = ValidateStableTemplateResourceClosure(selected, provider.templateID, provider.definition); !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != before {
		t.Fatalf("admission %v bytes=%d/%d", err, owner.Bytes(), before)
	}
	budget.cap = 1 << 24
	if err = ValidateStableTemplateResourceClosure(selected, provider.templateID, provider.definition); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != before {
		t.Fatal("validation scratch retained")
	}
	if err = validateStableEncodedResourceClosure(selected, rootpublication.ReachabilityTemplateGeneration, provider.templateID, int64(len(provider.definition)), [32]byte{1}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("changed digest accepted: %v", err)
	}
}

// An ordinary refusal with successful cleanup is an operation failure, not
// fatal physical cleanup debt. The wrapper retains only actual original receipts.
func TestStableAppendRefusalDoesNotPoisonDB(t *testing.T) {
	for _, returned := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil-result", true: "consumed-original"}[returned], func(t *testing.T) {
			database, _ := openDurableRootClosureDB3928(t)
			provider := newTestStableTemplateProvider(t, []byte("ordinary-refusal"))
			var resources *rootpublication.StableResourceSet
			var err error
			if returned {
				resources, err = provider.CaptureTemplateResources(context.Background(), provider.templateID)
				if err != nil {
					t.Fatal(err)
				}
			}
			first := errors.New("ordinary provider refusal")
			log := &leafPageLogWithRecordLengthHints{db: database, inner: &partialStableLeafProvider{resources: resources, err: first}}
			_, result, err := log.AppendLeafPageWithStableResources(nil)
			if result != nil || !errors.Is(err, first) {
				t.Fatalf("refusal result=%p err=%v", result, err)
			}
			if debt := database.backgroundError(); debt != nil {
				t.Fatalf("ordinary refusal poisoned DB: %v", debt)
			}
			if resources != nil && resources.Owner() != rootpublication.ResourceOwnerReleased {
				t.Fatal("successful original cleanup not consumed")
			}
		})
	}
}
