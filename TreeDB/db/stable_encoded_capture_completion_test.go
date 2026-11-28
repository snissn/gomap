package db

import (
	"crypto/sha256"
	"errors"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

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
	for _, route := range []string{"production-leaf-wrapper", "production-child-ref-wrapper", "nil-result-cleanup-receipt"} {
		t.Run(route, func(t *testing.T) {
			database, path := openDurableRootClosureDB3928(t)
			owner := database.valueLogIdentityPins.MetadataOwner()
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
			payload := []byte("leaf-partial-original")
			token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
				MetadataOwner: owner, PinRegistry: database.valueLogIdentityPins, OwnedOperations: operations,
				Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "leaf_vlog", ResourceID: "partial-original", Generation: 1, DiagnosticPath: "original-leaf",
				File: file, ContentSynced: true, Frontier: rootpublication.DurableFrontier{Bytes: 4096}, Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
				LogicalObligations: []rootpublication.StableLogicalObligation{{Class: "outer-leaf", Kind: "outer-leaf", Namespace: "test", Generation: 1, FileID: 1, Length: int64(len(payload)), Reachability: rootpublication.ReachabilityOuterLeafRawPointer, Digest: sha256.Sum256(payload)}},
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
			prior := errors.New("preexisting unrelated background error")
			database.retainOriginalCleanupError(prior)

			returned, cause := original, first
			if route == "nil-result-cleanup-receipt" {
				returned = nil
				cause = errors.Join(first, original.ReleaseWithError())
			}
			log := &leafPageLogWithRecordLengthHints{db: database, inner: &partialStableLeafProvider{resources: returned, err: cause}}
			var result *rootpublication.StableResourceSet
			if route == "production-child-ref-wrapper" {
				_, result, err = log.AppendPreparedLeafPageChildRefsWithStableResources([][]byte{{1}}, [][]byte{{1}}, nil)
			} else {
				_, result, err = log.AppendLeafPageWithStableResources(nil)
			}
			if result != nil || !errors.Is(err, first) || !errors.Is(err, os.ErrClosed) || operations.closeCalls.Load() != 1 {
				t.Fatalf("lost original result=%p err=%v actual Close attempts=%d", result, err, operations.closeCalls.Load())
			}
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

// An ordinary refusal with successful cleanup is an operation failure, not
// fatal physical cleanup debt. The wrapper retains only actual original receipts.
func TestStableAppendRefusalDoesNotPoisonDB(t *testing.T) {
	for _, returned := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil-result", true: "consumed-original"}[returned], func(t *testing.T) {
			database, path := openDurableRootClosureDB3928(t)
			var resources *rootpublication.StableResourceSet
			if returned {
				file, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				payload := []byte("ordinary-refusal")
				token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
					Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "leaf_vlog", ResourceID: "ordinary-refusal", Generation: 1, DiagnosticPath: "ordinary-refusal",
					File: file, ContentSynced: true, Frontier: rootpublication.DurableFrontier{Bytes: 4096}, Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
					Digest: sha256.Sum256(payload),
				})
				_ = file.Close()
				if err != nil {
					t.Fatal(err)
				}
				builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer)
				if err := builder.Add(token); err != nil {
					token.Release()
					builder.Abandon()
					t.Fatal(err)
				}
				resources, err = builder.Freeze()
				if err != nil {
					builder.Abandon()
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
