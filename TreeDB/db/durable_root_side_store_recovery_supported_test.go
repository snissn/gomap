//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

func TestDurableRootPublicLayoutDictionaryDependencyReopenAndNewestSlotFallback(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	dictDir := filepath.Join(root, "dictdb")
	if err := os.MkdirAll(mainDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dictDir, 0o700); err != nil {
		t.Fatal(err)
	}

	dictionary := []byte("public-layout dictionary dependency")
	dictionaryID := uint64(9701)
	dictionaryProvider := newPublicLayoutDictionaryProviderV1(t, dictDir, dictionaryID, dictionary)

	mainOptions := func() Options {
		return Options{Dir: mainDir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	}

	main, err := Open(mainOptions())
	if err != nil {
		t.Fatalf("open main DB: %v", err)
	}
	if err := main.SetSync([]byte("fallback"), []byte("root")); err != nil {
		t.Fatalf("seed fallback generation: %v", err)
	}
	resources, err := dictionaryProvider.CaptureDictionaryResources(context.Background(), dictionaryID)
	if err != nil {
		t.Fatalf("capture sibling dictionary dependency: %v", err)
	}
	logical := dictionaryProvider.logicalObligation()
	_, roots, err := main.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot:         0,
			Iter:             mustFrozenRawMemtable(t, "side-layout/root", []byte("published")).NewIterator(nil, nil),
			DurableResources: resources,
			DurableResourceRequirements: rootpublication.StableLogicalObligationRequirements{
				ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityDictionaryGeneration},
				Obligations:  []rootpublication.StableLogicalObligation{logical},
			},
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				return nil, fmt.Errorf("maintenance roots=%v", rootIDs)
			}
			return mustFrozenSystemMemtable(t, "side-layout/descriptor", "published").NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("publish sibling dictionary dependency: %v", err)
	}
	if len(roots) != 1 || roots[0] == 0 {
		t.Fatalf("published roots=%v", roots)
	}
	if err := main.Checkpoint(); err != nil {
		t.Fatalf("checkpoint sibling dictionary dependency: %v", err)
	}
	latestCommit := main.State().CommitSeq
	fallbackSlot := uint64(1) - main.durableRoot.slot
	fallbackCommit := main.durableRoot.slotCommit[fallbackSlot]
	if fallbackCommit == 0 || fallbackCommit >= latestCommit {
		t.Fatalf("fallback commit=%d latest=%d slots=%v", fallbackCommit, latestCommit, main.durableRoot.slotCommit)
	}
	if err := main.Close(); err != nil {
		t.Fatalf("close main DB: %v", err)
	}

	intact, err := Open(mainOptions())
	if err != nil {
		t.Fatalf("reopen intact public sibling layout: %v", err)
	}
	if got := intact.State().CommitSeq; got != latestCommit {
		_ = intact.Close()
		t.Fatalf("intact reopen selected commit=%d want latest=%d", got, latestCommit)
	}
	if err := intact.Close(); err != nil {
		t.Fatal(err)
	}
	replaceFileWithCopyV1(t, filepath.Join(dictDir, indexFileName))

	fallback, err := Open(mainOptions())
	if err != nil {
		t.Fatalf("reopen main with newest dependency identity replaced: %v", err)
	}
	defer func() { _ = fallback.Close() }()
	if got := fallback.State().CommitSeq; got != fallbackCommit {
		t.Fatalf("identity mismatch selected commit=%d want bounded fallback=%d latest=%d", got, fallbackCommit, latestCommit)
	}
}

type publicLayoutDictionaryProviderV1 struct {
	file       *os.File
	dictionary []byte
	id         uint64
}

func (provider *publicLayoutDictionaryProviderV1) logicalObligation() rootpublication.StableLogicalObligation {
	digest := sha256.Sum256(provider.dictionary)
	return rootpublication.StableLogicalObligation{
		Class: "dictionary-generation", Kind: "dictionary", Namespace: "dictdb",
		Generation: provider.id, FileID: provider.id, Length: int64(len(provider.dictionary)),
		Reachability: rootpublication.ReachabilityDictionaryGeneration, Digest: digest,
	}
}

func newPublicLayoutDictionaryProviderV1(t *testing.T, dir string, id uint64, dictionary []byte) *publicLayoutDictionaryProviderV1 {
	t.Helper()
	path := filepath.Join(dir, indexFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(dictionary); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := rootpublication.SyncStableFile(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return &publicLayoutDictionaryProviderV1{file: file, dictionary: append([]byte(nil), dictionary...), id: id}
}

func (provider *publicLayoutDictionaryProviderV1) CaptureDictionaryResources(_ context.Context, id uint64) (*rootpublication.StableResourceSet, error) {
	if provider == nil || provider.file == nil || id != provider.id {
		return nil, fmt.Errorf("dictionary %d unavailable", id)
	}
	parent, err := os.Open(filepath.Dir(provider.file.Name()))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	parentGeneration, err := rootpublication.StableNamespaceParentGeneration(parent)
	if err != nil {
		return nil, err
	}
	namespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: provider.file, ParentGeneration: parentGeneration,
		Operation: rootpublication.NamespaceCreate, NewName: indexFileName, DiagnosticPath: indexFileName,
	})
	if err != nil {
		return nil, err
	}
	if err := namespace.Stabilize(); err != nil {
		namespace.Release()
		return nil, err
	}
	logical := provider.logicalObligation()
	token, err := rootpublication.NewStableProducerResourceTokenForDomain(
		rootpublication.StableProducerDictionary,
		rootpublication.StableResourceSpec{
			Kind: rootpublication.ResourceDictionary, LogicalLane: "dictdb/index", ResourceID: "index",
			Generation: id, DiagnosticPath: indexFileName, File: provider.file,
			Frontier:           rootpublication.DurableFrontier{Bytes: uint64(len(provider.dictionary))},
			Digest:             sha256.Sum256([]byte("public-layout-dictionary-index-v1")),
			Reachability:       rootpublication.ReachabilityDictionaryGeneration,
			LogicalObligations: []rootpublication.StableLogicalObligation{logical},
			Namespace:          namespace, ContentSynced: true,
		},
		"authoritative-transitive",
	)
	if err != nil {
		namespace.Release()
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

func replaceFileWithCopyV1(t *testing.T, path string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	temporary := path + ".replacement"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := rootpublication.SyncStableFile(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	syncErr := rootpublication.SyncStableNamespace(parent)
	closeErr := parent.Close()
	if syncErr != nil || closeErr != nil {
		t.Fatalf("persist replacement namespace: sync=%v close=%v", syncErr, closeErr)
	}
}
