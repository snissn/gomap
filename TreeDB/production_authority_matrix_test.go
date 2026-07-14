//go:build darwin || linux || freebsd || netbsd || openbsd

package treedb

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type productionAuthorityContext interface {
	Helper()
	TempDir() string
	Cleanup(func())
}

type productionAuthorityCapture func(productionAuthorityContext) (*rootpublication.StableResourceSet, error)

type productionAuthorityWitness struct {
	field          rootpublication.ReachabilityField
	kind           rootpublication.ResourceKind
	classification string
	capture        productionAuthorityCapture
}

// productionAuthorityWitnesses is a literal executable registry maintained
// independently of the canonical root-publication inventory.
var productionAuthorityWitnesses = []productionAuthorityWitness{
	{rootpublication.ReachabilityIndexFile, rootpublication.ResourceIndex, "authoritative", captureProductionIndexAuthority},
	{rootpublication.ReachabilityValueLogPointer, rootpublication.ResourceValueLog, "authoritative", captureProductionValueLogAuthority},
	{rootpublication.ReachabilityOuterLeafRawPointer, rootpublication.ResourceOuterLeafLog, "authoritative", captureProductionRawOuterLeafAuthority},
	{rootpublication.ReachabilityOuterLeafPackedPointer, rootpublication.ResourceOuterLeafPack, "authoritative", captureProductionPackedOuterLeafAuthority},
	{rootpublication.ReachabilityOuterLeafGeneration, rootpublication.ResourceOuterLeafManifest, "authoritative", captureProductionOuterLeafGenerationAuthority},
	{rootpublication.ReachabilityDictionaryGeneration, rootpublication.ResourceDictionary, "authoritative-transitive", captureProductionDictionaryAuthority},
	{rootpublication.ReachabilityTemplateGeneration, rootpublication.ResourceTemplate, "authoritative-transitive", captureProductionTemplateAuthority},
	{rootpublication.ReachabilityColumnManifest, rootpublication.ResourceColumnAsset, "authoritative", captureProductionColumnManifestAuthority},
	{rootpublication.ReachabilityTypedColumnMultipart, rootpublication.ResourceTypedColumnAsset, "authoritative", captureProductionTypedColumnMultipartAuthority},
	{rootpublication.ReachabilityTypedColumnValue, rootpublication.ResourceTypedColumnAsset, "authoritative", captureProductionTypedColumnValueAuthority},
	{rootpublication.ReachabilityTypedColumnCode, rootpublication.ResourceTypedColumnAsset, "authoritative", captureProductionTypedColumnCodeAuthority},
	{rootpublication.ReachabilityHNSWSearchPack, rootpublication.ResourceVectorGraphPack, "authoritative", captureProductionHNSWSearchPackAuthority},
	{rootpublication.ReachabilityVectorGraphPack, rootpublication.ResourceVectorGraphPack, "authoritative-transitive", captureProductionVectorGraphAuthority},
	{rootpublication.ReachabilityCommandWALActive, rootpublication.ResourceCommandWAL, "authoritative", captureProductionActiveCommandWALAuthority},
	{rootpublication.ReachabilityCommandWALRotated, rootpublication.ResourceCommandWAL, "authoritative", captureProductionRotatedCommandWALAuthority},
	{rootpublication.ReachabilityCommandWALExternalRIDFence, rootpublication.ResourceCommandWALExternalRID, "authoritative-transitive", captureProductionExternalRIDAuthority},
}

type productionAuthorityBehaviorWitness struct {
	field            rootpublication.ReachabilityField
	stability        rootpublication.ResourceStability
	producerAPI      string
	omissionEvidence string
}

type productionAuthorityCompositeOmissionWitness struct {
	name        string
	packagePath string
	testName    string
	fields      []rootpublication.ReachabilityField
}

// productionAuthorityBehaviorWitnesses keeps identity policy and focused
// composite-omission evidence literal instead of deriving either from policy.
var productionAuthorityBehaviorWitnesses = []productionAuthorityBehaviorWitness{
	{rootpublication.ReachabilityIndexFile, rootpublication.ResourceMutableAppend, "(*db.Snapshot).CaptureStableIndexFileResource", "TestCaptureStableIndexFileResourceRejectsReboundPathBeforeFreeze"},
	{rootpublication.ReachabilityValueLogPointer, rootpublication.ResourceMutableAppend, "(*valuelog.Writer).RotateToWithStableResources", "TestStableWriterCreationProofCannotBeOmitted"},
	{rootpublication.ReachabilityOuterLeafRawPointer, rootpublication.ResourceMutableAppend, "db.LeafPageStableLog.AppendLeafPageWithStableResources", "TestLeafPageStableWrapperRejectsMalformedProviderAuthority"},
	{rootpublication.ReachabilityOuterLeafPackedPointer, rootpublication.ResourceImmutable, "(*db.DB).PrepareLeafGenerationPackStableClosure", "TestLeafGenerationPackPromotionAuthorityRejectsMalformedPointers"},
	{rootpublication.ReachabilityOuterLeafGeneration, rootpublication.ResourceImmutable, "(*db.DB).PrepareLeafGenerationManifestStableClosure", "TestStableLeafGenerationManifestDestinationRebindFailsClosed"},
	{rootpublication.ReachabilityDictionaryGeneration, rootpublication.ResourceImmutable, "(*dictdb.Store).CaptureDictionaryResources", "TestCaptureDictionaryResourcesRejectsEachMissingPointerChild"},
	{rootpublication.ReachabilityTemplateGeneration, rootpublication.ResourceImmutable, "(*templatedb.Store).CaptureTemplateResources", "TestCaptureTemplateResourcesRejectsEachMissingPointerChild"},
	{rootpublication.ReachabilityColumnManifest, rootpublication.ResourceMutableAppend, "collections.AppendColumnPhysicalAssetsWithStableResources", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{rootpublication.ReachabilityTypedColumnMultipart, rootpublication.ResourceMutableAppend, "collections.AppendColumnPhysicalAssetsWithStableResources", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{rootpublication.ReachabilityTypedColumnValue, rootpublication.ResourceMutableAppend, "collections.AppendColumnPhysicalAssetsWithStableResources", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{rootpublication.ReachabilityTypedColumnCode, rootpublication.ResourceMutableAppend, "collections.AppendColumnPhysicalAssetsWithStableResources", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{rootpublication.ReachabilityHNSWSearchPack, rootpublication.ResourceMutableAppend, "collections.AppendColumnPhysicalAssetsWithStableResources", "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation"},
	{rootpublication.ReachabilityVectorGraphPack, rootpublication.ResourceMutableAppend, "(*collections.Collection).PrepareVectorIndexStableClosure", "TestColumnVectorGraphStableAuthorityRejectsEachMissingTransitiveChild"},
	{rootpublication.ReachabilityCommandWALActive, rootpublication.ResourceMutableAppend, "(*commitlog.CommandJournal).RotateActiveSegmentWithStableResources", "TestCommandJournalStableRotationRejectsEachMissingSegment"},
	{rootpublication.ReachabilityCommandWALRotated, rootpublication.ResourceMutableAppend, "(*commitlog.CommandJournal).RotateActiveSegmentWithStableResources", "TestCommandJournalStableRotationRejectsEachMissingSegment"},
	{rootpublication.ReachabilityCommandWALExternalRIDFence, rootpublication.ResourceMutableAppend, "(*valuelog.Manager).CaptureStableExternalRIDFence", "TestCaptureStableExternalRIDFenceRequiresEveryManagerChild"},
}

// productionAuthorityCompositeOmissionWitnesses is a literal execution
// matrix for the producer-owned transitive closures. These focused tests use
// the real dictionary, template, column, vector, and value-log producers and
// fail closed with ErrUnresolvedResource when each physical child is omitted.
// Keep the package and test names explicit: this matrix must not derive its
// coverage from the canonical inventory or merely discover source symbols.
var productionAuthorityCompositeOmissionWitnesses = []productionAuthorityCompositeOmissionWitness{
	{
		name:        "dictionary-index-and-value-log",
		packagePath: "./internal/dictdb",
		testName:    "TestCaptureDictionaryResourcesRejectsEachMissingPointerChild",
		fields:      []rootpublication.ReachabilityField{rootpublication.ReachabilityDictionaryGeneration},
	},
	{
		name:        "template-index-and-value-log",
		packagePath: "./internal/templatedb",
		testName:    "TestCaptureTemplateResourcesRejectsEachMissingPointerChild",
		fields:      []rootpublication.ReachabilityField{rootpublication.ReachabilityTemplateGeneration},
	},
	{
		name:        "typed-column-physical-assets",
		packagePath: "./collections",
		testName:    "TestStableColumnPreparedValidationRejectsEachMissingProductionObligation",
		fields: []rootpublication.ReachabilityField{
			rootpublication.ReachabilityColumnManifest,
			rootpublication.ReachabilityTypedColumnMultipart,
			rootpublication.ReachabilityTypedColumnValue,
			rootpublication.ReachabilityTypedColumnCode,
			rootpublication.ReachabilityHNSWSearchPack,
		},
	},
	{
		name:        "vector-graph-transitive-assets",
		packagePath: "./collections",
		testName:    "TestColumnVectorGraphStableAuthorityRejectsEachMissingTransitiveChild",
		fields:      []rootpublication.ReachabilityField{rootpublication.ReachabilityVectorGraphPack},
	},
	{
		name:        "external-rid-segments-and-rid-mappings",
		packagePath: "./internal/valuelog",
		testName:    "TestCaptureStableExternalRIDFenceRequiresEveryManagerChild",
		fields:      []rootpublication.ReachabilityField{rootpublication.ReachabilityCommandWALExternalRIDFence},
	},
}

type productionAuthorityNegativeWitness struct {
	field          rootpublication.ReachabilityField
	kind           rootpublication.ResourceKind
	classification string
	reject         func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)
}

// productionAuthorityNegativeWitnesses is a literal excluded-field registry.
var productionAuthorityNegativeWitnesses = []productionAuthorityNegativeWitness{
	{rootpublication.ReachabilityMetaPage, rootpublication.ResourceIndex, "adjacent-root-publication", backenddb.NewStableDBResourceToken},
	{rootpublication.ReachabilityUserRoot, rootpublication.ResourceIndex, "adjacent-root-publication", backenddb.NewStableDBResourceToken},
	{rootpublication.ReachabilitySystemRoot, rootpublication.ResourceIndex, "adjacent-root-publication", backenddb.NewStableDBResourceToken},
	{rootpublication.ReachabilityFreelist, rootpublication.ResourceIndex, "adjacent-freelist-publication", backenddb.NewStableDBResourceToken},
	{rootpublication.ReachabilityCollectionSystemRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionPrimaryRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionTemplateRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionIndexStateRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionColumnRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionSecondaryRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionVectorRoot, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionTextDictionary, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionTextPosting, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityCollectionTextPosition, rootpublication.ResourceIndex, "adjacent-root-publication", collections.NewStableCollectionResourceToken},
	{rootpublication.ReachabilityLegacyVectorSnapshot, rootpublication.ResourceLegacyVectorSnapshot, "rebuildable-non-authoritative", collections.NewStableLegacyVectorResourceToken},
	{rootpublication.ReachabilityQueryReadyBase, rootpublication.ResourceQueryReadyAsset, "rebuildable-non-authoritative", collections.NewStableColumnAssetResourceToken},
	{rootpublication.ReachabilityQueryReadyDelta, rootpublication.ResourceQueryReadyAsset, "rebuildable-non-authoritative", collections.NewStableColumnAssetResourceToken},
	{rootpublication.ReachabilityQueryReadyConsolidatedBase, rootpublication.ResourceQueryReadyAsset, "rebuildable-non-authoritative", collections.NewStableColumnAssetResourceToken},
	{rootpublication.ReachabilityLegacyActiveSlab, rootpublication.ResourceLegacyTreeDBField, "explicit-legacy-exclusion", rejectProductionLegacySlabAuthority},
	{rootpublication.ReachabilityRaftSnapshot, rootpublication.ResourceSeparateDurability, "explicit-separate-domain", rejectProductionRaftSnapshotAuthority},
}

func rejectProductionLegacySlabAuthority(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerLegacyExcluded, spec, "explicit-legacy-exclusion")
}

func rejectProductionRaftSnapshotAuthority(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerRaftSnapshot, spec, "explicit-separate-domain")
}

func freezeProductionAuthorityToken(field rootpublication.ReachabilityField, token *rootpublication.StableResourceToken) (*rootpublication.StableResourceSet, error) {
	if token == nil {
		return nil, fmt.Errorf("%w: producer returned no token for %q", rootpublication.ErrUnresolvedResource, field)
	}
	builder := rootpublication.NewStableResourceSetBuilder(field)
	if err := builder.Add(token); err != nil {
		token.Release()
		builder.Abandon()
		return nil, err
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	return resources, nil
}

func captureProductionIndexAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		return nil, fmt.Errorf("%w: AcquireStableSnapshot returned nil", rootpublication.ErrUnresolvedResource)
	}
	token, err := snapshot.CaptureStableIndexFileResource()
	if err != nil {
		_ = snapshot.Close()
		return nil, err
	}
	return freezeProductionAuthorityToken(rootpublication.ReachabilityIndexFile, token)
}

func captureProductionValueLogAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.Append(0, nil, 1, []byte(strings.Repeat("production-value-log-", 64))); err != nil {
		return nil, err
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, true,
		valuelog.StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		valuelog.StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if err != nil {
		return nil, err
	}
	token := rotation.TakeClosed()
	rotation.Release()
	return freezeProductionAuthorityToken(rootpublication.ReachabilityValueLogPointer, token)
}

func buildProductionAuthorityLeafPage(t productionAuthorityContext, tag byte) ([]byte, error) {
	t.Helper()
	buf := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		key := []byte{'p', 'r', 'o', 'd', '-', tag, '-', byte('a' + i)}
		value := []byte{'v', 'a', 'l', 'u', 'e', '-', tag, '-', byte('a' + i)}
		if err := builder.AddLeafEntry(key, value, node.FlagInline, page.ValuePtr{}); err != nil {
			return nil, fmt.Errorf("build production authority leaf entry %d: %w", i, err)
		}
	}
	builder.FinishNoNode()
	return buf, nil
}

func captureProductionRawOuterLeafAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	dir := t.TempDir()
	database, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		return nil, err
	}
	log, err := backenddb.NewStandaloneLeafPageLog(dir, backenddb.StandaloneLeafPageLogOptions{Compression: backenddb.ValueLogCompressionOff})
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	database.SetLeafPageLog(log)
	t.Cleanup(func() {
		_ = database.Close()
		_ = log.Close()
	})
	stable, ok := log.(backenddb.LeafPageStableLog)
	if !ok {
		return nil, fmt.Errorf("%w: standalone leaf producer %T has no stable append", rootpublication.ErrUnresolvedResource, log)
	}
	leafPage, err := buildProductionAuthorityLeafPage(t, 'r')
	if err != nil {
		return nil, err
	}
	_, resources, err := stable.AppendLeafPageWithStableResources(leafPage)
	return resources, err
}

func captureProductionPackedOuterLeafAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	leafPage, err := buildProductionAuthorityLeafPage(t, 'p')
	if err != nil {
		return nil, err
	}
	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{leafPage})
	if err != nil {
		return nil, err
	}
	if closure == nil || len(closure.Segments()) == 0 || closure.Observations().ContentSyncs == 0 {
		if closure != nil {
			closure.Release()
		}
		return nil, fmt.Errorf("%w: packed producer returned no physical promotion", rootpublication.ErrUnresolvedResource)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		return nil, err
	}
	return resources, nil
}

func captureProductionOuterLeafGenerationAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	closure, err := database.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		return nil, err
	}
	if closure == nil || closure.Revision() == 0 || closure.Digest() == ([32]byte{}) || closure.Observations().ContentSyncs == 0 {
		if closure != nil {
			closure.Release()
		}
		return nil, fmt.Errorf("%w: manifest producer returned incomplete replacement", rootpublication.ErrUnresolvedResource)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		return nil, err
	}
	return resources, nil
}

func captureProductionDictionaryAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	store := dictdb.New(database.backend)
	t.Cleanup(func() { _ = store.Close() })
	dictionary := make([]byte, 4096)
	for i := range dictionary {
		dictionary[i] = byte(i*31 + 7)
	}
	dictionaryID, err := store.PutDictBytes(context.Background(), dictionary)
	if err != nil {
		return nil, err
	}
	return store.CaptureDictionaryResources(context.Background(), dictionaryID)
}

func captureProductionTemplateAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	store := templatedb.New(templateKV{db: database}, templatedb.Config{})
	definition := make([]byte, 4096)
	for i := range definition {
		definition[i] = byte(i*17 + 11)
	}
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		return nil, err
	}
	return store.CaptureTemplateResources(context.Background(), templateID)
}

func captureProductionColumnManifestAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionColumnAssetAuthority(t, collections.ColumnAssetKindTCS1PartImage)
}

func captureProductionTypedColumnMultipartAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionColumnAssetAuthority(t, collections.ColumnAssetKindTCS1TypedColumnPart)
}

func captureProductionTypedColumnValueAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionColumnAssetAuthority(t, collections.ColumnAssetKindTCS1Int64Values)
}

func captureProductionTypedColumnCodeAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionColumnAssetAuthority(t, collections.ColumnAssetKindTCS1DictionaryCodes)
}

func captureProductionHNSWSearchPackAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionColumnAssetAuthority(t, collections.ColumnAssetKindTCS1HNSWSearchPack)
}

func captureProductionColumnAssetAuthority(t productionAuthorityContext, kind collections.ColumnAssetKind) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	cfg := collections.ColumnStoreConfig{Enabled: true, AssetManager: &collections.ColumnAssetManagerConfig{
		Kind: collections.ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "production-authority",
	}}
	fileID, err := productionAuthorityColumnFileID(kind)
	if err != nil {
		return nil, err
	}
	lease, err := database.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	_, resources, err := collections.AppendColumnPhysicalAssetsWithStableResources(
		database.ColumnAssetRootDir(), cfg, fileID,
		[]collections.StableColumnPhysicalAssetAppend{{
			Payload: []byte(strings.Repeat("production-column-asset-", 32)), Kind: kind, Generation: 11, PartID: 13,
		}}, database.StableResourceIdentityPinRegistry(), lease,
	)
	return resources, err
}

func productionAuthorityColumnFileID(kind collections.ColumnAssetKind) (uint32, error) {
	switch kind {
	case collections.ColumnAssetKindTCS1PartImage:
		return 37, nil
	case collections.ColumnAssetKindTCS1TypedColumnPart:
		return 38, nil
	case collections.ColumnAssetKindTCS1Int64Values:
		return 39, nil
	case collections.ColumnAssetKindTCS1DictionaryCodes:
		return 40, nil
	case collections.ColumnAssetKindTCS1HNSWSearchPack:
		return 41, nil
	default:
		return 0, fmt.Errorf("%w: no physical file ID for column kind %q", rootpublication.ErrUnresolvedResource, kind)
	}
}

func captureProductionVectorGraphAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return nil, err
	}
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	manager := collections.NewCollectionManager(database)
	meta := collections.CollectionMeta{
		Name: "docs",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled: true,
				Columns: []collections.ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
					{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 3},
				},
				SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine,
			Dimensions: 3, M: 2, Strategy: collections.VectorIndexStrategyColumnGraph,
		}},
	}
	if _, err := manager.CreateCollection(&meta); err != nil {
		return nil, err
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		return nil, err
	}
	ids := [][]byte{[]byte("doc-a"), []byte("doc-b")}
	docs := make([][]byte, len(ids))
	for i, vector := range [][]float32{{1, 0, 0}, {0, 1, 0}} {
		docs[i], err = json.Marshal(map[string]any{"time_us": i + 1, "embedding": vector})
		if err != nil {
			return nil, err
		}
	}
	if _, err := collection.InsertBatch(ids, docs); err != nil {
		return nil, err
	}
	if err := collection.Flush(); err != nil {
		return nil, err
	}
	closure, err := collection.PrepareVectorIndexStableClosure("embedding_graph")
	if err != nil {
		return nil, err
	}
	observed := closure.Observations()
	if observed.Segments == 0 || observed.Descriptors == 0 || observed.LogicalObligations == 0 {
		closure.Release()
		return nil, fmt.Errorf("%w: vector producer returned incomplete physical closure: %+v", rootpublication.ErrUnresolvedResource, observed)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		return nil, err
	}
	return resources, nil
}

func captureProductionActiveCommandWALAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionCommandWALField(t, rootpublication.ReachabilityCommandWALActive)
}

func captureProductionRotatedCommandWALAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	return captureProductionCommandWALField(t, rootpublication.ReachabilityCommandWALRotated)
}

func captureProductionCommandWALField(t productionAuthorityContext, field rootpublication.ReachabilityField) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	journal, err := commitlog.OpenCommandJournal(filepath.Join(t.TempDir(), "wal"), commitlog.CommandJournalOptions{Lane: 3})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = journal.Close() })
	if _, err := journal.AppendCommand(commitlog.CommandEnvelope{
		Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}); err != nil {
		return nil, err
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		return nil, err
	}
	var stableToken *rootpublication.StableResourceToken
	if field == rootpublication.ReachabilityCommandWALActive {
		stableToken = rotation.TakeActive()
	} else {
		stableToken = rotation.TakeClosed()
	}
	rotation.Release()
	return freezeProductionAuthorityToken(field, stableToken)
}

func captureProductionExternalRIDAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, error) {
	t.Helper()
	dir := t.TempDir()
	segmentRIDs := [][]uint64{{9, 2}, {14, 4}}
	segmentPointers := make([][]page.ValuePtr, len(segmentRIDs))
	fileIDs := make([]uint32, len(segmentRIDs))
	for i := range fileIDs {
		fileID, err := valuelog.EncodeFileID(1, uint32(i+1))
		if err != nil {
			return nil, err
		}
		fileIDs[i] = fileID
		writer, err := valuelog.NewWriter(valuelog.SegmentPath(dir, fileID), fileID)
		if err != nil {
			return nil, err
		}
		for _, rid := range segmentRIDs[i] {
			ptr, appendErr := writer.Append(0, nil, rid, []byte("production-external-rid-child"))
			if appendErr != nil {
				_ = writer.Close()
				return nil, appendErr
			}
			segmentPointers[i] = append(segmentPointers[i], ptr)
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
	}
	manager, err := valuelog.NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = manager.Close() })
	children := []valuelog.StableExternalRIDSegment{
		{FileID: fileIDs[0], RIDs: []uint64{9, 2}, Pointers: segmentPointers[0], Digest: sha256.Sum256([]byte("segment-1"))},
		{FileID: fileIDs[1], RIDs: []uint64{14, 4}, Pointers: segmentPointers[1], Digest: sha256.Sum256([]byte("segment-2"))},
	}
	fence, err := valuelog.NewStableExternalRIDFence([]uint64{14, 9, 4, 2})
	if err != nil {
		return nil, err
	}
	return manager.CaptureStableExternalRIDFence(fence, children)
}

func TestProductionAuthorityWitnessRegistryExactEquality(t *testing.T) {
	got := make([]rootpublication.ReachabilityField, 0, len(productionAuthorityWitnesses))
	seen := make(map[rootpublication.ReachabilityField]struct{}, len(productionAuthorityWitnesses))
	for _, witness := range productionAuthorityWitnesses {
		if witness.field == "" || witness.kind == "" || witness.classification == "" || witness.capture == nil {
			t.Fatalf("incomplete production witness: %+v", witness)
		}
		if _, duplicate := seen[witness.field]; duplicate {
			t.Fatalf("duplicate production witness for %q", witness.field)
		}
		policy, ok := rootpublication.StableResourcePolicyFor(witness.field)
		if !ok || !policy.Registerable || policy.Kind != witness.kind || policy.Classification != witness.classification {
			t.Fatalf("positive witness %q disagrees with canonical policy: witness=%+v policy=%+v", witness.field, witness, policy)
		}
		seen[witness.field] = struct{}{}
		got = append(got, witness.field)
	}
	want := rootpublication.RequiredReachabilityFields()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("independent positive fields=%q canonical registerable fields=%q", got, want)
	}
}

func TestProductionAuthorityNegativeRegistryExactEquality(t *testing.T) {
	got := make([]rootpublication.ReachabilityField, 0, len(productionAuthorityNegativeWitnesses))
	seen := make(map[rootpublication.ReachabilityField]struct{}, len(productionAuthorityNegativeWitnesses))
	for _, witness := range productionAuthorityNegativeWitnesses {
		if _, duplicate := seen[witness.field]; duplicate {
			t.Fatalf("duplicate negative witness for %q", witness.field)
		}
		seen[witness.field] = struct{}{}
		got = append(got, witness.field)
		policy, ok := rootpublication.StableResourcePolicyFor(witness.field)
		if !ok || policy.Registerable || policy.Kind != witness.kind || policy.Classification != witness.classification {
			t.Fatalf("negative witness %q disagrees with canonical policy: witness=%+v policy=%+v", witness.field, witness, policy)
		}
		token, err := witness.reject(rootpublication.StableResourceSpec{Kind: witness.kind, Reachability: witness.field})
		if token != nil {
			token.Release()
			t.Fatalf("excluded field %q returned authority", witness.field)
		}
		if !errors.Is(err, rootpublication.ErrResourceExcluded) {
			t.Fatalf("excluded field %q error=%v want ErrResourceExcluded", witness.field, err)
		}
	}
	want := make([]rootpublication.ReachabilityField, 0, len(productionAuthorityNegativeWitnesses))
	for _, row := range rootpublication.StableResourceInventory() {
		policy, ok := rootpublication.StableResourcePolicyFor(row.Field)
		if !ok {
			t.Fatalf("inventory field %q has no policy", row.Field)
		}
		if !policy.Registerable {
			want = append(want, row.Field)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("independent negative fields=%q canonical excluded fields=%q", got, want)
	}
}

func TestProductionAuthorityBehaviorRegistryExactEquality(t *testing.T) {
	if len(productionAuthorityBehaviorWitnesses) != 16 {
		t.Fatalf("literal behavior rows=%d want 16", len(productionAuthorityBehaviorWitnesses))
	}
	positive := make(map[rootpublication.ReachabilityField]productionAuthorityWitness, len(productionAuthorityWitnesses))
	for _, witness := range productionAuthorityWitnesses {
		positive[witness.field] = witness
	}
	testFunctions := productionAuthorityTestFunctionNames(t)
	seen := make(map[rootpublication.ReachabilityField]struct{}, len(productionAuthorityBehaviorWitnesses))
	for _, behavior := range productionAuthorityBehaviorWitnesses {
		if behavior.field == "" || behavior.producerAPI == "" || behavior.omissionEvidence == "" {
			t.Fatalf("incomplete behavior row: %+v", behavior)
		}
		if _, duplicate := seen[behavior.field]; duplicate {
			t.Fatalf("duplicate behavior row for %q", behavior.field)
		}
		if _, ok := positive[behavior.field]; !ok {
			t.Fatalf("behavior row %q has no executable positive callback", behavior.field)
		}
		policy, ok := rootpublication.StableResourcePolicyFor(behavior.field)
		if !ok || policy.Stability != behavior.stability {
			t.Fatalf("behavior %q stability=%v canonical=%+v", behavior.field, behavior.stability, policy)
		}
		if _, ok := testFunctions[behavior.omissionEvidence]; !ok {
			t.Fatalf("behavior %q omission evidence %q is not an executable repository test", behavior.field, behavior.omissionEvidence)
		}
		seen[behavior.field] = struct{}{}
	}
	if len(seen) != len(positive) {
		t.Fatalf("behavior rows=%d positive rows=%d", len(seen), len(positive))
	}
}

func TestProductionAuthorityExecutableCompositeOmissionMatrix(t *testing.T) {
	behaviorByField := make(map[rootpublication.ReachabilityField]productionAuthorityBehaviorWitness, len(productionAuthorityBehaviorWitnesses))
	for _, behavior := range productionAuthorityBehaviorWitnesses {
		behaviorByField[behavior.field] = behavior
	}

	var gotFields []rootpublication.ReachabilityField
	seenNames := make(map[string]struct{}, len(productionAuthorityCompositeOmissionWitnesses))
	for _, witness := range productionAuthorityCompositeOmissionWitnesses {
		witness := witness
		if witness.name == "" || witness.packagePath == "" || witness.testName == "" || len(witness.fields) == 0 {
			t.Fatalf("incomplete executable composite omission row: %+v", witness)
		}
		if _, duplicate := seenNames[witness.name]; duplicate {
			t.Fatalf("duplicate executable composite omission row %q", witness.name)
		}
		seenNames[witness.name] = struct{}{}
		for _, field := range witness.fields {
			behavior, ok := behaviorByField[field]
			if !ok {
				t.Fatalf("composite omission row %q field %q has no literal behavior witness", witness.name, field)
			}
			if behavior.omissionEvidence != witness.testName {
				t.Fatalf("composite omission row %q field %q executes %q but behavior registry names %q",
					witness.name, field, witness.testName, behavior.omissionEvidence)
			}
			gotFields = append(gotFields, field)
		}

		t.Run(witness.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			command := exec.CommandContext(ctx, "go", "test", witness.packagePath,
				"-run", "^"+regexp.QuoteMeta(witness.testName)+"$", "-count=1", "-v")
			command.Env = append(os.Environ(), "GOWORK=off")
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("execute producer-owned omission test %s.%s: %v\n%s",
					witness.packagePath, witness.testName, ctx.Err(), output)
			}
			if err != nil {
				t.Fatalf("execute producer-owned omission test %s.%s: %v\n%s",
					witness.packagePath, witness.testName, err, output)
			}
			text := string(output)
			if !strings.Contains(text, "=== RUN   "+witness.testName) || strings.Contains(text, "--- SKIP: "+witness.testName) {
				t.Fatalf("producer-owned omission test %s.%s did not execute to completion:\n%s",
					witness.packagePath, witness.testName, output)
			}
		})
	}

	wantFields := []rootpublication.ReachabilityField{
		rootpublication.ReachabilityDictionaryGeneration,
		rootpublication.ReachabilityTemplateGeneration,
		rootpublication.ReachabilityColumnManifest,
		rootpublication.ReachabilityTypedColumnMultipart,
		rootpublication.ReachabilityTypedColumnValue,
		rootpublication.ReachabilityTypedColumnCode,
		rootpublication.ReachabilityHNSWSearchPack,
		rootpublication.ReachabilityVectorGraphPack,
		rootpublication.ReachabilityCommandWALExternalRIDFence,
	}
	slices.Sort(gotFields)
	slices.Sort(wantFields)
	if !slices.Equal(gotFields, wantFields) {
		t.Fatalf("executable composite omission fields=%q want literal transitive fields=%q", gotFields, wantFields)
	}
}

func productionAuthorityTestFunctionNames(t testing.TB) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	fileSet := token.NewFileSet()
	err := fs.WalkDir(os.DirFS("."), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv == nil && strings.HasPrefix(function.Name.Name, "Test") {
				names[function.Name.Name] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan focused omission evidence: %v", err)
	}
	return names
}

func TestProductionAuthorityRealProducerCaptureMatrix(t *testing.T) {
	if len(productionAuthorityWitnesses) != 16 || len(productionAuthorityNegativeWitnesses) != 20 {
		t.Fatalf("literal matrix positives=%d negatives=%d want 16/20", len(productionAuthorityWitnesses), len(productionAuthorityNegativeWitnesses))
	}
	supported := 0
	for _, witness := range productionAuthorityWitnesses {
		witness := witness
		t.Run(string(witness.field), func(t *testing.T) {
			resources, err := witness.capture(t)
			if productionAuthorityExpectedUnsupported(witness, err) {
				if resources != nil {
					resources.Release()
					t.Fatal("unsupported packed producer returned authority")
				}
				return
			}
			if err != nil {
				t.Fatalf("real producer capture: %v", err)
			}
			if err := validateProductionAuthorityCapture(witness, resources); err != nil {
				resources.Release()
				t.Fatal(err)
			}
			supported++
			assertProductionAuthorityReleaseToZero(t, resources)
		})
	}
	want := 16
	if runtime.GOOS == "darwin" {
		want = 15
	}
	if runtime.GOOS != "darwin" && (!rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported()) {
		want = 15
	}
	if runtime.GOOS == "linux" {
		want = 16
	}
	if supported != want {
		t.Fatalf("supported real producer rows=%d want %d on %s", supported, want, runtime.GOOS)
	}
}

func productionAuthorityExpectedUnsupported(witness productionAuthorityWitness, err error) bool {
	return witness.field == rootpublication.ReachabilityOuterLeafPackedPointer &&
		errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported)
}

func validateProductionAuthorityCapture(witness productionAuthorityWitness, resources *rootpublication.StableResourceSet) error {
	if resources == nil || resources.Owner() != rootpublication.ResourceOwnerBuilder {
		return fmt.Errorf("%w: real %q producer returned no builder-owned closure", rootpublication.ErrUnresolvedResource, witness.field)
	}
	covered := false
	for _, descriptor := range resources.Descriptors() {
		if descriptor.Identity() == (rootpublication.StableIdentity{}) {
			return fmt.Errorf("%w: %q descriptor has no physical identity", rootpublication.ErrUnresolvedResource, witness.field)
		}
		if descriptor.Frontier().Bytes == 0 && witness.field != rootpublication.ReachabilityCommandWALActive {
			return fmt.Errorf("%w: %q descriptor has no physical byte frontier", rootpublication.ErrUnresolvedResource, witness.field)
		}
		if slices.Contains(descriptor.ReachabilityFields(), witness.field) {
			if descriptor.Kind() != witness.kind {
				return fmt.Errorf("%w: %q descriptor kind=%q want %q", rootpublication.ErrResourceConflict, witness.field, descriptor.Kind(), witness.kind)
			}
			covered = true
		}
	}
	if !covered {
		return fmt.Errorf("%w: real producer closure does not cover %q", rootpublication.ErrUnresolvedResource, witness.field)
	}
	return nil
}

func TestProductionAuthorityFullClosureAndAllButOneRetryMatrix(t *testing.T) {
	full, supported, err := captureProductionFullAuthority(t)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Release()
	if full.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatalf("full closure owner=%v want builder", full.Owner())
	}
	if got, want := productionAuthorityCoveredFields(full), productionAuthorityWitnessFields(supported); !slices.Equal(got, want) {
		t.Fatalf("full closure fields=%q supported literal fields=%q", got, want)
	}

	for index, omitted := range supported {
		omitted := omitted
		t.Run(fmt.Sprintf("all-but-one-%02d-%s", index, omitted.field), func(t *testing.T) {
			required := productionAuthorityWitnessFields(supported)
			builder := rootpublication.NewStableResourceSetBuilder(required...)
			defer builder.Abandon()
			for _, included := range supported {
				if included.field == omitted.field {
					continue
				}
				resources, err := included.capture(t)
				if err != nil {
					t.Fatalf("capture included %q: %v", included.field, err)
				}
				if err := validateProductionAuthorityCapture(included, resources); err != nil {
					resources.Release()
					t.Fatal(err)
				}
				if err := builder.Merge(resources); err != nil {
					resources.Release()
					t.Fatalf("merge included %q: %v", included.field, err)
				}
			}

			candidate, freezeErr := builder.Freeze()
			if candidate != nil {
				candidate.Release()
				t.Fatalf("omitting %q exposed a candidate before visibility", omitted.field)
			}
			if !productionAuthorityMissingField(freezeErr, omitted.field) {
				t.Fatalf("omitting %q error=%v want exact typed missing field", omitted.field, freezeErr)
			}
			if builder.State() != rootpublication.ResourceOwnerBuilder {
				t.Fatalf("omitting %q changed live builder state=%v", omitted.field, builder.State())
			}

			// Freeze failure is retryable on the same builder: merge the exact real
			// producer that was missing, then expose one complete candidate.
			resources, err := omitted.capture(t)
			if err != nil {
				t.Fatalf("capture retry %q: %v", omitted.field, err)
			}
			if err := validateProductionAuthorityCapture(omitted, resources); err != nil {
				resources.Release()
				t.Fatal(err)
			}
			if err := builder.Merge(resources); err != nil {
				resources.Release()
				t.Fatalf("merge retry %q: %v", omitted.field, err)
			}
			candidate, err = builder.Freeze()
			if err != nil {
				t.Fatalf("retry freeze %q: %v", omitted.field, err)
			}
			assertProductionAuthorityReleaseToZero(t, candidate)
		})
	}
}

func captureProductionFullAuthority(t productionAuthorityContext) (*rootpublication.StableResourceSet, []productionAuthorityWitness, error) {
	t.Helper()
	supported := make([]productionAuthorityWitness, 0, len(productionAuthorityWitnesses))
	for _, witness := range productionAuthorityWitnesses {
		if witness.field == rootpublication.ReachabilityOuterLeafPackedPointer &&
			(!rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported()) {
			continue
		}
		supported = append(supported, witness)
	}
	builder := rootpublication.NewStableResourceSetBuilder(productionAuthorityWitnessFields(supported)...)
	for _, witness := range supported {
		resources, err := witness.capture(t)
		if err != nil {
			builder.Abandon()
			return nil, nil, fmt.Errorf("capture full %q: %w", witness.field, err)
		}
		if err := validateProductionAuthorityCapture(witness, resources); err != nil {
			resources.Release()
			builder.Abandon()
			return nil, nil, err
		}
		if err := builder.Merge(resources); err != nil {
			resources.Release()
			builder.Abandon()
			return nil, nil, fmt.Errorf("merge full %q: %w", witness.field, err)
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, nil, err
	}
	return resources, supported, nil
}

func productionAuthorityWitnessFields(witnesses []productionAuthorityWitness) []rootpublication.ReachabilityField {
	fields := make([]rootpublication.ReachabilityField, len(witnesses))
	for i, witness := range witnesses {
		fields[i] = witness.field
	}
	slices.Sort(fields)
	return fields
}

func productionAuthorityCoveredFields(resources *rootpublication.StableResourceSet) []rootpublication.ReachabilityField {
	seen := make(map[rootpublication.ReachabilityField]struct{})
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			seen[field] = struct{}{}
		}
	}
	fields := make([]rootpublication.ReachabilityField, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return fields
}

func productionAuthorityMissingField(err error, field rootpublication.ReachabilityField) bool {
	return errors.Is(err, rootpublication.ErrUnresolvedResource) && strings.Contains(err.Error(), fmt.Sprintf("%q", field))
}

type productionAuthorityNamespaceTestContext struct {
	t        *testing.T
	root     string
	next     int
	cleanups []func()
}

func newProductionAuthorityNamespaceTestContext(t *testing.T) *productionAuthorityNamespaceTestContext {
	t.Helper()
	return &productionAuthorityNamespaceTestContext{t: t, root: t.TempDir()}
}

func (context *productionAuthorityNamespaceTestContext) Helper() { context.t.Helper() }

func (context *productionAuthorityNamespaceTestContext) TempDir() string {
	context.t.Helper()
	context.next++
	dir := filepath.Join(context.root, fmt.Sprintf("producer-%03d", context.next))
	if err := os.Mkdir(dir, 0o755); err != nil {
		context.t.Fatal(err)
	}
	return dir
}

func (context *productionAuthorityNamespaceTestContext) Cleanup(cleanup func()) {
	context.cleanups = append(context.cleanups, cleanup)
}

func (context *productionAuthorityNamespaceTestContext) shutdown() {
	for index := len(context.cleanups) - 1; index >= 0; index-- {
		context.cleanups[index]()
	}
	context.cleanups = nil
}

type productionAuthorityNamespaceFileWitness struct {
	token      *rootpublication.StableResourceToken
	descriptor rootpublication.StableResourceDescriptor
	path       string
	relative   string
	data       []byte
}

type productionAuthorityRecoveryManifest struct {
	Root         string                                  `json:"root"`
	Dependencies []productionAuthorityRecoveryDependency `json:"dependencies,omitempty"`
}

type productionAuthorityRecoveryDependency struct {
	PowerLossKind  powerlossoracle.ResourceKind        `json:"power_loss_kind"`
	DirtyID        string                              `json:"dirty_id"`
	ResourceKind   rootpublication.ResourceKind        `json:"resource_kind"`
	ResourceID     string                              `json:"resource_id"`
	LogicalLane    string                              `json:"logical_lane"`
	Generation     uint64                              `json:"generation"`
	Reachability   []rootpublication.ReachabilityField `json:"reachability"`
	DiagnosticPath string                              `json:"diagnostic_path"`
	SourceIdentity string                              `json:"source_identity"`
	Path           string                              `json:"path"`
	Size           int                                 `json:"size"`
	SHA256         string                              `json:"sha256"`
}

func productionAuthorityNamespaceWitness(root string, token *rootpublication.StableResourceToken, descriptor rootpublication.StableResourceDescriptor) (productionAuthorityNamespaceFileWitness, error) {
	if token == nil || token.Namespace() == nil {
		return productionAuthorityNamespaceFileWitness{}, fmt.Errorf("namespace witness requires a namespace-bearing token")
	}
	if token.Kind() != descriptor.Kind() || token.Generation() != descriptor.Generation() ||
		!rootpublication.SamePhysicalIdentity(token.Identity(), descriptor.Identity()) {
		return productionAuthorityNamespaceFileWitness{}, fmt.Errorf("token/descriptor identity mismatch for %q", token.ResourceID())
	}
	var matches []productionAuthorityNamespaceFileWitness
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !entry.Type().IsRegular() || entry.Name() == "LOCK" || entry.Name() == "command-wal-journal-owner.lock" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		identity, err := rootpublication.StableIdentityFromFile(file)
		if err != nil {
			_ = file.Close()
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, token.Identity()) {
			return file.Close()
		}
		parent, err := os.Open(filepath.Dir(path))
		if err != nil {
			_ = file.Close()
			return err
		}
		parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
		parentCloseErr := parent.Close()
		if err != nil {
			_ = file.Close()
			return err
		}
		if parentCloseErr != nil {
			_ = file.Close()
			return parentCloseErr
		}
		if !rootpublication.SamePhysicalIdentity(parentIdentity, token.Namespace().ParentIdentity()) {
			return file.Close()
		}
		// Read the crash-image bytes through the same child handle whose exact
		// physical identity was matched to the producer token. The pathname is
		// diagnostic discovery only and never substitutes authority.
		data, err := io.ReadAll(file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		matches = append(matches, productionAuthorityNamespaceFileWitness{
			token: token, descriptor: descriptor, path: path,
			relative: filepath.ToSlash(relative), data: data,
		})
		return nil
	})
	if err != nil {
		return productionAuthorityNamespaceFileWitness{}, err
	}
	if len(matches) != 1 {
		return productionAuthorityNamespaceFileWitness{}, fmt.Errorf("namespace token kind=%q id=%q generation=%d exact child matches=%d want 1", token.Kind(), token.ResourceID(), token.Generation(), len(matches))
	}
	return matches[0], nil
}

func productionAuthorityRecoveryBytes(manifest productionAuthorityRecoveryManifest) ([]byte, error) {
	return json.Marshal(manifest)
}

// openProductionAuthorityRecoveryManifest is the test recovery consumer for
// this producer-only graph node. It never publishes a product root: it returns
// the candidate root only after every actual producer-backed dependency has
// passed exact content validation, which makes typed previsibility observable
// without implementing the later root-consumption issue here.
func openProductionAuthorityRecoveryManifest(root, targetPath string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(targetPath)))
	if err != nil {
		return "", err
	}
	var manifest productionAuthorityRecoveryManifest
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return "", err
	}
	switch manifest.Root {
	case "old":
		if len(manifest.Dependencies) != 0 {
			return "", fmt.Errorf("old authority root unexpectedly carries %d dependencies", len(manifest.Dependencies))
		}
		return manifest.Root, nil
	case "new":
		if len(manifest.Dependencies) == 0 {
			return "", fmt.Errorf("%w: new authority root carries no dependencies", rootpublication.ErrUnresolvedResource)
		}
	default:
		return "", fmt.Errorf("unknown authority root %q", manifest.Root)
	}
	for _, dependency := range manifest.Dependencies {
		qualifier := string(dependency.PowerLossKind) + "/" + dependency.DirtyID
		if dependency.ResourceKind == "" || dependency.ResourceID == "" ||
			dependency.Generation == 0 || len(dependency.Reachability) == 0 ||
			dependency.SourceIdentity == "" || dependency.Path == "" || dependency.Size < 0 || dependency.SHA256 == "" {
			return "", fmt.Errorf("%w: dependency %s has incomplete producer authority", rootpublication.ErrUnresolvedResource, qualifier)
		}
		path := filepath.Join(root, filepath.FromSlash(dependency.Path))
		actual, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%w: dependency %s read %q: %v", rootpublication.ErrUnresolvedResource, qualifier, dependency.Path, err)
		}
		digest := sha256.Sum256(actual)
		if len(actual) != dependency.Size || fmt.Sprintf("%x", digest) != dependency.SHA256 {
			return "", fmt.Errorf("%w: dependency %s content mismatch path=%q", rootpublication.ErrUnresolvedResource, qualifier, dependency.Path)
		}
	}
	return manifest.Root, nil
}

func TestProductionAuthorityNamespaceAsymmetryVariants(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("production namespace asymmetry requires exact relative namespace support")
	}
	context := newProductionAuthorityNamespaceTestContext(t)
	defer context.shutdown()
	candidate, _, err := captureProductionFullAuthority(context)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	descriptors := candidate.Descriptors()
	tokens := candidate.Tokens()
	if len(descriptors) != len(tokens) || len(descriptors) == 0 {
		t.Fatalf("full closure descriptors=%d tokens=%d", len(descriptors), len(tokens))
	}
	if err := candidate.SyncThrough(); err != nil {
		t.Fatalf("sync real production authority: %v", err)
	}

	witnesses := make([]productionAuthorityNamespaceFileWitness, 0, len(tokens))
	for _, token := range tokens {
		if token.Namespace() == nil {
			continue
		}
		matchingDescriptors := make([]rootpublication.StableResourceDescriptor, 0, 1)
		for _, descriptor := range descriptors {
			if token.Kind() == descriptor.Kind() && token.Generation() == descriptor.Generation() &&
				rootpublication.SamePhysicalIdentity(token.Identity(), descriptor.Identity()) {
				matchingDescriptors = append(matchingDescriptors, descriptor)
			}
		}
		if len(matchingDescriptors) != 1 {
			t.Fatalf("namespace token kind=%q id=%q generation=%d matching descriptors=%d want 1", token.Kind(), token.ResourceID(), token.Generation(), len(matchingDescriptors))
		}
		witness, err := productionAuthorityNamespaceWitness(context.root, token, matchingDescriptors[0])
		if err != nil {
			t.Fatal(err)
		}
		witnesses = append(witnesses, witness)
	}
	if len(witnesses) == 0 {
		t.Fatal("real producer closure carried no namespace persistence evidence")
	}
	slices.SortFunc(witnesses, func(left, right productionAuthorityNamespaceFileWitness) int {
		return strings.Compare(left.relative, right.relative)
	})
	for index := 1; index < len(witnesses); index++ {
		if witnesses[index-1].relative == witnesses[index].relative {
			t.Fatalf("namespace producer paths are not one-to-one: %q", witnesses[index].relative)
		}
	}

	manifest := productionAuthorityRecoveryManifest{Root: "new", Dependencies: make([]productionAuthorityRecoveryDependency, 0, len(witnesses))}
	dependencies := make([]powerlossoracle.DirtyResource, 0, len(witnesses))
	for index, witness := range witnesses {
		token := witness.token
		descriptor := witness.descriptor
		dirtyID := fmt.Sprintf("%03d-%s-%s-%d", index, token.LogicalLane(), token.ResourceID(), token.Generation())
		kind := productionAuthorityPowerLossKind(descriptor.Kind())
		digest := sha256.Sum256(witness.data)
		identity := token.Identity()
		manifest.Dependencies = append(manifest.Dependencies, productionAuthorityRecoveryDependency{
			PowerLossKind: kind, DirtyID: dirtyID,
			ResourceKind: descriptor.Kind(), ResourceID: token.ResourceID(), LogicalLane: token.LogicalLane(),
			Generation: descriptor.Generation(), Reachability: descriptor.ReachabilityFields(), DiagnosticPath: token.DiagnosticPath(),
			SourceIdentity: fmt.Sprintf("%s/%d/%x/%d", identity.Platform, identity.VolumeID, identity.ObjectID, identity.Generation),
			Path:           witness.relative, Size: len(witness.data), SHA256: fmt.Sprintf("%x", digest),
		})
		dependencies = append(dependencies, powerlossoracle.DirtyResource{
			Kind: kind, ID: dirtyID, Path: witness.relative, NewName: true,
			NamespaceDirs: []string{filepath.ToSlash(filepath.Dir(witness.relative))},
		})
	}

	const targetPath = "production-authority-root.json"
	oldBytes, err := productionAuthorityRecoveryBytes(productionAuthorityRecoveryManifest{Root: "old"})
	if err != nil {
		t.Fatal(err)
	}
	newBytes, err := productionAuthorityRecoveryBytes(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(context.root, targetPath), oldBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(context.root)
	if err != nil {
		t.Fatal(err)
	}
	parents := make(map[string]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		if err := model.Unlink(dependency.Path); err != nil {
			t.Fatal(err)
		}
		parents[dependency.NamespaceDirs[0]] = struct{}{}
	}
	parentPaths := make([]string, 0, len(parents))
	for parent := range parents {
		parentPaths = append(parentPaths, parent)
	}
	slices.Sort(parentPaths)
	for _, parent := range parentPaths {
		if err := model.SyncDir(parent); err != nil {
			t.Fatal(err)
		}
	}
	for index, dependency := range dependencies {
		if err := model.Create(dependency.Path, witnesses[index].data); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Write(targetPath, newBytes); err != nil {
		t.Fatal(err)
	}

	target := powerlossoracle.DirtyResource{Kind: powerlossoracle.ResourceIndex, ID: "all-kind-target", Path: targetPath}
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID: "production-authority-real-namespace", Point: powerlossoracle.AfterMetaWrite, Occurrence: 1,
		Model: model, TargetMeta: &target, Dependencies: dependencies,
		RequiredFamilies: []powerlossoracle.VariantFamily{
			powerlossoracle.VariantSyncedOnly,
			powerlossoracle.VariantTargetMetaOnly,
			powerlossoracle.VariantOneMissingDependency,
			powerlossoracle.VariantDataWithoutNamespace,
			powerlossoracle.VariantNamespaceWithoutData,
			powerlossoracle.VariantFullWriteback,
		},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:           powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantTargetMetaOnly:       powerlossoracle.ExpectedTypedError,
			powerlossoracle.VariantOneMissingDependency: powerlossoracle.ExpectedTypedError,
			powerlossoracle.VariantDataWithoutNamespace: powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantNamespaceWithoutData: powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantFullWriteback:        powerlossoracle.ExpectedNewRoot,
		},
		MaxVariants: powerlossoracle.MaxVariantsPerCut,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantVariants := len(dependencies) + 3 + 2*len(dependencies)
	if len(variants) != wantVariants || coverage.ByFamily[powerlossoracle.VariantOneMissingDependency] != len(dependencies) ||
		coverage.ByFamily[powerlossoracle.VariantDataWithoutNamespace] != len(dependencies) ||
		coverage.ByFamily[powerlossoracle.VariantNamespaceWithoutData] != len(dependencies) {
		t.Fatalf("namespace variants=%d want=%d coverage=%v dependencies=%d namespace=%d",
			len(variants), wantVariants, coverage.ByFamily, len(dependencies), len(dependencies))
	}
	dependencyByQualifier := make(map[string]powerlossoracle.DirtyResource, len(dependencies))
	for _, dependency := range dependencies {
		dependencyByQualifier[string(dependency.Kind)+"/"+dependency.ID] = dependency
	}
	for _, variant := range variants {
		materialized := t.TempDir()
		if err := variant.Model.MaterializeStable(materialized); err != nil {
			t.Fatalf("materialize %s: %v", variant.ID, err)
		}
		visibleRoot, openErr := openProductionAuthorityRecoveryManifest(materialized, targetPath)
		observation := powerlossoracle.VariantObservation{Opened: true}
		if openErr != nil {
			if !errors.Is(openErr, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("open %s: %v", variant.ID, openErr)
			}
			if visibleRoot != "" {
				t.Fatalf("variant %s exposed root %q before resolving authority: %v", variant.ID, visibleRoot, openErr)
			}
			observation = powerlossoracle.VariantObservation{Result: powerlossoracle.ExpectedTypedError, TypedSentinel: "rootpublication.ErrUnresolvedResource"}
			if variant.Family == powerlossoracle.VariantOneMissingDependency && !strings.Contains(openErr.Error(), variant.Qualifier) {
				t.Fatalf("one-missing variant %s error=%v lacks exact omitted qualifier %q", variant.ID, openErr, variant.Qualifier)
			}
		} else {
			switch visibleRoot {
			case "old":
				observation.Result = powerlossoracle.ExpectedOldRoot
			case "new":
				observation.Result = powerlossoracle.ExpectedNewRoot
			default:
				t.Fatalf("variant %s recovered unknown root %q", variant.ID, visibleRoot)
			}
		}
		if err := powerlossoracle.ValidateVariantObservation(variant, observation, nil); err != nil {
			t.Fatalf("validate %s: %v (visible_root=%q open_error=%v)", variant.ID, err, visibleRoot, openErr)
		}
		if variant.Family == powerlossoracle.VariantDataWithoutNamespace || variant.Family == powerlossoracle.VariantNamespaceWithoutData {
			dependency, ok := dependencyByQualifier[variant.Qualifier]
			if !ok {
				t.Fatalf("variant %s qualifier %q has no real producer dependency", variant.ID, variant.Qualifier)
			}
			path := filepath.Join(materialized, filepath.FromSlash(dependency.Path))
			_, statErr := os.Stat(path)
			if variant.Family == powerlossoracle.VariantDataWithoutNamespace && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("data-only variant %s path %q stat=%v want absent namespace", variant.ID, dependency.Path, statErr)
			}
			if variant.Family == powerlossoracle.VariantNamespaceWithoutData {
				if statErr != nil {
					t.Fatalf("namespace-only variant %s path %q stat=%v want present name", variant.ID, dependency.Path, statErr)
				}
				actual, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if len(actual) != 0 {
					t.Fatalf("namespace-only variant %s path %q bytes=%d want unstable data omitted", variant.ID, dependency.Path, len(actual))
				}
			}
		}
	}
	t.Logf("real authority namespace recovery: descriptors=%d namespace_dependencies=%d generated=%d", len(descriptors), len(dependencies), len(variants))
}

func productionAuthorityPowerLossKind(kind rootpublication.ResourceKind) powerlossoracle.ResourceKind {
	switch kind {
	case rootpublication.ResourceIndex:
		return powerlossoracle.ResourceIndex
	case rootpublication.ResourceValueLog:
		return powerlossoracle.ResourceValueLog
	case rootpublication.ResourceOuterLeafLog, rootpublication.ResourceOuterLeafPack, rootpublication.ResourceOuterLeafManifest:
		return powerlossoracle.ResourceOuterLeaf
	case rootpublication.ResourceCommandWAL, rootpublication.ResourceCommandWALExternalRID:
		return powerlossoracle.ResourceCommandWAL
	default:
		return powerlossoracle.ResourceAuxiliary
	}
}

func TestProductionAuthorityLifecycleIdentityConflictMatrix(t *testing.T) {
	behaviorByField := make(map[rootpublication.ReachabilityField]productionAuthorityBehaviorWitness, len(productionAuthorityBehaviorWitnesses))
	for _, behavior := range productionAuthorityBehaviorWitnesses {
		behaviorByField[behavior.field] = behavior
	}
	for _, witness := range productionAuthorityWitnesses {
		witness := witness
		t.Run(string(witness.field), func(t *testing.T) {
			behavior := behaviorByField[witness.field]
			resources, err := witness.capture(t)
			if productionAuthorityExpectedUnsupported(witness, err) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := validateProductionAuthorityCapture(witness, resources); err != nil {
				resources.Release()
				t.Fatal(err)
			}
			builder := rootpublication.NewStableResourceSetBuilder(witness.field)
			candidate, freezeErr := builder.Freeze()
			if candidate != nil {
				candidate.Release()
				resources.Release()
				t.Fatal("incomplete lifecycle builder exposed candidate")
			}
			if !productionAuthorityMissingField(freezeErr, witness.field) || builder.State() != rootpublication.ResourceOwnerBuilder {
				resources.Release()
				builder.Abandon()
				t.Fatalf("retry precondition error=%v state=%v", freezeErr, builder.State())
			}
			if err := builder.Merge(resources); err != nil {
				resources.Release()
				builder.Abandon()
				t.Fatal(err)
			}
			candidate, err = builder.Freeze()
			if err != nil {
				builder.Abandon()
				t.Fatal(err)
			}
			assertProductionAuthorityReleaseToZero(t, candidate)

			abandoned, err := witness.capture(t)
			if err != nil {
				t.Fatal(err)
			}
			abandonBuilder := rootpublication.NewStableResourceSetBuilder(witness.field)
			if err := abandonBuilder.Merge(abandoned); err != nil {
				abandoned.Release()
				t.Fatal(err)
			}
			abandonBuilder.Abandon()
			abandonBuilder.Abandon()
			if abandonBuilder.State() != rootpublication.ResourceOwnerReleased {
				t.Fatalf("abandon state=%v want released", abandonBuilder.State())
			}

			first, err := witness.capture(t)
			if err != nil {
				t.Fatal(err)
			}
			second, err := witness.capture(t)
			if err != nil {
				first.Release()
				t.Fatal(err)
			}
			if err := requireProductionAuthorityRecreatedLogicalIdentity(witness.field, first, second); err != nil {
				first.Release()
				second.Release()
				t.Fatal(err)
			}
			conflictBuilder := rootpublication.NewStableResourceSetBuilder(witness.field)
			if err := conflictBuilder.Merge(first); err != nil {
				first.Release()
				second.Release()
				t.Fatal(err)
			}
			err = conflictBuilder.Merge(second)
			if !errors.Is(err, rootpublication.ErrResourceConflict) || second.Owner() != rootpublication.ResourceOwnerBuilder || conflictBuilder.State() != rootpublication.ResourceOwnerBuilder {
				second.Release()
				conflictBuilder.Abandon()
				t.Fatalf("identity conflict error=%v incoming=%v builder=%v stability=%v API=%s",
					err, second.Owner(), conflictBuilder.State(), behavior.stability, behavior.producerAPI)
			}
			assertProductionAuthorityReleaseToZero(t, second)
			conflictBuilder.Abandon()
		})
	}
}

func requireProductionAuthorityRecreatedLogicalIdentity(field rootpublication.ReachabilityField, first, second *rootpublication.StableResourceSet) error {
	for _, left := range first.Tokens() {
		if left.Reachability() != field {
			continue
		}
		for _, right := range second.Tokens() {
			if right.Reachability() != field || left.Kind() != right.Kind() ||
				left.LogicalLane() != right.LogicalLane() || left.ResourceID() != right.ResourceID() ||
				left.Generation() != right.Generation() {
				continue
			}
			if rootpublication.SamePhysicalIdentity(left.Identity(), right.Identity()) {
				return fmt.Errorf("%w: independent fixtures reused %q physical identity", rootpublication.ErrResourceConflict, field)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: independent fixtures for %q had no matching logical resource", rootpublication.ErrUnresolvedResource, field)
}

func TestProductionAuthorityRepeatedCapturePostShutdownPlateau(t *testing.T) {
	for _, witness := range productionAuthorityWitnesses {
		witness := witness
		t.Run(string(witness.field), func(t *testing.T) {
			baselineFDs, countFDs := productionAuthorityOpenFDCount()
			var wantDescriptors, wantObligations int
			for iteration := 0; iteration < 3; iteration++ {
				var resources *rootpublication.StableResourceSet
				completed := t.Run(fmt.Sprintf("iteration-%d-capture-and-shutdown", iteration), func(t *testing.T) {
					captured, err := witness.capture(t)
					if productionAuthorityExpectedUnsupported(witness, err) {
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					resources = captured
					descriptors := captured.Descriptors()
					obligations := 0
					for _, descriptor := range descriptors {
						obligations += len(descriptor.LogicalObligations())
					}
					if iteration == 0 {
						wantDescriptors, wantObligations = len(descriptors), obligations
					} else if len(descriptors) != wantDescriptors || obligations != wantObligations {
						captured.Release()
						resources = nil
						t.Fatalf("repeated shape=%d/%d want %d/%d", len(descriptors), obligations, wantDescriptors, wantObligations)
					}
				})
				// Returning from the nested test executes the real producer's
				// registered Close/shutdown callbacks while stable authority remains
				// live. Its exact retained handles must still complete the barrier.
				if !completed || resources == nil {
					continue
				}
				if err := resources.SyncThrough(); err != nil {
					resources.Release()
					t.Fatalf("iteration %d exact authority failed after producer shutdown: %v", iteration, err)
				}
				registryStats := resources.IdentityPinRegistryStats()
				if len(registryStats) == 0 {
					resources.Release()
					t.Fatalf("iteration %d retained authority exposes no identity-pin registry", iteration)
				}
				for index, stats := range registryStats {
					if stats.ActivePins == 0 || stats.ActiveIdentities == 0 {
						resources.Release()
						t.Fatalf("iteration %d live registry %d stats=%+v want retained pins and identities", iteration, index, stats)
					}
				}
				assertProductionAuthorityReleaseToZero(t, resources)
				for index, stats := range resources.IdentityPinRegistryStats() {
					if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
						t.Fatalf("iteration %d released registry %d retained state: %+v", iteration, index, stats)
					}
				}
				if !countFDs {
					continue
				}
				afterFDs, ok := productionAuthorityOpenFDCount()
				if ok && afterFDs > baselineFDs+1 {
					t.Fatalf("iteration %d open FDs=%d baseline=%d tolerance=1 after shutdown", iteration, afterFDs, baselineFDs)
				}
			}
		})
	}
}

func productionAuthorityOpenFDCount() (int, bool) {
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func assertProductionAuthorityReleaseToZero(t testing.TB, resources *rootpublication.StableResourceSet) {
	t.Helper()
	if resources == nil {
		t.Fatal("nil production resources")
	}
	descriptors := resources.Descriptors()
	guard := resources.DeletionGuard()
	before := resources.Stats(time.Now())
	if len(descriptors) == 0 || len(before) == 0 {
		resources.Release()
		t.Fatal("production resources have no descriptor or pin observations")
	}
	for _, descriptor := range descriptors {
		if err := guard.Check(descriptor.Identity(), descriptor.Generation()); !errors.Is(err, rootpublication.ErrResourcePinned) {
			resources.Release()
			t.Fatalf("live generation %d guard=%v want pinned", descriptor.Generation(), err)
		}
	}
	resources.Release()
	resources.Release()
	if resources.Owner() != rootpublication.ResourceOwnerReleased {
		t.Fatalf("released owner=%v", resources.Owner())
	}
	for _, descriptor := range descriptors {
		if err := guard.Check(descriptor.Identity(), descriptor.Generation()); err != nil {
			t.Fatalf("released generation %d remains pinned: %v", descriptor.Generation(), err)
		}
	}
	for _, stats := range resources.Stats(time.Now()) {
		if stats.ActivePins != 0 {
			t.Fatalf("released %q active pins=%d", stats.Kind, stats.ActivePins)
		}
	}
}

func TestProductionAuthorityObservationsByKindAndAggregate(t *testing.T) {
	type observation struct {
		descriptors, obligations, pinHighWater, activePins, releasedPins uint64
		syncAttempts, physicalFileSyncs, namespaceSyncs                  uint64
		captureLatency, syncLatency                                      time.Duration
	}
	byKind := make(map[rootpublication.ResourceKind]*observation)
	for _, witness := range productionAuthorityWitnesses {
		witness := witness
		t.Run(string(witness.field), func(t *testing.T) {
			started := time.Now()
			resources, err := witness.capture(t)
			captureLatency := time.Since(started)
			if productionAuthorityExpectedUnsupported(witness, err) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, descriptor := range resources.Descriptors() {
				row := byKind[descriptor.Kind()]
				if row == nil {
					row = &observation{}
					byKind[descriptor.Kind()] = row
				}
				row.descriptors++
				row.obligations += uint64(len(descriptor.LogicalObligations()))
			}
			primary := byKind[witness.kind]
			if primary == nil {
				primary = &observation{}
				byKind[witness.kind] = primary
			}
			primary.captureLatency += captureLatency
			started = time.Now()
			if err := resources.SyncThrough(); err != nil {
				resources.Release()
				t.Fatal(err)
			}
			primary.syncLatency += time.Since(started)
			for _, stats := range resources.Stats(time.Now()) {
				row := byKind[stats.Kind]
				if row == nil {
					row = &observation{}
					byKind[stats.Kind] = row
				}
				row.pinHighWater += stats.PinHighWater
				row.activePins += stats.ActivePins
				row.syncAttempts += stats.Syncs
				row.physicalFileSyncs += stats.PhysicalFileSyncs
				row.namespaceSyncs += stats.NamespaceSyncs
			}
			resources.Release()
			for _, stats := range resources.Stats(time.Now()) {
				byKind[stats.Kind].releasedPins += stats.ActivePins
			}
		})
	}
	kinds := make([]rootpublication.ResourceKind, 0, len(byKind))
	for kind := range byKind {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	var total observation
	for _, kind := range kinds {
		row := byKind[kind]
		total.descriptors += row.descriptors
		total.obligations += row.obligations
		total.pinHighWater += row.pinHighWater
		total.activePins += row.activePins
		total.releasedPins += row.releasedPins
		total.syncAttempts += row.syncAttempts
		total.physicalFileSyncs += row.physicalFileSyncs
		total.namespaceSyncs += row.namespaceSyncs
		total.captureLatency += row.captureLatency
		total.syncLatency += row.syncLatency
		t.Logf("authority kind=%q descriptors=%d obligations=%d pin_high_water=%d active_pins=%d released_pins=%d sync_attempts=%d physical_file_syncs=%d namespace_syncs=%d capture_latency=%s sync_latency=%s",
			kind, row.descriptors, row.obligations, row.pinHighWater, row.activePins, row.releasedPins,
			row.syncAttempts, row.physicalFileSyncs, row.namespaceSyncs, row.captureLatency, row.syncLatency)
	}
	if total.descriptors == 0 || total.pinHighWater == 0 || total.activePins == 0 || total.releasedPins != 0 || total.physicalFileSyncs == 0 {
		t.Fatalf("invalid aggregate observations: %+v", total)
	}
	t.Logf("authority aggregate descriptors=%d obligations=%d pin_high_water=%d active_pins=%d released_pins=%d sync_attempts=%d physical_file_syncs=%d namespace_syncs=%d capture_latency=%s sync_latency=%s",
		total.descriptors, total.obligations, total.pinHighWater, total.activePins, total.releasedPins,
		total.syncAttempts, total.physicalFileSyncs, total.namespaceSyncs, total.captureLatency, total.syncLatency)
}

var productionAuthorityBenchmarkSink int

type productionAuthorityBenchmarkContext struct {
	b        *testing.B
	root     string
	cleanups []func()
}

func newProductionAuthorityBenchmarkContext(b *testing.B, root string) *productionAuthorityBenchmarkContext {
	return &productionAuthorityBenchmarkContext{b: b, root: root}
}

func (context *productionAuthorityBenchmarkContext) Helper() { context.b.Helper() }

func (context *productionAuthorityBenchmarkContext) TempDir() string {
	context.b.Helper()
	dir, err := os.MkdirTemp(context.root, "capture-")
	if err != nil {
		context.b.Fatal(err)
	}
	context.cleanups = append(context.cleanups, func() { _ = os.RemoveAll(dir) })
	return dir
}

func (context *productionAuthorityBenchmarkContext) Cleanup(cleanup func()) {
	context.cleanups = append(context.cleanups, cleanup)
}

func (context *productionAuthorityBenchmarkContext) shutdown() {
	for index := len(context.cleanups) - 1; index >= 0; index-- {
		context.cleanups[index]()
	}
	context.cleanups = nil
}

func BenchmarkProductionAuthorityConstruction(b *testing.B) {
	root, err := os.MkdirTemp("", "gomap-production-authority-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(root) })
	for _, witness := range productionAuthorityWitnesses {
		witness := witness
		if witness.field == rootpublication.ReachabilityOuterLeafPackedPointer &&
			(!rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported()) {
			continue
		}
		b.Run(fmt.Sprintf("%s/%s", witness.kind, witness.field), func(b *testing.B) {
			benchmarkProductionAuthorityConstruction(b, root, func(context productionAuthorityContext) (*rootpublication.StableResourceSet, int, error) {
				resources, err := witness.capture(context)
				return resources, 1, err
			})
		})
	}
	b.Run("aggregate", func(b *testing.B) {
		benchmarkProductionAuthorityConstruction(b, root, func(context productionAuthorityContext) (*rootpublication.StableResourceSet, int, error) {
			resources, supported, err := captureProductionFullAuthority(context)
			return resources, len(supported), err
		})
	})
}

func benchmarkProductionAuthorityConstruction(
	b *testing.B,
	root string,
	capture func(productionAuthorityContext) (*rootpublication.StableResourceSet, int, error),
) {
	b.ReportAllocs()
	b.ResetTimer()
	var descriptors, obligations, producerFields int
	for iteration := 0; iteration < b.N; iteration++ {
		context := newProductionAuthorityBenchmarkContext(b, root)
		resources, fields, err := capture(context)
		if err != nil {
			b.StopTimer()
			if resources != nil {
				resources.Release()
			}
			context.shutdown()
			b.Fatal(err)
		}
		descriptors = resources.Len()
		obligations = 0
		for _, descriptor := range resources.Descriptors() {
			obligations += len(descriptor.LogicalObligations())
		}
		producerFields = fields
		productionAuthorityBenchmarkSink = descriptors
		b.StopTimer()
		resources.Release()
		context.shutdown()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(producerFields), "producer-fields")
	b.ReportMetric(float64(descriptors), "descriptors")
	b.ReportMetric(float64(obligations), "logical-obligations")
}

func BenchmarkProductionAuthorityPreparedClosureCoalescing(b *testing.B) {
	for _, witness := range productionAuthorityWitnesses {
		witness := witness
		if witness.field == rootpublication.ReachabilityOuterLeafPackedPointer &&
			(!rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported()) {
			continue
		}
		b.Run(fmt.Sprintf("%s/%s", witness.kind, witness.field), func(b *testing.B) {
			resources, err := witness.capture(b)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(resources.Release)
			benchmarkProductionAuthorityPreparedClosure(b, resources)
		})
	}
	b.Run("aggregate", func(b *testing.B) {
		resources, _, err := captureProductionFullAuthority(b)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(resources.Release)
		benchmarkProductionAuthorityPreparedClosure(b, resources)
	})
}

func benchmarkProductionAuthorityPreparedClosure(b *testing.B, resources *rootpublication.StableResourceSet) {
	obligations := 0
	for _, descriptor := range resources.Descriptors() {
		obligations += len(descriptor.LogicalObligations())
	}
	b.ReportMetric(float64(resources.Len()), "descriptors")
	b.ReportMetric(float64(obligations), "logical-obligations")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := rootpublication.UnionStableResourceSets(resources, resources)
		if err != nil {
			b.Fatal(err)
		}
		productionAuthorityBenchmarkSink = view.Len()
	}
}
