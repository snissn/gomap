package collections

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var syncStableColumnAssetResourceForPublish = func(file *os.File, _ rootpublication.DurableFrontier) error {
	return file.Sync()
}

// NewStableCollectionResourceToken rejects collection/text named roots until
// their adjacent publication milestone (#3679); they have no independent
// external identity that #3677 can register.
func NewStableCollectionResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	switch spec.Reachability {
	case rootpublication.ReachabilityCollectionSystemRoot,
		rootpublication.ReachabilityCollectionPrimaryRoot,
		rootpublication.ReachabilityCollectionTemplateRoot,
		rootpublication.ReachabilityCollectionIndexStateRoot,
		rootpublication.ReachabilityCollectionColumnRoot,
		rootpublication.ReachabilityCollectionSecondaryRoot,
		rootpublication.ReachabilityCollectionVectorRoot,
		rootpublication.ReachabilityCollectionTextDictionary,
		rootpublication.ReachabilityCollectionTextPosting,
		rootpublication.ReachabilityCollectionTextPosition:
		return nil, fmt.Errorf("%w: %s is owned by adjacent collection root publication issue #3679", rootpublication.ErrResourceExcluded, spec.Reachability)
	default:
		return nil, fmt.Errorf("%w: collection producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
}

// NewStableColumnAssetResourceToken registers an exact open segment for
// authoritative column, typed-column, and graph/HNSW asset references.
// Rebuildable query-ready references are mapped here so the canonical
// production boundary can reject them as non-authoritative.
func NewStableColumnAssetResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	classification := ""
	switch spec.Reachability {
	case rootpublication.ReachabilityColumnManifest,
		rootpublication.ReachabilityTypedColumnMultipart,
		rootpublication.ReachabilityTypedColumnValue,
		rootpublication.ReachabilityTypedColumnCode,
		rootpublication.ReachabilityHNSWSearchPack:
		classification = "authoritative"
	case rootpublication.ReachabilityVectorGraphPack:
		classification = "authoritative-transitive"
	case rootpublication.ReachabilityQueryReadyBase,
		rootpublication.ReachabilityQueryReadyDelta,
		rootpublication.ReachabilityQueryReadyConsolidatedBase:
		classification = "rebuildable-non-authoritative"
	default:
		return nil, fmt.Errorf("%w: column-asset producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerColumnAsset, spec, classification)
}

// NewStableLegacyVectorResourceToken rejects legacy vector sidecars because
// they are rebuildable exact-search accelerators, not publication authority.
func NewStableLegacyVectorResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if spec.Reachability != rootpublication.ReachabilityLegacyVectorSnapshot {
		return nil, fmt.Errorf("%w: legacy-vector producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return nil, fmt.Errorf("%w: legacy vector sidecars are rebuildable and non-authoritative", rootpublication.ErrResourceExcluded)
}

// stableColumnAssetResourceClassification is the checked producer policy for
// every currently-declared physical column asset kind. Unknown future kinds
// fail closed until their durability ownership is reviewed.
func stableColumnAssetResourceClassification(kind ColumnAssetKind) (rootpublication.ResourceKind, rootpublication.ReachabilityField, string, error) {
	switch kind {
	case ColumnAssetKindTCS1PartImage:
		return rootpublication.ResourceColumnAsset, rootpublication.ReachabilityColumnManifest, "authoritative", nil
	case ColumnAssetKindTCS1TypedColumnPart:
		return rootpublication.ResourceTypedColumnAsset, rootpublication.ReachabilityTypedColumnMultipart, "authoritative", nil
	case ColumnAssetKindTCS1AggregateMetadata, ColumnAssetKindTCS1Int64Values:
		return rootpublication.ResourceTypedColumnAsset, rootpublication.ReachabilityTypedColumnValue, "authoritative", nil
	case ColumnAssetKindTCS1DictionaryCodes:
		return rootpublication.ResourceTypedColumnAsset, rootpublication.ReachabilityTypedColumnCode, "authoritative", nil
	case ColumnAssetKindTCS1HNSWSearchPack:
		return rootpublication.ResourceVectorGraphPack, rootpublication.ReachabilityHNSWSearchPack, "authoritative", nil
	case ColumnAssetKindQueryReadyBase:
		return rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyBase, "rebuildable-non-authoritative", nil
	case ColumnAssetKindQueryReadyDelta:
		return rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyDelta, "rebuildable-non-authoritative", nil
	case ColumnAssetKindQueryReadyConsolidatedBase:
		return rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyConsolidatedBase, "rebuildable-non-authoritative", nil
	default:
		return "", "", "", fmt.Errorf("collections: stable resource inventory missing column asset kind %q", kind)
	}
}

func stableColumnSegmentDigest(ref ColumnAssetRef) [32]byte {
	// The digest must be invariant for every ref sharing one append-only segment;
	// per-ref offset/length/checksum remains authoritative in the manifest while
	// the token supplies the greatest required durable byte frontier.
	namespace := []byte(ref.Namespace)
	raw := make([]byte, 4+len(namespace)+4)
	binary.LittleEndian.PutUint32(raw[:4], uint32(len(namespace)))
	copy(raw[4:], namespace)
	binary.LittleEndian.PutUint32(raw[4+len(namespace):], ref.FileID)
	return sha256.Sum256(raw)
}

func stableColumnLogicalObligation(ref ColumnAssetRef, reachability rootpublication.ReachabilityField) rootpublication.StableLogicalObligation {
	const obligationClass = "column-asset-ref-v1"
	appendString := func(raw []byte, value string) []byte {
		var size [4]byte
		binary.LittleEndian.PutUint32(size[:], uint32(len(value)))
		raw = append(raw, size[:]...)
		return append(raw, value...)
	}
	appendUint64 := func(raw []byte, value uint64) []byte {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		return append(raw, encoded[:]...)
	}
	appendUint32 := func(raw []byte, value uint32) []byte {
		var encoded [4]byte
		binary.LittleEndian.PutUint32(encoded[:], value)
		return append(raw, encoded[:]...)
	}
	// Four length prefixes, the fixed class, and the six numeric fields add 75
	// bytes beyond the variable strings. Normal production namespaces fit the
	// stack buffer; unusually large names retain the exact heap fallback.
	encodedLen := len(ref.Kind) + len(ref.Namespace) + len(reachability) + 75
	var stack [512]byte
	var raw []byte
	if encodedLen <= len(stack) {
		raw = stack[:0:encodedLen]
	} else {
		raw = make([]byte, 0, encodedLen)
	}
	raw = appendString(raw, obligationClass)
	raw = appendString(raw, string(ref.Kind))
	raw = appendString(raw, ref.Namespace)
	raw = appendUint64(raw, ref.Generation)
	raw = appendUint64(raw, ref.PartID)
	raw = appendUint32(raw, ref.FileID)
	raw = appendUint64(raw, uint64(ref.Offset))
	raw = appendUint64(raw, uint64(ref.Length))
	raw = appendUint32(raw, ref.Checksum)
	raw = appendString(raw, string(reachability))
	return rootpublication.StableLogicalObligation{
		Class: obligationClass, Kind: string(ref.Kind), Namespace: ref.Namespace,
		Generation: ref.Generation, PartID: ref.PartID, FileID: uint64(ref.FileID),
		Offset: ref.Offset, Length: ref.Length, Checksum: ref.Checksum,
		Reachability: reachability, Digest: sha256.Sum256(raw),
	}
}

func stableColumnAssetDiagnosticPath(ref ColumnAssetRef) string {
	return filepath.Join("column_assets", filepath.FromSlash(ref.Namespace), columnAssetManagerAssetsDirName,
		columnAssetManagerSegmentsDirName, columnAssetSegmentFileName(ref.FileID))
}

// stableColumnAssetResourceToken captures the exact already-open segment
// identity and greatest required byte while preserving ref's immutable logical
// generation, part, range, and checksum obligation. Tokens for several refs in
// one segment coalesce physically without collapsing those logical obligations.
func stableColumnAssetResourceToken(file *os.File, ref ColumnAssetRef, namespace *rootpublication.StableNamespaceToken) (*rootpublication.StableResourceToken, error) {
	return stableColumnAssetResourceTokenWithRegistry(file, ref, namespace, nil)
}

func stableColumnAssetResourceTokenWithRegistry(file *os.File, ref ColumnAssetRef, namespace *rootpublication.StableNamespaceToken, registry *rootpublication.IdentityPinRegistry) (*rootpublication.StableResourceToken, error) {
	resourceKind, reachability, _, err := stableColumnAssetResourceClassification(ref.Kind)
	return stableColumnAssetResourceTokenWithPolicy(file, ref, namespace, registry, resourceKind, reachability, err)
}

// stableVectorGraphResourceTokenWithRegistry is the vector-rebuild-owned
// capture boundary. The physical ref kind remains part of the logical
// obligation, while the complete rebuild closure is registered under the
// transitive vector-graph reachability field.
func stableVectorGraphResourceTokenWithRegistry(file *os.File, ref ColumnAssetRef, namespace *rootpublication.StableNamespaceToken, registry *rootpublication.IdentityPinRegistry) (*rootpublication.StableResourceToken, error) {
	if _, _, classification, err := stableColumnAssetResourceClassification(ref.Kind); err != nil {
		return nil, err
	} else if classification != "authoritative" {
		return nil, fmt.Errorf("%w: vector graph ref kind %q is not authoritative", rootpublication.ErrResourceExcluded, ref.Kind)
	}
	return stableColumnAssetResourceTokenWithPolicy(file, ref, namespace, registry,
		rootpublication.ResourceVectorGraphPack, rootpublication.ReachabilityVectorGraphPack, nil)
}

var errColumnAssetStableTokenOmittedForTest = errors.New("collections: stable column token omitted by capture fault hook")

func stableColumnAssetResourceTokenWithPolicy(file *os.File, ref ColumnAssetRef, namespace *rootpublication.StableNamespaceToken, registry *rootpublication.IdentityPinRegistry, resourceKind rootpublication.ResourceKind, reachability rootpublication.ReachabilityField, policyErr error) (*rootpublication.StableResourceToken, error) {
	if policyErr != nil {
		return nil, policyErr
	}
	if ref.Generation == 0 || ref.Offset < 0 || ref.Length <= 0 || ref.Offset > int64(^uint64(0)>>1)-ref.Length {
		return nil, errors.New("collections: invalid stable column asset ref frontier")
	}
	logicalObligation := stableColumnLogicalObligation(ref, reachability)
	logicalObligations := []rootpublication.StableLogicalObligation{logicalObligation}
	if hook := columnAssetStableObligationHook(); hook != nil {
		switch hook(ref, logicalObligation, namespace) {
		case columnAssetStableCaptureOmitObligation:
			logicalObligations = nil
		case columnAssetStableCaptureOmitToken:
			return nil, errColumnAssetStableTokenOmittedForTest
		}
	}
	var identity rootpublication.StableIdentity
	var err error
	if registry != nil {
		identity, err = rootpublication.StableIdentityFromFile(file)
		if err != nil {
			return nil, err
		}
		if err := registry.Observe(identity); err != nil {
			return nil, err
		}
	}
	observed := registry != nil
	token, err := NewStableColumnAssetResourceToken(rootpublication.StableResourceSpec{
		Kind: resourceKind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
		Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: file,
		Frontier: rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)},
		Digest:   stableColumnSegmentDigest(ref), Reachability: reachability, Namespace: namespace,
		LogicalObligations: logicalObligations,
		PinRegistry:        registry,
		SyncThrough:        syncStableColumnAssetResourceForPublish,
		OnRelease: func() {
			if observed {
				_ = registry.Unobserve(identity)
			}
		},
		// The producer synchronizes the exact registered frontier before capture.
		// A later coalesced frontier still performs a real sync.
		ContentSynced: true,
	})
	if err != nil && observed {
		_ = registry.Unobserve(identity)
	}
	return token, err
}

func stableColumnAssetNamespaceToken(parent, resource *os.File, ref ColumnAssetRef) (*rootpublication.StableNamespaceToken, error) {
	if parent == nil || resource == nil {
		return nil, fmt.Errorf("%w: column asset namespace requires exact parent and resource handles", rootpublication.ErrUnresolvedResource)
	}
	return rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: uint64(ref.FileID), Operation: rootpublication.NamespaceCreate,
		NewName:        filepath.Base(stableColumnAssetDiagnosticPath(ref)),
		DiagnosticPath: filepath.Dir(stableColumnAssetDiagnosticPath(ref)),
	})
}

func validateStableColumnResourcesMatchPrepared(assets []ColumnPreparedAsset, resources *rootpublication.StableResourceSet) error {
	return validateStableColumnResourcesMatchPreparedPolicy(assets, resources, false)
}

func validateStableVectorGraphResourcesMatchPrepared(assets []ColumnPreparedAsset, resources *rootpublication.StableResourceSet) error {
	return validateStableColumnResourcesMatchPreparedPolicy(assets, resources, true)
}

func validateStableColumnResourcesMatchPreparedPolicy(assets []ColumnPreparedAsset, resources *rootpublication.StableResourceSet, vectorGraphAuthority bool) error {
	const linearLimit = 16
	expected := make([]rootpublication.StableLogicalObligation, 0, len(assets))
	var expectedIndex map[rootpublication.StableLogicalObligation]struct{}
	if len(assets) > linearLimit {
		expectedIndex = make(map[rootpublication.StableLogicalObligation]struct{}, len(assets))
	}
	for i, asset := range assets {
		_, reachability, classification, err := stableColumnAssetResourceClassification(asset.Ref.Kind)
		if err != nil {
			return fmt.Errorf("collections: prepared column asset[%d] stable classification: %w", i, err)
		}
		if classification == "rebuildable-non-authoritative" {
			continue
		}
		if vectorGraphAuthority {
			reachability = rootpublication.ReachabilityVectorGraphPack
		}
		obligation := stableColumnLogicalObligation(asset.Ref, reachability)
		if expectedIndex != nil {
			if _, duplicate := expectedIndex[obligation]; duplicate {
				return fmt.Errorf("collections: %w: duplicate authoritative prepared column asset ref %+v", rootpublication.ErrResourceConflict, asset.Ref)
			}
			expectedIndex[obligation] = struct{}{}
		} else {
			for _, existing := range expected {
				if existing == obligation {
					return fmt.Errorf("collections: %w: duplicate authoritative prepared column asset ref %+v", rootpublication.ErrResourceConflict, asset.Ref)
				}
			}
		}
		expected = append(expected, obligation)
	}
	if resources == nil {
		if len(expected) != 0 {
			return fmt.Errorf("collections: %w: %d authoritative prepared column assets have no stable resources", rootpublication.ErrUnresolvedResource, len(expected))
		}
		return nil
	}
	actual := make([]rootpublication.StableLogicalObligation, 0, len(expected))
	var actualIndex map[rootpublication.StableLogicalObligation]struct{}
	if expectedIndex != nil {
		actualIndex = make(map[rootpublication.StableLogicalObligation]struct{}, len(expected))
	}
	for _, descriptor := range resources.Descriptors() {
		for _, obligation := range descriptor.LogicalObligations() {
			resourceKind, _, classification, err := stableColumnAssetResourceClassification(ColumnAssetKind(obligation.Kind))
			if err != nil {
				return fmt.Errorf("collections: stable column obligation classification: %w", err)
			}
			if vectorGraphAuthority {
				resourceKind = rootpublication.ResourceVectorGraphPack
				if obligation.Reachability != rootpublication.ReachabilityVectorGraphPack {
					return fmt.Errorf("collections: %w: vector graph obligation kind=%q reachability=%q", rootpublication.ErrResourceConflict, obligation.Kind, obligation.Reachability)
				}
			}
			if classification != "authoritative" || resourceKind != descriptor.Kind() {
				return fmt.Errorf("collections: %w: stable column obligation kind=%q resource_kind=%q classification=%q", rootpublication.ErrResourceConflict, obligation.Kind, descriptor.Kind(), classification)
			}
			if actualIndex != nil {
				if _, duplicate := actualIndex[obligation]; duplicate {
					return fmt.Errorf("collections: %w: duplicate stable column logical obligation %+v", rootpublication.ErrResourceConflict, obligation)
				}
				actualIndex[obligation] = struct{}{}
			} else {
				for _, existing := range actual {
					if existing == obligation {
						return fmt.Errorf("collections: %w: duplicate stable column logical obligation %+v", rootpublication.ErrResourceConflict, obligation)
					}
				}
			}
			actual = append(actual, obligation)
		}
	}
	for _, obligation := range expected {
		if actualIndex != nil {
			if _, found := actualIndex[obligation]; !found {
				return fmt.Errorf("collections: %w: stable column resources missing prepared obligation %+v", rootpublication.ErrUnresolvedResource, obligation)
			}
			continue
		}
		found := false
		for _, candidate := range actual {
			if candidate == obligation {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("collections: %w: stable column resources missing prepared obligation %+v", rootpublication.ErrUnresolvedResource, obligation)
		}
	}
	for _, obligation := range actual {
		if expectedIndex != nil {
			if _, found := expectedIndex[obligation]; !found {
				return fmt.Errorf("collections: %w: stable column resources contain unprepared obligation %+v", rootpublication.ErrResourceConflict, obligation)
			}
			continue
		}
		found := false
		for _, candidate := range expected {
			if candidate == obligation {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("collections: %w: stable column resources contain unprepared obligation %+v", rootpublication.ErrResourceConflict, obligation)
		}
	}
	return nil
}
