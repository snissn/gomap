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

// NewStableColumnAssetResourceToken registers an exact open segment for all
// column, typed-column, graph/HNSW, and query-ready asset references.
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

// NewStableLegacyVectorResourceToken registers an exact immutable file in a
// legacy vector snapshot generation before its manifest becomes reachable.
func NewStableLegacyVectorResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if spec.Reachability != rootpublication.ReachabilityLegacyVectorSnapshot {
		return nil, fmt.Errorf("%w: legacy-vector producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerLegacyVector, spec, "authoritative-legacy")
}

// stableColumnAssetResourceClassification is the checked producer policy for
// every currently-declared physical column asset kind. Unknown future kinds
// fail closed until their durability ownership is reviewed.
func stableColumnAssetResourceClassification(kind ColumnAssetKind) (rootpublication.ResourceKind, rootpublication.ReachabilityField, string, error) {
	switch kind {
	case ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1TypedColumnPart:
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

func stableColumnAssetDiagnosticPath(ref ColumnAssetRef) string {
	return filepath.Join("column_assets", filepath.FromSlash(ref.Namespace), columnAssetManagerAssetsDirName,
		columnAssetManagerSegmentsDirName, columnAssetSegmentFileName(ref.FileID))
}

// stableColumnAssetResourceToken captures the exact already-open segment
// identity and the greatest byte required by ref. The manifest remains the
// authority for the ref's range and checksum; tokens for several refs in one
// segment therefore coalesce by stable identity and maximum frontier.
func stableColumnAssetResourceToken(file *os.File, ref ColumnAssetRef, namespace *rootpublication.StableNamespaceToken) (*rootpublication.StableResourceToken, error) {
	resourceKind, reachability, _, err := stableColumnAssetResourceClassification(ref.Kind)
	if err != nil {
		return nil, err
	}
	if ref.Generation == 0 || ref.Offset < 0 || ref.Length <= 0 || ref.Offset > int64(^uint64(0)>>1)-ref.Length {
		return nil, errors.New("collections: invalid stable column asset ref frontier")
	}
	return NewStableColumnAssetResourceToken(rootpublication.StableResourceSpec{
		Kind: resourceKind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
		Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: file,
		Frontier: rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)},
		Digest:   stableColumnSegmentDigest(ref), Reachability: reachability, Namespace: namespace,
		// The producer synchronizes the segment before it captures the token.
		// Publication must account for that step without syncing the same file a
		// second time.
		SyncThrough: func(*os.File, rootpublication.DurableFrontier) error { return nil },
	})
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
