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

// stableColumnAssetResourceClassification is the checked producer policy for
// every currently-declared physical column asset kind. Unknown future kinds
// fail closed until their durability ownership is reviewed.
func stableColumnAssetResourceClassification(kind ColumnAssetKind) (rootpublication.ResourceKind, rootpublication.ReachabilityField, error) {
	switch kind {
	case ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1TypedColumnPart:
		return rootpublication.ResourceTypedColumnAsset, rootpublication.ReachabilityTypedColumnMultipart, nil
	case ColumnAssetKindTCS1AggregateMetadata, ColumnAssetKindTCS1Int64Values:
		return rootpublication.ResourceTypedColumnAsset, rootpublication.ReachabilityTypedColumnValue, nil
	case ColumnAssetKindTCS1DictionaryCodes:
		return rootpublication.ResourceTypedColumnAsset, rootpublication.ReachabilityTypedColumnCode, nil
	case ColumnAssetKindTCS1HNSWSearchPack:
		return rootpublication.ResourceVectorGraphPack, rootpublication.ReachabilityHNSWSearchPack, nil
	case ColumnAssetKindQueryReadyBase:
		return rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyBase, nil
	case ColumnAssetKindQueryReadyDelta:
		return rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyDelta, nil
	case ColumnAssetKindQueryReadyConsolidatedBase:
		return rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyConsolidatedBase, nil
	default:
		return "", "", fmt.Errorf("collections: stable resource inventory missing column asset kind %q", kind)
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

// stableColumnAssetResourceToken captures the exact segment identity and the
// greatest byte required by ref. The manifest remains the authority for the
// ref's range and checksum; tokens for several refs in one segment therefore
// coalesce by stable identity and maximum frontier.
func stableColumnAssetResourceToken(rootDir string, ref ColumnAssetRef) (*rootpublication.StableResourceToken, error) {
	resourceKind, reachability, err := stableColumnAssetResourceClassification(ref.Kind)
	if err != nil {
		return nil, err
	}
	if ref.Generation == 0 || ref.Offset < 0 || ref.Length <= 0 || ref.Offset > int64(^uint64(0)>>1)-ref.Length {
		return nil, errors.New("collections: invalid stable column asset ref frontier")
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind: resourceKind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
		Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: file,
		Frontier: rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)},
		Digest:   stableColumnSegmentDigest(ref), Reachability: reachability,
	})
}
