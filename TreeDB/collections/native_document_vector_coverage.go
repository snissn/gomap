package collections

import (
	"bytes"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Native document coverage deliberately does not reuse the physical v3
// CommitSeq-derived generation or the imported cdir2 v4 header authority.
const (
	nativeDocumentVectorCoverageVersion = 5
	collectionMetaVersionNativeDocument = 8
	nativeDocumentGenerationPrefix      = "collections/native/v1/document-generation/"
)

// nativeDocumentVectorShape requires normalized metadata. It selects the
// existing ordinary float32 cosine HNSW contract, never an imported source or
// a different vector representation merely because it declares a vector.
func nativeDocumentVectorShape(meta CollectionMeta) bool {
	format := normalizedDocumentFormat(meta.Options.DocumentFormat)
	if meta.Options.ColumnStore != nil || len(meta.TextIndexes) != 0 || len(meta.VectorIndexes) == 0 || (format != DocumentFormatJSON && format != DocumentFormatBSON) {
		return false
	}
	for _, def := range meta.VectorIndexes {
		if !vectorIndexDefinitionUsesNativeRuntime(def) || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 || def.Dimensions <= 0 || def.Representation != "" || len(def.QuantizedIndexes) != 0 {
			return false
		}
	}
	return true
}

func validateNativeDocumentCoverageMeta(meta CollectionMeta) error {
	switch meta.nativeDocumentCoverageVersion {
	case 0:
		return nil
	case nativeDocumentVectorCoverageVersion:
		if nativeDocumentVectorShape(meta) {
			return nil
		}
	}
	return backenddb.ErrNativeSemanticUnsupported
}

func collectionMetaDiskValue(meta CollectionMeta) collectionMetaDisk {
	version := collectionMetaVersion
	if meta.nativeDocumentCoverageVersion != 0 {
		version = collectionMetaVersionNativeDocument
	}
	return collectionMetaDisk{
		Version: version, Name: meta.Name, Options: meta.Options, Indexes: meta.Indexes,
		VectorIndexes: meta.VectorIndexes, TextIndexes: meta.TextIndexes,
		NativeDocumentCoverageVersion: meta.nativeDocumentCoverageVersion,
	}
}

// The actual incarnation is allocated by the original catalog producer. A
// name, WAL LSN, physical root or runtime sequence cannot substitute for it.
func nativeDocumentGenerationKey(incarnation uint64) (string, error) {
	if incarnation == 0 {
		return "", errNativeCatalogIdentity
	}
	return fmt.Sprintf("%s%020d", nativeDocumentGenerationPrefix, incarnation), nil
}

// Creation is the only absent-key bootstrap. The caller has selected a new
// actual incarnation and owns the original serialized catalog publication;
// neither ordinary reads nor idempotent creates may synthesize this record.
func addNativeDocumentGenerationCreateUpdate(meta CollectionMeta, incarnation uint64, updates map[string][]byte) error {
	if err := validateNativeDocumentCoverageMeta(meta); err != nil {
		return err
	}
	if meta.nativeDocumentCoverageVersion == 0 {
		return nil
	}
	if updates == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	key, err := nativeDocumentGenerationKey(incarnation)
	if err != nil {
		return err
	}
	updates[key] = encodeRootID(0)
	return nil
}

// One canonical frame reduces each ID to its opening and final stored image.
// In particular A->B->A within one composite contributes zero; two separate
// frames must reduce against successive predecessors and contribute twice.
func nativeDocumentGenerationAfter(old uint64, rows *nativeJSONRows) (uint64, error) {
	if rows == nil {
		return old, backenddb.ErrNativeSemanticUnsupported
	}
	next := old
	for _, row := range rows.rows {
		if (row.opening == nil) == (row.current == nil) && bytes.Equal(row.opening, row.current) {
			continue
		}
		if next == ^uint64(0) {
			return 0, fmt.Errorf("collections: native document generation overflow")
		}
		next++
	}
	return next, nil
}
