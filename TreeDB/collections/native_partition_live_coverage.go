package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// Version 3 covers the original physical document-mutation generation. Version
// 4 covers the complete authenticated immutable cdir2 source, not a logical
// publication sequence or physical epoch. The existing field explicitly tags
// the different authority; both runtime and startup replay use this adapter.
const vectorIndexNativeSourceCoverageVersion = 4

type vectorIndexDocumentCoverage struct {
	version    int
	generation uint64
	source     VectorPartitionSourceIdentityV1
}

func vectorIndexCoverageAtRoot(meta CollectionMeta, snap *backenddb.Snapshot, manifestRoot uint64) (vectorIndexDocumentCoverage, error) {
	if nativeSourceImportMetaV2(meta) {
		header, err := nativeVectorGraphHeader(meta, snap, manifestRoot)
		if err != nil {
			return vectorIndexDocumentCoverage{}, err
		}
		cfg := meta.Options.ColumnStore
		return vectorIndexDocumentCoverage{
			version: vectorIndexNativeSourceCoverageVersion, generation: header.Generation,
			source: VectorPartitionSourceIdentityV1{Generation: header.Generation, Checksum: cfg.ActiveManifest.Checksum, SchemaHash: cfg.SchemaHash, RowCount: header.Rows},
		}, nil
	}
	generation, err := vectorIndexDocumentGenerationForCollection(snap, meta.Name)
	return vectorIndexDocumentCoverage{version: vectorIndexDocumentGenerationVersion, generation: generation}, err
}

func vectorIndexCoverageAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog) (vectorIndexDocumentCoverage, error) {
	if snap == nil || catalog == nil {
		return vectorIndexDocumentCoverage{}, errCollectionNotFound
	}
	return vectorIndexCoverageAtRoot(catalog.meta, snap, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)))
}

func (coverage vectorIndexDocumentCoverage) matches(meta vectorIndexPersistMeta) bool {
	if meta.SourceDocumentGenerationVersion != coverage.version || meta.SourceDocumentGeneration != coverage.generation {
		return false
	}
	switch coverage.version {
	case vectorIndexDocumentGenerationVersion:
		return true
	case vectorIndexNativeSourceCoverageVersion:
		return coverage.generation != 0 && meta.PartitionLive != nil &&
			meta.PartitionLive.Source == coverage.source && meta.PartitionLive.Coverage == coverage.generation
	default:
		return false
	}
}

// The source header is validated under the caller's original read scope before
// saving. No tag changes an ordinary graph snapshot or grants source access.
func (idx *VectorIndex) persistCoverageVersionLocked() int {
	if idx.partitionLive != nil && idx.collection != nil && nativeSourceImportMetaV2(idx.collection.meta) {
		return vectorIndexNativeSourceCoverageVersion
	}
	return vectorIndexDocumentGenerationVersion
}
