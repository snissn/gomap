package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

const columnVectorGraphDirectViewOwnerVectorIndexState = "vector_index_state"

func columnVectorGraphDirectViewExpectation(expectedRole, actualRole string, column string, rows int, ref ColumnAssetRef) typeddecode.GraphDirectViewExpectation {
	return typeddecode.GraphDirectViewExpectation{
		ExpectedOwner:  columnVectorGraphDirectViewOwnerVectorIndexState,
		ActualOwner:    columnVectorGraphDirectViewOwnerVectorIndexState,
		ExpectedRole:   expectedRole,
		ActualRole:     actualRole,
		Column:         column,
		Rows:           rows,
		AssetOffset:    ref.Offset,
		HasAssetOffset: true,
	}
}

func acquireColumnVectorGraphPreparedStateSection(rootDir, collection, scopeID, scopeReason, acquireReason string, ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, checksum uint32, manager *mappedresource.Manager) (*mappedresource.Handle, mappedresource.Key, error) {
	if manager == nil {
		return nil, mappedresource.Key{}, errors.New("collections: column_graph prepared state requires mappedresource manager")
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, mappedresource.Key{}, err
	}
	sectionOffset, err := columnVectorGraphTypedColumnSectionOffset(ref, section)
	if err != nil {
		return nil, mappedresource.Key{}, err
	}
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  ref.Namespace,
		Kind:       string(ref.Kind),
		Generation: ref.Generation,
		PartID:     ref.PartID,
		FileID:     ref.FileID,
		Offset:     sectionOffset,
		Length:     int64(section.Length),
		Checksum:   uint64(checksum),
		Version:    imageVersion,
		Encoding:   section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Name:     section.Name,
			Column:   section.Column,
		},
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: scopeID, Collection: collection, Namespace: ref.Namespace, Generation: ref.Generation, Reason: scopeReason}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{
		Reason:         acquireReason,
		ValidationMode: mappedresource.ValidationVerify,
		PreferMapped:   true,
		AllowHeapCopy:  true,
		ResourceRoot:   rootDir,
		ResourcePath:   path,
	})
	if err != nil {
		return nil, key, err
	}
	if got := checksumColumnVectorGraphPreparedStateSection(handle); got != checksum {
		releaseErr := handle.Release()
		return nil, key, errors.Join(fmt.Errorf("collections: %s checksum=%d want %d", acquireReason, got, checksum), releaseErr)
	}
	return handle, key, nil
}

func checksumColumnVectorGraphPreparedStateSection(handle *mappedresource.Handle) uint32 {
	if handle == nil {
		return 0
	}
	return page.Checksum(handle.Bytes())
}

func columnVectorGraphPreparedStateDirectFallbackAllowed(status typeddecode.Status) bool {
	switch status.Reason {
	case typeddecode.ReasonHandleSourceUnsupported, typeddecode.ReasonActualPointerUnaligned, typeddecode.ReasonUnaligned, typeddecode.ReasonWrongEndian:
		return true
	default:
		return false
	}
}
