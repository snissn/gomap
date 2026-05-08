package nativewire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type CollectionHandle uint64

const (
	maxCollectionMetaIndexDefinitions = 1 << 16
	minEncodedIndexDefinitionLen      = 6
	collectionRefTagName              = 1
	collectionRefTagHandle            = 2
)

func collectionNameRef(name string) iwire.Section {
	payload := []byte{collectionRefTagName}
	payload = append(payload, name...)
	return iwire.Section{ID: iwire.SectionCollectionRef, Bytes: payload}
}

func collectionHandleRef(handle CollectionHandle) iwire.Section {
	payload := []byte{collectionRefTagHandle}
	payload = binary.AppendUvarint(payload, uint64(handle))
	return iwire.Section{ID: iwire.SectionCollectionRef, Bytes: payload}
}

func encodeCollectionMeta(meta collections.CollectionMeta) []byte {
	dst := binary.AppendUvarint(nil, 1)
	dst = appendString(dst, meta.Name)
	dst = binary.AppendUvarint(dst, uint64(encodeDocumentFormat(meta.Options.DocumentFormat)))
	dst = binary.AppendUvarint(dst, uint64(encodeRootStorage(meta.Options.DataRootStoragePolicy)))
	dst = binary.AppendUvarint(dst, uint64(encodeRootStorage(meta.Options.IndexStateStoragePolicy)))
	dst = appendBool(dst, meta.Options.AllowArrayValuesInIndex)
	dst = appendBool(dst, meta.Options.DisableIndexedWriteMemtables)
	dst = appendBool(dst, meta.Options.BufferedIndexedWrites)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedWriteMaxDocuments))
	dst = binary.AppendVarint(dst, meta.Options.BufferedIndexedWriteMaxBytes)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedWriteMaxRootRuns))
	dst = appendBool(dst, meta.Options.BufferedIndexedAsyncFlush)
	dst = appendBool(dst, meta.Options.BufferedIndexedOverlayRoots)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits))
	dst = binary.AppendUvarint(dst, uint64(len(meta.Indexes)))
	for _, def := range meta.Indexes {
		dst = appendIndexDefinition(dst, def, false)
	}
	return dst
}

func decodeCollectionMeta(src []byte) (collections.CollectionMeta, error) {
	version, off, err := readUvarint(src)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if version != 1 {
		return collections.CollectionMeta{}, protocolError(iwire.ErrUnsupportedVersion, "collection_meta version %d", version)
	}
	name, err := readString(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	docFormat, err := readEnum(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	dataPolicy, err := readEnum(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	indexStatePolicy, err := readEnum(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	allowArray, err := readBool(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	disableMemtables, err := readBool(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	bufferedWrites, err := readBool(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxDocs, err := readVarint(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxBytes, err := readVarint(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxRootRuns, err := readVarint(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	asyncFlush, err := readBool(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	overlayRoots, err := readBool(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxQueued, err := readVarint(src, &off)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNonNegativeIntCapacity("buffered_indexed_write_max_documents", maxDocs); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNoNegativeInt64("buffered_indexed_write_max_bytes", maxBytes); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNonNegativeIntCapacity("buffered_indexed_write_max_root_runs", maxRootRuns); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNonNegativeIntCapacity("buffered_indexed_async_flush_max_queued_units", maxQueued); err != nil {
		return collections.CollectionMeta{}, err
	}
	indexCount, err := readUvarintField(src, &off, "index_count")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if indexCount > uint64(maxInt) {
		return collections.CollectionMeta{}, protocolError(iwire.ErrResourceExhausted, "index count exceeds int capacity")
	}
	if indexCount > maxCollectionMetaIndexDefinitions {
		return collections.CollectionMeta{}, protocolError(iwire.ErrResourceExhausted, "index count %d exceeds limit %d", indexCount, maxCollectionMetaIndexDefinitions)
	}
	if indexCount > uint64((len(src)-off)/minEncodedIndexDefinitionLen) {
		return collections.CollectionMeta{}, protocolError(iwire.ErrMalformedFrame, "index count %d exceeds remaining collection_meta payload", indexCount)
	}
	documentFormat, err := decodeDocumentFormatStrict(docFormat)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	dataRootStorage, err := decodeRootStorageStrict(dataPolicy)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	indexStateRootStorage, err := decodeRootStorageStrict(indexStatePolicy)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	meta := collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			DocumentFormat:                          documentFormat,
			DataRootStoragePolicy:                   dataRootStorage,
			IndexStateStoragePolicy:                 indexStateRootStorage,
			AllowArrayValuesInIndex:                 allowArray,
			DisableIndexedWriteMemtables:            disableMemtables,
			BufferedIndexedWrites:                   bufferedWrites,
			BufferedIndexedWriteMaxDocuments:        int(maxDocs),
			BufferedIndexedWriteMaxBytes:            maxBytes,
			BufferedIndexedWriteMaxRootRuns:         int(maxRootRuns),
			BufferedIndexedAsyncFlush:               asyncFlush,
			BufferedIndexedOverlayRoots:             overlayRoots,
			BufferedIndexedAsyncFlushMaxQueuedUnits: int(maxQueued),
		},
		Indexes: make([]collections.IndexDefinition, 0, int(indexCount)),
	}
	for i := uint64(0); i < indexCount; i++ {
		def, next, err := decodeIndexDefinitionAt(src, off, false)
		if err != nil {
			return collections.CollectionMeta{}, err
		}
		meta.Indexes = append(meta.Indexes, def)
		off = next
	}
	if off != len(src) {
		return collections.CollectionMeta{}, protocolError(iwire.ErrMalformedFrame, "collection_meta has %d trailing bytes", len(src)-off)
	}
	return meta, nil
}

func encodeIndexDefinition(def collections.IndexDefinition) []byte {
	return appendIndexDefinition(binary.AppendUvarint(nil, 1), def, true)
}

func appendIndexDefinition(dst []byte, def collections.IndexDefinition, withVersion bool) []byte {
	if withVersion && len(dst) == 0 {
		dst = binary.AppendUvarint(dst, 1)
	}
	dst = appendString(dst, def.Name)
	dst = appendString(dst, def.Field)
	dst = binary.AppendUvarint(dst, uint64(encodeIndexValueType(def.ValueType)))
	dst = appendBool(dst, def.Unique)
	dst = appendBool(dst, def.MultiKey)
	dst = binary.AppendUvarint(dst, uint64(encodeRootStorage(def.StoragePolicy)))
	return dst
}

func decodeIndexDefinition(src []byte) (collections.IndexDefinition, error) {
	def, off, err := decodeIndexDefinitionAt(src, 0, true)
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	if off != len(src) {
		return collections.IndexDefinition{}, protocolError(iwire.ErrMalformedFrame, "index_definition has %d trailing bytes", len(src)-off)
	}
	return def, nil
}

func decodeIndexDefinitionAt(src []byte, off int, withVersion bool) (collections.IndexDefinition, int, error) {
	if withVersion {
		version, n, err := readUvarint(src[off:])
		if err != nil {
			return collections.IndexDefinition{}, 0, err
		}
		if version != 1 {
			return collections.IndexDefinition{}, 0, protocolError(iwire.ErrUnsupportedVersion, "index_definition version %d", version)
		}
		off += n
	}
	name, err := readString(src, &off)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	field, err := readString(src, &off)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	valueType, err := readEnum(src, &off)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	unique, err := readBool(src, &off)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	multiKey, err := readBool(src, &off)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	storagePolicy, err := readEnum(src, &off)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	decodedValueType, err := decodeIndexValueTypeStrict(valueType)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	decodedStoragePolicy, err := decodeRootStorageStrict(storagePolicy)
	if err != nil {
		return collections.IndexDefinition{}, 0, err
	}
	return collections.IndexDefinition{
		Name:          name,
		Field:         field,
		ValueType:     decodedValueType,
		Unique:        unique,
		MultiKey:      multiKey,
		StoragePolicy: decodedStoragePolicy,
	}, off, nil
}

func encodeCollectionMetaVector(metas []collections.CollectionMeta) []byte {
	items := make([][]byte, len(metas))
	for i := range metas {
		items[i] = encodeCollectionMeta(metas[i])
	}
	return iwire.AppendByteVector(nil, items...)
}

func decodeCollectionMetaVector(src []byte, limits iwire.Limits) ([]collections.CollectionMeta, error) {
	vec, err := iwire.DecodeByteVector(src, limits)
	if err != nil {
		return nil, err
	}
	out := make([]collections.CollectionMeta, 0, vec.Len())
	for i := 0; i < vec.Len(); i++ {
		item, _ := vec.Item(i)
		meta, err := decodeCollectionMeta(item)
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	return out, nil
}

func encodeIndexDefinitionVector(indexes []collections.IndexDefinition) []byte {
	items := make([][]byte, len(indexes))
	for i := range indexes {
		items[i] = encodeIndexDefinition(indexes[i])
	}
	return iwire.AppendByteVector(nil, items...)
}

func decodeIndexDefinitionVector(src []byte, limits iwire.Limits) ([]collections.IndexDefinition, error) {
	vec, err := iwire.DecodeByteVector(src, limits)
	if err != nil {
		return nil, err
	}
	out := make([]collections.IndexDefinition, 0, vec.Len())
	for i := 0; i < vec.Len(); i++ {
		item, _ := vec.Item(i)
		def, err := decodeIndexDefinition(item)
		if err != nil {
			return nil, err
		}
		out = append(out, def)
	}
	return out, nil
}

func appendBool(dst []byte, value bool) []byte {
	if value {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func readBool(src []byte, off *int) (bool, error) {
	if off == nil || *off < 0 || *off >= len(src) {
		return false, protocolError(iwire.ErrMalformedFrame, "missing bool")
	}
	value := src[*off]
	*off = *off + 1
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, protocolError(iwire.ErrMalformedFrame, "invalid bool %d", value)
	}
}

func readEnum(src []byte, off *int) (uint64, error) {
	return readUvarintField(src, off, "enum")
}

func readUvarintField(src []byte, off *int, field string) (uint64, error) {
	if off == nil || *off < 0 || *off > len(src) {
		return 0, protocolError(iwire.ErrMalformedFrame, "invalid %s offset", field)
	}
	value, n, err := readUvarint(src[*off:])
	if err != nil {
		return 0, err
	}
	*off += n
	return value, nil
}

func readVarint(src []byte, off *int) (int64, error) {
	if off == nil || *off < 0 || *off > len(src) {
		return 0, protocolError(iwire.ErrMalformedFrame, "invalid varint offset")
	}
	value, n := binary.Varint(src[*off:])
	if n <= 0 {
		return 0, protocolError(iwire.ErrMalformedFrame, "invalid varint")
	}
	*off += n
	return value, nil
}

func encodeDocumentFormat(format collections.DocumentFormat) iwire.DocumentFormat {
	switch format {
	case collections.DocumentFormatDefault:
		return iwire.DocumentFormatDefault
	case collections.DocumentFormatJSON:
		return iwire.DocumentFormatJSON
	case collections.DocumentFormatBSON:
		return iwire.DocumentFormatBSON
	case collections.DocumentFormatTemplateV1:
		return iwire.DocumentFormatTemplateV1
	default:
		return iwire.DocumentFormatDefault
	}
}

func decodeDocumentFormat(format iwire.DocumentFormat) collections.DocumentFormat {
	switch format {
	case iwire.DocumentFormatJSON:
		return collections.DocumentFormatJSON
	case iwire.DocumentFormatBSON:
		return collections.DocumentFormatBSON
	case iwire.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1
	default:
		return collections.DocumentFormatDefault
	}
}

func decodeDocumentFormatStrict(format uint64) (collections.DocumentFormat, error) {
	switch iwire.DocumentFormat(format) {
	case iwire.DocumentFormatDefault:
		return collections.DocumentFormatDefault, nil
	case iwire.DocumentFormatJSON:
		return collections.DocumentFormatJSON, nil
	case iwire.DocumentFormatBSON:
		return collections.DocumentFormatBSON, nil
	case iwire.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1, nil
	default:
		return "", protocolError(iwire.ErrInvalidCommand, "unsupported document_format enum %d", format)
	}
}

func encodeRootStorage(policy collections.RootStoragePolicy) uint64 {
	switch policy {
	case collections.RootStorageFast:
		return 1
	case collections.RootStorageCompressed:
		return 2
	default:
		return 0
	}
}

func decodeRootStorage(policy uint64) collections.RootStoragePolicy {
	switch policy {
	case 1:
		return collections.RootStorageFast
	case 2:
		return collections.RootStorageCompressed
	default:
		return collections.RootStorageDefault
	}
}

func decodeRootStorageStrict(policy uint64) (collections.RootStoragePolicy, error) {
	switch policy {
	case 0:
		return collections.RootStorageDefault, nil
	case 1:
		return collections.RootStorageFast, nil
	case 2:
		return collections.RootStorageCompressed, nil
	default:
		return "", protocolError(iwire.ErrInvalidCommand, "unsupported root_storage enum %d", policy)
	}
}

func encodeIndexValueType(valueType collections.IndexValueType) uint64 {
	switch valueType {
	case collections.IndexValueString:
		return 1
	case collections.IndexValueBool:
		return 2
	case collections.IndexValueInt64:
		return 3
	case collections.IndexValueDouble:
		return 4
	default:
		return 0
	}
}

func decodeIndexValueType(valueType uint64) collections.IndexValueType {
	switch valueType {
	case 1:
		return collections.IndexValueString
	case 2:
		return collections.IndexValueBool
	case 3:
		return collections.IndexValueInt64
	case 4:
		return collections.IndexValueDouble
	default:
		return ""
	}
}

func decodeIndexValueTypeStrict(valueType uint64) (collections.IndexValueType, error) {
	switch valueType {
	case 1:
		return collections.IndexValueString, nil
	case 2:
		return collections.IndexValueBool, nil
	case 3:
		return collections.IndexValueInt64, nil
	case 4:
		return collections.IndexValueDouble, nil
	default:
		return "", protocolError(iwire.ErrInvalidCommand, "unsupported index_value_type enum %d", valueType)
	}
}

func decodeCollectionRef(state *connState, raw []byte) (string, bool, error) {
	if len(raw) == 0 {
		return "", false, protocolError(iwire.ErrInvalidCommand, "empty collection_ref")
	}
	switch raw[0] {
	case collectionRefTagName:
		name := string(raw[1:])
		if err := collections.ValidateCollectionName(name); err != nil {
			return "", false, protocolError(iwire.ErrInvalidCommand, "%v", err)
		}
		return name, false, nil
	case collectionRefTagHandle:
		handle, n, err := readUvarint(raw[1:])
		if err != nil {
			return "", true, err
		}
		if n+1 != len(raw) {
			return "", true, protocolError(iwire.ErrMalformedFrame, "collection handle ref has trailing bytes")
		}
		if state == nil {
			return "", true, protocolError(iwire.ErrInvalidCommand, "collection handle requires connection state")
		}
		name, ok := state.collectionForHandle(CollectionHandle(handle))
		if !ok {
			return "", true, protocolError(iwire.ErrCollectionNotFound, "collection handle %d not found", handle)
		}
		return name, true, nil
	default:
		name := string(raw)
		if err := collections.ValidateCollectionName(name); err != nil {
			return "", false, protocolError(iwire.ErrInvalidCommand, "%v", err)
		}
		return name, false, nil
	}
}

func collectionRefFromSections(state *connState, sections []iwire.Section) (string, bool, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionRef)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, protocolError(iwire.ErrInvalidCommand, "missing collection_ref")
	}
	return decodeCollectionRef(state, raw)
}

func collectionNameFromSections(sections []iwire.Section) (string, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionRef)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", protocolError(iwire.ErrInvalidCommand, "missing collection_ref")
	}
	if len(raw) > 0 && raw[0] == collectionRefTagHandle {
		return "", protocolError(iwire.ErrInvalidCommand, "collection handle is not valid for this command")
	}
	name, wasHandle, err := decodeCollectionRef(nil, raw)
	if err != nil {
		return "", err
	}
	if wasHandle {
		return "", protocolError(iwire.ErrInvalidCommand, "collection handle is not valid for this command")
	}
	return name, nil
}

func collectionHandleFromSections(state *connState, sections []iwire.Section) (CollectionHandle, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionRef)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, protocolError(iwire.ErrInvalidCommand, "missing collection_ref")
	}
	if len(raw) == 0 || raw[0] != collectionRefTagHandle {
		return 0, protocolError(iwire.ErrInvalidCommand, "close_collection requires a collection handle")
	}
	if state == nil {
		return 0, protocolError(iwire.ErrInvalidCommand, "collection handle requires connection state")
	}
	handle, n, err := readUvarint(raw[1:])
	if err != nil {
		return 0, err
	}
	if n+1 != len(raw) {
		return 0, protocolError(iwire.ErrMalformedFrame, "collection handle ref has trailing bytes")
	}
	return CollectionHandle(handle), nil
}

func expectedCatalogVersionFromSections(sections []iwire.Section) (uint64, bool, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionExpectedCatalogVersion)
	if err != nil || !ok {
		return 0, ok, err
	}
	version, n, err := readUvarint(raw)
	if err != nil {
		return 0, true, err
	}
	if n != len(raw) {
		return 0, true, protocolError(iwire.ErrMalformedFrame, "expected_catalog_version has trailing bytes")
	}
	return version, true, nil
}

func (s *Server) currentCatalogVersion() (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, protocolError(iwire.ErrInvalidCommand, "catalog version guard requires a backend")
	}
	snap := s.backend.AcquireSnapshot()
	if snap == nil {
		return 0, protocolError(iwire.ErrInternal, "catalog snapshot unavailable")
	}
	defer func() { _ = snap.Close() }()
	state := snap.State()
	if state == nil {
		return 0, protocolError(iwire.ErrInternal, "catalog snapshot state unavailable")
	}
	return state.CommitSeq, nil
}

func (s *Server) checkCatalogGuard(sections []iwire.Section) error {
	expected, ok, err := expectedCatalogVersionFromSections(sections)
	if err != nil || !ok {
		return err
	}
	actual, err := s.currentCatalogVersion()
	if err != nil {
		return err
	}
	if actual != expected {
		return protocolError(iwire.ErrCatalogVersionMismatch, "catalog version %d does not match expected %d", actual, expected)
	}
	return nil
}

func metadataSection(sections []iwire.Section, id iwire.SectionID) ([]byte, error) {
	raw, ok, err := singletonSection(sections, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrInvalidCommand, "missing section %d", id)
	}
	return raw, nil
}

func managerRequired(m *collections.CollectionManager) error {
	if m == nil {
		return protocolError(iwire.ErrInvalidCommand, "nativewire server has no collection manager")
	}
	return nil
}

func unsupportedDropCollection() error {
	return protocolError(iwire.ErrUnsupportedFeature, "drop_collection is reserved until collections expose a drop primitive")
}

func expectNoError(err error) error {
	if err != nil {
		return err
	}
	return nil
}

func collectionNotFound(name string) error {
	return protocolError(iwire.ErrCollectionNotFound, "collection %q not found", name)
}

func invalidMetadata(format string, args ...any) error {
	return protocolError(iwire.ErrInvalidCommand, format, args...)
}

func ensureIndexName(name string) error {
	if err := collections.ValidateIndexName(name); err != nil {
		return invalidMetadata("%v", err)
	}
	return nil
}

func ensureNoNegativeInt64(name string, value int64) error {
	if value < 0 {
		return invalidMetadata("%s cannot be negative", name)
	}
	return nil
}

func ensureNonNegativeIntCapacity(name string, value int64) error {
	if value < 0 {
		return invalidMetadata("%s cannot be negative", name)
	}
	if value > int64(maxInt) {
		return protocolError(iwire.ErrResourceExhausted, "%s exceeds int capacity", name)
	}
	return nil
}

func ensureKnownDocumentFormat(format collections.DocumentFormat) error {
	switch format {
	case collections.DocumentFormatDefault, collections.DocumentFormatJSON, collections.DocumentFormatBSON, collections.DocumentFormatTemplateV1:
		return nil
	default:
		return invalidMetadata("unsupported document format %q", format)
	}
}

func ensureKnownRootStoragePolicy(policy collections.RootStoragePolicy) error {
	switch policy {
	case collections.RootStorageDefault, collections.RootStorageFast, collections.RootStorageCompressed:
		return nil
	default:
		return invalidMetadata("unsupported root storage policy %q", policy)
	}
}

func ensureKnownIndexValueType(valueType collections.IndexValueType) error {
	switch valueType {
	case collections.IndexValueString, collections.IndexValueBool, collections.IndexValueInt64, collections.IndexValueDouble:
		return nil
	default:
		return invalidMetadata("unsupported index value type %q", valueType)
	}
}

func normalizeClientIndexDefinition(def collections.IndexDefinition) error {
	if err := ensureIndexName(def.Name); err != nil {
		return err
	}
	if err := collections.ValidateIndexPath(def.Field); err != nil {
		return invalidMetadata("%v", err)
	}
	if err := ensureKnownIndexValueType(def.ValueType); err != nil {
		return err
	}
	return ensureKnownRootStoragePolicy(def.StoragePolicy)
}

func normalizeClientCollectionMeta(meta collections.CollectionMeta) (collections.CollectionMeta, error) {
	if err := collections.ValidateCollectionName(meta.Name); err != nil {
		return collections.CollectionMeta{}, invalidMetadata("%v", err)
	}
	if err := ensureKnownDocumentFormat(meta.Options.DocumentFormat); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureKnownRootStoragePolicy(meta.Options.DataRootStoragePolicy); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureKnownRootStoragePolicy(meta.Options.IndexStateStoragePolicy); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNonNegativeIntCapacity("buffered_indexed_write_max_documents", int64(meta.Options.BufferedIndexedWriteMaxDocuments)); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNoNegativeInt64("buffered_indexed_write_max_bytes", meta.Options.BufferedIndexedWriteMaxBytes); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNonNegativeIntCapacity("buffered_indexed_write_max_root_runs", int64(meta.Options.BufferedIndexedWriteMaxRootRuns)); err != nil {
		return collections.CollectionMeta{}, err
	}
	if err := ensureNonNegativeIntCapacity("buffered_indexed_async_flush_max_queued_units", int64(meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits)); err != nil {
		return collections.CollectionMeta{}, err
	}
	for _, def := range meta.Indexes {
		if err := normalizeClientIndexDefinition(def); err != nil {
			return collections.CollectionMeta{}, err
		}
	}
	return meta, nil
}

func firstMetaFromResponse(sections []iwire.Section) (collections.CollectionMeta, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionMeta)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if !ok {
		return collections.CollectionMeta{}, protocolError(iwire.ErrMalformedFrame, "missing collection_meta response")
	}
	return decodeCollectionMeta(raw)
}

func firstIndexVectorFromResponse(sections []iwire.Section, limits iwire.Limits) ([]collections.IndexDefinition, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionIndexDefinition)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrMalformedFrame, "missing index_definition response")
	}
	return decodeIndexDefinitionVector(raw, limits)
}

func firstMetaVectorFromResponse(sections []iwire.Section, limits iwire.Limits) ([]collections.CollectionMeta, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionMeta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrMalformedFrame, "missing collection_meta response")
	}
	return decodeCollectionMetaVector(raw, limits)
}

func firstHandleFromResponse(sections []iwire.Section) (CollectionHandle, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionHandle)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, protocolError(iwire.ErrMalformedFrame, "missing collection_handle response")
	}
	handle, n, err := readUvarint(raw)
	if err != nil {
		return 0, err
	}
	if n != len(raw) {
		return 0, protocolError(iwire.ErrMalformedFrame, "collection_handle has trailing bytes")
	}
	return CollectionHandle(handle), nil
}

func encodeHandle(handle CollectionHandle) []byte {
	return binary.AppendUvarint(nil, uint64(handle))
}

func decodeIndexName(src []byte) (string, error) {
	off := 0
	name, err := readString(src, &off)
	if err != nil {
		return "", err
	}
	if off != len(src) {
		return "", protocolError(iwire.ErrMalformedFrame, "index_name has trailing bytes")
	}
	if err := ensureIndexName(name); err != nil {
		return "", err
	}
	return name, nil
}

func encodeIndexName(name string) []byte {
	return appendString(nil, name)
}

func metadataWrap(err error) error {
	if err == nil {
		return nil
	}
	var p *iwire.ProtocolError
	if errors.As(err, &p) {
		return err
	}
	return fmt.Errorf("nativewire metadata: %w", err)
}
