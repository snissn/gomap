package nativewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type CollectionHandle uint64

const (
	collectionMetaWireVersion          = 5
	maxCollectionMetaIndexDefinitions  = 1 << 16
	minEncodedIndexDefinitionLen       = 6
	minEncodedVectorIndexDefinitionLen = 7
	collectionRefTagName               = 1
	collectionRefTagHandle             = 2
)

type metadataIdempotencyEntry struct {
	commandID iwire.CommandID
	signature [sha256.Size]byte
	response  []iwire.Section
}

func collectionNameRef(name string) iwire.Section {
	return iwire.Section{ID: iwire.SectionCollectionRef, Bytes: appendCollectionNameRefPayload(nil, name)}
}

func collectionNameRefPayloadLen(name string) int {
	return 1 + len(name)
}

func appendCollectionNameRefPayload(dst []byte, name string) []byte {
	dst = append(dst, collectionRefTagName)
	return append(dst, name...)
}

func collectionHandleRef(handle CollectionHandle) iwire.Section {
	return iwire.Section{ID: iwire.SectionCollectionRef, Bytes: appendCollectionHandleRefPayload(nil, handle)}
}

func appendCollectionHandleRefPayload(dst []byte, handle CollectionHandle) []byte {
	dst = append(dst, collectionRefTagHandle)
	return binary.AppendUvarint(dst, uint64(handle))
}

func encodeCollectionMeta(meta collections.CollectionMeta) []byte {
	dst := binary.AppendUvarint(nil, collectionMetaWireVersion)
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
	dst = binary.AppendUvarint(dst, uint64(len(meta.VectorIndexes)))
	for _, def := range meta.VectorIndexes {
		dst = appendVectorIndexDefinitionForCollectionMeta(dst, def, collectionMetaWireVersion)
	}
	return dst
}

func decodeCollectionMeta(src []byte) (collections.CollectionMeta, error) {
	version, off, err := readUvarint(src)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if version < 1 || version > collectionMetaWireVersion {
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
	if version >= 2 {
		vectorIndexCount, err := readUvarintField(src, &off, "vector_index_count")
		if err != nil {
			return collections.CollectionMeta{}, err
		}
		if vectorIndexCount > uint64(maxInt) {
			return collections.CollectionMeta{}, protocolError(iwire.ErrResourceExhausted, "vector index count exceeds int capacity")
		}
		if vectorIndexCount > maxCollectionMetaIndexDefinitions {
			return collections.CollectionMeta{}, protocolError(iwire.ErrResourceExhausted, "vector index count %d exceeds limit %d", vectorIndexCount, maxCollectionMetaIndexDefinitions)
		}
		if vectorIndexCount > uint64((len(src)-off)/minEncodedVectorIndexDefinitionLen) {
			return collections.CollectionMeta{}, protocolError(iwire.ErrMalformedFrame, "vector index count %d exceeds remaining collection_meta payload", vectorIndexCount)
		}
		meta.VectorIndexes = make([]collections.VectorIndexDefinition, 0, int(vectorIndexCount))
		for i := uint64(0); i < vectorIndexCount; i++ {
			def, next, err := decodeVectorIndexDefinitionForCollectionMeta(src, off, version)
			if err != nil {
				return collections.CollectionMeta{}, err
			}
			meta.VectorIndexes = append(meta.VectorIndexes, def)
			off = next
		}
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

func appendVectorIndexDefinition(dst []byte, def collections.VectorIndexDefinition, withVersion bool) []byte {
	if withVersion && len(dst) == 0 {
		dst = binary.AppendUvarint(dst, 1)
	}
	return appendVectorIndexDefinitionFields(dst, def)
}

func appendVectorIndexDefinitionForCollectionMeta(dst []byte, def collections.VectorIndexDefinition, collectionMetaVersion uint64) []byte {
	dst = appendVectorIndexDefinitionFields(dst, def)
	if collectionMetaVersion >= 3 {
		dst = binary.AppendUvarint(dst, encodeVectorIndexStrategy(def.Strategy))
		dst = binary.AppendUvarint(dst, uint64(len(def.QuantizedIndexes)))
		for _, q := range def.QuantizedIndexes {
			dst = appendString(dst, q.Name)
			dst = appendString(dst, q.Codec)
			dst = binary.AppendUvarint(dst, uint64(q.Version))
			if collectionMetaVersion >= 4 {
				dst = appendBool(dst, q.ScalarU8Calibration != nil)
				if q.ScalarU8Calibration != nil {
					dst = appendString(dst, string(q.ScalarU8Calibration.Mode))
					dst = appendString(dst, string(q.ScalarU8Calibration.Grouping))
					dst = appendString(dst, string(q.ScalarU8Calibration.AlphaPolicy.Name))
					dst = binary.AppendUvarint(dst, uint64(q.ScalarU8Calibration.AlphaPolicy.QuantilePPM))
				}
			}
		}
	}
	return dst
}

func appendVectorIndexDefinitionFields(dst []byte, def collections.VectorIndexDefinition) []byte {
	dst = appendString(dst, def.Name)
	dst = appendString(dst, def.Field)
	dst = binary.AppendUvarint(dst, encodeVectorMetric(def.Metric))
	dst = binary.AppendVarint(dst, int64(def.Dimensions))
	dst = binary.AppendVarint(dst, int64(def.M))
	dst = binary.AppendVarint(dst, int64(def.EfConstruction))
	dst = binary.AppendVarint(dst, int64(def.EfSearch))
	dst = binary.AppendUvarint(dst, encodeVectorIndexEncoding(def.Encoding))
	return dst
}

func decodeVectorIndexDefinitionAt(src []byte, off int, withVersion bool) (collections.VectorIndexDefinition, int, error) {
	if withVersion {
		version, n, err := readUvarint(src[off:])
		if err != nil {
			return collections.VectorIndexDefinition{}, 0, err
		}
		if version != 1 {
			return collections.VectorIndexDefinition{}, 0, protocolError(iwire.ErrUnsupportedVersion, "vector_index_definition version %d", version)
		}
		off += n
	}
	name, err := readString(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	field, err := readString(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	metric, err := readEnum(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	dimensions, err := readVarint(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	m, err := readVarint(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	efConstruction, err := readVarint(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	efSearch, err := readVarint(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	encoding, err := readEnum(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	if err := ensureNonNegativeIntCapacity("vector_index dimensions", dimensions); err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	if err := ensureNonNegativeIntCapacity("vector_index m", m); err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	if err := ensureNonNegativeIntCapacity("vector_index ef_construction", efConstruction); err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	if err := ensureNonNegativeIntCapacity("vector_index ef_search", efSearch); err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	decodedMetric, err := decodeVectorMetricStrict(metric)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	decodedEncoding, err := decodeVectorIndexEncodingStrict(encoding)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	return collections.VectorIndexDefinition{
		Name:           name,
		Field:          field,
		Metric:         decodedMetric,
		Dimensions:     int(dimensions),
		M:              int(m),
		EfConstruction: int(efConstruction),
		EfSearch:       int(efSearch),
		Encoding:       decodedEncoding,
	}, off, nil
}

func decodeVectorIndexDefinitionForCollectionMeta(src []byte, off int, collectionMetaVersion uint64) (collections.VectorIndexDefinition, int, error) {
	def, off, err := decodeVectorIndexDefinitionAt(src, off, false)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	if collectionMetaVersion < 3 {
		return def, off, nil
	}
	strategy, err := readEnum(src, &off)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	decodedStrategy, err := decodeVectorIndexStrategyStrict(strategy)
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	quantizedCount, err := readUvarintField(src, &off, "quantized_index_count")
	if err != nil {
		return collections.VectorIndexDefinition{}, 0, err
	}
	if quantizedCount > uint64(maxInt) {
		return collections.VectorIndexDefinition{}, 0, protocolError(iwire.ErrResourceExhausted, "quantized vector index count exceeds int capacity")
	}
	if quantizedCount > maxCollectionMetaIndexDefinitions {
		return collections.VectorIndexDefinition{}, 0, protocolError(iwire.ErrResourceExhausted, "quantized vector index count %d exceeds limit %d", quantizedCount, maxCollectionMetaIndexDefinitions)
	}
	quantized := make([]collections.QuantizedVectorIndexDefinition, 0, int(quantizedCount))
	for i := uint64(0); i < quantizedCount; i++ {
		name, err := readString(src, &off)
		if err != nil {
			return collections.VectorIndexDefinition{}, 0, err
		}
		codec, err := readString(src, &off)
		if err != nil {
			return collections.VectorIndexDefinition{}, 0, err
		}
		codecVersion, err := readUvarintField(src, &off, "quantized_index_version")
		if err != nil {
			return collections.VectorIndexDefinition{}, 0, err
		}
		if codecVersion > uint64(^uint32(0)) {
			return collections.VectorIndexDefinition{}, 0, protocolError(iwire.ErrResourceExhausted, "quantized vector index version %d exceeds uint32 capacity", codecVersion)
		}
		var scalarU8Calibration *collections.ScalarU8CalibrationConfig
		if collectionMetaVersion >= 4 {
			hasScalarU8Calibration, err := readBool(src, &off)
			if err != nil {
				return collections.VectorIndexDefinition{}, 0, err
			}
			if hasScalarU8Calibration {
				mode, err := readString(src, &off)
				if err != nil {
					return collections.VectorIndexDefinition{}, 0, err
				}
				grouping, err := readString(src, &off)
				if err != nil {
					return collections.VectorIndexDefinition{}, 0, err
				}
				policyName, err := readString(src, &off)
				if err != nil {
					return collections.VectorIndexDefinition{}, 0, err
				}
				quantilePPM, err := readUvarintField(src, &off, "scalar_u8_alpha_policy_quantile_ppm")
				if err != nil {
					return collections.VectorIndexDefinition{}, 0, err
				}
				if quantilePPM > uint64(^uint32(0)) {
					return collections.VectorIndexDefinition{}, 0, protocolError(iwire.ErrResourceExhausted, "scalar_u8 alpha policy quantile_ppm %d exceeds uint32 capacity", quantilePPM)
				}
				scalarU8Calibration = &collections.ScalarU8CalibrationConfig{
					Mode:     collections.ScalarU8CalibrationMode(mode),
					Grouping: collections.ScalarU8CalibrationGrouping(grouping),
					AlphaPolicy: collections.ScalarU8AlphaPolicy{
						Name:        collections.ScalarU8AlphaPolicyName(policyName),
						QuantilePPM: uint32(quantilePPM),
					},
				}
			}
		}
		q := collections.QuantizedVectorIndexDefinition{
			Name:                name,
			Codec:               codec,
			Version:             uint32(codecVersion),
			ScalarU8Calibration: scalarU8Calibration,
		}
		if q.ScalarU8Calibration != nil {
			normalizedCalibration, err := collections.NormalizeScalarU8CalibrationConfig(def.Name, int(i), q)
			if err != nil {
				return collections.VectorIndexDefinition{}, 0, protocolError(iwire.ErrInvalidCommand, "%v", err)
			}
			q.ScalarU8Calibration = normalizedCalibration
		}
		quantized = append(quantized, q)
	}
	def.Strategy = decodedStrategy
	def.QuantizedIndexes = quantized
	return def, off, nil
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

func encodeVectorMetric(metric collections.VectorMetric) uint64 {
	switch metric {
	case collections.VectorMetricCosine:
		return 1
	case collections.VectorMetricL2:
		return 2
	case collections.VectorMetricInnerProduct:
		return 3
	default:
		return 0
	}
}

func decodeVectorMetricStrict(metric uint64) (collections.VectorMetric, error) {
	switch metric {
	case 1:
		return collections.VectorMetricCosine, nil
	case 2:
		return collections.VectorMetricL2, nil
	case 3:
		return collections.VectorMetricInnerProduct, nil
	default:
		return 0, protocolError(iwire.ErrInvalidCommand, "unsupported vector_metric enum %d", metric)
	}
}

func encodeVectorIndexEncoding(encoding collections.VectorIndexEncoding) uint64 {
	switch encoding {
	case collections.VectorIndexEncodingFloat32:
		return 1
	case collections.VectorIndexEncodingInt8:
		return 2
	default:
		return 0
	}
}

func decodeVectorIndexEncodingStrict(encoding uint64) (collections.VectorIndexEncoding, error) {
	switch encoding {
	case 1:
		return collections.VectorIndexEncodingFloat32, nil
	case 2:
		return collections.VectorIndexEncodingInt8, nil
	default:
		return collections.VectorIndexEncodingFloat32, protocolError(iwire.ErrInvalidCommand, "unsupported vector_index_encoding enum %d", encoding)
	}
}

func encodeVectorIndexStrategy(strategy collections.VectorIndexStrategy) uint64 {
	switch strategy {
	case "", collections.VectorIndexStrategyNativeRuntime:
		return 1
	case collections.VectorIndexStrategyColumnGraph:
		return 2
	default:
		return 0
	}
}

func decodeVectorIndexStrategyStrict(strategy uint64) (collections.VectorIndexStrategy, error) {
	switch strategy {
	case 1:
		return collections.VectorIndexStrategyNativeRuntime, nil
	case 2:
		return collections.VectorIndexStrategyColumnGraph, nil
	default:
		return "", protocolError(iwire.ErrInvalidCommand, "unsupported vector_index_strategy enum %d", strategy)
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

func decodeCollectionHandleRefPayload(raw []byte) (CollectionHandle, bool, error) {
	if len(raw) == 0 || raw[0] != collectionRefTagHandle {
		return 0, false, nil
	}
	handle, n, err := readUvarint(raw[1:])
	if err != nil {
		return 0, true, err
	}
	if n+1 != len(raw) {
		return 0, true, protocolError(iwire.ErrMalformedFrame, "collection handle ref has trailing bytes")
	}
	return CollectionHandle(handle), true, nil
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
	handle, isHandle, err := decodeCollectionHandleRefPayload(raw)
	if err != nil {
		return 0, err
	}
	if !isHandle {
		return 0, protocolError(iwire.ErrInvalidCommand, "close_collection requires a collection handle")
	}
	if state == nil {
		return 0, protocolError(iwire.ErrInvalidCommand, "collection handle requires connection state")
	}
	return handle, nil
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

func initialCatalogVersion(db *backenddb.DB) uint64 {
	if db == nil {
		return 0
	}
	state, ok := db.StateToken()
	if !ok {
		return 0
	}
	return state.CommitSeq
}

func (s *Server) catalogMetadataFingerprint() ([]byte, bool) {
	if s == nil || s.collections == nil {
		return nil, false
	}
	metas, err := s.collections.ListCollections()
	if err != nil {
		return nil, false
	}
	return encodeCollectionMetaVector(metas), true
}

func (s *Server) bumpCatalogVersionIfCatalogMetadataChanged(before []byte, beforeOK bool) {
	after, afterOK := s.catalogMetadataFingerprint()
	if !beforeOK || !afterOK || !bytes.Equal(before, after) {
		s.catalogVersion.Add(1)
	}
}

func (s *Server) currentCatalogVersion() (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, protocolError(iwire.ErrInvalidCommand, "catalog version guard requires a backend")
	}
	return s.catalogVersion.Load(), nil
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

func (s *Server) beginMetadataIdempotency(commandID iwire.CommandID, sections []iwire.Section) ([]iwire.Section, func([]iwire.Section) []iwire.Section, error) {
	key, err := metadataIdempotencyKey(sections)
	if err != nil {
		return nil, nil, err
	}
	signature, err := metadataMutationSignature(commandID, sections)
	if err != nil {
		return nil, nil, err
	}
	signatureHash := sha256.Sum256(signature)
	if s.metadataIdempotency == nil {
		s.metadataIdempotency = make(map[string]metadataIdempotencyEntry)
	}
	if entry, ok := s.metadataIdempotency[key]; ok {
		if entry.commandID != commandID || entry.signature != signatureHash {
			return nil, nil, protocolError(iwire.ErrIdempotencyConflict, "idempotency key reused for different metadata mutation")
		}
		return cloneSections(entry.response), nil, nil
	}
	remember := func(response []iwire.Section) []iwire.Section {
		copied := cloneSections(response)
		s.metadataIdempotency[key] = metadataIdempotencyEntry{
			commandID: commandID,
			signature: signatureHash,
			response:  copied,
		}
		s.metadataIdemOrder = append(s.metadataIdemOrder, key)
		s.trimMetadataIdempotencyLocked()
		return cloneSections(copied)
	}
	return nil, remember, nil
}

func (s *Server) trimMetadataIdempotencyLocked() {
	if s.maxMetadataIdempotencyEntries < 0 {
		return
	}
	for len(s.metadataIdemOrder) > s.maxMetadataIdempotencyEntries {
		key := s.metadataIdemOrder[0]
		copy(s.metadataIdemOrder, s.metadataIdemOrder[1:])
		s.metadataIdemOrder[len(s.metadataIdemOrder)-1] = ""
		s.metadataIdemOrder = s.metadataIdemOrder[:len(s.metadataIdemOrder)-1]
		delete(s.metadataIdempotency, key)
	}
}

func metadataIdempotencyKey(sections []iwire.Section) (string, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionIdempotencyKey)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", protocolError(iwire.ErrInvalidCommand, "missing idempotency key")
	}
	if len(raw) == 0 {
		return "", protocolError(iwire.ErrInvalidCommand, "idempotency key cannot be empty")
	}
	return string(raw), nil
}

func metadataMutationSignature(commandID iwire.CommandID, sections []iwire.Section) ([]byte, error) {
	dst := binary.AppendUvarint(nil, uint64(commandID))
	for _, id := range metadataMutationSignatureSections(commandID) {
		raw, ok, err := singletonSection(sections, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, protocolError(iwire.ErrInvalidCommand, "missing section %d", id)
		}
		var appendErr error
		dst, appendErr = iwire.AppendSection(dst, iwire.Section{ID: id, Bytes: raw})
		if appendErr != nil {
			return nil, appendErr
		}
	}
	return dst, nil
}

func metadataMutationSignatureSections(commandID iwire.CommandID) []iwire.SectionID {
	switch commandID {
	case iwire.CommandCreateCollection:
		return []iwire.SectionID{iwire.SectionCollectionMeta}
	case iwire.CommandCreateIndex:
		return []iwire.SectionID{iwire.SectionCollectionRef, iwire.SectionIndexDefinition}
	case iwire.CommandDropIndex:
		return []iwire.SectionID{iwire.SectionCollectionRef, iwire.SectionIndexName}
	default:
		return nil
	}
}

func cloneSections(sections []iwire.Section) []iwire.Section {
	if len(sections) == 0 {
		return nil
	}
	out := make([]iwire.Section, len(sections))
	for i := range sections {
		out[i] = sections[i]
		out[i].Bytes = append([]byte(nil), sections[i].Bytes...)
	}
	return out
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

func (s *Server) openCollectionRef(state *connState, sections []iwire.Section) (string, *collections.Collection, error) {
	if err := managerRequired(s.collections); err != nil {
		return "", nil, err
	}
	raw, ok, err := singletonSection(sections, iwire.SectionCollectionRef)
	if err != nil {
		return "", nil, err
	}
	if !ok {
		return "", nil, protocolError(iwire.ErrInvalidCommand, "missing collection_ref")
	}
	return s.openCollectionRawRef(state, raw)
}

func (s *Server) openCollectionRawRef(state *connState, raw []byte) (string, *collections.Collection, error) {
	if err := managerRequired(s.collections); err != nil {
		return "", nil, err
	}
	if handle, isHandle, err := decodeCollectionHandleRefPayload(raw); isHandle || err != nil {
		if err != nil {
			return "", nil, err
		}
		if state == nil {
			return "", nil, protocolError(iwire.ErrInvalidCommand, "collection handle requires connection state")
		}
		name, collection, ok := state.collectionForHandleRef(handle)
		if !ok {
			return "", nil, protocolError(iwire.ErrCollectionNotFound, "collection handle %d not found", handle)
		}
		if collection != nil {
			return name, collection, nil
		}
		return s.openNamedCollectionRef(state, name)
	}
	name, _, err := decodeCollectionRef(state, raw)
	if err != nil {
		return "", nil, err
	}
	return s.openNamedCollectionRef(state, name)
}

func (s *Server) openNamedCollectionRef(state *connState, name string) (string, *collections.Collection, error) {
	if state != nil {
		if collection, ok := state.cachedCollection(name); ok {
			return name, collection, nil
		}
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return "", nil, metadataWrap(err)
	}
	if state != nil {
		state.cacheCollection(name, collection, s.maxCachedCollections)
	}
	return name, collection, nil
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

func ensureCollectionName(name string) error {
	if err := collections.ValidateCollectionName(name); err != nil {
		return invalidMetadata("%v", err)
	}
	return nil
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
