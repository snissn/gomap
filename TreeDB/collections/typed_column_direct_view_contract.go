package collections

import "github.com/snissn/gomap/TreeDB/internal/typeddecode"

type typedColumnDirectViewStorageOwner string

const (
	typedColumnDirectViewStorageTypedColumnPart  typedColumnDirectViewStorageOwner = "typed_column_part"
	typedColumnDirectViewStoragePhysicalRowAsset typedColumnDirectViewStorageOwner = "physical_row_asset"
)

type typedColumnDirectViewConsumerPath string

const (
	typedColumnDirectViewConsumerTypedColumnPartGeneric          typedColumnDirectViewConsumerPath = "typed_column_part_generic"
	typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets typedColumnDirectViewConsumerPath = "typed_column_part_adjacency_offsets_list_adapter"
	typedColumnDirectViewConsumerColumnGraphTypedVector          typedColumnDirectViewConsumerPath = "column_graph_typed_column_vector_source"
	typedColumnDirectViewConsumerColumnGraphTypedAdjacency       typedColumnDirectViewConsumerPath = "column_graph_typed_column_adjacency_source"
	typedColumnDirectViewConsumerRowAssetVector                  typedColumnDirectViewConsumerPath = "row_asset_vector_consumer"
	typedColumnDirectViewConsumerRowAssetAdjacency               typedColumnDirectViewConsumerPath = "row_asset_adjacency_consumer"
	typedColumnDirectViewConsumerRowAssetGeneric                 typedColumnDirectViewConsumerPath = "row_asset_generic_consumer"
)

type typedColumnDirectViewAdjacencyLayout string

const (
	typedColumnDirectViewAdjacencyLayoutNone             typedColumnDirectViewAdjacencyLayout = ""
	typedColumnDirectViewAdjacencyLayoutRawUint32Dense   typedColumnDirectViewAdjacencyLayout = "raw_uint32_dense"
	typedColumnDirectViewAdjacencyLayoutRawUint32Offsets typedColumnDirectViewAdjacencyLayout = "raw_uint32_offsets_list"
	typedColumnDirectViewAdjacencyLayoutPhysicalRowAsset typedColumnDirectViewAdjacencyLayout = "physical_row_asset_legacy"
)

type typedColumnDirectViewSupport string

const (
	typedColumnDirectViewActiveLittleEndianCandidate typedColumnDirectViewSupport = "active_little_endian_candidate"
	typedColumnDirectViewFallbackOnly                typedColumnDirectViewSupport = "fallback_only"
	typedColumnDirectViewDeferredFallbackOnly        typedColumnDirectViewSupport = "deferred_fallback_only"
)

type typedColumnDirectViewCheckPlacement string

const (
	typedColumnDirectViewReadTime          typedColumnDirectViewCheckPlacement = "read_time"
	typedColumnDirectViewCertificationTime typedColumnDirectViewCheckPlacement = "certification_time"
	typedColumnDirectViewFallbackPolicy    typedColumnDirectViewCheckPlacement = "fallback_only"
	typedColumnDirectViewDeferredPolicy    typedColumnDirectViewCheckPlacement = "deferred"
)

type typedColumnDirectViewSafetyCheck struct {
	Name      string
	Placement typedColumnDirectViewCheckPlacement
	Counter   typeddecode.Counter
}

type typedColumnDirectViewClassification struct {
	ValueType              ColumnStoreValueType
	StorageOwner           typedColumnDirectViewStorageOwner
	Consumer               typedColumnDirectViewConsumerPath
	Support                typedColumnDirectViewSupport
	PayloadEndian          string
	ElementSize            int
	Alignment              int
	RequiresElementsPerRow bool
	AdjacencyLayout        typedColumnDirectViewAdjacencyLayout
	OffsetsElementSize     int
	OffsetsAlignment       int
	ValuesElementSize      int
	ValuesAlignment        int
	NativeScalarPayload    bool
	Reason                 string
	FollowUpIssue          int
	FollowUpIssues         []int
}

func typedColumnDirectViewSafetyChecks() []typedColumnDirectViewSafetyCheck {
	return []typedColumnDirectViewSafetyCheck{
		{Name: "actual Go pointer alignment", Placement: typedColumnDirectViewReadTime, Counter: typeddecode.CounterActualPointerUnaligned},
		{Name: "exact byte length and element count", Placement: typedColumnDirectViewReadTime},
		{Name: "host endian compatibility", Placement: typedColumnDirectViewReadTime},
		{Name: "handle lifetime and released-state", Placement: typedColumnDirectViewReadTime, Counter: typeddecode.CounterStaleHandle},
		{Name: "mappedresource source support", Placement: typedColumnDirectViewReadTime, Counter: typeddecode.CounterSourceUnsupported},
		{Name: "logical type and physical encoding", Placement: typedColumnDirectViewCertificationTime},
		{Name: "compression/null/default exclusion", Placement: typedColumnDirectViewCertificationTime},
		{Name: "row count and fixed dims/degree", Placement: typedColumnDirectViewCertificationTime},
		{Name: "offsets-list offset count is exactly row_count+1", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterOffsetsListValidation},
		{Name: "offsets-list offsets start at zero and are monotonic", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterOffsetsListValidation},
		{Name: "offsets-list final offset exactly matches uint32 value count", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterOffsetsListValidation},
		{Name: "offsets-list offsets fit Go int slice ranges before indexing", Placement: typedColumnDirectViewReadTime, Counter: typeddecode.CounterOffsetsListValidation},
		{Name: "manifest identity, checksum, and section bounds", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterCertificationFailure},
		{Name: "absolute asset+offsets-section storage offset alignment", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterAbsoluteOffsetUnaligned},
		{Name: "absolute asset+values-section storage offset alignment", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterAbsoluteOffsetUnaligned},
		{Name: "bool/string/dictionary/nullable/default/compressed/variable-width/delta fallback", Placement: typedColumnDirectViewFallbackPolicy, Counter: typeddecode.CounterStreamingFallback},
		{Name: "physical row asset and adjacency direct-view deferral", Placement: typedColumnDirectViewDeferredPolicy, Counter: typeddecode.CounterScratchDecode},
	}
}

func typedColumnDirectViewClassificationFor(valueType ColumnStoreValueType, owner typedColumnDirectViewStorageOwner, consumer typedColumnDirectViewConsumerPath) typedColumnDirectViewClassification {
	return typedColumnDirectViewClassificationForAdjacencyLayout(valueType, owner, consumer, typedColumnDirectViewAdjacencyLayoutNone)
}

func typedColumnDirectViewClassificationForAdjacencyLayout(valueType ColumnStoreValueType, owner typedColumnDirectViewStorageOwner, consumer typedColumnDirectViewConsumerPath, adjacencyLayout typedColumnDirectViewAdjacencyLayout) typedColumnDirectViewClassification {
	base := typedColumnDirectViewClassification{ValueType: valueType, StorageOwner: owner, Consumer: consumer, Support: typedColumnDirectViewFallbackOnly, Reason: "fallback_only", AdjacencyLayout: adjacencyLayout}
	if owner == typedColumnDirectViewStoragePhysicalRowAsset || consumer == typedColumnDirectViewConsumerRowAssetVector || consumer == typedColumnDirectViewConsumerRowAssetAdjacency || consumer == typedColumnDirectViewConsumerRowAssetGeneric {
		base.Support = typedColumnDirectViewDeferredFallbackOnly
		base.Reason = "physical row assets are deferred to #1897"
		base.FollowUpIssue = 1897
		base.FollowUpIssues = []int{1897}
		if valueType == ColumnStoreValueAdjacencyList || consumer == typedColumnDirectViewConsumerRowAssetAdjacency {
			base.AdjacencyLayout = typedColumnDirectViewAdjacencyLayoutPhysicalRowAsset
			base.Reason = "physical row assets are deferred to #1897; row-asset adjacency remains legacy/fallback while #1989 quarantines graph-specific adjacency-source storage"
			base.FollowUpIssues = []int{1897, 1901}
		}
		return base
	}
	if valueType == ColumnStoreValueUint32List {
		if consumer != typedColumnDirectViewConsumerTypedColumnPartGeneric {
			base.Reason = "uint32_list direct-view classification only supports the generic typed-column consumer"
			return base
		}
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 4
		base.Alignment = 4
		base.RequiresElementsPerRow = false
		base.OffsetsElementSize = 8
		base.OffsetsAlignment = 8
		base.ValuesElementSize = 4
		base.ValuesAlignment = 4
		base.Reason = "typed-column uint32_list raw_uint32_offsets_list uses certified uint64 offsets plus uint32 values direct views with fail-closed fallback diagnostics"
		return base
	}
	if valueType == ColumnStoreValueBytes {
		if consumer != typedColumnDirectViewConsumerTypedColumnPartGeneric {
			base.Reason = "bytes direct-view classification only supports the generic typed-column consumer"
			return base
		}
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 1
		base.Alignment = 1
		base.RequiresElementsPerRow = false
		base.OffsetsElementSize = 8
		base.OffsetsAlignment = 8
		base.ValuesElementSize = 1
		base.ValuesAlignment = 1
		base.Reason = "typed-column bytes raw_bytes_offsets uses certified uint64 offsets plus exact opaque byte values direct views with fail-closed fallback diagnostics"
		return base
	}
	if valueType == ColumnStoreValueByteVector {
		if consumer != typedColumnDirectViewConsumerTypedColumnPartGeneric {
			base.Reason = "byte_vector direct-view classification only supports the generic typed-column consumer"
			return base
		}
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 1
		base.Alignment = 1
		base.RequiresElementsPerRow = true
		base.Reason = "typed-column byte_vector raw_fixed_bytes uses certified fixed bytes_per_row payloads with byte-aligned direct row views"
		return base
	}
	if bitsPerElement, ok := columnStorePackedUintVectorBits(valueType); ok {
		if consumer != typedColumnDirectViewConsumerTypedColumnPartGeneric {
			base.Reason = "packed-code vector direct-view classification only supports the generic typed-column consumer"
			return base
		}
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 1
		base.Alignment = 1
		base.RequiresElementsPerRow = true
		base.Reason = "typed-column packed-code vector stores LSB-first packed uint elements with zero padding and little-endian word views"
		if bitsPerElement == 1 {
			base.Reason = "typed-column packed_bit_vector stores LSB-first bit codes with zero padding and little-endian word views"
		}
		return base
	}
	if width, ok := columnStoreDenseNumericVectorWidth(valueType); ok {
		if consumer != typedColumnDirectViewConsumerTypedColumnPartGeneric {
			base.Reason = "dense numeric vector direct-view classification only supports the generic typed-column consumer"
			return base
		}
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = width
		base.Alignment = width
		base.RequiresElementsPerRow = true
		base.Reason = "typed-column dense numeric vector row-major little-endian fixed-width candidate"
		return base
	}
	if width, ok := columnStorePrimitiveScalarWidth(valueType); ok {
		if consumer != typedColumnDirectViewConsumerTypedColumnPartGeneric {
			base.Reason = "primitive scalar direct-view classification only supports the generic typed-column consumer"
			return base
		}
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = width
		base.Alignment = width
		base.NativeScalarPayload = true
		base.Reason = "typed-column primitive scalar raw little-endian fixed-width candidate"
		if columnStoreValueTypeIsStorageOnlyFloatBits(valueType) {
			base.Reason = "typed-column float16/bfloat16 primitive scalar is a storage-only raw 16-bit bit payload direct-view candidate"
		}
		return base
	}
	if valueType == ColumnStoreValueAdjacencyList {
		if adjacencyLayout == typedColumnDirectViewAdjacencyLayoutNone {
			adjacencyLayout = typedColumnDirectViewAdjacencyLayoutRawUint32Dense
			if consumer == typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets {
				adjacencyLayout = typedColumnDirectViewAdjacencyLayoutRawUint32Offsets
			}
			base.AdjacencyLayout = adjacencyLayout
		}
		base.Support = typedColumnDirectViewDeferredFallbackOnly
		base.PayloadEndian = "little"
		base.ElementSize = 4
		base.Alignment = 4
		base.FollowUpIssue = 1901
		base.FollowUpIssues = []int{1901}
		switch adjacencyLayout {
		case typedColumnDirectViewAdjacencyLayoutRawUint32Offsets:
			base.RequiresElementsPerRow = false
			base.OffsetsElementSize = 8
			base.OffsetsAlignment = 8
			base.ValuesElementSize = 4
			base.ValuesAlignment = 4
			if consumer == typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets {
				base.Support = typedColumnDirectViewActiveLittleEndianCandidate
				base.Reason = "typed-column adapter raw_uint32_offsets_list variable adjacency reads use certified uint64 offsets plus uint32 values direct views with fail-closed fallback diagnostics; column_graph/search consumption remains separate follow-up work"
				base.FollowUpIssue = 0
				base.FollowUpIssues = nil
			} else {
				base.Reason = "typed-column adjacency_list raw_uint32_offsets_list uses uint64 offsets plus uint32 values; adapter direct reads are compatibility-only while #1989 keeps column_graph/search on generic uint32_list vector-index state"
				base.FollowUpIssues = []int{1901, 1915, 1916, 1917}
			}
		case typedColumnDirectViewAdjacencyLayoutRawUint32Dense:
			base.RequiresElementsPerRow = true
			base.Reason = "legacy dense fixed-degree raw_uint32_dense adjacency remains fallback/compatibility; #1989 quarantines graph-specific raw_uint32_offsets_list storage in favor of uint32_list"
		default:
			base.Reason = "unknown adjacency layout selector requires explicit direct-view classification"
		}
		return base
	}
	switch valueType {
	case ColumnStoreValueInt64:
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 8
		base.Alignment = 8
		base.NativeScalarPayload = true
		base.Reason = "typed-column raw int64 little-endian fixed-width candidate"
	case ColumnStoreValueFloat32:
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 4
		base.Alignment = 4
		base.NativeScalarPayload = true
		base.Reason = "typed-column native scalar float32 little-endian candidate; raw-int64 float carriers are fallback-only"
	case ColumnStoreValueDouble:
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 8
		base.Alignment = 8
		base.NativeScalarPayload = true
		base.Reason = "typed-column native scalar float64 little-endian candidate; raw-int64 float carriers are fallback-only"
	case ColumnStoreValueFloat32Vector:
		base.Support = typedColumnDirectViewActiveLittleEndianCandidate
		base.PayloadEndian = "little"
		base.ElementSize = 4
		base.Alignment = 4
		base.RequiresElementsPerRow = true
		base.Reason = "typed-column dense float32_vector little-endian fixed-width candidate"
		if consumer == typedColumnDirectViewConsumerColumnGraphTypedVector {
			base.Reason = "column_graph typed-column float32_vector source uses certified dense little-endian vector payloads"
		}
	case ColumnStoreValueBool:
		base.Reason = "bool bitpack/RLE is not a dense fixed-width direct-view payload"
	case ColumnStoreValueString:
		base.Reason = "string values and dictionaries are variable-width/fallback-only for direct views"
	default:
		base.Reason = "unknown ColumnStoreValueType requires explicit direct-view classification"
	}
	if consumer == typedColumnDirectViewConsumerColumnGraphTypedVector && valueType != ColumnStoreValueFloat32Vector {
		base.Support = typedColumnDirectViewFallbackOnly
		base.Reason = "column_graph typed-column vector source only consumes float32_vector payloads"
	}
	return base
}

func typedColumnDirectViewAllTypeInventory() []ColumnStoreValueType {
	return []ColumnStoreValueType{
		ColumnStoreValueBool,
		ColumnStoreValueInt64,
		ColumnStoreValueFloat32,
		ColumnStoreValueDouble,
		ColumnStoreValueString,
		ColumnStoreValueInt8,
		ColumnStoreValueUint8,
		ColumnStoreValueInt16,
		ColumnStoreValueUint16,
		ColumnStoreValueInt32,
		ColumnStoreValueUint32,
		ColumnStoreValueUint64,
		ColumnStoreValueFloat16,
		ColumnStoreValueBFloat16,
		ColumnStoreValueUint8Vector,
		ColumnStoreValueInt8Vector,
		ColumnStoreValueUint16Vector,
		ColumnStoreValueInt16Vector,
		ColumnStoreValueUint32Vector,
		ColumnStoreValueInt32Vector,
		ColumnStoreValueUint64Vector,
		ColumnStoreValueInt64Vector,
		ColumnStoreValueFloat16Vector,
		ColumnStoreValueBFloat16Vector,
		ColumnStoreValueFloat32Vector,
		ColumnStoreValueFloat64Vector,
		ColumnStoreValueByteVector,
		ColumnStoreValuePackedBitVector,
		ColumnStoreValuePackedUint2Vector,
		ColumnStoreValuePackedUint4Vector,
		ColumnStoreValueUint32List,
		ColumnStoreValueBytes,
		ColumnStoreValueAdjacencyList,
	}
}

func typedColumnDirectViewConformanceMatrix() []typedColumnDirectViewClassification {
	type rowSpec struct {
		valueType ColumnStoreValueType
		owner     typedColumnDirectViewStorageOwner
		consumer  typedColumnDirectViewConsumerPath
	}

	inventory := typedColumnDirectViewAllTypeInventory()
	rows := make([]rowSpec, 0, len(inventory)*2+4)
	for _, valueType := range inventory {
		rows = append(rows, rowSpec{valueType: valueType, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric})
	}
	rows = append(rows,
		rowSpec{valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector},
		rowSpec{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets},
		rowSpec{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedAdjacency},
	)
	for _, valueType := range inventory {
		rows = append(rows, rowSpec{valueType: valueType, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetGeneric})
	}
	rows = append(rows,
		rowSpec{valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetVector},
		rowSpec{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetAdjacency},
	)

	out := make([]typedColumnDirectViewClassification, 0, len(rows))
	for _, row := range rows {
		layout := typedColumnDirectViewAdjacencyLayoutNone
		if row.valueType == ColumnStoreValueAdjacencyList && row.owner == typedColumnDirectViewStorageTypedColumnPart {
			layout = typedColumnDirectViewAdjacencyLayoutRawUint32Dense
			if row.consumer == typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets || row.consumer == typedColumnDirectViewConsumerColumnGraphTypedAdjacency {
				layout = typedColumnDirectViewAdjacencyLayoutRawUint32Offsets
			}
		}
		out = append(out, typedColumnDirectViewClassificationForAdjacencyLayout(row.valueType, row.owner, row.consumer, layout))
	}
	return out
}
