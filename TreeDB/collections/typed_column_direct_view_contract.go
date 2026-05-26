package collections

import "github.com/snissn/gomap/TreeDB/internal/typeddecode"

type typedColumnDirectViewStorageOwner string

const (
	typedColumnDirectViewStorageTypedColumnPart  typedColumnDirectViewStorageOwner = "typed_column_part"
	typedColumnDirectViewStoragePhysicalRowAsset typedColumnDirectViewStorageOwner = "physical_row_asset"
)

type typedColumnDirectViewConsumerPath string

const (
	typedColumnDirectViewConsumerTypedColumnPartGeneric typedColumnDirectViewConsumerPath = "typed_column_part_generic"
	typedColumnDirectViewConsumerColumnGraphTypedVector typedColumnDirectViewConsumerPath = "column_graph_typed_column_vector_source"
	typedColumnDirectViewConsumerRowAssetVector         typedColumnDirectViewConsumerPath = "row_asset_vector_consumer"
	typedColumnDirectViewConsumerRowAssetAdjacency      typedColumnDirectViewConsumerPath = "row_asset_adjacency_consumer"
	typedColumnDirectViewConsumerRowAssetGeneric        typedColumnDirectViewConsumerPath = "row_asset_generic_consumer"
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
	NativeScalarPayload    bool
	Reason                 string
	FollowUpIssue          int
}

func typedColumnDirectViewSafetyChecks() []typedColumnDirectViewSafetyCheck {
	return []typedColumnDirectViewSafetyCheck{
		{Name: "actual Go pointer alignment", Placement: typedColumnDirectViewReadTime, Counter: typeddecode.CounterActualPointerUnaligned},
		{Name: "exact byte length and element count", Placement: typedColumnDirectViewReadTime},
		{Name: "host endian compatibility", Placement: typedColumnDirectViewReadTime},
		{Name: "handle lifetime and released-state", Placement: typedColumnDirectViewReadTime, Counter: typeddecode.CounterStaleHandle},
		{Name: "logical type and physical encoding", Placement: typedColumnDirectViewCertificationTime},
		{Name: "compression/null/default exclusion", Placement: typedColumnDirectViewCertificationTime},
		{Name: "row count and fixed dims/degree", Placement: typedColumnDirectViewCertificationTime},
		{Name: "manifest identity, checksum, and section bounds", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterCertificationFailure},
		{Name: "absolute asset+payload storage offset alignment", Placement: typedColumnDirectViewCertificationTime, Counter: typeddecode.CounterAbsoluteOffsetUnaligned},
		{Name: "bool/string/dictionary/nullable/default/compressed/variable-width/delta fallback", Placement: typedColumnDirectViewFallbackPolicy, Counter: typeddecode.CounterStreamingFallback},
		{Name: "physical row asset and adjacency direct-view deferral", Placement: typedColumnDirectViewDeferredPolicy, Counter: typeddecode.CounterScratchDecode},
	}
}

func typedColumnDirectViewClassificationFor(valueType ColumnStoreValueType, owner typedColumnDirectViewStorageOwner, consumer typedColumnDirectViewConsumerPath) typedColumnDirectViewClassification {
	base := typedColumnDirectViewClassification{ValueType: valueType, StorageOwner: owner, Consumer: consumer, Support: typedColumnDirectViewFallbackOnly, Reason: "fallback_only"}
	if owner == typedColumnDirectViewStoragePhysicalRowAsset || consumer == typedColumnDirectViewConsumerRowAssetVector || consumer == typedColumnDirectViewConsumerRowAssetAdjacency || consumer == typedColumnDirectViewConsumerRowAssetGeneric {
		base.Support = typedColumnDirectViewDeferredFallbackOnly
		base.Reason = "physical row assets are deferred to #1897"
		base.FollowUpIssue = 1897
		return base
	}
	if valueType == ColumnStoreValueAdjacencyList {
		base.Support = typedColumnDirectViewDeferredFallbackOnly
		base.PayloadEndian = "little"
		base.ElementSize = 4
		base.Alignment = 4
		base.RequiresElementsPerRow = true
		base.Reason = "adjacency_list direct-view certification is deferred to #1901"
		base.FollowUpIssue = 1901
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

func typedColumnDirectViewConformanceMatrix() []typedColumnDirectViewClassification {
	valueTypes := []ColumnStoreValueType{
		ColumnStoreValueBool,
		ColumnStoreValueInt64,
		ColumnStoreValueFloat32,
		ColumnStoreValueDouble,
		ColumnStoreValueString,
		ColumnStoreValueFloat32Vector,
		ColumnStoreValueAdjacencyList,
	}
	owners := []typedColumnDirectViewStorageOwner{
		typedColumnDirectViewStorageTypedColumnPart,
		typedColumnDirectViewStoragePhysicalRowAsset,
	}
	consumers := []typedColumnDirectViewConsumerPath{
		typedColumnDirectViewConsumerTypedColumnPartGeneric,
		typedColumnDirectViewConsumerColumnGraphTypedVector,
		typedColumnDirectViewConsumerRowAssetVector,
		typedColumnDirectViewConsumerRowAssetAdjacency,
		typedColumnDirectViewConsumerRowAssetGeneric,
	}
	out := make([]typedColumnDirectViewClassification, 0, len(valueTypes)*len(owners)*len(consumers))
	for _, valueType := range valueTypes {
		for _, owner := range owners {
			for _, consumer := range consumers {
				out = append(out, typedColumnDirectViewClassificationFor(valueType, owner, consumer))
			}
		}
	}
	return out
}
