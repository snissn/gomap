package collections

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeFreshVectorStateShape describes ONLY a locally constructed V2 TVIS.
// Strings are counted, never retained; callers can accumulate the actual asset
// metadata while walking their real level/asset census without fake assets.
// It is not a validator or an admission substitute for captured stored records.
type nativeFreshVectorStateShape struct {
	assetCount  uint64
	stringBytes uint64
}

func newNativeFreshVectorStateShape(indexName, field, metric, encoding string) (nativeFreshVectorStateShape, error) {
	var s nativeFreshVectorStateShape
	for _, value := range [...]string{indexName, field, metric, encoding} {
		if err := nativeTypedCheckedAdd(&s.stringBytes, uint64(len(value))); err != nil {
			return nativeFreshVectorStateShape{}, err
		}
	}
	return s, nil
}

// withAsset is transactional on overflow. These are precisely the six strings
// written for one asset by encodeColumnVectorIndexStateRecordV2.
func (s nativeFreshVectorStateShape) withAsset(role, id, logicalType, encoding, kind, namespace string) (nativeFreshVectorStateShape, error) {
	if err := nativeTypedCheckedAdd(&s.assetCount, 1); err != nil {
		return nativeFreshVectorStateShape{}, err
	}
	for _, value := range [...]string{role, id, logicalType, encoding, kind, namespace} {
		if err := nativeTypedCheckedAdd(&s.stringBytes, uint64(len(value))); err != nil {
			return nativeFreshVectorStateShape{}, err
		}
	}
	return s, nil
}

func nativeFreshVectorStateShapeFromSnapshot(state columnVectorIndexStateSnapshot) (nativeFreshVectorStateShape, error) {
	s, err := newNativeFreshVectorStateShape(state.IndexName, state.Field, state.Metric.String(), state.Encoding.String())
	if err != nil {
		return nativeFreshVectorStateShape{}, err
	}
	for _, asset := range state.Assets {
		s, err = s.withAsset(asset.Role, asset.AssetID, asset.LogicalType, asset.PhysicalEncoding, string(asset.Ref.Kind), asset.Ref.Namespace)
		if err != nil {
			return nativeFreshVectorStateShape{}, err
		}
	}
	return s, nil
}

// rawBytes is the exact V2 encoded LENGTH, not bytes.Buffer capacity or its
// cumulative construction allocation. The maintained encoder writes a 6-byte
// magic/version, four uint64 string lengths, ten uint64 fields, then six uint64
// string lengths and nine uint64 fields per asset. Numeric values, inferred
// AdjacencyLayerCount and asset ordering do not change this fixed-width size.
func (s nativeFreshVectorStateShape) rawBytes() (uint64, error) {
	assets, err := nativeTypedCheckedMul(s.assetCount, 6*8+9*8)
	if err != nil {
		return 0, err
	}
	total := uint64(4 + 2 + 4*8 + 10*8)
	if err = nativeTypedCheckedAdd(&total, assets); err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, s.stringBytes); err != nil {
		return 0, err
	}
	return total, nil
}

// nativeFreshVectorStateCapacityV2 is ONE source-requirement decode/normalize
// slot on the ORIGINAL construction receipt. It retains the existing slot's
// three raw-image cohorts, exact actual asset headers, and duplicate-map growth
// allowance. Independent Walk/Lookup slots must each be charged by the caller.
// This excludes encode-buffer/Snappy construction, state emission, authority
// freeze, and all other graph phases: those still need their original promise.
//
// storedUpper must be a proven bound on the stored inline record (actual raw
// length for uncompressed output). Raw length alone cannot prove compressed
// output fits inline: unsupported larger fresh shapes need the original path
// or preaccept refusal. Never use this helper for an untrusted stored record;
// nativeSourceRequirementStateCapacityV2 and its envelope checks stay unchanged.
func nativeFreshVectorStateCapacityV2(storedUpper, decodedBytes, assetCount uint64) (uint64, error) {
	const fixed = uint64(4 + 2 + 4*8 + 10*8)
	const perAsset = uint64(6*8 + 9*8)
	if storedUpper == 0 || storedUpper > columnVectorIndexStateMaxInlineRecordBytes || decodedBytes < fixed || decodedBytes > columnVectorIndexStateMaxDecodedBytes || assetCount > (decodedBytes-fixed)/perAsset {
		return 0, memtable.ErrCOWCapacity
	}
	headers, err := nativeTypedCheckedMul(assetCount, uint64(unsafe.Sizeof(columnVectorIndexStateAssetSnapshot{})))
	if err != nil {
		return 0, err
	}
	maps, err := nativeTypedCheckedMul(assetCount, 128)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, term := range [...]struct{ bytes, copies uint64 }{{storedUpper, 1}, {decodedBytes, 3}, {headers, 1}, {maps, 1}} {
		charge := memtable.COWAllocationCharge(term.bytes)
		if charge == ^uint64(0) {
			return 0, memtable.ErrCOWCapacity
		}
		n, e := nativeTypedCheckedMul(charge, term.copies)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, n); e != nil {
			return 0, e
		}
	}
	return total, nil
}
