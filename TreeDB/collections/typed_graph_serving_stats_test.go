package collections

import "testing"

func TestColumnGraphServingAssetLogicalPayloadBytes(t *testing.T) {
	tests := []struct {
		name, role, logicalType, encoding string
		want                              int64
	}{
		{"fp32", columnVectorIndexStateAssetRoleNormalizedVectors, columnVectorIndexStateLogicalTypeFloat32Vector, columnVectorIndexStateEncodingRawFloat32Vector, 3 * 768 * 4},
		{"sq8", columnVectorIndexStateAssetRoleQuantizedCodes, columnVectorIndexStateLogicalTypeByteVector, columnVectorIndexStateEncodingRawFixedBytes, 3 * 768},
		{"packed_bits", columnVectorIndexStateAssetRoleQuantizedCodes, columnVectorIndexStateLogicalTypePackedBitVector, columnVectorIndexStateEncodingRawPackedBitVector, 0},
		{"unknown_encoding", columnVectorIndexStateAssetRoleQuantizedCodes, columnVectorIndexStateLogicalTypeByteVector, "unknown", 0},
		{"unknown_type", columnVectorIndexStateAssetRoleQuantizedCodes, "unknown", columnVectorIndexStateEncodingRawFixedBytes, 0},
		{"topology", columnVectorIndexStateAssetRoleHNSWSearchPack, columnVectorIndexStateLogicalTypeSearchPack, columnVectorIndexStateEncodingHNSWSearchPackV2, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asset := columnVectorIndexStateAssetSnapshot{Role: tt.role, LogicalType: tt.logicalType, PhysicalEncoding: tt.encoding, RowCount: 3}
			if got := columnGraphServingAssetLogicalPayloadBytes(asset, 768); got != tt.want {
				t.Fatalf("logical payload bytes=%d want=%d", got, tt.want)
			}
		})
	}
}
