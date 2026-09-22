package valuelog

const DefaultKeepSafetyMargin = 0.10

const (
	// For large grouped payloads we periodically probe dict compression even when
	// backoff is active so streams can recover quickly after a short no-benefit
	// streak.
	largeDictProbeMinPayloadBytes = 16 << 10
	largeDictBackoffProbeStride   = 4

	// Keep very strong large-payload dict wins even when a conservative
	// cost model estimates encode cost above IO savings.
	largeDictForceKeepRatioNum = 3
	largeDictForceKeepRatioDen = 10
)

func shouldForceDictProbe(rawPayloadBytes int) bool {
	return rawPayloadBytes >= largeDictProbeMinPayloadBytes
}

func shouldProbeLargeDictDuringBackoff(skipRemain uint16, rawPayloadBytes int) bool {
	if skipRemain == 0 || !shouldForceDictProbe(rawPayloadBytes) {
		return false
	}
	return skipRemain%largeDictBackoffProbeStride == 0
}

func shouldForceKeepLargeDictCompressed(rawBytes, encodedBytes int) bool {
	if !shouldForceDictProbe(rawBytes) {
		return false
	}
	if encodedBytes <= 0 || encodedBytes >= rawBytes {
		return false
	}
	return encodedBytes*largeDictForceKeepRatioDen <= rawBytes*largeDictForceKeepRatioNum
}

// ShouldKeepCompressed decides whether to keep compressed bytes for a frame.
// It compares estimated IO savings against encode cost with a safety margin.
func ShouldKeepCompressed(rawBytes, encodedBytes int, encodeNs int64, ioNsPerStoredByte, safetyMargin float64) bool {
	if rawBytes <= 0 || encodedBytes < 0 {
		return false
	}
	if encodedBytes >= rawBytes {
		return false
	}
	if ioNsPerStoredByte <= 0 || encodeNs <= 0 {
		// Fall back to size-based decision when we lack cost estimates.
		return true
	}
	if safetyMargin < 0 {
		safetyMargin = 0
	}
	savings := float64(rawBytes-encodedBytes) * ioNsPerStoredByte
	cost := float64(encodeNs) * (1 + safetyMargin)
	return savings > cost
}
