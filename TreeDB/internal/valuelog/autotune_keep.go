package valuelog

const DefaultKeepSafetyMargin = 0.10

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
