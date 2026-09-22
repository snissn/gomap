package valuelog

import "math"

const (
	defaultBlockTargetCompressedBytes = 4096
	minBlockTargetCompressedBytes     = 256
	maxBlockTargetCompressedBytes     = 1 << 20
)

// NormalizeBlockTargetCompressedBytes returns a bounded grouped block target.
func NormalizeBlockTargetCompressedBytes(v int) int {
	if v <= 0 {
		return defaultBlockTargetCompressedBytes
	}
	if v < minBlockTargetCompressedBytes {
		return minBlockTargetCompressedBytes
	}
	if v > maxBlockTargetCompressedBytes {
		return maxBlockTargetCompressedBytes
	}
	return v
}

// ChooseBlockGroupK estimates grouped frame size K for block compression.
//
// observedRatio is stored/raw payload ratio. Values >= 0.98 are treated as
// expansion-risk/incompressible and force k=1.
func ChooseBlockGroupK(records, rawPayloadBytes, targetCompressedBytes int, observedRatio float64) int {
	if records <= 1 || rawPayloadBytes <= 0 {
		return 1
	}
	target := NormalizeBlockTargetCompressedBytes(targetCompressedBytes)
	if observedRatio <= 0 || math.IsNaN(observedRatio) || math.IsInf(observedRatio, 0) {
		observedRatio = 1.0
	}
	if observedRatio >= 0.98 {
		return 1
	}
	avgRaw := float64(rawPayloadBytes) / float64(records)
	if avgRaw <= 0 {
		return 1
	}
	estCompressedPerRecord := avgRaw * observedRatio
	if estCompressedPerRecord <= 0 {
		return 1
	}
	k := int(float64(target) / estCompressedPerRecord)
	if k < 1 {
		k = 1
	}
	if k > records {
		k = records
	}
	if k > MaxFrameK {
		k = MaxFrameK
	}
	return k
}
