package caching

import (
	"math"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestVlogAutotuneMetricsVirtualTime(t *testing.T) {
	clock := valuelog.NewVirtualClock(time.Unix(0, 0))
	var metrics vlogAutotuneMetrics
	metrics.init(clock)

	rawBytes := 1024
	storedBytes := 512
	encodeNs := int64(1024 * 10)
	encodeRawBytes := rawBytes
	nsPerStoredByte := int64(200)

	start := metrics.now()
	clock.Advance(int64(storedBytes) * nsPerStoredByte)
	metrics.observe(start, rawBytes, storedBytes, encodeNs, encodeRawBytes)

	snap := metrics.snapshot()
	expectedIoNs := float64(nsPerStoredByte)
	if encodeNs > 0 && encodeNs < int64(storedBytes)*nsPerStoredByte {
		expectedIoNs = float64(int64(storedBytes)*nsPerStoredByte-encodeNs) / float64(storedBytes)
	}
	if math.Abs(snap.IoNsPerStoredByte-expectedIoNs) > 0.001 {
		t.Fatalf("io_ns_per_stored_byte got %.3f want %.3f", snap.IoNsPerStoredByte, expectedIoNs)
	}
	if snap.EncodeNsPerRawByte <= 0 {
		t.Fatalf("expected encode_ns_per_raw_byte to be set")
	}
	if snap.ThroughputRawMBps <= 0 {
		t.Fatalf("expected throughput to be > 0")
	}
}

func TestValueLogAutotuneShouldSwitch_DwellAndGain(t *testing.T) {
	db := &DB{}
	db.valueLogAutotuneOptions = valuelog.AutotuneOptions{
		Mode:            valuelog.AutotuneMedium,
		MinGainToSwitch: 0.10,
		MinDwellFrames:  100,
	}
	db.valueLogDictFrames.total.Store(150)
	db.valueLogAutotuneLastSwitchFrames.Store(80)
	current := &vlogAutotuneProfile{
		TotalRatio:       0.9,
		EncodeNsEstimate: 1000,
		AvgSampleBytes:   100,
	}
	db.valueLogAutotuneLastProfile.Store(current)

	candidate := &vlogAutotuneProfile{
		TotalRatio:       0.89,
		EncodeNsEstimate: 1000,
		AvgSampleBytes:   100,
	}
	if db.valueLogAutotuneShouldSwitch(candidate, 10) {
		t.Fatalf("expected dwell gating to block switch")
	}

	db.valueLogAutotuneLastSwitchFrames.Store(10)
	if db.valueLogAutotuneShouldSwitch(candidate, 10) {
		t.Fatalf("expected gain gating to block switch")
	}

	candidate.TotalRatio = 0.7
	if !db.valueLogAutotuneShouldSwitch(candidate, 10) {
		t.Fatalf("expected switch when gain is sufficient")
	}
}

func TestValueLogAutotuneShouldSwitch_SizePolicyUsesRatioGain(t *testing.T) {
	db := &DB{valueLogAutoPolicy: uint8(vlogAutoSize)}
	db.valueLogAutotuneOptions = valuelog.AutotuneOptions{
		Mode:            valuelog.AutotuneMedium,
		MinGainToSwitch: 0.10,
	}
	current := &vlogAutotuneProfile{
		TotalRatio:       0.80,
		EncodeNsEstimate: 100,
		AvgSampleBytes:   100,
	}
	db.valueLogAutotuneLastProfile.Store(current)

	candidate := &vlogAutotuneProfile{
		TotalRatio:       0.75, // ~6.25% better ratio: below 10% gain threshold.
		EncodeNsEstimate: 1_000_000_000,
		AvgSampleBytes:   100,
	}
	if db.valueLogAutotuneShouldSwitch(candidate, 0) {
		t.Fatalf("expected size policy gain gating to block small ratio improvement")
	}

	candidate.TotalRatio = 0.70 // 12.5% better ratio: clears 10% gain threshold.
	if !db.valueLogAutotuneShouldSwitch(candidate, 0) {
		t.Fatalf("expected size policy to switch on sufficient ratio gain")
	}
}
