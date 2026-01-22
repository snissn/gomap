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
	if math.Abs(snap.IoNsPerStoredByte-float64(nsPerStoredByte)) > 0.001 {
		t.Fatalf("io_ns_per_stored_byte got %.3f want %.3f", snap.IoNsPerStoredByte, float64(nsPerStoredByte))
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
