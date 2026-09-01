package caching

import (
	"log"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const (
	vlogAutotuneEWMAAlpha        = 0.1
	vlogAutotuneLogEveryDefault  = 1024
	vlogAutotuneLogBudgetDefault = 200
	vlogAutotuneMetricsEnv       = "TREEDB_VLOG_AUTOTUNE_METRICS"
	vlogAutotuneMetricsEveryEnv  = "TREEDB_VLOG_AUTOTUNE_METRICS_EVERY"
	vlogAutotuneMetricsBudgetEnv = "TREEDB_VLOG_AUTOTUNE_METRICS_BUDGET"
)

var (
	vlogAutotuneMetricsEnabled atomic.Bool
	vlogAutotuneMetricsEvery   atomic.Uint64
	vlogAutotuneMetricsBudget  atomic.Int64
)

func init() {
	vlogAutotuneMetricsEvery.Store(vlogAutotuneLogEveryDefault)
	if os.Getenv(vlogAutotuneMetricsEnv) == "" {
		return
	}
	vlogAutotuneMetricsEnabled.Store(true)
	if s := os.Getenv(vlogAutotuneMetricsEveryEnv); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v > 0 {
			vlogAutotuneMetricsEvery.Store(v)
		}
	}
	budget := int64(vlogAutotuneLogBudgetDefault)
	if s := os.Getenv(vlogAutotuneMetricsBudgetEnv); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			budget = v
		}
	}
	vlogAutotuneMetricsBudget.Store(budget)
}

type vlogAutotuneMetrics struct {
	clock              valuelog.Clock
	encodeNsPerRawByte atomic.Uint64
	ioNsPerStoredByte  atomic.Uint64
	throughputRawMBps  atomic.Uint64
	observedRatio      atomic.Uint64
	updates            atomic.Uint64
}

type vlogAutotuneSnapshot struct {
	EncodeNsPerRawByte float64
	IoNsPerStoredByte  float64
	ThroughputRawMBps  float64
	ObservedRatio      float64
}

func (s vlogAutotuneSnapshot) hasData() bool {
	return s.EncodeNsPerRawByte > 0 || s.IoNsPerStoredByte > 0 || s.ThroughputRawMBps > 0 || s.ObservedRatio > 0
}

func (m *vlogAutotuneMetrics) init(clock valuelog.Clock) {
	if m == nil {
		return
	}
	if clock == nil {
		clock = valuelog.RealClock{}
	}
	m.clock = clock
}

func (m *vlogAutotuneMetrics) setClock(clock valuelog.Clock) {
	if m == nil {
		return
	}
	if clock == nil {
		clock = valuelog.RealClock{}
	}
	m.clock = clock
}

func (m *vlogAutotuneMetrics) now() time.Time {
	if m == nil {
		return time.Time{}
	}
	if m.clock == nil {
		return time.Now()
	}
	return m.clock.Now()
}

func (m *vlogAutotuneMetrics) observe(start time.Time, rawBytes, storedBytes int, encodeNs int64, encodeRawBytes int) {
	if m == nil || rawBytes <= 0 || start.IsZero() {
		return
	}
	wallNs := m.now().Sub(start).Nanoseconds()
	if wallNs <= 0 {
		return
	}
	if storedBytes <= 0 {
		storedBytes = rawBytes
	}
	ioWallNs := wallNs
	if encodeNs > 0 && encodeNs < wallNs {
		ioWallNs = wallNs - encodeNs
	}
	throughput := float64(rawBytes) * 1e3 / float64(wallNs)
	ioNsPerStored := float64(ioWallNs) / float64(storedBytes)
	observedRatio := float64(storedBytes) / float64(rawBytes)

	updateEWMA(&m.throughputRawMBps, throughput)
	updateEWMA(&m.ioNsPerStoredByte, ioNsPerStored)
	updateEWMA(&m.observedRatio, observedRatio)
	if encodeNs > 0 && encodeRawBytes > 0 {
		encodeNsPerRaw := float64(encodeNs) / float64(encodeRawBytes)
		updateEWMA(&m.encodeNsPerRawByte, encodeNsPerRaw)
	}

	updates := m.updates.Add(1)
	if !vlogAutotuneMetricsEnabled.Load() {
		return
	}
	every := vlogAutotuneMetricsEvery.Load()
	if every == 0 || updates%every != 0 {
		return
	}
	if vlogAutotuneMetricsBudget.Add(-1) < 0 {
		return
	}
	snap := m.snapshot()
	log.Printf("treedb: vlog_autotune_metrics raw=%d stored=%d wall_ns=%d encode_ns=%d encode_raw=%d encode_ns_per_raw_byte=%.3f io_ns_per_stored_byte=%.3f throughput_raw_MBps=%.3f observed_ratio=%.3f",
		rawBytes,
		storedBytes,
		wallNs,
		encodeNs,
		encodeRawBytes,
		snap.EncodeNsPerRawByte,
		snap.IoNsPerStoredByte,
		snap.ThroughputRawMBps,
		snap.ObservedRatio,
	)
}

func (m *vlogAutotuneMetrics) snapshot() vlogAutotuneSnapshot {
	if m == nil {
		return vlogAutotuneSnapshot{}
	}
	return vlogAutotuneSnapshot{
		EncodeNsPerRawByte: math.Float64frombits(m.encodeNsPerRawByte.Load()),
		IoNsPerStoredByte:  math.Float64frombits(m.ioNsPerStoredByte.Load()),
		ThroughputRawMBps:  math.Float64frombits(m.throughputRawMBps.Load()),
		ObservedRatio:      math.Float64frombits(m.observedRatio.Load()),
	}
}

func (m *vlogAutotuneMetrics) seed(encodeNsPerRawByte, ioNsPerStoredByte float64) {
	if m == nil {
		return
	}
	if encodeNsPerRawByte > 0 {
		m.encodeNsPerRawByte.Store(math.Float64bits(encodeNsPerRawByte))
	}
	if ioNsPerStoredByte > 0 {
		m.ioNsPerStoredByte.Store(math.Float64bits(ioNsPerStoredByte))
	}
}

func updateEWMA(dst *atomic.Uint64, sample float64) float64 {
	if dst == nil || sample <= 0 || math.IsNaN(sample) || math.IsInf(sample, 0) {
		if dst == nil {
			return 0
		}
		return math.Float64frombits(dst.Load())
	}
	for {
		oldBits := dst.Load()
		old := math.Float64frombits(oldBits)
		next := sample
		if old > 0 && !math.IsNaN(old) && !math.IsInf(old, 0) {
			next = (1-vlogAutotuneEWMAAlpha)*old + vlogAutotuneEWMAAlpha*sample
		}
		if dst.CompareAndSwap(oldBits, math.Float64bits(next)) {
			return next
		}
	}
}
