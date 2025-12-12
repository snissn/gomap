package adaptive

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

// CommitMetrics are per-commit samples provided to the controller.
type CommitMetrics struct {
	LeafFillAvg     float64
	LeafCount       uint64
	IndexWriteBytes uint64
	SlabWriteBytes  uint64
	SlabDeadRatio   float64
	CompactionIOBPS float64
	Ops             int
}

// Controller maintains EWMA telemetry and updates InlineThreshold.
type Controller struct {
	cfg Config

	threshold atomic.Int64
	lastLatch atomic.Int64

	mu sync.Mutex

	// EWMA telemetry.
	leafFillAvg     float64
	splitRate       float64
	indexWriteBytes float64
	slabWriteBytes  float64
	slabDeadRatio   float64
	compactionIOBPS float64
	opsAvg          float64

	commitCount   uint64
	evalCount     uint64
	lastLeafCount uint64
}

// New creates a controller with cfg and initial threshold.
func New(cfg Config, initialThreshold int) *Controller {
	cfg = cfg.Normalize()
	cur := clampInt(initialThreshold, cfg.HardMin, cfg.HardMax)
	c := &Controller{cfg: cfg}
	c.threshold.Store(int64(cur))
	c.lastLatch.Store(int64(cur))
	return c
}

// Enabled reports whether adaptive tuning is enabled.
func (c *Controller) Enabled() bool { return c != nil && c.cfg.Enabled }

// Latch returns the current threshold and records it as last latched.
func (c *Controller) Latch() int {
	if c == nil {
		return 0
	}
	t := int(c.threshold.Load())
	c.lastLatch.Store(int64(t))
	return t
}

// Current returns the latest threshold value.
func (c *Controller) Current() int {
	if c == nil {
		return 0
	}
	return int(c.threshold.Load())
}

// LastLatched returns the last threshold handed to a commit.
func (c *Controller) LastLatched() int {
	if c == nil {
		return 0
	}
	return int(c.lastLatch.Load())
}

// EvaluationCount returns how many evaluations have run.
func (c *Controller) EvaluationCount() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evalCount
}

// RecordCommit ingests per-commit telemetry and potentially updates the threshold.
func (c *Controller) RecordCommit(m CommitMetrics) {
	if c == nil || !c.cfg.Enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.commitCount++

	// Derive split_rate sample from leaf count delta.
	var splitSample float64
	if c.lastLeafCount > 0 && m.LeafCount > c.lastLeafCount {
		splitSample = float64(m.LeafCount - c.lastLeafCount)
	}
	c.lastLeafCount = m.LeafCount

	alpha := c.cfg.EwmaAlpha
	c.leafFillAvg = ewma(c.leafFillAvg, m.LeafFillAvg, alpha)
	c.splitRate = ewma(c.splitRate, splitSample, alpha)
	c.indexWriteBytes = ewma(c.indexWriteBytes, float64(m.IndexWriteBytes), alpha)
	c.slabWriteBytes = ewma(c.slabWriteBytes, float64(m.SlabWriteBytes), alpha)
	c.slabDeadRatio = ewma(c.slabDeadRatio, m.SlabDeadRatio, alpha)
	c.compactionIOBPS = ewma(c.compactionIOBPS, m.CompactionIOBPS, alpha)
	if m.Ops > 0 {
		c.opsAvg = ewma(c.opsAvg, float64(m.Ops), alpha)
	}

	if c.commitCount%c.cfg.K != 0 {
		return
	}

	indexPerOp := c.indexWriteBytes / math.Max(1, c.opsAvg)
	slabPerOp := c.slabWriteBytes / math.Max(1, c.opsAvg)

	indexPressure := c.cfg.W1*max0(c.leafFillAvg-c.cfg.LeafFillTarget) +
		c.cfg.W2*max0(c.splitRate-c.cfg.SplitRateTarget) +
		c.cfg.W3*max0(indexPerOp-c.cfg.IndexWriteTarget)

	slabPressure := c.cfg.V1*max0(c.slabDeadRatio-c.cfg.SlabDeadTarget) +
		c.cfg.V2*max0(c.compactionIOBPS-c.cfg.CompactionIOTarget) +
		c.cfg.V3*max0(slabPerOp-c.cfg.SlabWriteTarget)

	delta := c.cfg.Alpha * (slabPressure - indexPressure)
	step := float64(c.cfg.Step)
	delta = clamp(delta, -step, step)

	cur := int(c.threshold.Load())
	next := clampInt(cur+int(math.Round(delta)), c.cfg.HardMin, c.cfg.HardMax)
	c.threshold.Store(int64(next))
	c.evalCount++
}

// SetThreshold forcefully sets a new threshold (clamped). Intended for tests.
func (c *Controller) SetThreshold(th int) {
	if c == nil {
		return
	}
	th = clampInt(th, c.cfg.HardMin, c.cfg.HardMax)
	c.threshold.Store(int64(th))
}

// StatsMap returns adaptive Stats() keys when enabled.
func (c *Controller) StatsMap() map[string]string {
	if c == nil || !c.cfg.Enabled {
		return nil
	}
	c.mu.Lock()
	leafFill := c.leafFillAvg
	splitRate := c.splitRate
	indexWrite := c.indexWriteBytes
	slabDead := c.slabDeadRatio
	slabWrite := c.slabWriteBytes
	compIO := c.compactionIOBPS
	c.mu.Unlock()

	return map[string]string{
		"treedb.inline_threshold.current":           fmt.Sprintf("%d", c.Current()),
		"treedb.inline_threshold.hard_min":          fmt.Sprintf("%d", c.cfg.HardMin),
		"treedb.inline_threshold.hard_max":          fmt.Sprintf("%d", c.cfg.HardMax),
		"treedb.inline_threshold.leaf_fill_avg":     fmt.Sprintf("%.6f", leafFill),
		"treedb.inline_threshold.split_rate":        fmt.Sprintf("%.6f", splitRate),
		"treedb.inline_threshold.index_write_bytes": fmt.Sprintf("%.0f", indexWrite),
		"treedb.inline_threshold.slab_dead_ratio":   fmt.Sprintf("%.6f", slabDead),
		"treedb.inline_threshold.slab_write_bytes":  fmt.Sprintf("%.0f", slabWrite),
		"treedb.inline_threshold.compaction_io_bps": fmt.Sprintf("%.0f", compIO),
	}
}

func ewma(old, sample, alpha float64) float64 {
	if alpha <= 0 || alpha > 1 {
		return sample
	}
	if old == 0 {
		return sample
	}
	return alpha*sample + (1-alpha)*old
}

func max0(v float64) float64 {
	if v > 0 {
		return v
	}
	return 0
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
