package adaptive

import "testing"

func TestControllerBoundedStepAndBounds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.K = 1
	cfg.Step = 64
	cfg.Alpha = 1
	cfg.HardMin = 64
	cfg.HardMax = 256
	cfg.W1 = 1000
	cfg.W2, cfg.W3 = 0, 0
	cfg.V1 = 1000
	cfg.V2, cfg.V3 = 0, 0

	c := New(cfg, 192)

	// Sustained index pressure should decrease by at most step.
	prev := c.Current()
	for i := 0; i < 3; i++ {
		c.RecordCommit(CommitMetrics{
			LeafFillAvg:   1.0,
			LeafCount:     uint64(10 + i),
			SlabDeadRatio: 0.0,
			Ops:           1,
		})
		cur := c.Current()
		if prev-cur > cfg.Step {
			t.Fatalf("threshold moved by %d > step", prev-cur)
		}
		prev = cur
	}
	if got := c.Current(); got != cfg.HardMin {
		t.Fatalf("expected clamp to hard min %d, got %d", cfg.HardMin, got)
	}

	// Sustained slab pressure should increase by at most step and not exceed max.
	prev = c.Current()
	for i := 0; i < 4; i++ {
		c.RecordCommit(CommitMetrics{
			LeafFillAvg:   0.0,
			LeafCount:     uint64(20 + i),
			SlabDeadRatio: 1.0,
			Ops:           1,
		})
		cur := c.Current()
		if cur-prev > cfg.Step {
			t.Fatalf("threshold moved by %d > step", cur-prev)
		}
		if cur > cfg.HardMax {
			t.Fatalf("threshold exceeded hard max %d", cfg.HardMax)
		}
		prev = cur
	}
	if got := c.Current(); got != cfg.HardMax {
		t.Fatalf("expected clamp to hard max %d, got %d", cfg.HardMax, got)
	}
}

func TestControllerEvaluationFrequency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.K = 100
	cfg.Step = 64
	cfg.W1, cfg.W2, cfg.W3 = 0, 0, 0
	cfg.V1, cfg.V2, cfg.V3 = 0, 0, 0

	c := New(cfg, 128)
	for i := 0; i < 10000; i++ {
		c.RecordCommit(CommitMetrics{Ops: 1})
	}
	want := uint64(10000 / cfg.K)
	if got := c.EvaluationCount(); got != want {
		t.Fatalf("expected %d evaluations, got %d", want, got)
	}
	if c.Current() != 128 {
		t.Fatalf("expected threshold unchanged, got %d", c.Current())
	}
}

func TestControllerMixedWorkloadConvergenceSmoke(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.K = 1
	cfg.Step = 32
	cfg.Alpha = 1
	cfg.W1 = 1
	cfg.V1 = 1
	cfg.W2, cfg.W3, cfg.V2, cfg.V3 = 0, 0, 0, 0
	cfg.HardMin = 64
	cfg.HardMax = 512

	c := New(cfg, 256)
	for i := 0; i < 200; i++ {
		c.RecordCommit(CommitMetrics{
			LeafFillAvg:   0.86,
			LeafCount:     100,
			SlabDeadRatio: 0.36,
			Ops:           10,
		})
	}
	got := c.Current()
	if got < cfg.HardMin || got > cfg.HardMax {
		t.Fatalf("threshold out of bounds: %d", got)
	}
	if diff := got - 256; diff < -cfg.Step || diff > cfg.Step {
		t.Fatalf("expected near-stable threshold, got %d", got)
	}
}
