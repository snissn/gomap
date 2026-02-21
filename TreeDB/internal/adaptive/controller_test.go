package adaptive

import "testing"

func TestNewDefaults(t *testing.T) {
	c := New()
	if got := c.GetThreshold(); got != DefaultInlineThreshold {
		t.Fatalf("GetThreshold()=%d, want %d", got, DefaultInlineThreshold)
	}
}

func TestRecordCommitAdjustsOnlyAtInterval(t *testing.T) {
	c := New()
	start := c.GetThreshold()
	inc := Metrics{
		LeafFill:       1.0,
		Splits:         0,
		SlabWriteBytes: 1024,
		SlabDeadBytes:  900,
	}

	for i := 0; i < UpdateInterval-1; i++ {
		c.RecordCommit(inc)
	}
	if got := c.GetThreshold(); got != start {
		t.Fatalf("threshold changed before interval: got %d, want %d", got, start)
	}

	c.RecordCommit(inc)
	if got := c.GetThreshold(); got != start+DefaultStep {
		t.Fatalf("threshold after interval=%d, want %d", got, start+DefaultStep)
	}
}

func TestAdjustThresholdIncreasesAndClamps(t *testing.T) {
	c := New()
	c.currentThreshold = InlineHardMax - DefaultStep
	c.leafFillAvg = 0.95
	c.splitRateAvg = 0
	c.slabDeadRatioAvg = 0.80
	c.slabWriteBytesAvg = 128

	c.adjustThreshold()
	if got := c.currentThreshold; got != InlineHardMax {
		t.Fatalf("currentThreshold=%d, want %d", got, InlineHardMax)
	}

	c.adjustThreshold()
	if got := c.currentThreshold; got != InlineHardMax {
		t.Fatalf("currentThreshold exceeded hard max: got %d", got)
	}
}

func TestAdjustThresholdDecreasesAndClamps(t *testing.T) {
	c := New()
	c.currentThreshold = InlineHardMin + DefaultStep
	c.leafFillAvg = 0.20
	c.splitRateAvg = 0.70
	c.slabDeadRatioAvg = 0
	c.slabWriteBytesAvg = 0

	c.adjustThreshold()
	if got := c.currentThreshold; got != InlineHardMin {
		t.Fatalf("currentThreshold=%d, want %d", got, InlineHardMin)
	}

	c.adjustThreshold()
	if got := c.currentThreshold; got != InlineHardMin {
		t.Fatalf("currentThreshold went below hard min: got %d", got)
	}
}
