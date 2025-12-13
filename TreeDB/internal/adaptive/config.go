package adaptive

import "github.com/snissn/gomap/TreeDB/internal/page"

// Config controls the adaptive inline threshold controller.
// All fields are optional; zero values are replaced by defaults in Normalize.
type Config struct {
	Enabled bool

	// K is the evaluation interval in commits.
	K uint64

	// EwmaAlpha is the EWMA smoothing factor (0<alpha<=1).
	EwmaAlpha float64

	// Index weights.
	W1 float64
	W2 float64
	W3 float64
	// Slab/compaction weights.
	V1 float64
	V2 float64
	V3 float64

	// Step is the bounded adjustment step in bytes.
	Step int
	// Alpha is the controller gain for delta computation.
	Alpha float64

	// Hard bounds for InlineThreshold.
	HardMin int
	HardMax int

	// Targets used in pressure functions.
	LeafFillTarget     float64
	SplitRateTarget    float64
	IndexWriteTarget   float64
	SlabDeadTarget     float64
	CompactionIOTarget float64
	SlabWriteTarget    float64
}

// DefaultConfig returns recommended defaults from the spec.
func DefaultConfig() Config {
	return Config{
		Enabled:            false,
		K:                  100,
		EwmaAlpha:          0.2,
		W1:                 2.0,
		W2:                 2.0,
		W3:                 0.0,
		V1:                 1.0,
		V2:                 1.0,
		V3:                 0.0,
		Step:               64,
		Alpha:              1.0,
		HardMin:            page.InlineHardMin,
		HardMax:            page.InlineHardMax,
		LeafFillTarget:     0.85,
		SplitRateTarget:    0.0,
		IndexWriteTarget:   0.0,
		SlabDeadTarget:     0.35,
		CompactionIOTarget: 0.0,
		SlabWriteTarget:    0.0,
	}
}

// Normalize fills zero fields with defaults and clamps bounds.
func (c Config) Normalize() Config {
	d := DefaultConfig()
	if c.K == 0 {
		c.K = d.K
	}
	if c.EwmaAlpha <= 0 || c.EwmaAlpha > 1 {
		c.EwmaAlpha = d.EwmaAlpha
	}
	if c.Step <= 0 {
		c.Step = d.Step
	}
	if c.Alpha == 0 {
		c.Alpha = d.Alpha
	}
	if c.HardMin <= 0 {
		c.HardMin = d.HardMin
	}
	if c.HardMax <= 0 {
		c.HardMax = d.HardMax
	}
	if c.HardMin > c.HardMax {
		c.HardMin, c.HardMax = c.HardMax, c.HardMin
	}
	if c.LeafFillTarget == 0 {
		c.LeafFillTarget = d.LeafFillTarget
	}
	if c.SlabDeadTarget == 0 {
		c.SlabDeadTarget = d.SlabDeadTarget
	}
	// Leave other targets/weights as provided; zero disables a term.
	return c
}
