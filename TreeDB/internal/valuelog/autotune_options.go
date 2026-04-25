package valuelog

type AutotuneMode uint8

const (
	// AutotuneUnset indicates the caller did not explicitly configure the
	// autotuner mode. NormalizeAutotuneOptions maps this to a sensible default
	// based on whether the value log is enabled.
	AutotuneUnset AutotuneMode = iota
	AutotuneOff
	AutotuneMedium
	AutotuneAggressive
)

type AutotuneOptions struct {
	Mode AutotuneMode

	CandidateK            []int
	CandidateHistoryBytes []int
	CandidateDictBytes    []int

	MinGainToSwitch float64
	MinDwellFrames  uint64

	SampleStride     uint64
	MaxSampleBytes   uint64
	TrainCPUFraction float64

	ProbeBytes uint64
	PauseBytes uint64

	DisableBelowValueBytes int
}

func (opts AutotuneOptions) isZero() bool {
	return opts.Mode == AutotuneUnset &&
		len(opts.CandidateK) == 0 &&
		len(opts.CandidateHistoryBytes) == 0 &&
		len(opts.CandidateDictBytes) == 0 &&
		opts.MinGainToSwitch == 0 &&
		opts.MinDwellFrames == 0 &&
		opts.SampleStride == 0 &&
		opts.MaxSampleBytes == 0 &&
		opts.TrainCPUFraction == 0 &&
		opts.ProbeBytes == 0 &&
		opts.PauseBytes == 0 &&
		opts.DisableBelowValueBytes == 0
}

func NormalizeAutotuneOptions(opts AutotuneOptions, valueLogEnabled bool) AutotuneOptions {
	if opts.Mode == AutotuneUnset {
		// Default behavior:
		// - Value log enabled: enable autotune by default.
		// - Otherwise: leave compression/autotune off.
		if valueLogEnabled {
			opts.Mode = AutotuneMedium
		} else {
			opts.Mode = AutotuneOff
		}
	}
	if opts.Mode == AutotuneOff {
		return opts
	}
	if len(opts.CandidateK) == 0 {
		opts.CandidateK = []int{1, 2, 4, 8, 16, 32}
	}
	if len(opts.CandidateHistoryBytes) == 0 {
		if opts.Mode == AutotuneAggressive {
			opts.CandidateHistoryBytes = []int{64 << 10, 96 << 10, 128 << 10, 192 << 10, 256 << 10, 512 << 10}
		} else {
			opts.CandidateHistoryBytes = []int{64 << 10, 96 << 10, 128 << 10, 192 << 10}
		}
	}
	if len(opts.CandidateDictBytes) == 0 {
		if opts.Mode == AutotuneAggressive {
			opts.CandidateDictBytes = []int{40 << 10, 64 << 10, 96 << 10, 128 << 10, 192 << 10, 256 << 10}
		} else {
			opts.CandidateDictBytes = []int{40 << 10, 64 << 10, 96 << 10, 128 << 10}
		}
	}
	if opts.MinGainToSwitch <= 0 {
		opts.MinGainToSwitch = 0.05
	}
	if opts.MinDwellFrames == 0 {
		opts.MinDwellFrames = 1 << 16
	}
	if opts.SampleStride == 0 {
		opts.SampleStride = 4
	}
	if opts.MaxSampleBytes == 0 {
		if opts.Mode == AutotuneAggressive {
			opts.MaxSampleBytes = 32 << 20
		} else {
			opts.MaxSampleBytes = 8 << 20
		}
	}
	if opts.TrainCPUFraction == 0 {
		opts.TrainCPUFraction = 0.02
	}
	if opts.ProbeBytes == 0 {
		opts.ProbeBytes = 16 << 20
	}
	if opts.PauseBytes == 0 {
		opts.PauseBytes = 64 << 20
	}
	return opts
}
