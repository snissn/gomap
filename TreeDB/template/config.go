package template

import (
	"runtime"
	"time"
)

// Config controls template encoding, routing, and training behavior.
// Zero values use defaults via NormalizeConfig.
type Config struct {
	// Encoding/decoding caps.
	MinSavingsBytes       int
	MaxGaps               int
	MaxDecodedBytes       int
	MaxAnchorsPerTemplate int
	MinAnchorLen          int
	MaxAnchorLen          int
	MaxAnchorBytesTotal   int
	MaxAnchorSearchOps    int

	// Fingerprinting (routing).
	FingerprintK          int
	FingerprintW          int
	MaxFingerprints       int
	MaxFPReads            int
	MaxTemplateFetch      int
	MaxCandidatesPerFP    int
	MaxCandidateListBytes int
	RoutePrefixBytes      int
	RouteSuffixBytes      int
	LengthBucketMinLen    int
	DefCacheSize          int
	RecentTemplates       int
	FastPathMinSavings    int
	FastPathMinHits       int
	FastPathSavingsSlack  int
	FastPathMaxMisses     int
	// ColdSearchAfter controls when Encode enters a "cold" mode after a long
	// stretch of non-kept values. In cold mode, Encode skips expensive candidate
	// lookup/matching for most values and only probes periodically.
	//
	// Values <= 0 use a default.
	ColdSearchAfter int
	// ColdSearchProbeEvery controls how often Encode probes candidates while in
	// cold mode (every N values). Values <= 0 use a default.
	ColdSearchProbeEvery int

	// Training / publishing bounds.
	MaxBuckets                   int
	MaxValuesPerBucket           int
	MaxBytesPerBucket            int
	TrainSampleStride            int
	SynthesizeEverySamples       int
	MaxAnchorScanPerSynthesis    int
	MaxValuesScannedPerSynthesis int
	MaskMaxValuesScanned         int
	MinAnchorFreq                int
	MinPresenceRatio             float64
	AmbiguityPct                 float64
	MinPublishSavingsBytes       int
	MinPublishRatio              float64
	MinActivateHits              int
	MinActivateSavedBytes        int
	RouteFPCount                 int
	DisableMaskTemplates         bool
	MaskMinPresenceRatio         float64
	MaskMinConstBytes            int
	MaskMinConstFrac             float64
	CooldownValues               int
	MaxTemplatesPerBucket        int
	MaxTemplatesTotal            int

	// Async training / publishing.
	//
	// These settings control the background pipeline used to ingest samples,
	// synthesize templates, and publish them without stalling writers.
	TrainShards         int
	TrainRouters        int
	TrainQueueSize      int
	TrainShardQueueSize int
	TrainMaxValueBytes  int
	PublishBatchSize    int
	PublishFlushEvery   time.Duration
}

// NormalizeConfig applies defaults and bounds for a config.
func NormalizeConfig(cfg Config) Config {
	if cfg.MinSavingsBytes <= 0 {
		cfg.MinSavingsBytes = 4
	}
	if cfg.MaxGaps <= 0 {
		cfg.MaxGaps = 64
	}
	if cfg.MaxAnchorsPerTemplate <= 0 {
		cfg.MaxAnchorsPerTemplate = 32
	}
	if cfg.MinAnchorLen <= 0 {
		cfg.MinAnchorLen = 16
	}
	if cfg.MaxAnchorLen <= 0 {
		cfg.MaxAnchorLen = 64
	}
	if cfg.MaxAnchorBytesTotal <= 0 {
		cfg.MaxAnchorBytesTotal = 2048
	}
	if cfg.MaxAnchorSearchOps <= 0 {
		cfg.MaxAnchorSearchOps = 128
	}
	if cfg.FingerprintK <= 0 {
		cfg.FingerprintK = 16
	}
	if cfg.FingerprintW <= 0 {
		cfg.FingerprintW = 64
	}
	if cfg.MaxFingerprints <= 0 {
		cfg.MaxFingerprints = 64
	}
	if cfg.MaxFPReads <= 0 {
		cfg.MaxFPReads = cfg.MaxFingerprints
	}
	if cfg.MaxTemplateFetch <= 0 {
		cfg.MaxTemplateFetch = 32
	}
	if cfg.MaxCandidatesPerFP <= 0 {
		cfg.MaxCandidatesPerFP = 32
	}
	if cfg.MaxCandidateListBytes <= 0 {
		cfg.MaxCandidateListBytes = 4 << 10
	}
	if cfg.RoutePrefixBytes <= 0 {
		cfg.RoutePrefixBytes = 32
	}
	if cfg.RouteSuffixBytes <= 0 {
		cfg.RouteSuffixBytes = 32
	}
	if cfg.LengthBucketMinLen <= 0 {
		cfg.LengthBucketMinLen = 64
	}
	if cfg.DefCacheSize == 0 {
		cfg.DefCacheSize = 64
	}
	if cfg.RecentTemplates < 0 {
		cfg.RecentTemplates = 0
	}
	if cfg.RecentTemplates == 0 {
		cfg.RecentTemplates = 2
	}
	if cfg.FastPathMinSavings <= 0 {
		cfg.FastPathMinSavings = cfg.MinSavingsBytes * 4
		if cfg.FastPathMinSavings < cfg.MinSavingsBytes {
			cfg.FastPathMinSavings = cfg.MinSavingsBytes
		}
	}
	if cfg.FastPathMinHits <= 0 {
		cfg.FastPathMinHits = 2
	}
	if cfg.FastPathSavingsSlack < 0 {
		cfg.FastPathSavingsSlack = 0
	}
	if cfg.FastPathSavingsSlack == 0 {
		cfg.FastPathSavingsSlack = 8
	}
	if cfg.FastPathMaxMisses <= 0 {
		cfg.FastPathMaxMisses = 2
	}
	if cfg.ColdSearchAfter <= 0 {
		cfg.ColdSearchAfter = 256
	}
	if cfg.ColdSearchProbeEvery <= 0 {
		cfg.ColdSearchProbeEvery = 256
	}
	if cfg.MaxBuckets <= 0 {
		cfg.MaxBuckets = 256
	}
	if cfg.MaxValuesPerBucket <= 0 {
		cfg.MaxValuesPerBucket = 256
	}
	if cfg.MaxBytesPerBucket <= 0 {
		cfg.MaxBytesPerBucket = 8 << 20
	}
	if cfg.TrainSampleStride <= 0 {
		cfg.TrainSampleStride = 4
	}
	if cfg.SynthesizeEverySamples <= 0 {
		cfg.SynthesizeEverySamples = 64
	}
	if cfg.MaxAnchorScanPerSynthesis <= 0 {
		cfg.MaxAnchorScanPerSynthesis = 256
	}
	if cfg.MaxValuesScannedPerSynthesis <= 0 {
		cfg.MaxValuesScannedPerSynthesis = 64
	}
	if cfg.MaskMaxValuesScanned <= 0 {
		cfg.MaskMaxValuesScanned = 256
	}
	if cfg.MinAnchorFreq <= 0 {
		cfg.MinAnchorFreq = 16
	}
	if cfg.MinPresenceRatio <= 0 {
		cfg.MinPresenceRatio = 0.95
	}
	if cfg.AmbiguityPct <= 0 {
		cfg.AmbiguityPct = 0.10
	}
	if cfg.MinPublishSavingsBytes <= 0 {
		cfg.MinPublishSavingsBytes = 32
	}
	if cfg.MinPublishRatio <= 0 {
		cfg.MinPublishRatio = 0.98
	}
	if cfg.MinActivateHits <= 0 {
		cfg.MinActivateHits = 4
	}
	if cfg.MinActivateSavedBytes <= 0 {
		cfg.MinActivateSavedBytes = 256
	}
	if cfg.RouteFPCount <= 0 {
		cfg.RouteFPCount = 12
	}
	if cfg.MaskMinPresenceRatio <= 0 {
		cfg.MaskMinPresenceRatio = 0.9
	}
	if cfg.MaskMinConstBytes <= 0 {
		cfg.MaskMinConstBytes = 32
	}
	if cfg.MaskMinConstFrac <= 0 {
		cfg.MaskMinConstFrac = 0.2
	}
	if cfg.CooldownValues <= 0 {
		cfg.CooldownValues = 512
	}
	if cfg.MaxTemplatesPerBucket <= 0 {
		cfg.MaxTemplatesPerBucket = 4
	}
	if cfg.MaxTemplatesTotal <= 0 {
		cfg.MaxTemplatesTotal = 1 << 20
	}
	if cfg.TrainShards <= 0 {
		cfg.TrainShards = runtime.GOMAXPROCS(0)
		if cfg.TrainShards > 8 {
			cfg.TrainShards = 8
		}
		if cfg.TrainShards < 1 {
			cfg.TrainShards = 1
		}
	}
	if cfg.MaxBuckets > 0 && cfg.TrainShards > cfg.MaxBuckets {
		cfg.TrainShards = cfg.MaxBuckets
	}
	if cfg.TrainRouters <= 0 {
		cfg.TrainRouters = cfg.TrainShards
		if cfg.TrainRouters > 4 {
			cfg.TrainRouters = 4
		}
		if cfg.TrainRouters < 1 {
			cfg.TrainRouters = 1
		}
	}
	if cfg.TrainQueueSize <= 0 {
		cfg.TrainQueueSize = 4096
	}
	if cfg.TrainShardQueueSize <= 0 {
		cfg.TrainShardQueueSize = cfg.TrainQueueSize / cfg.TrainShards
		if cfg.TrainShardQueueSize < 64 {
			cfg.TrainShardQueueSize = 64
		}
	}
	if cfg.TrainMaxValueBytes == 0 {
		cfg.TrainMaxValueBytes = 64 << 10
	}
	if cfg.PublishBatchSize <= 0 {
		cfg.PublishBatchSize = 8
	}
	if cfg.PublishFlushEvery <= 0 {
		cfg.PublishFlushEvery = 50 * time.Millisecond
	}
	return cfg
}
