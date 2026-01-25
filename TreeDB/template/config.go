package template

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
	return cfg
}
