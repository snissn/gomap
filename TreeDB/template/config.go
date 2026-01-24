package template

// Config controls the template engine behavior for value-log encoding.
// Zero values use defaults via NormalizeConfig.
type Config struct {
	// MinPrefixBytes is the minimum prefix length required to form a template.
	MinPrefixBytes int
	// MinSuffixBytes is the minimum suffix length required to form a template.
	MinSuffixBytes int
	// MinTotalBytes is the minimum combined prefix+suffix length required.
	MinTotalBytes int
	// MaxTemplateBytes caps prefix+suffix bytes stored per template.
	MaxTemplateBytes int
	// MinSavingsBytes is the minimum byte savings required to keep a template encoding.
	MinSavingsBytes int
	// HistoryEntries controls how many recent values are considered for template creation.
	HistoryEntries int
}

// NormalizeConfig applies defaults and bounds for a config.
func NormalizeConfig(cfg Config) Config {
	if cfg.MinPrefixBytes <= 0 {
		cfg.MinPrefixBytes = 8
	}
	if cfg.MinSuffixBytes <= 0 {
		cfg.MinSuffixBytes = 8
	}
	if cfg.MinTotalBytes <= 0 {
		cfg.MinTotalBytes = 32
	}
	if cfg.MaxTemplateBytes <= 0 {
		cfg.MaxTemplateBytes = 256
	}
	if cfg.MinSavingsBytes <= 0 {
		cfg.MinSavingsBytes = 4
	}
	if cfg.HistoryEntries <= 0 {
		cfg.HistoryEntries = 8
	}
	return cfg
}
