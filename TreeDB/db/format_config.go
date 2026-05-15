package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const formatConfigFileName = "format.json"

const formatConfigVersion = 2
const formatConfigRequiredFeaturesVersion = 3

const RequiredFeatureCommandWALV1 = "command_wal_v1"

// FormatConfig captures the format-affecting knobs that maintenance tooling
// should preserve when rewriting index/value-log state.
//
// This file is best-effort and pre-alpha; callers should tolerate it being
// absent. Versioned files written by SaveFormatConfig are expected to be fully
// populated; if new fields are added in the future, the version should be
// bumped so older binaries do not accidentally apply zero-values.
type FormatConfig struct {
	Version int `json:"version"`

	RequiredFeatures []string `json:"required_features,omitempty"`

	IndexOuterLeavesInValueLog bool `json:"index_outer_leaves_in_vlog"`

	LeafPrefixCompression     bool `json:"leaf_prefix_compression"`
	IndexColumnarLeaves       bool `json:"index_columnar_leaves"`
	IndexPackedValuePtr       bool `json:"index_packed_valueptr"`
	IndexInternalBaseDelta    bool `json:"index_internal_base_delta"`
	IndexAdaptiveLeafEncoding bool `json:"index_adaptive_leaf_encoding"`

	ValueLogCompression string `json:"vlog_compression"`
	ValueLogBlockCodec  string `json:"vlog_block_codec"`
	ValueLogAutoPolicy  string `json:"vlog_auto_policy"`
}

func (cfg FormatConfig) RequiresCommandWALV1() bool {
	for _, feature := range cfg.RequiredFeatures {
		if normalizeFormatConfigMode(feature) == RequiredFeatureCommandWALV1 {
			return true
		}
	}
	return false
}

func formatConfigPath(dir string) string {
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, formatConfigFileName)
}

func normalizeFormatConfigMode(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func formatConfigFromOptions(opts Options) FormatConfig {
	cfg := FormatConfig{
		Version: formatConfigVersion,

		IndexOuterLeavesInValueLog: opts.IndexOuterLeavesInValueLog,

		LeafPrefixCompression:     opts.LeafPrefixCompression,
		IndexColumnarLeaves:       opts.IndexColumnarLeaves,
		IndexPackedValuePtr:       opts.IndexPackedValuePtr,
		IndexInternalBaseDelta:    opts.IndexInternalBaseDelta,
		IndexAdaptiveLeafEncoding: opts.IndexAdaptiveLeafEncoding,

		ValueLogCompression: normalizeFormatConfigMode(formatValueLogCompressionMode(opts.ValueLog.Compression)),
		ValueLogBlockCodec:  normalizeFormatConfigMode(formatValueLogBlockCodec(opts.ValueLog.BlockCodec)),
		ValueLogAutoPolicy:  normalizeFormatConfigMode(formatValueLogAutoPolicy(opts.ValueLog.AutoPolicy)),
	}
	// Leaf-log child pages use an explicit LogRecordRef layout instead of page
	// child IDs, so keep base-delta disabled for outer-leaf-in-vlog roots until
	// mixed internal-level policy is supported.
	if cfg.IndexOuterLeavesInValueLog && cfg.IndexInternalBaseDelta {
		cfg.IndexInternalBaseDelta = false
	}
	if opts.CommandWAL {
		cfg.Version = formatConfigRequiredFeaturesVersion
		cfg.RequiredFeatures = appendRequiredFormatFeature(cfg.RequiredFeatures, RequiredFeatureCommandWALV1)
	}

	return cfg
}

func formatConfigFromOptionsPreservingRequiredFeatures(opts Options) (FormatConfig, bool, error) {
	cfg := formatConfigFromOptions(opts)
	existing, ok, err := LoadFormatConfig(opts.Dir)
	if err != nil {
		return FormatConfig{}, false, err
	}
	requiresCommandWAL := false
	if ok {
		requiresCommandWAL = existing.RequiresCommandWALV1()
		for _, feature := range existing.RequiredFeatures {
			cfg.RequiredFeatures = appendRequiredFormatFeature(cfg.RequiredFeatures, feature)
		}
		if len(cfg.RequiredFeatures) != 0 {
			cfg.Version = formatConfigRequiredFeaturesVersion
		}
	}
	return cfg, requiresCommandWAL, nil
}

func appendRequiredFormatFeature(features []string, feature string) []string {
	normalized := normalizeFormatConfigMode(feature)
	for _, existing := range features {
		if normalizeFormatConfigMode(existing) == normalized {
			return features
		}
	}
	return append(features, feature)
}

// ApplyToOptions overwrites format-affecting knobs in opts from cfg.
//
// Callers should treat cfg as best-effort (it may be absent) and may apply
// explicit overrides after this call.
func (cfg FormatConfig) ApplyToOptions(opts *Options) {
	if opts == nil {
		return
	}
	opts.IndexOuterLeavesInValueLog = cfg.IndexOuterLeavesInValueLog

	opts.LeafPrefixCompression = cfg.LeafPrefixCompression
	opts.IndexColumnarLeaves = cfg.IndexColumnarLeaves
	opts.IndexPackedValuePtr = cfg.IndexPackedValuePtr
	opts.IndexInternalBaseDelta = cfg.IndexInternalBaseDelta
	opts.IndexAdaptiveLeafEncoding = cfg.IndexAdaptiveLeafEncoding

	if m, ok := parseValueLogCompressionMode(cfg.ValueLogCompression); ok {
		opts.ValueLog.Compression = m
	}
	if c, ok := parseValueLogBlockCodec(cfg.ValueLogBlockCodec); ok {
		opts.ValueLog.BlockCodec = c
	}
	if p, ok := parseValueLogAutoPolicy(cfg.ValueLogAutoPolicy); ok {
		opts.ValueLog.AutoPolicy = p
	}
}

// ApplyIndexFormatToOptions overwrites index-format-affecting knobs in opts
// from cfg.
//
// This is intentionally narrower than ApplyToOptions: it is safe for normal
// DB opens where callers may want to tune runtime policies (e.g. value-log
// compression) via env vars or flags, while still ensuring the index encoding
// matches on-disk state.
func (cfg FormatConfig) ApplyIndexFormatToOptions(opts *Options) {
	if opts == nil {
		return
	}
	opts.IndexOuterLeavesInValueLog = cfg.IndexOuterLeavesInValueLog

	opts.LeafPrefixCompression = cfg.LeafPrefixCompression
	opts.IndexColumnarLeaves = cfg.IndexColumnarLeaves
	opts.IndexPackedValuePtr = cfg.IndexPackedValuePtr
	opts.IndexInternalBaseDelta = cfg.IndexInternalBaseDelta
	opts.IndexAdaptiveLeafEncoding = cfg.IndexAdaptiveLeafEncoding
}

// LoadFormatConfig loads the best-effort persisted format config for dir.
// The returned bool reports whether the file was found.
func LoadFormatConfig(dir string) (FormatConfig, bool, error) {
	var cfg FormatConfig
	path := formatConfigPath(dir)
	if path == "" {
		return cfg, false, errors.New("missing db dir")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, false, nil
		}
		return cfg, false, err
	}
	if len(data) == 0 {
		return cfg, false, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return FormatConfig{}, false, fmt.Errorf("treedb: decode %s: %w", filepath.Base(path), err)
	}
	switch cfg.Version {
	case formatConfigVersion:
		if len(cfg.RequiredFeatures) != 0 {
			return FormatConfig{}, false, fmt.Errorf("treedb: decode %s: required_features require format version %d", filepath.Base(path), formatConfigRequiredFeaturesVersion)
		}
	case formatConfigRequiredFeaturesVersion:
	default:
		return FormatConfig{}, false, fmt.Errorf("treedb: unsupported %s version %d", filepath.Base(path), cfg.Version)
	}
	if err := validateRequiredFormatFeatures(cfg.RequiredFeatures); err != nil {
		return FormatConfig{}, false, fmt.Errorf("treedb: decode %s: %w", filepath.Base(path), err)
	}
	return cfg, true, nil
}

// ValidateFormatRequiredFeatureGate checks only the required-feature gate in
// format.json. It is intentionally narrower than LoadFormatConfig so
// IgnoreFormatConfig remains an escape hatch for malformed or future ordinary
// format configs, while still failing closed for explicitly required features.
func ValidateFormatRequiredFeatureGate(dir string) error {
	_, err := commandWALRequiredFeatureGate(dir)
	return err
}

func CommandWALRequiredFeatureEnabled(dir string) (bool, error) {
	return commandWALRequiredFeatureGate(dir)
}

func commandWALRequiredFeatureGate(dir string) (bool, error) {
	path := formatConfigPath(dir)
	if path == "" {
		return false, errors.New("missing db dir")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if len(data) == 0 {
		return false, nil
	}

	var gate struct {
		Version          int      `json:"version"`
		RequiredFeatures []string `json:"required_features,omitempty"`
	}
	if err := json.Unmarshal(data, &gate); err != nil {
		if bytes.Contains(data, []byte("required_features")) {
			return false, fmt.Errorf("treedb: decode %s required-feature gate: %w", filepath.Base(path), err)
		}
		return false, nil
	}
	if len(gate.RequiredFeatures) == 0 {
		return false, nil
	}
	if gate.Version < formatConfigRequiredFeaturesVersion {
		return false, fmt.Errorf("treedb: decode %s: required_features require format version %d", filepath.Base(path), formatConfigRequiredFeaturesVersion)
	}
	if err := validateRequiredFormatFeatures(gate.RequiredFeatures); err != nil {
		return false, fmt.Errorf("treedb: decode %s: %w", filepath.Base(path), err)
	}
	requiresCommandWAL := false
	for _, feature := range gate.RequiredFeatures {
		if normalizeFormatConfigMode(feature) == RequiredFeatureCommandWALV1 {
			requiresCommandWAL = true
		}
	}
	return requiresCommandWAL, nil
}

// SaveFormatConfig writes cfg to dir/format.json atomically. A transition into
// command_wal_v1 validates that legacy WAL state is clean; re-saving a config
// that already requires command_wal_v1 does not re-run activation validation
// because command-WAL segments are expected after activation.
func SaveFormatConfig(dir string, cfg FormatConfig) error {
	if cfg.RequiresCommandWALV1() {
		existing, ok, err := LoadFormatConfig(dir)
		if err != nil {
			return err
		}
		if !ok || !existing.RequiresCommandWALV1() {
			if err := ValidateCommandWALActivationClean(dir); err != nil {
				return err
			}
		}
	}
	return writeFormatConfig(dir, cfg)
}

func writeFormatConfig(dir string, cfg FormatConfig) error {
	path := formatConfigPath(dir)
	if path == "" {
		return errors.New("missing db dir")
	}
	if len(cfg.RequiredFeatures) != 0 {
		cfg.Version = formatConfigRequiredFeaturesVersion
	} else if cfg.Version == 0 {
		cfg.Version = formatConfigVersion
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
}

func commandWALFormatNeedsActivation(opts Options) (bool, error) {
	if !opts.CommandWAL || opts.ReadOnly {
		return false, nil
	}
	requiresCommandWAL, err := CommandWALRequiredFeatureEnabled(opts.Dir)
	if err != nil {
		return false, err
	}
	return !requiresCommandWAL, nil
}

// saveOpenFormatConfig is an openWithLock post-open refresh hook. It must not
// be used as a command-WAL activation helper; fresh activation is gated earlier
// by SaveFormatConfig/ValidateCommandWALActivationClean before the journal opens.
func saveOpenFormatConfig(opts Options) error {
	if opts.CommandWAL && opts.ReadOnly {
		return nil
	}
	cfg, _, err := formatConfigFromOptionsPreservingRequiredFeatures(opts)
	if err != nil {
		return err
	}
	if opts.CommandWAL {
		// By the time saveOpenFormatConfig is called at the end of openWithLock,
		// the command journal is already open. Skip ValidateCommandWALActivationClean
		// (which asserts the WAL directory is clean) because it only applies to
		// fresh activations; those are handled by the needsCommandWALFormat path
		// in openWithLock before the journal is opened.
		return writeFormatConfig(opts.Dir, cfg)
	}
	return SaveFormatConfig(opts.Dir, cfg)
}

func formatValueLogCompressionMode(mode ValueLogCompressionMode) string {
	switch mode {
	case ValueLogCompressionOff:
		return "off"
	case ValueLogCompressionBlock:
		return "block"
	case ValueLogCompressionDict:
		return "dict"
	case ValueLogCompressionAuto:
		return "auto"
	default:
		return fmt.Sprintf("mode_%d", mode)
	}
}

func parseValueLogCompressionMode(raw string) (ValueLogCompressionMode, bool) {
	switch normalizeFormatConfigMode(raw) {
	case "off":
		return ValueLogCompressionOff, true
	case "block":
		return ValueLogCompressionBlock, true
	case "dict":
		return ValueLogCompressionDict, true
	case "auto":
		return ValueLogCompressionAuto, true
	default:
		return 0, false
	}
}

func formatValueLogBlockCodec(codec ValueLogBlockCodec) string {
	switch codec {
	case ValueLogBlockSnappy:
		return "snappy"
	case ValueLogBlockLZ4:
		return "lz4"
	default:
		return fmt.Sprintf("codec_%d", codec)
	}
}

func parseValueLogBlockCodec(raw string) (ValueLogBlockCodec, bool) {
	switch normalizeFormatConfigMode(raw) {
	case "snappy":
		return ValueLogBlockSnappy, true
	case "lz4":
		return ValueLogBlockLZ4, true
	default:
		return 0, false
	}
}

func formatValueLogAutoPolicy(policy ValueLogAutoPolicy) string {
	switch policy {
	case ValueLogAutoBalanced:
		return "balanced"
	case ValueLogAutoThroughput:
		return "throughput"
	case ValueLogAutoSize:
		return "size"
	default:
		return fmt.Sprintf("policy_%d", policy)
	}
}

func parseValueLogAutoPolicy(raw string) (ValueLogAutoPolicy, bool) {
	switch normalizeFormatConfigMode(raw) {
	case "balanced":
		return ValueLogAutoBalanced, true
	case "throughput":
		return ValueLogAutoThroughput, true
	case "size":
		return ValueLogAutoSize, true
	default:
		return 0, false
	}
}

func applyFormatConfigForMaintenance(opts *Options) error {
	if opts == nil {
		return nil
	}
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := CommandWALRequiredFeatureEnabled(opts.Dir)
		if err != nil {
			return err
		}
		if requiresCommandWAL {
			return ErrCommandWALUnsupported
		}
		return nil
	}
	cfg, ok, err := LoadFormatConfig(opts.Dir)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if cfg.RequiresCommandWALV1() {
		return ErrCommandWALUnsupported
	}
	cfg.ApplyToOptions(opts)
	return nil
}

func validateRequiredFormatFeatures(features []string) error {
	seen := make(map[string]struct{}, len(features))
	for _, raw := range features {
		feature := normalizeFormatConfigMode(raw)
		if feature == "" {
			return fmt.Errorf("%w: empty feature", ErrUnsupportedRequiredFeature)
		}
		if _, dup := seen[feature]; dup {
			continue
		}
		seen[feature] = struct{}{}
		switch feature {
		case RequiredFeatureCommandWALV1:
			// Known by this binary, but DB open still fails closed until the
			// execution/recovery path is enabled by later command-WAL PRs.
		default:
			return fmt.Errorf("%w: %s", ErrUnsupportedRequiredFeature, raw)
		}
	}
	return nil
}
