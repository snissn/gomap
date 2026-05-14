package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const formatConfigFileName = "format.json"

const formatConfigVersion = 2
const formatConfigVersionRequiredFeatures = 3

const FormatFeatureCollectionWALV1 = "collection_wal_v1"

// FormatConfig captures the format-affecting knobs that maintenance tooling
// should preserve when rewriting index/value-log state.
//
// This file is best-effort and pre-alpha; callers should tolerate it being
// absent. Versioned files written by SaveFormatConfig are expected to be fully
// populated; if new fields are added in the future, the version should be
// bumped so older binaries do not accidentally apply zero-values.
type FormatConfig struct {
	Version int `json:"version"`

	IndexOuterLeavesInValueLog bool `json:"index_outer_leaves_in_vlog"`

	LeafPrefixCompression     bool `json:"leaf_prefix_compression"`
	IndexColumnarLeaves       bool `json:"index_columnar_leaves"`
	IndexPackedValuePtr       bool `json:"index_packed_valueptr"`
	IndexInternalBaseDelta    bool `json:"index_internal_base_delta"`
	IndexAdaptiveLeafEncoding bool `json:"index_adaptive_leaf_encoding"`

	ValueLogCompression string `json:"vlog_compression"`
	ValueLogBlockCodec  string `json:"vlog_block_codec"`
	ValueLogAutoPolicy  string `json:"vlog_auto_policy"`

	RequiredFeatures []string `json:"required_features,omitempty"`
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

	return cfg
}

func formatConfigFromDB(db *DB) FormatConfig {
	if db == nil {
		return FormatConfig{Version: formatConfigVersion}
	}
	cfg := FormatConfig{
		Version: formatConfigVersion,

		IndexOuterLeavesInValueLog: db.indexOuterLeavesInValueLog,

		LeafPrefixCompression:     db.leafPrefixCompression,
		IndexColumnarLeaves:       db.indexColumnarLeaves,
		IndexPackedValuePtr:       db.indexPackedValuePtr,
		IndexInternalBaseDelta:    db.indexInternalBaseDelta,
		IndexAdaptiveLeafEncoding: db.indexAdaptiveLeafEncoding,

		ValueLogCompression: normalizeFormatConfigMode(formatValueLogCompressionMode(db.valueLogCompression)),
		ValueLogBlockCodec:  normalizeFormatConfigMode(formatValueLogBlockCodec(db.valueLogBlockCodec)),
		ValueLogAutoPolicy:  normalizeFormatConfigMode(formatValueLogAutoPolicy(db.valueLogAutoPolicy)),
	}
	if cfg.IndexOuterLeavesInValueLog && cfg.IndexInternalBaseDelta {
		cfg.IndexInternalBaseDelta = false
	}
	return cfg
}

func (cfg FormatConfig) HasRequiredFeature(feature string) bool {
	for _, existing := range cfg.RequiredFeatures {
		if existing == feature {
			return true
		}
	}
	return false
}

func (cfg *FormatConfig) AddRequiredFeature(feature string) {
	if cfg == nil || feature == "" || cfg.HasRequiredFeature(feature) {
		return
	}
	cfg.RequiredFeatures = append(cfg.RequiredFeatures, feature)
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
			return FormatConfig{}, false, fmt.Errorf("treedb: %s version %d cannot declare required_features", filepath.Base(path), cfg.Version)
		}
	case formatConfigVersionRequiredFeatures:
		required, err := normalizeFormatRequiredFeatures(cfg.RequiredFeatures)
		if err != nil {
			return FormatConfig{}, false, fmt.Errorf("treedb: %s: %w", filepath.Base(path), err)
		}
		cfg.RequiredFeatures = required
	default:
		return FormatConfig{}, false, fmt.Errorf("treedb: unsupported %s version %d", filepath.Base(path), cfg.Version)
	}
	return cfg, true, nil
}

// SaveFormatConfig writes cfg to dir/format.json atomically.
func SaveFormatConfig(dir string, cfg FormatConfig) error {
	path := formatConfigPath(dir)
	if path == "" {
		return errors.New("missing db dir")
	}
	if cfg.Version == 0 {
		cfg.Version = formatConfigVersion
	}
	required, err := normalizeFormatRequiredFeatures(cfg.RequiredFeatures)
	if err != nil {
		return err
	}
	cfg.RequiredFeatures = required
	if len(cfg.RequiredFeatures) != 0 && cfg.Version < formatConfigVersionRequiredFeatures {
		cfg.Version = formatConfigVersionRequiredFeatures
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
}

func normalizeFormatRequiredFeatures(features []string) ([]string, error) {
	if len(features) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(features))
	out := make([]string, 0, len(features))
	for _, feature := range features {
		feature = normalizeFormatConfigMode(feature)
		if feature == "" {
			return nil, errors.New("empty required format feature")
		}
		if !supportedFormatRequiredFeature(feature) {
			return nil, fmt.Errorf("unsupported required format feature %q", feature)
		}
		if _, ok := seen[feature]; ok {
			continue
		}
		seen[feature] = struct{}{}
		out = append(out, feature)
	}
	sort.Strings(out)
	return out, nil
}

func supportedFormatRequiredFeature(feature string) bool {
	switch feature {
	case FormatFeatureCollectionWALV1:
		return true
	default:
		return false
	}
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
		_, _, err := LoadFormatConfig(opts.Dir)
		return err
	}
	cfg, ok, err := LoadFormatConfig(opts.Dir)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	cfg.ApplyToOptions(opts)
	return nil
}
