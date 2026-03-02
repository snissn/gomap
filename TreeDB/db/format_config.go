package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const formatConfigFileName = "format.json"

const formatConfigVersion = 1

// FormatConfig captures the format-affecting knobs that maintenance tooling
// should preserve when rewriting index/value-log state.
//
// This file is best-effort and pre-alpha; callers should tolerate it being
// absent and may provide explicit overrides via Options.
type FormatConfig struct {
	Version int `json:"version"`

	IndexOuterLeafMode         string `json:"index_outer_leaf_mode"`
	IndexOuterLeavesInValueLog bool   `json:"index_outer_leaves_in_vlog"`

	LeafPrefixCompression     bool `json:"leaf_prefix_compression"`
	IndexColumnarLeaves       bool `json:"index_columnar_leaves"`
	IndexPackedValuePtr       bool `json:"index_packed_valueptr"`
	IndexInternalBaseDelta    bool `json:"index_internal_base_delta"`
	IndexAdaptiveLeafEncoding bool `json:"index_adaptive_leaf_encoding"`

	ValueLogCompression string `json:"vlog_compression"`
	ValueLogBlockCodec  string `json:"vlog_block_codec"`
	ValueLogAutoPolicy  string `json:"vlog_auto_policy"`

	ValueLogOuterLeafBlockTargetBytes     int    `json:"vlog_outer_leaf_block_target_bytes"`
	ValueLogOuterLeafBlockCodec           string `json:"vlog_outer_leaf_block_codec"`
	ValueLogOuterLeafBlockRestartInterval int    `json:"vlog_outer_leaf_block_restart_interval"`
	ValueLogOuterLeafBlobThresholdBytes   int    `json:"vlog_outer_leaf_blob_threshold_bytes"`

	ValueLogWALFenceMode string `json:"vlog_wal_fence_mode"`
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

		IndexOuterLeafMode:         normalizeIndexOuterLeafMode(opts.IndexOuterLeafMode),
		IndexOuterLeavesInValueLog: opts.IndexOuterLeavesInValueLog,

		LeafPrefixCompression:     opts.LeafPrefixCompression,
		IndexColumnarLeaves:       opts.IndexColumnarLeaves,
		IndexPackedValuePtr:       opts.IndexPackedValuePtr,
		IndexInternalBaseDelta:    opts.IndexInternalBaseDelta,
		IndexAdaptiveLeafEncoding: opts.IndexAdaptiveLeafEncoding,

		ValueLogCompression: normalizeFormatConfigMode(formatValueLogCompressionMode(opts.ValueLog.Compression)),
		ValueLogBlockCodec:  normalizeFormatConfigMode(formatValueLogBlockCodec(opts.ValueLog.BlockCodec)),
		ValueLogAutoPolicy:  normalizeFormatConfigMode(formatValueLogAutoPolicy(opts.ValueLog.AutoPolicy)),

		ValueLogOuterLeafBlockTargetBytes:     opts.ValueLog.OuterLeafBlockTargetBytes,
		ValueLogOuterLeafBlockCodec:           normalizeFormatConfigMode(formatValueLogBlockCodec(opts.ValueLog.OuterLeafBlockCodec)),
		ValueLogOuterLeafBlockRestartInterval: opts.ValueLog.OuterLeafBlockRestartInterval,
		ValueLogOuterLeafBlobThresholdBytes:   opts.ValueLog.OuterLeafBlobThresholdBytes,

		ValueLogWALFenceMode: normalizeFormatConfigMode(string(normalizeValueLogWALFenceMode(opts.ValueLog.WALFenceMode))),
	}

	// Leaf refs encode value-log pointers in internal child IDs, which are
	// incompatible with internal base-delta encodings. Preserve the effective
	// behavior by forcing this false in persisted config.
	if cfg.IndexOuterLeavesInValueLog && cfg.IndexInternalBaseDelta {
		cfg.IndexInternalBaseDelta = false
	}

	return cfg
}

// ApplyToOptions overwrites format-affecting knobs in opts from cfg.
//
// Callers should treat cfg as best-effort (it may be absent or partially
// populated) and may apply explicit overrides after this call.
func (cfg FormatConfig) ApplyToOptions(opts *Options) {
	if opts == nil {
		return
	}
	opts.IndexOuterLeafMode = normalizeIndexOuterLeafMode(cfg.IndexOuterLeafMode)
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

	opts.ValueLog.OuterLeafBlockTargetBytes = cfg.ValueLogOuterLeafBlockTargetBytes
	if c, ok := parseValueLogBlockCodec(cfg.ValueLogOuterLeafBlockCodec); ok {
		opts.ValueLog.OuterLeafBlockCodec = c
	}
	opts.ValueLog.OuterLeafBlockRestartInterval = cfg.ValueLogOuterLeafBlockRestartInterval
	opts.ValueLog.OuterLeafBlobThresholdBytes = cfg.ValueLogOuterLeafBlobThresholdBytes
	if cfg.ValueLogWALFenceMode != "" {
		opts.ValueLog.WALFenceMode = ValueLogWALFenceMode(cfg.ValueLogWALFenceMode)
	}
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
	if cfg.Version != 0 && cfg.Version != formatConfigVersion {
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
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
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
