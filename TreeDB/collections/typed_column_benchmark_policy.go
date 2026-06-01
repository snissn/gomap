package collections

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const (
	typedColumnBenchmarkCompressionEnv         = "TREEDB_COLUMN_STORE_TYPED_COMPRESSION"
	typedColumnBenchmarkInt64EncodingEnv       = "TREEDB_COLUMN_STORE_TYPED_INT64_ENCODING"
	typedColumnBenchmarkRowsPerGranuleEnv      = "TREEDB_COLUMN_STORE_TYPED_ROWS_PER_GRANULE"
	typedColumnBenchmarkAdaptiveEnabledEnv     = "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_ENABLED"
	typedColumnBenchmarkAdaptiveTargetBytesEnv = "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_TARGET_BYTES"
	typedColumnBenchmarkAdaptiveMinRowsEnv     = "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_MIN_ROWS"
	typedColumnBenchmarkAdaptiveMaxRowsEnv     = "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_MAX_ROWS"
)

func typedColumnPublicationAdapterOptionsFromConfig(cfg ColumnStoreConfig, partID uint64, fields []TypedStorageField, sortKey []ColumnSortKey) (typedColumnAdapterOptions, error) {
	opts := typedColumnAdapterOptions{
		Collection:    "",
		Namespace:     cfg.AssetManager.Namespace,
		SchemaVersion: uint32(cfg.SchemaHash),
		PartID:        partID,
		Fields:        fields,
		SortKey:       sortKey,
	}
	if err := applyTypedColumnBenchmarkPolicyFromEnv(cfg, &opts); err != nil {
		return typedColumnAdapterOptions{}, err
	}
	return opts, nil
}

func applyTypedColumnBenchmarkPolicyFromEnv(cfg ColumnStoreConfig, opts *typedColumnAdapterOptions) error {
	if opts == nil {
		return nil
	}
	active := false
	if raw, ok := os.LookupEnv(typedColumnBenchmarkCompressionEnv); ok {
		compression, set, err := parseTypedColumnBenchmarkCompression(raw)
		if err != nil {
			return err
		}
		if set {
			opts.DefaultCompression = compression
			opts.DefaultCompressionSet = true
			active = true
		}
	}
	if raw, ok := os.LookupEnv(typedColumnBenchmarkInt64EncodingEnv); ok {
		encoding, set, err := parseTypedColumnBenchmarkInt64Encoding(raw)
		if err != nil {
			return err
		}
		if set {
			opts.Int64Encoding = encoding
			opts.Int64EncodingSet = true
			active = true
		}
	}
	if raw, ok := os.LookupEnv(typedColumnBenchmarkRowsPerGranuleEnv); ok && strings.TrimSpace(raw) != "" {
		rows, err := parsePositiveTypedColumnBenchmarkInt(typedColumnBenchmarkRowsPerGranuleEnv, raw)
		if err != nil {
			return err
		}
		opts.RowsPerGranule = rows
		active = true
	}
	adaptive, adaptiveSet, err := parseTypedColumnBenchmarkAdaptiveSizingFromEnv()
	if err != nil {
		return err
	}
	if adaptiveSet {
		opts.AdaptiveMarkSizing = adaptive
		active = true
	}
	if active && cfg.ProfileSupport != ColumnStoreProfileBenchmarkRelaxed {
		return fmt.Errorf("collections: typed-column benchmark codec/granule policy requires column_store profile_support=%q (got %q); policy is controlled by internal benchmark env vars, not public config", ColumnStoreProfileBenchmarkRelaxed, cfg.ProfileSupport)
	}
	return nil
}

func parseTypedColumnBenchmarkCompression(raw string) (typedcolumn.Compression, bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default":
		return typedcolumn.CompressionNone, false, nil
	case "none", "off", "compression_off":
		return typedcolumn.CompressionNone, true, nil
	case "snappy":
		return typedcolumn.CompressionSnappy, true, nil
	case "lz4":
		return typedcolumn.CompressionLZ4, true, nil
	case "zstd":
		return 0, false, fmt.Errorf("%w: unsupported compression zstd (production zstd encode/decode is deferred)", errTypedColumnProductionLayoutUnsupported)
	case "zstd_dict", "zstd-dict":
		return 0, false, fmt.Errorf("%w: unsupported compression zstd_dict (production zstd dictionary encode/decode is deferred)", errTypedColumnProductionLayoutUnsupported)
	default:
		return 0, false, fmt.Errorf("%w: unknown compression %q", errTypedColumnProductionLayoutUnsupported, raw)
	}
}

func parseTypedColumnBenchmarkInt64Encoding(raw string) (typedcolumn.Encoding, bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default":
		return 0, false, nil
	case "raw", "raw_int64":
		return typedcolumn.EncodingRawInt64, true, nil
	case "delta", "delta_varint":
		return typedcolumn.EncodingDeltaVarint, true, nil
	case "double_delta", "double-delta", "double_delta_varint":
		return typedcolumn.EncodingDoubleDeltaVarint, true, nil
	default:
		return 0, false, fmt.Errorf("%w: unsupported int64 encoding override %q", errTypedColumnProductionLayoutUnsupported, raw)
	}
}

func parseTypedColumnBenchmarkAdaptiveSizingFromEnv() (typedcolumn.ColumnAdaptiveMarkSizing, bool, error) {
	cfg := typedcolumn.ColumnAdaptiveMarkSizing{}
	set := false
	if raw, ok := os.LookupEnv(typedColumnBenchmarkAdaptiveEnabledEnv); ok {
		enabled, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return cfg, false, fmt.Errorf("collections: invalid %s=%q: %w", typedColumnBenchmarkAdaptiveEnabledEnv, raw, err)
		}
		cfg.Enabled = enabled
		set = true
	}
	if raw, ok := os.LookupEnv(typedColumnBenchmarkAdaptiveTargetBytesEnv); ok && strings.TrimSpace(raw) != "" {
		value, err := parsePositiveTypedColumnBenchmarkInt(typedColumnBenchmarkAdaptiveTargetBytesEnv, raw)
		if err != nil {
			return cfg, false, err
		}
		cfg.TargetBytes = value
		cfg.Enabled = true
		set = true
	}
	if raw, ok := os.LookupEnv(typedColumnBenchmarkAdaptiveMinRowsEnv); ok && strings.TrimSpace(raw) != "" {
		value, err := parsePositiveTypedColumnBenchmarkInt(typedColumnBenchmarkAdaptiveMinRowsEnv, raw)
		if err != nil {
			return cfg, false, err
		}
		cfg.MinRows = value
		cfg.Enabled = true
		set = true
	}
	if raw, ok := os.LookupEnv(typedColumnBenchmarkAdaptiveMaxRowsEnv); ok && strings.TrimSpace(raw) != "" {
		value, err := parsePositiveTypedColumnBenchmarkInt(typedColumnBenchmarkAdaptiveMaxRowsEnv, raw)
		if err != nil {
			return cfg, false, err
		}
		cfg.MaxRows = value
		cfg.Enabled = true
		set = true
	}
	if !set || !cfg.Enabled {
		return cfg, set, nil
	}
	normalized, err := typedcolumn.NormalizeColumnAdaptiveMarkSizing(cfg, 0)
	if err != nil {
		return cfg, false, err
	}
	return normalized, true, nil
}

func parsePositiveTypedColumnBenchmarkInt(name, raw string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("collections: invalid %s=%q: %w", name, raw, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("collections: invalid %s=%d: must be positive", name, value)
	}
	return value, nil
}
