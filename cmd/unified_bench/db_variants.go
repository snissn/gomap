package main

import (
	"fmt"
	"strconv"
	"strings"
)

type benchVariantMode uint8

const (
	benchVariantDefault benchVariantMode = iota
	benchVariantOn
	benchVariantOff
	benchVariantBoth
)

func parseBenchVariantMode(flagName, s string) (benchVariantMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default", "auto", "engine", "unset":
		return benchVariantDefault, nil
	case "on", "true", "1", "yes", "enable", "enabled":
		return benchVariantOn, nil
	case "off", "false", "0", "no", "disable", "disabled":
		return benchVariantOff, nil
	case "both", "matrix":
		return benchVariantBoth, nil
	default:
		return benchVariantDefault, fmt.Errorf("unsupported -%s=%q (expected default|on|off|both)", flagName, s)
	}
}

type treeDBVlogCompressionVariant uint8

const (
	treeDBVlogCompressionVariantDefault treeDBVlogCompressionVariant = iota
	treeDBVlogCompressionVariantOff
	treeDBVlogCompressionVariantDict
	treeDBVlogCompressionVariantBlockSnappy
	treeDBVlogCompressionVariantBlockLZ4
	treeDBVlogCompressionVariantBlockZSTD
	treeDBVlogCompressionVariantAuto
	treeDBVlogCompressionVariantAll
)

func parseTreeDBVlogCompressionVariant(flagName, s string) (treeDBVlogCompressionVariant, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default", "unset", "engine":
		return treeDBVlogCompressionVariantDefault, nil
	case "off":
		return treeDBVlogCompressionVariantOff, nil
	case "dict":
		return treeDBVlogCompressionVariantDict, nil
	case "block_snappy", "block-snappy", "snappy":
		return treeDBVlogCompressionVariantBlockSnappy, nil
	case "block_lz4", "block-lz4", "lz4":
		return treeDBVlogCompressionVariantBlockLZ4, nil
	case "block_zstd", "block-zstd", "zstd":
		return treeDBVlogCompressionVariantBlockZSTD, nil
	case "auto":
		return treeDBVlogCompressionVariantAuto, nil
	case "all", "both", "matrix":
		return treeDBVlogCompressionVariantAll, nil
	default:
		return treeDBVlogCompressionVariantDefault, fmt.Errorf("unsupported -%s=%q (expected default|off|dict|block_snappy|block_lz4|block_zstd|auto|all)", flagName, s)
	}
}

func treeDBVlogCompressionVariantNames(v treeDBVlogCompressionVariant) []string {
	switch v {
	case treeDBVlogCompressionVariantOff:
		return []string{"treedb_vlog_off"}
	case treeDBVlogCompressionVariantDict:
		return []string{"treedb_vlog_dict"}
	case treeDBVlogCompressionVariantBlockSnappy:
		return []string{"treedb_vlog_block_snappy"}
	case treeDBVlogCompressionVariantBlockLZ4:
		return []string{"treedb_vlog_block_lz4"}
	case treeDBVlogCompressionVariantBlockZSTD:
		return []string{"treedb_vlog_block_zstd"}
	case treeDBVlogCompressionVariantAuto:
		return []string{"treedb_vlog_auto"}
	case treeDBVlogCompressionVariantAll:
		return []string{
			"treedb_vlog_off",
			"treedb_vlog_dict",
			"treedb_vlog_block_snappy",
			"treedb_vlog_block_lz4",
			"treedb_vlog_block_zstd",
			"treedb_vlog_auto",
		}
	default:
		return nil
	}
}

func parseTreeDBVlogDictFrameEncodeLevels(flagName, s string) (engine bool, levels []string, err error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "", "engine", "auto", "unset":
		return true, nil, nil
	case "fastest", "default", "better", "best":
		return false, []string{s}, nil
	case "all", "matrix":
		return false, []string{"fastest", "default", "better", "best"}, nil
	default:
		n, convErr := strconv.Atoi(s)
		if convErr != nil {
			return false, nil, fmt.Errorf("unsupported -%s=%q (expected engine|fastest|default|better|best|all|<int>)", flagName, s)
		}
		if n <= 0 {
			return true, nil, nil
		}
		switch {
		case n < 3:
			return false, []string{"fastest"}, nil
		case n >= 3 && n < 6:
			return false, []string{"default"}, nil
		case n >= 6 && n < 10:
			return false, []string{"better"}, nil
		default:
			return false, []string{"best"}, nil
		}
	}
}

func parseTreeDBVlogDictFrameEntropyMode(flagName, s string) (engine bool, entropies []bool, err error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "", "engine", "auto", "unset":
		return true, nil, nil
	case "on", "true", "1", "yes", "enable", "enabled":
		return false, []bool{true}, nil
	case "off", "false", "0", "no", "disable", "disabled":
		return false, []bool{false}, nil
	case "both", "matrix":
		return false, []bool{false, true}, nil
	default:
		return false, nil, fmt.Errorf("unsupported -%s=%q (expected engine|on|off|both)", flagName, s)
	}
}

func treeDBVlogDictOnVariantName(level string, enableEntropy bool) (string, error) {
	switch level {
	case "fastest":
		if enableEntropy {
			return "treedb_vlog_dict_on_entropy", nil
		}
		return "treedb_vlog_dict_on", nil
	case "default", "better", "best":
		name := "treedb_vlog_dict_on_level_" + level
		if enableEntropy {
			name += "_entropy"
		}
		return name, nil
	default:
		return "", fmt.Errorf("unsupported TreeDB dict frame encode level: %q", level)
	}
}

func treeDBVlogDictOnVariantNames() ([]string, error) {
	levelEngine, levels, err := parseTreeDBVlogDictFrameEncodeLevels("treedb-vlog-dict-frame-encode-level", *treedbVlogDictFrameEncodeLevel)
	if err != nil {
		return nil, err
	}
	entropyEngine, entropies, err := parseTreeDBVlogDictFrameEntropyMode("treedb-vlog-dict-frame-entropy", *treedbVlogDictFrameEntropyMode)
	if err != nil {
		return nil, err
	}
	if levelEngine {
		levels = []string{"fastest"}
	}
	if entropyEngine {
		entropies = []bool{false}
	}
	if levelEngine && entropyEngine {
		return []string{"treedb_vlog_dict_on"}, nil
	}

	out := make([]string, 0, len(levels)*len(entropies))
	for _, level := range levels {
		for _, enableEntropy := range entropies {
			name, err := treeDBVlogDictOnVariantName(level, enableEntropy)
			if err != nil {
				return nil, err
			}
			out = append(out, name)
		}
	}
	return out, nil
}

func applyCompressionVariants(dbNames []string, excludeArg string) ([]string, error) {
	treedbCompressionVariantMode, err := parseTreeDBVlogCompressionVariant("treedb-vlog-compression-variant", *treedbVlogCompressionVariant)
	if err != nil {
		return nil, err
	}
	treedbMode, err := parseBenchVariantMode("treedb-vlog-dict", *treedbVlogDictMode)
	if err != nil {
		return nil, err
	}
	leveldbMode, err := parseBenchVariantMode("leveldb-block-compression", *leveldbBlockCompressionMode)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(dbNames)*2)
	for _, name := range dbNames {
		switch name {
		case "treedb":
			if treedbCompressionVariantMode != treeDBVlogCompressionVariantDefault {
				out = append(out, treeDBVlogCompressionVariantNames(treedbCompressionVariantMode)...)
				continue
			}
			onVariants, err := treeDBVlogDictOnVariantNames()
			if err != nil {
				return nil, err
			}
			switch treedbMode {
			case benchVariantDefault:
				out = append(out, name)
			case benchVariantOn:
				out = append(out, onVariants...)
			case benchVariantOff:
				out = append(out, "treedb_vlog_dict_off")
			case benchVariantBoth:
				out = append(out, "treedb_vlog_dict_off")
				out = append(out, onVariants...)
			}
		case "leveldb":
			switch leveldbMode {
			case benchVariantDefault:
				out = append(out, name)
			case benchVariantOn:
				out = append(out, "leveldb_block_comp_on")
			case benchVariantOff:
				out = append(out, "leveldb_block_comp_off")
			case benchVariantBoth:
				out = append(out, "leveldb_block_comp_off", "leveldb_block_comp_on")
			}
		default:
			out = append(out, name)
		}
	}

	// Apply exclude list again, since variant expansion happens after resolveDBs().
	excluded := parseList(excludeArg)
	if len(excluded) == 0 {
		return out, nil
	}
	filtered := out[:0]
	for _, name := range out {
		if contains(excluded, name) {
			continue
		}
		filtered = append(filtered, name)
	}
	return filtered, nil
}
