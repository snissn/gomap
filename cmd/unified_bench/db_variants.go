package main

import (
	"fmt"
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
	case "", "default", "auto":
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

func applyCompressionVariants(dbNames []string, excludeArg string) ([]string, error) {
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
			switch treedbMode {
			case benchVariantDefault:
				out = append(out, name)
			case benchVariantOn:
				out = append(out, "treedb_vlog_dict_on")
			case benchVariantOff:
				out = append(out, "treedb_vlog_dict_off")
			case benchVariantBoth:
				out = append(out, "treedb_vlog_dict_off", "treedb_vlog_dict_on")
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
