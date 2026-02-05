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

func treeDBVlogDictOnVariantName(level string, enableEntropy bool, pipeline bool) (string, error) {
	switch level {
	case "fastest":
		if enableEntropy {
			if pipeline {
				return "treedb_vlog_dict_on_entropy_pipeline", nil
			}
			return "treedb_vlog_dict_on_entropy", nil
		}
		if pipeline {
			return "treedb_vlog_dict_on_pipeline", nil
		}
		return "treedb_vlog_dict_on", nil
	case "default", "better", "best":
		name := "treedb_vlog_dict_on_level_" + level
		if enableEntropy {
			name += "_entropy"
		}
		if pipeline {
			name += "_pipeline"
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
	pipeMode, err := parseBenchVariantMode("treedb-vlog-dict-frame-pipeline", *treedbVlogDictFramePipelineMode)
	if err != nil {
		return nil, err
	}
	var pipelines []bool
	switch pipeMode {
	case benchVariantDefault:
		pipelines = []bool{*treedbVlogDictFramePipelineW > 1}
	case benchVariantOn:
		pipelines = []bool{true}
	case benchVariantOff:
		pipelines = []bool{false}
	case benchVariantBoth:
		pipelines = []bool{false, true}
	}

	out := make([]string, 0, len(levels)*len(entropies)*len(pipelines))
	for _, level := range levels {
		for _, enableEntropy := range entropies {
			for _, pipeline := range pipelines {
				name, err := treeDBVlogDictOnVariantName(level, enableEntropy, pipeline)
				if err != nil {
					return nil, err
				}
				out = append(out, name)
			}
		}
	}
	return out, nil
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
