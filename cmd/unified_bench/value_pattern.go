package main

import (
	"fmt"
	"strings"
)

const defaultWriteValuePoolSize = 2048

func makeWriteValuePool(seed int64, pattern string, size int, poolSize int) ([][]byte, error) {
	mode, err := normalizeWriteValuePattern(pattern)
	if err != nil {
		return nil, err
	}
	resolvedPool := poolSize
	if resolvedPool <= 0 {
		if mode == "zero" {
			resolvedPool = 1
		} else {
			resolvedPool = defaultWriteValuePoolSize
		}
	}
	// Keep "zero" allocations minimal even if the caller asks for a larger pool.
	if mode == "zero" {
		resolvedPool = 1
	}
	return makeValuePool(seed, mode, size, resolvedPool), nil
}

func normalizeWriteValuePattern(pattern string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(pattern))
	switch mode {
	case "", "zero", "zeros":
		return "zero", nil
	case "repeat", "repeat_tail64", "highly_compressible", "highly_compressible_tail64":
		return "repeat", nil
	case "half_repeat_half_random":
		return "half_repeat_half_random", nil
	case "random", "rand", "incompressible":
		return "incompressible", nil
	case "ultra_compressible", "ultra_compressible_repeat":
		return "ultra_compressible_repeat", nil
	case "highly_compressible_notail":
		return "highly_compressible_notail", nil
	case "medium_compressible", "medium_compressible_sparse":
		return "medium_compressible_sparse", nil
	case "celestia_height_prefix_fill":
		return "celestia_height_prefix_fill", nil
	default:
		return "", fmt.Errorf("unsupported -val-pattern=%q (expected zero|repeat|repeat_tail64|ultra_compressible_repeat|highly_compressible_notail|half_repeat_half_random|medium_compressible_sparse|celestia_height_prefix_fill|random)", pattern)
	}
}
