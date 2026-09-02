package db

import (
	"fmt"
	"strconv"
)

// ValidateFragmentationReport validates basic invariants on a FragmentationReport
// output map. It is intended for tests and operational "health" tooling.
func ValidateFragmentationReport(rep map[string]string) error {
	getU64 := func(key string) (uint64, bool, error) {
		s, ok := rep[key]
		if !ok || s == "" {
			return 0, false, nil
		}
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, true, fmt.Errorf("%s: parse uint: %w", key, err)
		}
		return v, true, nil
	}

	requireU64 := func(key string) (uint64, error) {
		v, ok, err := getU64(key)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%s: missing", key)
		}
		return v, nil
	}

	totalPages, err := requireU64("treedb.pages.total")
	if err != nil {
		return err
	}
	userPages, err := requireU64("treedb.user.pages")
	if err != nil {
		return err
	}
	leafPages, err := requireU64("treedb.user.pages.leaf")
	if err != nil {
		return err
	}
	internalPages, err := requireU64("treedb.user.pages.internal")
	if err != nil {
		return err
	}
	if leafPages+internalPages != userPages {
		return fmt.Errorf("treedb.user.pages: expected leaf+internal == total, got leaf=%d internal=%d total=%d", leafPages, internalPages, userPages)
	}
	if userPages > totalPages {
		return fmt.Errorf("treedb.user.pages: exceeds treedb.pages.total (user=%d total=%d)", userPages, totalPages)
	}

	if userPages > 0 {
		minID, err := requireU64("treedb.user.pages.min")
		if err != nil {
			return err
		}
		maxID, err := requireU64("treedb.user.pages.max")
		if err != nil {
			return err
		}
		span, err := requireU64("treedb.user.pages.span")
		if err != nil {
			return err
		}
		spanRatioPPM, err := requireU64("treedb.user.pages.span_ratio_ppm")
		if err != nil {
			return err
		}
		if minID > maxID {
			return fmt.Errorf("treedb.user.pages.min/max: min=%d > max=%d", minID, maxID)
		}
		if span != (maxID-minID)+1 {
			return fmt.Errorf("treedb.user.pages.span: expected %d, got %d", (maxID-minID)+1, span)
		}
		if span < userPages {
			return fmt.Errorf("treedb.user.pages.span: span < pages (span=%d pages=%d)", span, userPages)
		}
		wantRatio := (span * 1_000_000) / userPages
		if spanRatioPPM != wantRatio {
			return fmt.Errorf("treedb.user.pages.span_ratio_ppm: expected %d, got %d", wantRatio, spanRatioPPM)
		}
	}

	checkFill := func(prefix string, pages uint64) error {
		if pages == 0 {
			return nil
		}

		avgKey := fmt.Sprintf("treedb.user.%s_fill_ppm_avg", prefix)
		avg, ok, err := getU64(avgKey)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%s: missing", avgKey)
		}
		if avg > 1_000_000 {
			return fmt.Errorf("%s: out of range: %d", avgKey, avg)
		}

		keys := []string{
			fmt.Sprintf("treedb.user.%s_fill_ppm_min", prefix),
			fmt.Sprintf("treedb.user.%s_fill_ppm_p10", prefix),
			fmt.Sprintf("treedb.user.%s_fill_ppm_p50", prefix),
			fmt.Sprintf("treedb.user.%s_fill_ppm_p90", prefix),
			fmt.Sprintf("treedb.user.%s_fill_ppm_p99", prefix),
			fmt.Sprintf("treedb.user.%s_fill_ppm_max", prefix),
		}
		vals := make([]uint64, 0, len(keys))
		for _, k := range keys {
			v, err := requireU64(k)
			if err != nil {
				return err
			}
			if v > 1_000_000 {
				return fmt.Errorf("%s: out of range: %d", k, v)
			}
			vals = append(vals, v)
		}
		for i := 1; i < len(vals); i++ {
			if vals[i-1] > vals[i] {
				return fmt.Errorf("treedb.user.%s_fill_ppm: expected monotonic percentiles, got %d > %d", prefix, vals[i-1], vals[i])
			}
		}
		return nil
	}

	if err := checkFill("leaf", leafPages); err != nil {
		return err
	}
	if err := checkFill("internal", internalPages); err != nil {
		return err
	}

	freelistHead, err := requireU64("treedb.freelist.head")
	if err != nil {
		return err
	}
	if freelistHead != 0 && totalPages > 0 {
		if _, ok := rep["treedb.freelist.error"]; ok {
			return fmt.Errorf("treedb.freelist.error: %s", rep["treedb.freelist.error"])
		}
		flPages, err := requireU64("treedb.freelist.pages")
		if err != nil {
			return err
		}
		flFreeIDs, err := requireU64("treedb.freelist.free_ids")
		if err != nil {
			return err
		}
		reclaimable, err := requireU64("treedb.freelist.reclaimable_pages")
		if err != nil {
			return err
		}
		if reclaimable != flPages+flFreeIDs {
			return fmt.Errorf("treedb.freelist.reclaimable_pages: expected %d, got %d", flPages+flFreeIDs, reclaimable)
		}
		ratio, err := requireU64("treedb.freelist.reclaimable_ratio_ppm")
		if err != nil {
			return err
		}
		wantRatio := (reclaimable * 1_000_000) / totalPages
		if ratio != wantRatio {
			return fmt.Errorf("treedb.freelist.reclaimable_ratio_ppm: expected %d, got %d", wantRatio, ratio)
		}
		if ratio > 1_000_000 {
			return fmt.Errorf("treedb.freelist.reclaimable_ratio_ppm: out of range: %d", ratio)
		}
	}

	return nil
}
