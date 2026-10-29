package caching

import (
	"math"
	"strconv"
	"testing"
)

func TestStatsMemoryCountersPreserveUnsignedDecimalAndOwnership(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Values beyond MaxInt64 expose accidental signed formatting. Peaks cannot
	// be replaced by the lower live process samples collected inside Stats.
	want := map[string]string{
		"treedb.process.memory.peak_rss_bytes":        "9223372036854775808",
		"treedb.process.memory.peak_heap_alloc_bytes": "18446744073709551615",
		"treedb.process.memory.peak_heap_inuse_bytes": "9223372036854775809",
		"treedb.process.memory.peak_total_sys_bytes":  "18446744073709551614",
	}
	db.processPeakRSSBytes.Store(uint64(1) << 63)
	db.processPeakHeapAllocBytes.Store(math.MaxUint64)
	db.processPeakHeapInuseBytes.Store((uint64(1) << 63) + 1)
	db.processPeakTotalSysBytes.Store(math.MaxUint64 - 1)
	first := db.Stats()
	for key, value := range want {
		if got := first[key]; got != value {
			t.Errorf("%s=%q, want %q", key, got, value)
		}
	}
	// RSS is sampled live, so require its unsigned decimal contract rather than
	// freezing a value or replacing the sampler. Include both directly formatted
	// live counters and all four peaks in the schema check.
	for _, key := range []string{"treedb.process.memory.rss_bytes", "treedb.process.memory.rss_hwm_bytes", "treedb.process.memory.peak_rss_bytes", "treedb.process.memory.peak_heap_alloc_bytes", "treedb.process.memory.peak_heap_inuse_bytes", "treedb.process.memory.peak_total_sys_bytes"} {
		got, ok := first[key]
		if !ok {
			t.Errorf("missing %s", key)
			continue
		}
		value, err := strconv.ParseUint(got, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != got {
			t.Errorf("%s not canonical unsigned decimal: %q (%v)", key, got, err)
		}
	}
	for key := range want {
		first[key] = "caller mutation"
	}
	second := db.Stats()
	for key, value := range want {
		if second[key] != value {
			t.Errorf("caller mutation escaped for %s: %q", key, second[key])
		}
	}
}
