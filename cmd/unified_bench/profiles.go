package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
)

var (
	profileArg = flag.String("profile", "", "Cross-DB benchmark preset to use (balanced, durable, fast, wal_on_fast). Not a TreeDB server profile. Overrides default flags unless explicitly set.")
)

type Profile struct {
	Description string
	Apply       func(isSet map[string]bool)
}

var profiles map[string]Profile

func init() {
	applyFast := func(isSet map[string]bool) {
		// TreeDB
		applyTreeDBProfileIfUnset(treedb.ProfileBenchUnsafe, isSet)
		setBoolIfUnset("treedb-allow-unsafe", true, isSet, treedbAllowUnsafe)

		// Badger
		setBoolIfUnset("badger-nosync", true, isSet, badgerNoSync)

		// Bbolt
		setBoolIfUnset("bbolt-nosync", true, isSet, bboltNoSync)

		// LMDB
		setBoolIfUnset("lmdb-nosync", true, isSet, lmdbNoSync)
		setBoolIfUnset("lmdb-nometasync", true, isSet, lmdbNoMetaSync)

		// Pebble
		setBoolIfUnset("pebble-nosync", true, isSet, pebbleNoSync)

		// Pogreb
		setBoolIfUnset("pogreb-nosync", true, isSet, pogrebNoSync)

		// BuntDB (sync policy 0 = Never)
		setIntIfUnset("buntdb-sync", 0, isSet, buntdbSyncPolicy)
	}

	applyWALOnFast := func(isSet map[string]bool) {
		// TreeDB: keep WAL enabled and relax ordinary ACK durability. Read
		// integrity remains verified under the canonical production contract.
		applyTreeDBProfileIfUnset(treedb.ProfileCommandWALRelaxed, isSet)
		setBoolIfUnset("treedb-allow-unsafe", true, isSet, treedbAllowUnsafe)

		// Other DBs: match "fast" behavior (nosync).
		setBoolIfUnset("badger-nosync", true, isSet, badgerNoSync)
		setBoolIfUnset("bbolt-nosync", true, isSet, bboltNoSync)
		setBoolIfUnset("lmdb-nosync", true, isSet, lmdbNoSync)
		setBoolIfUnset("lmdb-nometasync", true, isSet, lmdbNoMetaSync)
		setBoolIfUnset("pebble-nosync", true, isSet, pebbleNoSync)
		setBoolIfUnset("pogreb-nosync", true, isSet, pogrebNoSync)
		setIntIfUnset("buntdb-sync", 0, isSet, buntdbSyncPolicy)
	}

	profiles = map[string]Profile{
		"fast": {
			Description: "Cross-DB no-sync throughput preset. For TreeDB this selects the explicit bench_unsafe boundary with no WAL, relaxed read integrity, and Celestia-aligned auto/snappy/balanced value-log compression. UNSAFE for production data.",
			Apply:       applyFast,
		},
		"wal_on_fast": {
			Description: "Cross-DB relaxed-WAL preset. For TreeDB this selects command_wal_relaxed with verified read integrity and Celestia-aligned auto/snappy/balanced value-log compression.",
			Apply:       applyWALOnFast,
		},
		"unsafe": { // Alias for fast
			Description: "Alias for 'fast'",
			Apply:       applyFast,
		},
		"durable": {
			Description: "Strict durability: WAL/sync enabled (defaults). Ideal for correctness verification.",
			Apply: func(isSet map[string]bool) {
				// TreeDB
				setBoolIfUnset("treedb-disable-wal", false, isSet, treedbDisableWAL)
				setBoolIfUnset("treedb-relaxed-sync", false, isSet, treedbRelaxedSync)
				setBoolIfUnset("treedb-disable-read-checksum", false, isSet, treedbDisableReadChecksum)

				// Badger
				setBoolIfUnset("badger-nosync", false, isSet, badgerNoSync)

				// Bbolt
				setBoolIfUnset("bbolt-nosync", false, isSet, bboltNoSync)

				// LMDB
				setBoolIfUnset("lmdb-nosync", false, isSet, lmdbNoSync)
				setBoolIfUnset("lmdb-nometasync", false, isSet, lmdbNoMetaSync)

				// Pebble
				setBoolIfUnset("pebble-nosync", false, isSet, pebbleNoSync)

				// Pogreb
				setBoolIfUnset("pogreb-nosync", false, isSet, pogrebNoSync)

				// BuntDB (sync policy 2 = Always)
				setIntIfUnset("buntdb-sync", 2, isSet, buntdbSyncPolicy)
			},
		},
		"balanced": {
			Description: "Balanced performance and safety (default).",
			Apply: func(isSet map[string]bool) {
				// No-op, just explicitly allows the name
			},
		},
	}
}

func applyTreeDBProfileIfUnset(profile treedb.Profile, isSet map[string]bool) {
	resolved, ok := treedb.NormalizeProfile(profile)
	if !ok {
		panic(fmt.Sprintf("unified-bench: unsupported TreeDB profile %q", profile))
	}
	var opts treedb.Options
	if resolved == treedb.ProfileBenchUnsafe {
		opts = treedb.OptionsForBenchmark(profile, "")
	} else {
		opts = treedb.OptionsFor(profile, "")
	}

	switch opts.Durability {
	case treedb.DurabilityWALOffRelaxed:
		setBoolIfUnset("treedb-disable-wal", true, isSet, treedbDisableWAL)
		setBoolIfUnset("treedb-relaxed-sync", true, isSet, treedbRelaxedSync)
	case treedb.DurabilityWALOnRelaxed:
		setBoolIfUnset("treedb-disable-wal", false, isSet, treedbDisableWAL)
		setBoolIfUnset("treedb-relaxed-sync", true, isSet, treedbRelaxedSync)
	default:
		setBoolIfUnset("treedb-disable-wal", false, isSet, treedbDisableWAL)
		setBoolIfUnset("treedb-relaxed-sync", false, isSet, treedbRelaxedSync)
	}
	setBoolIfUnset("treedb-disable-read-checksum", opts.ValueLog.ReadIntegrity == treedb.IntegritySkipChecksums, isSet, treedbDisableReadChecksum)
	setBoolIfUnset("treedb-index-optimizations",
		opts.LeafPrefixCompression && opts.IndexColumnarLeaves && opts.IndexPackedValuePtr,
		isSet,
		treedbIndexOptimizations,
	)
	setBoolIfUnset("treedb-index-outer-leaves-in-vlog", opts.IndexOuterLeavesInValueLog, isSet, treedbIndexOuterLeavesInVlog)
	setBoolIfUnset("treedb-prefer-append-alloc", opts.PreferAppendAlloc, isSet, treedbPreferAppendAlloc)
	profileCompression := formatTreeDBProfileVlogCompressionFlagValue(opts.ValueLog.Compression)
	setStringIfUnset("treedb-vlog-compression", profileCompression, isSet, treedbVlogCompression)
	setStringIfUnset("treedb-vlog-block-codec", formatTreeDBVlogBlockCodec(opts.ValueLog.BlockCodec), isSet, treedbVlogBlockCodec)
	setStringIfUnset("treedb-vlog-auto-policy", formatTreeDBVlogAutoPolicy(opts.ValueLog.AutoPolicy), isSet, treedbVlogAutoPolicy)
	setStringIfUnset("treedb-vlog-compression-autotune", formatTreeDBVlogCompressionAutotune(opts.ValueLog.CompressionAutotune.Mode), isSet, treedbVlogCompressionAutotune)
	setIntIfUnset("treedb-vlog-dict-incompressible-hold-bytes", opts.ValueLog.DictIncompressibleHoldBytes, isSet, treedbVlogDictIncompressibleHoldBytes)
	setIntIfUnset("treedb-vlog-dict-probe-interval-bytes", opts.ValueLog.DictProbeIntervalBytes, isSet, treedbVlogDictProbeIntervalBytes)
}

func formatTreeDBProfileVlogCompressionFlagValue(mode treedb.ValueLogCompressionMode) string {
	if mode == treedb.ValueLogCompressionAuto {
		return "default"
	}
	return formatTreeDBVlogCompressionFlagValue(mode)
}

func setBoolIfUnset(name string, val bool, isSet map[string]bool, target *bool) {
	if !isSet[name] {
		*target = val
	}
}

func setIntIfUnset(name string, val int, isSet map[string]bool, target *int) {
	if !isSet[name] {
		*target = val
	}
}

func setInt64IfUnset(name string, val int64, isSet map[string]bool, target *int64) {
	if !isSet[name] {
		*target = val
	}
}

func setStringIfUnset(name, val string, isSet map[string]bool, target *string) {
	if !isSet[name] {
		*target = val
	}
}

func applyProfile(name string, isSet map[string]bool) error {
	if name == "" {
		return nil
	}
	p, ok := profiles[strings.ToLower(name)]
	if !ok {
		return fmt.Errorf("unknown profile: %q", name)
	}
	p.Apply(isSet)
	return nil
}

func stderrSupportsANSI() bool {
	if fi, err := os.Stderr.Stat(); err == nil {
		return (fi.Mode()&os.ModeCharDevice) != 0 && strings.ToLower(strings.TrimSpace(os.Getenv("TERM"))) != "dumb"
	}
	return false
}

func styleUsageText(text string, bold bool) string {
	if !bold {
		return text
	}
	return "\x1b[1m" + text + "\x1b[0m"
}

func treeDBUsageGroup(name string) string {
	switch name {
	case "treedb-disable-wal", "treedb-relaxed-sync", "treedb-disable-read-checksum", "treedb-allow-unsafe":
		return "TreeDB Unsafe Knobs"
	case "treedb-vlog-compression", "treedb-vlog-block-codec", "treedb-vlog-auto-policy", "treedb-vlog-generation-policy",
		"treedb-vlog-compression-autotune", "treedb-vlog-dict-class-mode", "treedb-vlog-rewrite-after-run",
		"treedb-vacuum-after-vlog-rewrite-run":
		return "TreeDB Compression Knobs"
	case "treedb-flush-threshold", "treedb-chunk-size", "treedb-maintenance-mode", "treedb-memtable-mode",
		"treedb-index-optimizations", "treedb-index-outer-leaves-in-vlog", "treedb-prefer-append-alloc",
		"treedb-force-value-pointers", "treedb-value-log-threshold", "treedb-disable-piggyback-compaction":
		return "TreeDB Main Knobs"
	default:
		return "TreeDB Advanced Tuning"
	}
}

func customUsage() {
	useANSI := stderrSupportsANSI()
	fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Unified Benchmark Runner for GoMap DBs\n\n")
	fmt.Fprintf(os.Stderr, "Profiles (-profile):\n")
	keys := make([]string, 0, len(profiles))
	for k := range profiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(os.Stderr, "  %-10s %s\n", k, profiles[k].Description)
	}
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "TreeDB flags are grouped into main knobs, compression knobs, advanced tuning, and unsafe knobs.\n")
	fmt.Fprintf(os.Stderr, "In most runs, start with -profile plus the TreeDB main knobs and leave advanced tuning alone.\n\n")

	// Group flags
	groups := map[string][]*flag.Flag{
		"General":                  {},
		"Workload":                 {},
		"TreeDB Main Knobs":        {},
		"TreeDB Compression Knobs": {},
		"TreeDB Advanced Tuning":   {},
		"TreeDB Unsafe Knobs":      {},
		"Profiling":                {},
		"Safety/Limits":            {},
	}

	flag.VisitAll(func(f *flag.Flag) {
		if f.Name == "profile" { // Handle separately or in General
			groups["General"] = append(groups["General"], f)
			return
		}
		if strings.HasPrefix(f.Name, "treedb-") {
			groupName := treeDBUsageGroup(f.Name)
			groups[groupName] = append(groups[groupName], f)
		} else if strings.Contains(f.Name, "profile") || f.Name == "trace" {
			groups["Profiling"] = append(groups["Profiling"], f)
		} else if strings.HasPrefix(f.Name, "max-") || strings.HasPrefix(f.Name, "checkpoint-") {
			groups["Safety/Limits"] = append(groups["Safety/Limits"], f)
		} else if isWorkloadFlag(f.Name) {
			groups["Workload"] = append(groups["Workload"], f)
		} else {
			groups["General"] = append(groups["General"], f)
		}
	})

	groupOrder := []string{"General", "Workload", "TreeDB Main Knobs", "TreeDB Compression Knobs", "TreeDB Advanced Tuning", "TreeDB Unsafe Knobs", "Safety/Limits", "Profiling"}
	for _, gName := range groupOrder {
		flags := groups[gName]
		if len(flags) == 0 {
			continue
		}
		fmt.Fprintf(os.Stderr, "%s:\n", styleUsageText(gName, useANSI))
		// Sort flags in group
		sort.Slice(flags, func(i, j int) bool { return flags[i].Name < flags[j].Name })
		for _, f := range flags {
			// standard flag print
			s := fmt.Sprintf("  %s", styleUsageText("-"+f.Name, useANSI)) // Two spaces as indent
			name, usage := flag.UnquoteUsage(f)
			if len(name) > 0 {
				s += " " + name
			}
			// Boolean flags of one ASCII letter are so common we
			// treat them specially, saving a space.
			if len(s) <= 4 { // space, space, '-', 'x'
				s += "\t"
			} else {
				// Four spaces before the tab triggers good alignment
				// for both 4- and 8-space tab stops.
				s += "\n    \t"
			}
			s += strings.ReplaceAll(usage, "\n", "\n    \t")
			if f.DefValue != "" {
				s += fmt.Sprintf(" (default %v)", f.DefValue)
			}
			fmt.Fprintf(os.Stderr, "%s\n", s)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}
}

func isWorkloadFlag(name string) bool {
	switch name {
	case "keys", "valsize", "batchsize", "range-queries", "range-span",
		"batch-delete-range-width", "batch-delete-ranges-per-batch", "batch-delete-range-validate", "batch-delete-range-refill",
		"keycounts", "keyscale", "keys-min", "keys-max", "key-shape", "dbs", "test",
		"suite", "outdir", "keep", "progress", "seed", "settle-before-scans":
		return true
	}
	return false
}
