package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

var (
	profileArg = flag.String("profile", "", "Benchmark profile to use (fast, wal_on_fast, durable, balanced). Overrides default flags unless explicitly set.")
)

type Profile struct {
	Description string
	Apply       func(isSet map[string]bool)
}

var profiles map[string]Profile

func init() {
	applyFast := func(isSet map[string]bool) {
		// TreeDB
		setBoolIfUnset("treedb-disable-wal", true, isSet, treedbDisableWAL)
		setBoolIfUnset("treedb-relaxed-sync", true, isSet, treedbRelaxedSync)
		setBoolIfUnset("treedb-disable-read-checksum", true, isSet, treedbDisableReadChecksum)
		setBoolIfUnset("treedb-allow-unsafe", true, isSet, treedbAllowUnsafe)
		setBoolIfUnset("treedb-index-optimizations", true, isSet, treedbIndexOptimizations)
		setStringIfUnset("treedb-vlog-auto-policy", "throughput", isSet, treedbVlogAutoPolicy)
		setStringIfUnset("treedb-vlog-compression-autotune", "medium", isSet, treedbVlogCompressionAutotune)

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
		// TreeDB: keep WAL enabled, but relax durability and read integrity.
		setBoolIfUnset("treedb-disable-wal", false, isSet, treedbDisableWAL)
		setBoolIfUnset("treedb-relaxed-sync", true, isSet, treedbRelaxedSync)
		setBoolIfUnset("treedb-disable-read-checksum", true, isSet, treedbDisableReadChecksum)
		setBoolIfUnset("treedb-allow-unsafe", true, isSet, treedbAllowUnsafe)
		setBoolIfUnset("treedb-index-optimizations", true, isSet, treedbIndexOptimizations)
		setStringIfUnset("treedb-vlog-auto-policy", "throughput", isSet, treedbVlogAutoPolicy)
		setStringIfUnset("treedb-vlog-compression-autotune", "medium", isSet, treedbVlogCompressionAutotune)

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
			Description: "Maximize throughput: disables fsync for supported DBs; for TreeDB also disables WAL, enables -treedb-index-optimizations, and sets throughput-biased value-log auto policy. UNSAFE for production data.",
			Apply:       applyFast,
		},
		"wal_on_fast": {
			Description: "TreeDB fast WAL-on profile: relaxed durability + disabled read checksums (WAL on, fsync/checksums off), enables -treedb-index-optimizations, and sets throughput-biased value-log auto policy.",
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

func customUsage() {
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

	// Group flags
	groups := map[string][]*flag.Flag{
		"General":         {},
		"Workload":        {},
		"Tuning":          {},
		"TreeDB Specific": {},
		"Profiling":       {},
		"Safety/Limits":   {},
	}

	flag.VisitAll(func(f *flag.Flag) {
		if f.Name == "profile" { // Handle separately or in General
			groups["General"] = append(groups["General"], f)
			return
		}
		if strings.HasPrefix(f.Name, "treedb-") {
			groups["TreeDB Specific"] = append(groups["TreeDB Specific"], f)
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

	groupOrder := []string{"General", "Workload", "Tuning", "TreeDB Specific", "Safety/Limits", "Profiling"}
	for _, gName := range groupOrder {
		flags := groups[gName]
		if len(flags) == 0 {
			continue
		}
		fmt.Fprintf(os.Stderr, "%s Flags:\n", gName)
		// Sort flags in group
		sort.Slice(flags, func(i, j int) bool { return flags[i].Name < flags[j].Name })
		for _, f := range flags {
			// standard flag print
			s := fmt.Sprintf("  -%s", f.Name) // Two spaces as indent
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
		"keycounts", "keyscale", "keys-min", "keys-max", "key-shape", "dbs", "test",
		"suite", "outdir", "keep", "progress", "seed", "settle-before-scans":
		return true
	}
	return false
}
