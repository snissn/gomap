package cowbench

import (
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"strconv"
)

// ACKRouting validates observed routing without acquiring resources.
// Shared by the C3/C4 public fixtures; disabled WAL does not disable the vlog.
func ACKRouting(profile treedb.Profile, stats map[string]string) error {
	enabled, mode := "true", "external_command_wal"
	switch profile {
	case treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed:
	case treedb.ProfileNoWALFast:
		enabled, mode = "false", "disabled_unsafe"
	default:
		return fmt.Errorf("unknown ACK profile %q", profile)
	}
	for key, expected := range map[string]string{
		"treedb.command_wal.enabled":                   enabled,
		"treedb.cache.command_wal.external_durability": enabled,
		"treedb.cache.redo_log.enabled":                "false",
		"treedb.cache.redo_log.mode":                   mode,
	} {
		if stats[key] != expected {
			return fmt.Errorf("actual ACK routing %s=%q, want %q", key, stats[key], expected)
		}
	}
	return nil
}

// ValueLayout checks the physical entry, without calls or ownership.
// A zero length hint is valid for persistent vlog pointers.
func ValueLayout(entry node.LeafEntry, pointers bool) error {
	if entry.Flags&node.FlagTombstone != 0 {
		return fmt.Errorf("physical tombstone in value layout proof")
	}
	actual := entry.Flags&node.FlagPointer != 0
	if actual != pointers {
		return fmt.Errorf("actual pointer=%v, requested=%v", actual, pointers)
	}
	if actual {
		if entry.ValuePtr == (page.ValuePtr{}) || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
			return fmt.Errorf("invalid persistent value pointer")
		}
	} else if entry.ValuePtr != (page.ValuePtr{}) {
		return fmt.Errorf("inline entry has value pointer")
	}
	return nil
}

// WALCounters requires actual boundary observations even in NoWAL.
func WALCounters(profile treedb.Profile, before, after map[string]string, key string) (uint64, uint64, error) {
	values := [2]uint64{}
	for i, stats := range []map[string]string{before, after} {
		raw, ok := stats[key]
		if !ok {
			return 0, 0, fmt.Errorf("missing actual counter %s", key)
		}
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("counter %s: %w", key, err)
		}
		values[i] = value
	}
	if values[1] < values[0] {
		return 0, 0, fmt.Errorf("WAL counter regression")
	}
	if profile == treedb.ProfileNoWALFast && (values[0] != 0 || values[1] != 0) {
		return 0, 0, fmt.Errorf("actual NoWAL counter nonzero: %s", key)
	}
	return values[0], values[1], nil
}
