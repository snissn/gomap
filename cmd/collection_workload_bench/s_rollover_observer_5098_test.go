package main

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"testing"
)

type s5098PhysicalCounters struct {
	exercised, thresholds, handoffs uint64
	identity                        string
}

// The inventory indices are stable within one DB lifetime: ordinary lanes are
// fixed at open, auxiliary native-root lanes are appended at initialization,
// and leaf workers only extend their indexed slice. Reopen starts a new epoch.
type s5098PhysicalThresholdEpoch struct {
	loaded, previous map[string]s5098PhysicalCounters
}

func s5098PhysicalSnapshot(stats map[string]string) (map[string]s5098PhysicalCounters, error) {
	out := make(map[string]s5098PhysicalCounters)
	read := func(key string) (uint64, error) {
		v, err := strconv.ParseUint(stats[key], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("physical observer %s: %w", key, err)
		}
		return v, nil
	}
	for _, family := range []struct {
		root, kind, activity string
		leaf                 bool
	}{
		{"treedb.cache.leaf_log_lanes", "lane", "append_calls_total", true},
		{"treedb.cache.physical_vlog_writers", "writer", "records_total", false},
	} {
		count, err := read(family.root + ".configured")
		if err != nil {
			return nil, err
		}
		// Every configured slot emits at least one field. This bounds malformed
		// declarations by the actual snapshot size without imposing a worker cap.
		if count > uint64(len(stats)) {
			return nil, fmt.Errorf("configured topology exceeds snapshot: %s", family.root)
		}
		base := family.root + "." + family.kind + "."
		indices := make(map[uint64]struct{})
		for key, raw := range stats {
			if !strings.HasPrefix(key, base) {
				continue
			}
			index, _, hasField := strings.Cut(strings.TrimPrefix(key, base), ".")
			i, err := strconv.ParseUint(index, 10, 64)
			canonical := fmt.Sprint(i)
			if family.leaf {
				canonical = fmt.Sprintf("%02d", i)
			}
			if err != nil || !hasField || i >= count || index != canonical {
				return nil, fmt.Errorf("invalid physical index: %s", key)
			}
			if strings.HasSuffix(key, ".identity_error") && raw != "" {
				return nil, fmt.Errorf("%s: %s", key, raw)
			}
			indices[i] = struct{}{}
		}
		if uint64(len(indices)) != count {
			return nil, fmt.Errorf("incomplete configured topology: %s", family.root)
		}
		for i := uint64(0); i < count; i++ {
			index := fmt.Sprint(i)
			if family.leaf {
				index = fmt.Sprintf("%02d", i)
			}
			prefix := base + index
			if family.leaf {
				switch stats[prefix+".installed"] {
				case "false":
					if _, exists := stats[prefix+".append_calls_total"]; exists {
						return nil, fmt.Errorf("unused leaf slot has writer counters: %s", prefix)
					}
					continue
				case "true":
				default:
					return nil, fmt.Errorf("missing leaf installation state: %s", prefix)
				}
			}
			exercised, err := read(prefix + "." + family.activity)
			if err != nil {
				return nil, err
			}
			thresholds, err := read(prefix + ".threshold_rotations_total")
			if err != nil {
				return nil, err
			}
			handoffs, err := read(prefix + ".maintenance_handoffs_total")
			if err != nil {
				return nil, err
			}
			identity := "leaf"
			if !family.leaf {
				lane, err := read(prefix + ".logical_lane")
				if err != nil {
					return nil, err
				}
				class, err := read(prefix + ".generation_class")
				if err != nil {
					return nil, err
				}
				target, err := read(prefix + ".segment_target_bytes")
				if err != nil {
					return nil, err
				}
				if target == 0 {
					return nil, fmt.Errorf("zero segment target at %s", prefix)
				}
				identity = fmt.Sprintf("%d/%d/%d", lane, class, target)
			}
			out[prefix] = s5098PhysicalCounters{exercised, thresholds, handoffs, identity}
		}
	}
	return out, nil
}

func s5098ComponentTopology(leafCount, vlogCount int) map[string]string {
	stats := map[string]string{"treedb.cache.leaf_log_lanes.configured": fmt.Sprint(leafCount), "treedb.cache.physical_vlog_writers.configured": fmt.Sprint(vlogCount)}
	for i := range leafCount {
		stats[fmt.Sprintf("treedb.cache.leaf_log_lanes.lane.%02d.installed", i)] = "false"
	}
	for i := range vlogCount {
		prefix := fmt.Sprintf("treedb.cache.physical_vlog_writers.writer.%d", i)
		for key, value := range map[string]string{"records_total": "0", "threshold_rotations_total": "0", "maintenance_handoffs_total": "0", "logical_lane": "0", "generation_class": "1", "segment_target_bytes": "268435456"} {
			stats[prefix+"."+key] = value
		}
	}
	return stats
}

func s5098NewPhysicalThresholdEpoch(stats map[string]string) (*s5098PhysicalThresholdEpoch, error) {
	loaded, err := s5098PhysicalSnapshot(stats)
	if err != nil {
		return nil, err
	}
	return &s5098PhysicalThresholdEpoch{loaded: loaded, previous: maps.Clone(loaded)}, nil
}

func (e *s5098PhysicalThresholdEpoch) observe(stats map[string]string) (map[string]uint64, bool, error) {
	current, err := s5098PhysicalSnapshot(stats)
	if err != nil {
		return nil, false, err
	}
	for prefix, prev := range e.previous {
		now, ok := current[prefix]
		if !ok {
			return nil, false, fmt.Errorf("physical writer disappeared: %s", prefix)
		}
		if now.identity != prev.identity || now.exercised < prev.exercised || now.thresholds < prev.thresholds || now.handoffs < prev.handoffs {
			return nil, false, fmt.Errorf("physical identity or counter regressed: %s", prefix)
		}
	}
	counts := make(map[string]uint64)
	passed := false
	for prefix, now := range current {
		if now.exercised == 0 {
			continue
		}
		initial := e.loaded[prefix].thresholds
		if now.thresholds < initial {
			return nil, false, fmt.Errorf("physical threshold baseline regressed: %s", prefix)
		}
		counts[prefix] = now.thresholds - initial
	}
	if len(counts) > 0 {
		passed = true
		for _, count := range counts {
			if count < 2 {
				passed = false
			}
		}
	}
	e.previous = current
	return counts, passed, nil
}

func TestS5098PhysicalEpochRequiresAllWritersAndFreshReopenBaseline(t *testing.T) {
	leaf := "treedb.cache.leaf_log_lanes.lane.65"
	vlog := "treedb.cache.physical_vlog_writers.writer.3"
	stats := s5098ComponentTopology(66, 4)
	maps.Copy(stats, map[string]string{leaf + ".append_calls_total": "10", leaf + ".threshold_rotations_total": "6", leaf + ".maintenance_handoffs_total": "0", vlog + ".records_total": "10", vlog + ".threshold_rotations_total": "5", vlog + ".maintenance_handoffs_total": "0", vlog + ".logical_lane": "0", vlog + ".generation_class": "1", vlog + ".segment_target_bytes": "268435456"})
	stats[leaf+".installed"] = "true"
	e, err := s5098NewPhysicalThresholdEpoch(stats)
	if err != nil {
		t.Fatal(err)
	}
	stats[leaf+".threshold_rotations_total"] = "8"
	_, pass, err := e.observe(stats)
	if err != nil || pass {
		t.Fatalf("hot writer incorrectly excluded: pass=%v err=%v", pass, err)
	}
	stats[vlog+".threshold_rotations_total"] = "7"
	counts, pass, err := e.observe(stats)
	if err != nil || !pass || counts[leaf] != 2 || counts[vlog] != 2 {
		t.Fatalf("all writers: %v %v %v", counts, pass, err)
	}
	reopened, err := s5098NewPhysicalThresholdEpoch(stats)
	if err != nil {
		t.Fatal(err)
	}
	_, pass, err = reopened.observe(stats)
	if err != nil || pass {
		t.Fatal("prior epoch rotations reused after reopen")
	}
	stats[vlog+".maintenance_handoffs_total"] = "99"
	_, pass, err = reopened.observe(stats)
	if err != nil || pass {
		t.Fatal("maintenance handoff counted as natural rotation")
	}
}

func TestS5098PhysicalEpochRejectsMissingLateWorkerRegressionAndBadIdentity(t *testing.T) {
	prefix := "treedb.cache.leaf_log_lanes.lane.65"
	for _, corruption := range []string{"missing", "threshold", "append", "handoff", "overflow", "identity"} {
		t.Run(corruption, func(t *testing.T) {
			e, err := s5098NewPhysicalThresholdEpoch(s5098ComponentTopology(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			stats := s5098ComponentTopology(66, 0)
			stats[prefix+".installed"] = "true"
			maps.Copy(stats, map[string]string{prefix + ".append_calls_total": "10", prefix + ".threshold_rotations_total": "6", prefix + ".maintenance_handoffs_total": "2"})
			if _, _, err := e.observe(stats); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "missing":
				stats = s5098ComponentTopology(0, 0)
			case "threshold":
				stats[prefix+".threshold_rotations_total"] = "5"
			case "append":
				stats[prefix+".append_calls_total"] = "9"
			case "handoff":
				stats[prefix+".maintenance_handoffs_total"] = "1"
			case "overflow":
				stats[prefix+".threshold_rotations_total"] = "18446744073709551616"
			case "identity":
				stats[prefix+".identity_error"] = "invalid physical identity"
			}
			if _, _, err := e.observe(stats); err == nil {
				t.Fatal("invalid inventory accepted")
			}
		})
	}
}

func TestS5098PhysicalEpochRejectsIncompleteDeclaredInventory(t *testing.T) {
	for _, fault := range []string{"missing-initial-writer", "missing-initial-leaf", "missing-count", "bad-count", "overflow-count", "missing-field", "bad-index", "nil-slot-counters"} {
		t.Run(fault, func(t *testing.T) {
			stats := s5098ComponentTopology(2, 2)
			switch fault {
			case "missing-initial-writer":
				for key := range stats {
					if strings.HasPrefix(key, "treedb.cache.physical_vlog_writers.writer.1.") {
						delete(stats, key)
					}
				}
			case "missing-initial-leaf":
				delete(stats, "treedb.cache.leaf_log_lanes.lane.01.installed")
			case "missing-count":
				delete(stats, "treedb.cache.physical_vlog_writers.configured")
			case "bad-count":
				stats["treedb.cache.physical_vlog_writers.configured"] = "-1"
			case "overflow-count":
				stats["treedb.cache.physical_vlog_writers.configured"] = "18446744073709551616"
			case "missing-field":
				delete(stats, "treedb.cache.physical_vlog_writers.writer.1.threshold_rotations_total")
			case "bad-index":
				stats["treedb.cache.physical_vlog_writers.writer.01.records_total"] = "0"
			case "nil-slot-counters":
				stats["treedb.cache.leaf_log_lanes.lane.01.append_calls_total"] = "1"
			}
			if _, err := s5098NewPhysicalThresholdEpoch(stats); err == nil {
				t.Fatal("incomplete initial topology accepted")
			}
		})
	}
}
