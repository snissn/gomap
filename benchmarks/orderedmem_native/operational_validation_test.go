package orderedmemnative

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// These are operational-trace activation checks, not performance gates.
func checkOperational(c config, f *fixture, p *packet) error {
	if c.OperationalMinRotations == 0 && c.OperationalMinPayloadBytes == 0 {
		return nil
	}
	var payload uint64
	for _, op := range f.operations {
		for _, mutation := range op.mutations {
			payload += uint64(len(mutation.Value))
		}
	}
	p.Verification["operational_foreground_payload_bytes"] = payload
	if payload < c.OperationalMinPayloadBytes {
		return fmt.Errorf("operational payload %d below declared %d", payload, c.OperationalMinPayloadBytes)
	}
	delta := func(key string, after map[string]string) (uint64, error) {
		a, err := strconv.ParseUint(p.StatsBefore[key], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("missing/invalid initial counter %s=%q", key, p.StatsBefore[key])
		}
		b, err := strconv.ParseUint(after[key], 10, 64)
		if err != nil || b < a {
			return 0, fmt.Errorf("invalid/nonmonotone final counter %s=%q", key, after[key])
		}
		return b - a, nil
	}
	observed := make(map[string]uint64)
	require := func(name, key string, after map[string]string, minimum uint64) error {
		n, err := delta(key, after)
		if err != nil {
			return err
		}
		observed[name] = n
		if n < minimum {
			return fmt.Errorf("operational %s=%d below declared %d", name, n, minimum)
		}
		return nil
	}
	p.Verification["operational_events"] = observed
	if c.Engine == "badger" {
		before, after := p.FilesBeforeForeground.MaxBadgerMemFID, p.FilesAfterForeground.MaxBadgerMemFID
		if before == 0 || after < before {
			return fmt.Errorf("invalid Badger memtable file IDs before=%d after=%d", before, after)
		}
		observed["foreground_memtable_file_id_increments"] = after - before
		if after-before < c.OperationalMinRotations {
			return fmt.Errorf("operational Badger foreground rotations=%d below declared %d", after-before, c.OperationalMinRotations)
		}
		tables, err := strconv.ParseUint(p.StatsPersisted["badger.table_count"], 10, 64)
		if err != nil || tables == 0 || p.FilesBeforeClose.BadgerSST == 0 {
			return fmt.Errorf("operational Badger has no completed SST flush before Close: tables=%q SSTbytes=%d", p.StatsPersisted["badger.table_count"], p.FilesBeforeClose.BadgerSST)
		}
		observed["tables_after_sync"] = tables
	} else if strings.HasPrefix(c.MemtableMode, "cow_") {
		// Rollovers include checkpoint seals. Handoffs are completed basis
		// replacements, rather than merely queued persistence work.
		if err := require("foreground_rollovers_including_checkpoints", "treedb.cache.cow.rollovers_total", p.StatsAfter, c.OperationalMinRotations); err != nil {
			return err
		}
		if err := require("completed_handoffs_through_final_checkpoint", "treedb.cache.cow.handoffs_total", p.StatsPersisted, c.OperationalMinRotations); err != nil {
			return err
		}
	} else {
		if err := require("completed_flush_batches_through_final_checkpoint", "treedb.cache.flush_apply.batches_total", p.StatsPersisted, c.OperationalMinRotations); err != nil {
			return err
		}
		if err := require("completed_flush_units_through_final_checkpoint", "treedb.cache.flush_apply.units_total", p.StatsPersisted, c.OperationalMinRotations); err != nil {
			return err
		}
		if err := require("checkpoint_runs", "treedb.cache.checkpoint.runs", p.StatsPersisted, 1); err != nil {
			return err
		}
	}
	p.Verification["operational_activation"] = true
	return nil
}

func TestOperationalRequiresCompletedHandoff(t *testing.T) {
	c := config{Engine: "treedb", MemtableMode: "cow_ordered", OperationalMinRotations: 2}
	p := &packet{Verification: make(map[string]any),
		StatsBefore:    map[string]string{"treedb.cache.cow.rollovers_total": "3", "treedb.cache.cow.handoffs_total": "3"},
		StatsAfter:     map[string]string{"treedb.cache.cow.rollovers_total": "5"},
		StatsPersisted: map[string]string{"treedb.cache.cow.handoffs_total": "4"}}
	if err := checkOperational(c, &fixture{}, p); err == nil {
		t.Fatal("queued rollovers must not pass without completed handoffs")
	}
	p.StatsPersisted["treedb.cache.cow.handoffs_total"] = "5"
	if err := checkOperational(c, &fixture{}, p); err != nil {
		t.Fatal(err)
	}
	if p.Verification["operational_activation"] != true {
		t.Fatal("missing completed-work activation")
	}
}
