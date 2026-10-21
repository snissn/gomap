package orderedmemnative

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

func TestHistoricalOracleDeletionAndBackfill(t *testing.T) {
	// This hand-written chronology includes a deletion followed by a later
	// value. Historical lookup must never resurrect the pre-delete value.
	history := []record{
		{Timestamp: 3, State: mvcc.Present, Value: []byte("first")},
		{Timestamp: 7, State: mvcc.Tombstone},
		{Timestamp: 11, State: mvcc.Present, Value: []byte("last")},
	}
	for _, tc := range []struct {
		at, timestamp uint64
		value         string
		state         mvcc.ReadState
	}{
		{1, 0, "", mvcc.Absent}, {3, 3, "first", mvcc.Present},
		{6, 3, "first", mvcc.Present}, {7, 0, "", mvcc.Absent},
		{10, 0, "", mvcc.Absent}, {11, 11, "last", mvcc.Present},
	} {
		got := pointExpected(history, tc.at)
		if got.Timestamp != tc.timestamp || got.State != tc.state || string(got.Value) != tc.value {
			t.Fatalf("at %d: got %+v", tc.at, got)
		}
	}
}

func TestFixtureContractAcrossArms(t *testing.T) {
	c := config{Case: "mixed", Engine: "treedb", Profile: "command_wal_relaxed", MemtableMode: "adaptive",
		Operations: 20, BatchSize: 1, ValueBytes: 32, KeyCount: 17, HistoryDepth: 1,
		Workers: 1, RangeWidth: 4, FlushThreshold: 1 << 20, Seed: 1}
	f, err := makeFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	reads, writes, deletes := 0, 0, 0
	for _, op := range f.operations {
		if op.kind == 'g' {
			reads++
			continue
		}
		if op.mutations[0].Delete {
			deletes++
		} else {
			writes++
		}
	}
	if reads != 12 || writes != 4 || deletes != 4 || len(f.allHistory()) != 25 {
		t.Fatalf("wrong 60/20/20 useful work: reads=%d writes=%d deletes=%d history=%d", reads, writes, deletes, len(f.allHistory()))
	}
	c.Engine, c.MemtableMode, c.Profile, c.Dir, c.ProfileDir = "badger", "", "command_wal_durable", "/unused", "/unused-profiles"
	c.RetainedDiagnostics = true
	other, err := makeFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	if f.trace != other.trace {
		t.Fatal("engine/durability/instrumentation changed useful-work fingerprint")
	}
	c.ValueBytes++
	changed, err := makeFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	if f.trace == changed.trace {
		t.Fatal("different input values must change fingerprint")
	}
	if bytes.Equal(fixtureValue(1, 3, 32), fixtureValue(1, 4, 32)) {
		t.Fatal("reopen oracle requires distinct per-version values")
	}
}

func TestRangeFixtureFiltersSelectedTombstone(t *testing.T) {
	c := config{Case: "range", Operations: 18, BatchSize: 1, ValueBytes: 32, KeyCount: 17,
		HistoryDepth: 18, Workers: 1, RangeWidth: 17, Seed: 0}
	f, err := makeFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range f.operations {
		for _, row := range op.rows {
			// The explicit seeding rule deletes key k at timestamp t when
			// (k+t) is divisible by 17. Every timestamp is seeded, so this
			// checks selection-before-filtering independently of lookup.
			if row.Timestamp != op.timestamp || row.State != mvcc.Present {
				t.Fatalf("range resurrected wrong selected version: %+v", row)
			}
			for keyID, key := range f.keys {
				if bytes.Equal(row.Key, key) && (uint64(keyID)+op.timestamp)%17 == 0 {
					t.Fatalf("range returned deleted key %d at %d", keyID, op.timestamp)
				}
			}
		}
	}
}

func TestHistoryReadCoverageAcrossCutoffs(t *testing.T) {
	c := config{Case: "history", Operations: 16, BatchSize: 1, ValueBytes: 32,
		KeyCount: 4, HistoryDepth: 4, Workers: 1, RangeWidth: 4, Seed: 1}
	f, err := makeFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]map[uint64]bool)
	for _, op := range f.operations {
		key := string(op.key)
		if seen[key] == nil {
			seen[key] = make(map[uint64]bool)
		}
		seen[key][op.timestamp] = true
	}
	for _, key := range f.keys {
		if len(seen[string(key)]) != 4 {
			t.Fatalf("key %x exercised %d cutoffs, want four", key, len(seen[string(key)]))
		}
	}
}
