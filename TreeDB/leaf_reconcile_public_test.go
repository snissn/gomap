package treedb

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestPublicOnlineVacuumPreservesLeafWritersAndReopenablePointers(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace inspection unsupported")
	}
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.BackgroundIndexVacuumInterval = -1
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d != nil {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	const rows = 256
	value := func(i int, updated bool) []byte {
		return []byte(fmt.Sprintf("%04d/%t/%s", i, updated, strings.Repeat("persistent", 48)))
	}
	key := func(i int) []byte { return []byte(fmt.Sprintf("reconcile-%04d", i)) }
	for i := 0; i < rows; i++ {
		if err := d.Set(key(i), value(i, false)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held := d.AcquireSnapshot()
	if held == nil {
		t.Fatal("held snapshot unavailable")
	}
	defer held.Close()
	// These fields identify every actual installed physical producer. Appending
	// rebuilt pages may grow its file; maintenance must not rotate it solely
	// because another healthy peer has a higher shared sequence.
	inventory := func() map[string]string {
		t.Helper()
		result := make(map[string]string)
		stats := d.Stats()
		for name, id := range stats {
			if strings.HasPrefix(name, "treedb.cache.leaf_log_lanes.lane.") && strings.HasSuffix(name, ".current_file_id") && id != "0" {
				prefix := strings.TrimSuffix(name, ".current_file_id")
				if _, err := strconv.ParseUint(id, 10, 32); err != nil {
					t.Fatal(err)
				}
				result[prefix] = id + "/" + stats[prefix+".current_sequence"] + "/" + stats[prefix+".segment_rotations_total"] + "/" + stats[prefix+".maintenance_handoffs_total"]
			}
		}
		return result
	}
	before := inventory()
	if len(before) == 0 {
		t.Fatal("public writes did not create physical leaf producers")
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := d.VacuumIndexOnline(context.Background()); err != nil {
			t.Fatal(err)
		}
		after := inventory()
		if len(after) != len(before) {
			t.Fatalf("vacuum changed writer inventory: %v -> %v", before, after)
		}
		for name, state := range before {
			if after[name] != state {
				t.Fatalf("vacuum changed healthy writer %s: %s -> %s", name, state, after[name])
			}
		}
		for i := 0; i < rows; i++ {
			got, err := d.Get(key(i))
			if err != nil || !bytes.Equal(got, value(i, false)) {
				t.Fatalf("current row%d: %v", i, err)
			}
			old, err := held.Get(key(i))
			if err != nil || !bytes.Equal(old, value(i, false)) {
				t.Fatalf("held row%d: %v", i, err)
			}
		}
	}
	for i := 0; i < rows; i++ {
		if err := d.Set(key(i), value(i, true)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		old, err := held.Get(key(i))
		if err != nil || !bytes.Equal(old, value(i, false)) {
			t.Fatalf("held after append row%d: %v", i, err)
		}
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		got, err := d.Get(key(i))
		if err != nil || !bytes.Equal(got, value(i, true)) {
			t.Fatalf("reopened row%d: %v", i, err)
		}
	}
}
