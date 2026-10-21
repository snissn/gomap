package memtable

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Fixed semantic fixtures also provide an offline old/new charge and error
// fingerprint without keeping a second implementation of the estimator.
func TestCOWOrderedEstimateValidatedPrefixFixtures(t *testing.T) {
	mutation := func(key string, ts uint64) COWMutation {
		return cowOrderedStorageMutation(t, key, ts)
	}
	low, high := mutation("a\x00escaped", 50), mutation("a\x00escaped", 100)
	basis := cowOrderedStorageBuild(t, []COWMutation{low, high, mutation("z", 1)})
	remove := high
	remove.MVCC, remove.Timestamp, remove.Remove = false, 0, true
	qualifiedRemove := remove
	qualifiedRemove.MVCC, qualifiedRemove.Timestamp = true, 100
	implicit := low
	implicit.MVCC, implicit.Timestamp, implicit.Value = false, 0, []byte("raw replacement of ordered member")
	badFlags := mutation("invalid", 1)
	badFlags.Flags = node.FlagPointer | node.FlagTombstone
	mismatch := badFlags
	mismatch.Timestamp++
	malformed := badFlags
	malformed.Key = []byte{0}
	truncated := badFlags
	truncated.Key = append([]byte(nil), low.Key[:len(low.Key)-1]...)
	extra := badFlags
	extra.Key = append(append([]byte(nil), low.Key...), 0)
	rawMalformed := malformed
	rawMalformed.MVCC = false
	rawGap := COWMutation{Key: []byte{0}, Value: []byte("unrouted short raw key")}
	newlyQualified := mutation("new", 7)
	subsequentRaw := newlyQualified
	subsequentRaw.MVCC, subsequentRaw.Timestamp = false, 0
	subsequentRaw.Value = []byte("same-group replacement")
	capacity := newCOWOrderedTree()
	capacity.versions = int(^uint(0) >> 1)
	fixtures := []struct {
		name         string
		basis        *cowOrderedTree
		entries      []COWMutation
		wantError    string
		wantCapacity bool
	}{
		{name: "empty", basis: basis},
		{name: "short_raw", basis: basis, entries: []COWMutation{{}, rawGap, rawMalformed}},
		{name: "ascending_raw_gaps", basis: basis, entries: []COWMutation{mutation("a\x00escaped", 75), rawGap, mutation("a\x00escaped", math.MaxUint64), mutation("z", 2)}},
		{name: "descending_raw_gaps", basis: basis, entries: []COWMutation{mutation("z", 2), rawGap, mutation("a\x00escaped", 75), mutation("a\x00escaped", 25)}},
		{name: "noncontiguous_runs", basis: basis, entries: []COWMutation{mutation("a\x00escaped", 75), mutation("z", 2), mutation("a\x00escaped", 25), remove, implicit}},
		{name: "implicit_member", basis: basis, entries: []COWMutation{implicit}},
		{name: "remove_timestamp_zero", basis: basis, entries: []COWMutation{remove}},
		{name: "qualified_remove", basis: basis, entries: []COWMutation{qualifiedRemove}},
		{name: "same_group_qualification", basis: basis, entries: []COWMutation{newlyQualified, subsequentRaw}},
		{name: "malformed_before_flags", basis: basis, entries: []COWMutation{malformed}, wantError: "COW ordered mutation has invalid canonical key or timestamp"},
		{name: "timestamp_before_flags", basis: basis, entries: []COWMutation{mismatch}, wantError: "COW ordered mutation has invalid canonical key or timestamp"},
		{name: "truncated", basis: basis, entries: []COWMutation{truncated}, wantError: "COW ordered mutation has invalid canonical key or timestamp"},
		{name: "extra_suffix", basis: basis, entries: []COWMutation{extra}, wantError: "COW ordered mutation has invalid canonical key or timestamp"},
		{name: "flags_before_later_malformed", basis: basis, entries: []COWMutation{badFlags, malformed}, wantError: "COW entry has pointer and tombstone flags"},
		{name: "partial_payload_then_flags", basis: basis, entries: []COWMutation{low, badFlags}, wantError: "COW entry has pointer and tombstone flags"},
		{name: "capacity_identity", basis: capacity, entries: []COWMutation{low}, wantError: ErrCOWCapacity.Error(), wantCapacity: true},
	}
	var qualifiedCharge, implicitCharge COWCharge
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			charge, err := fixture.basis.estimate(fixture.entries)
			message := ""
			if err != nil {
				message = err.Error()
			}
			if message != fixture.wantError || errors.Is(err, ErrCOWCapacity) != fixture.wantCapacity {
				t.Fatalf("charge=%+v error=%v, want error %q capacity=%t", charge, err, fixture.wantError, fixture.wantCapacity)
			}
			record := struct {
				Charge   COWCharge
				Error    string
				Capacity bool
			}{charge, message, errors.Is(err, ErrCOWCapacity)}
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("estimate-parity %s %s", fixture.name, encoded)
			if fixture.name == "qualified_remove" {
				qualifiedCharge = charge
			}
			if fixture.name == "remove_timestamp_zero" {
				implicitCharge = charge
			}
		})
	}
	if implicitCharge != qualifiedCharge {
		t.Fatalf("unqualified Remove must decode the physical timestamp: implicit=%+v qualified=%+v", implicitCharge, qualifiedCharge)
	}
}

func TestCOWOrderedEstimateValidatedPrefixMixedPublication(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	defer func() { cowTestClose(writer); budget.Close() }()
	mutation := func(key string, ts uint64, value string) COWMutation {
		m := cowOrderedStorageMutation(t, key, ts)
		m.Value = []byte(value)
		m.Revision = page.EntryRevision(math.MaxUint64 - ts)
		return m
	}
	low, high := mutation("a\x00escaped", 50, "old low"), mutation("a\x00escaped", 100, "old high")
	seed := []COWMutation{low, high, mutation("z", 1, "old z")}
	old := cowTestPublish(t, writer, seed)
	defer cowTestRelease(old)
	remove := high
	remove.MVCC, remove.Timestamp, remove.Remove = false, 0, true
	replacement := low
	replacement.MVCC, replacement.Timestamp, replacement.Value = false, 0, []byte("replacement")
	tombstone := mutation("z", 2, "")
	tombstone.Flags = node.FlagTombstone
	entries := []COWMutation{
		mutation("a\x00escaped", 75, "backfill"),
		{Key: []byte{0}, Value: []byte("raw short"), Revision: 7},
		tombstone,
		mutation("a\x00escaped", math.MaxUint64, "full timestamp"),
		remove, replacement,
		mutation("", 1, "empty logical key"),
	}
	want := make(map[string]COWMutation)
	for _, entry := range seed {
		want[string(entry.Key)] = entry
	}
	for _, entry := range entries {
		if entry.Remove {
			delete(want, string(entry.Key))
		} else {
			want[string(entry.Key)] = entry
		}
	}
	prepared, err := writer.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	upper := prepared.Charge()
	if prepared.private.AllocationCharge()+prepared.ordered.nodeAllocationCharge() != upper.Nodes {
		t.Fatal("actual private node receipt does not reconcile the original preparation")
	}
	fresh := prepared.Publish()
	defer cowTestRelease(fresh)
	if fresh.Len() != len(want) {
		t.Fatalf("physical cardinality %d want %d", fresh.Len(), len(want))
	}
	for key, expected := range want {
		got, found := fresh.Get([]byte(key))
		if !found || got.Value != string(expected.Value) || got.Flags != expected.Flags || got.Revision != expected.Revision {
			t.Fatalf("new physical projection %x: got %+v/%t want %+v", key, got, found, expected)
		}
	}
	if _, found := fresh.Get(high.Key); found {
		t.Fatal("unqualified Timestamp0 removal left the physical version")
	}
	for _, expected := range seed {
		got, found := old.Get(expected.Key)
		if !found || got.Value != string(expected.Value) || got.Revision != expected.Revision {
			t.Fatal("mixed private update changed an old captured postimage", got, found)
		}
	}
}
