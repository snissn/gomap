package memtable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWOrderedLocalLazyRoutingMatchesQualification(t *testing.T) {
	seed := cowOrderedStorageMutation(t, "existing\x00member", 9)
	basis := cowOrderedStorageBuild(t, []COWMutation{seed})
	qualified := cowOrderedStorageMutation(t, "new\x00key", 7)
	raw := qualified
	raw.MVCC, raw.Timestamp = false, 0
	raw.Value = []byte("unqualified replacement")
	remove := raw
	remove.Remove = true
	existing := seed
	existing.MVCC, existing.Timestamp = false, 0
	malformed := qualified
	malformed.Key = append(bytes.Clone(qualified.Key), 0)
	malformedRaw := malformed
	malformedRaw.MVCC, malformedRaw.Timestamp = false, 0
	miss := COWMutation{Key: []byte{0}, Value: []byte("raw miss")}
	groups := [][]COWMutation{
		{qualified, seed},
		{existing, existing},
		{qualified, raw, remove, raw},
		{raw, qualified, raw},
		{qualified, miss, existing, raw, remove},
		{miss, raw, existing, qualified, miss, raw},
		{malformed, malformedRaw, qualified, raw},
		{miss, existing, miss},
	}
	for groupIndex, entries := range groups {
		for _, active := range []bool{false, true} {
			var current *cowOrderedTree
			if active {
				current = basis
			}
			plan := current.routing(entries)
			// Query in reverse order too: metadata must describe the whole stable
			// group, while inherited qualification only comes from earlier rows.
			for pass := 0; pass < 2; pass++ {
				for offset := range entries {
					index := offset
					if pass == 0 {
						index = len(entries) - offset - 1
					}
					e := entries[index]
					want := false
					if active {
						want = e.MVCC || bytes.Equal(e.Key, seed.Key)
						if !want {
							if _, _, err := mvcckey.Decode(e.Key); err == nil {
								for prior := 0; prior < index; prior++ {
									want = want || entries[prior].MVCC && bytes.Equal(entries[prior].Key, e.Key)
								}
							}
						}
					}
					if got := plan.routes(index); got != want {
						t.Fatalf("group=%d active=%t pass=%d index=%d got=%t want=%t", groupIndex, active, pass, index, got, want)
					}
				}
			}
			if !active && plan.metadataReady {
				t.Fatal("nil basis computed duplicate metadata")
			}
			if active && groupIndex < 2 && plan.metadataReady {
				t.Fatal("typed mutations or exact members computed unused metadata")
			}
		}
	}
}

func TestCOWOrderedLocalRoutingHasNoAllocation(t *testing.T) {
	seed := cowOrderedStorageMutation(t, "existing", 3)
	basis := cowOrderedStorageBuild(t, []COWMutation{seed})
	qualified := cowOrderedStorageMutation(t, "new", 5)
	raw := qualified
	raw.MVCC, raw.Timestamp = false, 0
	groups := [][]COWMutation{{qualified, seed}, {qualified, {Key: []byte("raw")}, raw}}
	var count int
	for _, entries := range groups {
		if allocations := testing.AllocsPerRun(50, func() {
			plan := basis.routing(entries)
			count = 0
			for index := range entries {
				if plan.routes(index) {
					count++
				}
			}
		}); allocations != 0 {
			t.Fatalf("routing allocated %g objects; its local state has no admitted backing", allocations)
		}
		if count != 2 {
			t.Fatalf("routing count=%d want 2", count)
		}
	}
}

func TestCOWOrderedLocalEncodingChargeBoundaries(t *testing.T) {
	lengths := []int{0, 1, 15, 16, 17, 19, 31, 32, 33, 43, 63, 64, 65, 255, 256, 257, 487, 488, 511, 512, 513, 1023, 1024, 1025, 32742, 32743, 32767, 32768, 32769, 65535}
	for _, keyBytes := range lengths {
		for _, payloadBytes := range lengths {
			charge, packed, ok := cowOrderedEncodingCharge(keyBytes, payloadBytes)
			valueBytes := cowValueMetadataBytes + payloadBytes
			separate := cowAllocation(uint64(keyBytes)) + cowAllocation(uint64(valueBytes))
			combined := cowAllocation(uint64(keyBytes + valueBytes))
			if !ok || charge > separate || packed != (combined <= separate) {
				t.Fatalf("key=%d payload=%d charge=%d packed=%t ok=%t separate=%d combined=%d", keyBytes, payloadBytes, charge, packed, ok, separate, combined)
			}
			want := separate
			if packed {
				want = combined
			}
			if charge != want {
				t.Fatalf("charge=%d want selected backing=%d", charge, want)
			}
		}
	}
	for _, shape := range [][2]int{{-1, 0}, {0, -1}, {0, math.MaxInt}, {math.MaxInt, 0}, {math.MaxInt, math.MaxInt}} {
		if _, _, ok := cowOrderedEncodingCharge(shape[0], shape[1]); ok {
			t.Fatalf("overflow/negative encoding shape accepted: %v", shape)
		}
	}
	for _, shape := range []struct {
		payload int
		charge  uint64
		packed  bool
	}{{0, 96, false}, {128, 256, true}, {8192, 16384, true}} {
		charge, packed, ok := cowOrderedEncodingCharge(43, shape.payload)
		if !ok || charge != shape.charge || packed != shape.packed {
			t.Fatalf("43-byte physical key payload=%d charge=%d packed=%t ok=%t", shape.payload, charge, packed, ok)
		}
	}
}

func TestCOWOrderedLocalEncodingOwnsMetadataAndInputs(t *testing.T) {
	for _, logical := range []string{"", strings.Repeat("k", 24), "binary\x00\xff\x00key", strings.Repeat("long", 300)} {
		for _, size := range []int{0, 1, 7, 128, 487, 488, 8192, 32743} {
			entry := cowOrderedStorageMutation(t, logical, math.MaxUint64)
			entry.Value = bytes.Repeat([]byte{0, 0xff, 'v'}, (size+2)/3)[:size]
			entry.Ptr = page.ValuePtr{Offset: math.MaxUint64, Length: math.MaxUint32, FileID: math.MaxUint32}
			entry.Revision, entry.Flags = page.EntryRevision(math.MaxUint64), node.FlagInline
			want := cowEncodeValue(entry).record(string(entry.Key))
			encoded := cowEncodeOrderedValue(entry)
			if got := encoded.value.record(encoded.key); got != want {
				t.Fatalf("logical=%q size=%d got=%+v want=%+v", logical, size, got, want)
			}
			charge, packed, ok := cowOrderedEncodingCharge(len(entry.Key), size)
			if !ok {
				t.Fatal("valid fixture did not have an encoding charge")
			}
			if packed && unsafe.Pointer(unsafe.StringData(string(encoded.value))) != unsafe.Add(unsafe.Pointer(unsafe.StringData(encoded.key)), len(encoded.key)) {
				t.Fatal("packed key/value strings do not share their admitted backing")
			}
			estimate, err := newCOWOrderedTree().estimate([]COWMutation{entry})
			if err != nil || estimate.Payload != charge {
				t.Fatalf("estimate payload=%d want encoding=%d error=%v", estimate.Payload, charge, err)
			}
			entry.Key[0] ^= 0xff
			for index := range entry.Value {
				entry.Value[index] ^= 0xff
			}
			if got := encoded.value.record(encoded.key); got != want {
				t.Fatal("owned encoding retained mutable input")
			}
		}
	}
}

func TestCOWOrderedLocalEncodingAllocationFitsSelectedCharge(t *testing.T) {
	cowTestIsolateAllocationWitness(t)
	for _, size := range []int{0, 128, 8192, 32743} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			entry := cowOrderedStorageMutation(t, strings.Repeat("k", 24), 7)
			entry.Value, entry.Flags = bytes.Repeat([]byte{'v'}, size), node.FlagInline
			charge, packed, ok := cowOrderedEncodingCharge(len(entry.Key), size)
			if !ok {
				t.Fatal("invalid fixture charge")
			}
			var encoded cowOrderedValue
			wantObjects := float64(2)
			if packed {
				wantObjects = 1
			}
			if allocations := testing.AllocsPerRun(20, func() { encoded = cowEncodeOrderedValue(entry) }); allocations != wantObjects {
				t.Fatalf("encoding allocated=%g objects want=%g", allocations, wantObjects)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			encoded = cowEncodeOrderedValue(entry)
			runtime.ReadMemStats(&after)
			if actual := after.TotalAlloc - before.TotalAlloc; actual > charge {
				t.Fatalf("encoding allocated=%d > selected charge=%d", actual, charge)
			}
			runtime.KeepAlive(encoded)
		})
	}
}

func TestCOWOrderedLocalValidatedApplyMatchesChecked(t *testing.T) {
	low, high := cowOrderedStorageMutation(t, "a\x00key", 50), cowOrderedStorageMutation(t, "a\x00key", 100)
	basis := cowOrderedStorageBuild(t, []COWMutation{low, high, cowOrderedStorageMutation(t, "z", 1)})
	remove := high
	remove.MVCC, remove.Timestamp, remove.Remove = false, 0, true
	put := low
	put.MVCC, put.Timestamp, put.Value = false, 0, bytes.Repeat([]byte{'x'}, 128)
	fresh := cowOrderedStorageMutation(t, "", math.MaxUint64)
	rawFresh := fresh
	rawFresh.MVCC, rawFresh.Timestamp, rawFresh.Value = false, 0, []byte("same group last wins")
	groups := [][]COWMutation{
		{cowOrderedStorageMutation(t, "a\x00key", 75), {Key: []byte{0}}, remove, put, fresh, rawFresh},
		{fresh, rawFresh, remove, cowOrderedStorageMutation(t, "z", 2), put},
	}
	collect := func(tree *cowOrderedTree) []COWRecord {
		var records []COWRecord
		tree.scanOwned(func(record COWRecord) bool { records = append(records, record); return true })
		return records
	}
	for index, entries := range groups {
		if _, err := basis.estimate(entries); err != nil {
			t.Fatal(err)
		}
		checked, validated := basis.fork(), basis.fork()
		checked.applyFrom(basis, entries)
		validated.applyValidatedFrom(basis, entries)
		if got, want := collect(validated), collect(checked); !reflect.DeepEqual(got, want) {
			t.Fatalf("group=%d validated=%+v checked=%+v", index, got, want)
		}
		if validated.nodeAllocationCharge() != checked.nodeAllocationCharge() {
			t.Fatal("validated prefix reuse changed the private node receipt")
		}
	}
}

func TestCOWOrderedLocalPackedPreparationOwnershipAndPrefix(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	t.Cleanup(func() { cowTestClose(writer); budget.Close() })
	low := cowOrderedStorageMutation(t, strings.Repeat("k", 24), 50)
	low.Value, low.Flags = bytes.Repeat([]byte{'l'}, 128), node.FlagInline
	old := cowTestPublish(t, writer, []COWMutation{low})
	t.Cleanup(func() { cowTestRelease(old) })
	high := cowOrderedStorageMutation(t, strings.Repeat("k", 24), 100)
	high.Value, high.Flags = bytes.Repeat([]byte{'h'}, 8192), node.FlagInline
	high.Revision = page.EntryRevision(math.MaxUint64)
	entries := []COWMutation{high}
	charge, err := writer.Estimate(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	originalLimit := budget.limits.MaxInFlightBytes
	budget.limits.MaxInFlightBytes = charge.Total() - 1
	if prepared, err := writer.Prepare(entries, COWPrepareOptions{}); prepared != nil || !errors.Is(err, ErrCOWCapacity) {
		t.Fatal("encoding escaped original admission", prepared, err)
	}
	budget.limits.MaxInFlightBytes = originalLimit
	if budget.Stats() != before {
		t.Fatal("refused preparation changed accounting")
	}
	prepared, err := writer.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	retirement := prepared.Cancel()
	retirement.Drain()
	if budget.Stats().TotalBytes != before.TotalBytes || budget.Stats().ReservedBytes != 0 {
		t.Fatal("cancel retained packed preparation charge")
	}
	prepared, err = writer.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	physical, value := bytes.Clone(high.Key), string(high.Value)
	high.Key[0], high.Value[0] = 0xff, 0
	fresh := prepared.Publish()
	t.Cleanup(func() { cowTestRelease(fresh) })
	if got, found := fresh.Get(physical); !found || got.Value != value || got.Revision != high.Revision {
		t.Fatal("published encoding borrowed staged input", got, found)
	}
	if got, found := old.Get(low.Key); !found || got.Value != string(low.Value) {
		t.Fatal("private history preparation changed the old cut")
	}
	_, oldHistory, ok := fresh.ordered.keys.Min()
	if !ok || oldHistory.maximum != 100 {
		t.Fatal("large latest fixture missing")
	}
	erase := COWMutation{Key: physical, Remove: true} // unqualified TS must be parsed
	remaining := cowTestPublish(t, writer, []COWMutation{erase})
	t.Cleanup(func() { cowTestRelease(remaining) })
	prefix, history, ok := remaining.ordered.keys.Min()
	if !ok || history.maximum != 50 || history.versions != nil {
		t.Fatal("erase did not collapse to the live singleton")
	}
	if unsafe.StringData(prefix) != unsafe.StringData(history.latest.key) || unsafe.StringData(prefix) == unsafe.StringData(oldHistory.latest.key) {
		t.Fatal("primary prefix retained the erased value backing")
	}
	if got, found := fresh.Get(physical); !found || got.Value != value {
		t.Fatal("later erase invalidated an old cut alias")
	}
}
