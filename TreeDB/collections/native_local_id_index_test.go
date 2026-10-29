package collections

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeLocalIDIndexCollisionBoundaryAndPresence(t *testing.T) {
	// Collision position changes CPU probes, never the admitted backing size.
	var charged uint64
	index, err := newLocalIDIndex(897, func(n uint64) error { charged += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	originalCharge, originalCapacity := charged, len(index.cells)
	for i := 0; i < 897; i++ {
		index.insertHash(fmt.Sprintf("collision-%04d", i), i, 7)
	}
	index.insertHash("", 897, 7)
	index.insertHash("collision-0010", 9000, 7)
	for i := 0; i < 897; i++ {
		want := i
		if i == 10 {
			want = 9000
		}
		if got, ok := index.lookupHash(fmt.Sprintf("collision-%04d", i), 7); !ok || got != want {
			t.Fatalf("collision lookup %d: %d/%v", i, got, ok)
		}
	}
	if got, ok := index.lookupHash("", 7); !ok || got != 897 {
		t.Fatal("empty ID lost", got, ok)
	}
	if _, ok := index.lookupHash("absent", 7); ok {
		t.Fatal("absent collision ID matched")
	}
	if index.count != 898 || len(index.cells) != originalCapacity || charged != originalCharge {
		t.Fatal("collision changed capacity/admission", index.count, len(index.cells), charged)
	}
	for _, n := range []int{896, 897, 4096, 4097} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			native, err := newLocalIDIndex(0, func(uint64) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			legacy := make(hybridScalarAllowSet, n)
			for i := 0; i < n; i++ {
				id := []byte(fmt.Sprintf("doc-%05d", i))
				key := string(id)
				legacy[key] = struct{}{}
				if err := native.setCopy(id, i); err != nil {
					t.Fatal(err)
				}
				clear(id)
			}
			carrier := nativeScalarIDs(native)
			if !carrier.present() || carrier.len() != n {
				t.Fatal("cardinality", carrier.len())
			}
			for id := range legacy {
				if !carrier.contains(id) {
					t.Fatal("borrowed ID mutated", id)
				}
			}
			for id := range carrier.all {
				if len(id) > 0 && id[len(id)-1]%2 == 0 {
					carrier.delete(id)
					delete(legacy, id)
				}
			}
			if carrier.len() != len(legacy) {
				t.Fatal("delete-only refinement cardinality")
			}
			for id := range carrier.all {
				if _, ok := legacy[id]; !ok {
					t.Fatal("delete-only refinement", id)
				}
			}
		})
	}
	if (hybridScalarIDs{}).present() || legacyScalarIDs(nil).present() {
		t.Fatal("nil filter became present")
	}
	empty, err := newLocalIDIndex(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !nativeScalarIDs(empty).present() || !legacyScalarIDs(hybridScalarAllowSet{}).present() {
		t.Fatal("present empty filter became absent")
	}
	if _, err = newLocalIDIndex(int(^uint(0)>>1), nil); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("host overflow", err)
	}
}

func TestNativeLocalIDIndexHalfLoadBoundary(t *testing.T) {
	for _, tc := range []struct {
		hint, count, capacity int
		replacements          []int
	}{
		{hint: 0, count: 4, capacity: 8},
		{hint: 64, count: 128, capacity: 256},
		{hint: 64, count: 256, capacity: 512, replacements: []int{512}},
	} {
		t.Run(fmt.Sprintf("hint%d-count%d", tc.hint, tc.count), func(t *testing.T) {
			var charged uint64
			index, err := newLocalIDIndex(tc.hint, func(n uint64) error {
				charged += n
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			wantCharge := charged
			for i := 0; i < tc.count; i++ {
				id := []byte(fmt.Sprintf("id-%04d", i))
				if err := index.setCopy(id, i); err != nil {
					t.Fatal(err)
				}
				wantCharge += memtable.COWAllocationCharge(uint64(len(id)))
				clear(id)
			}
			for _, capacity := range tc.replacements {
				backing, err := localIDCellsCharge(capacity)
				if err != nil {
					t.Fatal(err)
				}
				wantCharge += backing
			}
			if index.count != tc.count || index.used != tc.count || len(index.cells) != tc.capacity || charged != wantCharge {
				t.Fatalf("half load: count=%d used=%d capacity=%d charge=%d; want %d/%d/%d/%d", index.count, index.used, len(index.cells), charged, tc.count, tc.count, tc.capacity, wantCharge)
			}
			for i := 0; i < tc.count; i++ {
				if got, ok := index.lookup(fmt.Sprintf("id-%04d", i)); !ok || got != i {
					t.Fatalf("owned lookup %d: %d/%v", i, got, ok)
				}
			}
			// Updating an existing key at half load must not replace backing.
			if err := index.setBorrowed("id-0000", 99); err != nil {
				t.Fatal(err)
			}
			if err := index.setCopy([]byte("id-0000"), 100); err != nil {
				t.Fatal(err)
			}
			if got, ok := index.lookup("id-0000"); !ok || got != 99 || index.count != tc.count || index.used != tc.count || len(index.cells) != tc.capacity || charged != wantCharge {
				t.Fatal("replacement changed table size, receipt, or copyKey's existing ordinal", got, ok)
			}
			const next = "next-distinct"
			if err := index.setCopy([]byte(next), tc.count); err != nil {
				t.Fatal(err)
			}
			backing, err := localIDCellsCharge(2 * tc.capacity)
			if err != nil {
				t.Fatal(err)
			}
			wantCharge += backing + memtable.COWAllocationCharge(uint64(len(next)))
			if index.count != tc.count+1 || index.used != tc.count+1 || len(index.cells) != 2*tc.capacity || charged != wantCharge {
				t.Fatal("next distinct insertion did not admit complete replacement backing plus key", index.count, index.used, len(index.cells), charged, wantCharge)
			}
			if got, ok := index.lookup(next); !ok || got != tc.count {
				t.Fatal("growth lost new key", got, ok)
			}
		})
	}
}

func TestNativeLocalIDIndexHalfLoadCollisionTombstoneRefusal(t *testing.T) {
	var charged, requested uint64
	refuse := false
	index, err := newLocalIDIndex(0, func(n uint64) error {
		requested = n
		if refuse {
			return memtable.ErrCOWCapacity
		}
		charged += n
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Select real colliding keys so production lookup and deletion traverse
	// a wrapped cluster, including tombstones before a surviving key.
	var keys []string
	for i := 0; len(keys) < 6; i++ {
		key := fmt.Sprintf("collision-%d", i)
		if maphash.String(index.seed, key)&7 == 7 {
			keys = append(keys, key)
		}
	}
	initialCharge := charged
	for i := 0; i < 4; i++ {
		if err := index.setBorrowed(keys[i], i); err != nil {
			t.Fatal(err)
		}
	}
	if index.count != 4 || index.used != 4 || len(index.cells) != 8 || charged != initialCharge {
		t.Fatal("collision cluster prematurely replaced backing", index.count, index.used, len(index.cells), charged)
	}
	ids := nativeScalarIDs(index)
	ids.delete(keys[1])
	ids.delete(keys[2])
	ids.delete(keys[1])
	if got, ok := index.lookup(keys[3]); !ok || got != 3 || index.count != 2 || index.used != 4 {
		t.Fatal("deletion lost colliding survivor or hole accounting", got, ok, index.count, index.used)
	}
	if _, ok := index.lookup(keys[4]); ok {
		t.Fatal("missing colliding key matched across tombstones")
	}
	if err := index.setBorrowed(keys[3], 77); err != nil {
		t.Fatal(err)
	}
	before := append([]localIDCell(nil), index.cells...)
	backing, err := localIDCellsCharge(8)
	if err != nil {
		t.Fatal(err)
	}
	refuse = true
	if err := index.setBorrowed(keys[4], 4); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("same-capacity rebuild did not propagate admission refusal", err)
	}
	if requested != backing || charged != initialCharge || index.count != 2 || index.used != 4 || !reflect.DeepEqual(index.cells, before) {
		t.Fatal("refused tombstone rebuild changed storage or omitted full backing admission")
	}
	refuse = false
	if err := index.setBorrowed(keys[4], 4); err != nil {
		t.Fatal(err)
	}
	if index.count != 3 || index.used != 3 || len(index.cells) != 8 || charged != initialCharge+backing {
		t.Fatal("tombstone rebuild grew capacity, retained holes, or refunded old backing", index.count, index.used, len(index.cells), charged)
	}
	for _, i := range []int{0, 3, 4} {
		want := i
		if i == 3 {
			want = 77
		}
		if got, ok := index.lookup(keys[i]); !ok || got != want {
			t.Fatal("rebuild lost colliding live key", i, got, ok)
		}
	}
	for _, i := range []int{1, 2} {
		if _, ok := index.lookup(keys[i]); ok {
			t.Fatal("rebuild resurrected deleted key", i)
		}
	}
	if err := index.setBorrowed(keys[5], 5); err != nil {
		t.Fatal(err)
	}
	if index.count != 4 || index.used != 4 || len(index.cells) != 8 || charged != initialCharge+backing {
		t.Fatal("rebuilt table did not retain half-load boundary")
	}
}

func TestNativeLocalIDIndexOriginalReceiptGrowthRefusal(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	var charged uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	index, err := newLocalIDIndex(0, admit)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := index.setCopy([]byte(fmt.Sprint(i)), i); err != nil {
			t.Fatal(err)
		}
	}
	oldCharge, oldCapacity := charged, len(index.cells)
	before := budget.Stats()
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	blocker, err := budget.AcquireExternal(limits.MaxInFlightBytes - before.ReservedBytes - header)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.setCopy([]byte("growth"), 4); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("actual full-parent growth refusal", err)
	}
	if index.count != 4 || len(index.cells) != oldCapacity || charged != oldCharge {
		t.Fatal("refusal changed existing table/cumulative receipt")
	}
	for i := 0; i < 4; i++ {
		if got, ok := index.lookup(fmt.Sprint(i)); !ok || got != i {
			t.Fatal("refusal lost old content")
		}
	}
	blocker.Close()
	if err := index.setCopy([]byte("growth"), 4); err != nil {
		t.Fatal(err)
	}
	newBacking, err := localIDCellsCharge(2 * oldCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if charged != oldCharge+newBacking+memtable.COWAllocationCharge(uint64(len("growth"))) {
		t.Fatal("old backing was refunded or new backing/key omitted", oldCharge, charged, newBacking)
	}
	index = nil
	admit = nil
	lease.Close()
	if stats := budget.Stats(); stats.ReservedBytes != 0 || stats.ExternalLeases != 0 || stats.TotalBytes != stats.ControlBytes {
		t.Fatal("lease Close retained operation storage", stats)
	}
	budget.Close()
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatal("Close retained original admission", stats)
	}
}

func TestNativeLocalIDFusionAndAdaptiveProofParity(t *testing.T) {
	candidates := []HybridSearchCandidate{
		hybridFusionCandidate("parent#0", HybridCandidateSourceText, 1, 3, HybridScoreKindBM25),
		hybridFusionCandidate("parent#1", HybridCandidateSourceText, 2, 2, HybridScoreKindBM25),
		hybridFusionCandidate("same", HybridCandidateSourceText, 3, 1, HybridScoreKindBM25),
		hybridFusionCandidate("same", HybridCandidateSourceVector, 1, .9, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("parent#0", HybridCandidateSourceVector, 2, .8, HybridScoreKindVectorSimilarity),
		hybridFusionCandidate("same", HybridCandidateSourceText, 4, .5, HybridScoreKindBM25),
		hybridFusionCandidate("tie", HybridCandidateSourceVector, 2, .8, HybridScoreKindVectorSimilarity),
	}
	for _, method := range []HybridFusionMethod{HybridFusionMethodRRF, HybridFusionMethodWeightedRRF, HybridFusionMethodNormalizedScore} {
		opts := HybridFusionOptions{Method: method}
		want, wantStats, err := FuseHybridSearchCandidates(candidates, opts, 4)
		if err != nil {
			t.Fatal(err)
		}
		var charged uint64
		admit := func(n uint64) error { charged += n; return nil }
		got, gotStats, err := fuseHybridSearchCandidatesAdmitted(candidates, opts, 4, admit)
		if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotStats, wantStats) || charged == 0 {
			t.Fatalf("%s fusion parity: %v\n%+v\n%+v", method, err, got, want)
		}
		// Mutating source IDs after return cannot mutate independently owned output.
		clone := append([]byte(nil), candidates[0].ID...)
		candidates[0].ID[0] ^= 1
		if !reflect.DeepEqual(got, want) {
			t.Fatal("fused output retained input ID alias")
		}
		copy(candidates[0].ID, clone)
		params := hybridCandidateBudgetRRFParams{rrfK: 60, textWeight: 1, vectorWeight: 1}
		state := [2]hybridCandidateBudgetSourceState{{present: true, requested: 8, effective: 4, returned: 4}, {present: true, requested: 8, effective: 3, returned: 3}}
		expected := hybridCandidateBudgetExactRRFStop(candidates, want, state, params, 4)
		actual, err := hybridCandidateBudgetExactRRFStopAdmitted(candidates, got, state, params, 4, admit)
		if err != nil || actual != expected {
			t.Fatal("adaptive exact stop parity", actual, expected, err)
		}
		nativeStats, legacyStats := gotStats, wantStats
		collapsed, err := hybridCollapseResultsByParentAdmitted(got, 4, 1, &nativeStats, admit)
		reference := hybridCollapseResultsByParent(want, 4, 1, &legacyStats)
		if err != nil || !reflect.DeepEqual(collapsed, reference) || !reflect.DeepEqual(nativeStats, legacyStats) {
			t.Fatal("parent collapse parity", err)
		}
	}
	denied := func(uint64) error { return memtable.ErrCOWCapacity }
	if out, _, err := fuseHybridSearchCandidatesAdmitted(candidates, HybridFusionOptions{}, 4, denied); !errors.Is(err, memtable.ErrCOWCapacity) || out != nil {
		t.Fatal("fusion allocation refusal", out, err)
	}
	params := hybridCandidateBudgetRRFParams{rrfK: 60, textWeight: 1, vectorWeight: 1}
	if _, ok, err := hybridCandidateBudgetProofAccumulatorsAdmitted(candidates, params, denied); !errors.Is(err, memtable.ErrCOWCapacity) || ok {
		t.Fatal("adaptive proof allocation refusal", ok, err)
	}
}

func TestNativeLocalIDScannerStableAndSentinelParity(t *testing.T) {
	const rows = 4097
	table := newCollectionRunTable(2 * rows)
	defer resetCollectionRunTable(table)
	for _, value := range []string{"a", "b"} {
		component, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString(value)})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < rows; i++ {
			id := []byte(fmt.Sprintf("id-%05d", i))
			if i == 10 {
				id = []byte("escaped\x00id")
			}
			key, err := bsonIndexEntryKeyV2(component, id)
			if err != nil {
				t.Fatal(err)
			}
			setCollectionRunValue(table, key, nil)
		}
	}
	table.Freeze()
	for _, stable := range []bool{false, true} {
		for _, limit := range []int{4096, 4097} {
			type outcome struct {
				ids       []string
				inspected int
				truncated bool
			}
			run := func(admit func(uint64) error) outcome {
				it := table.NewIterator(nil, nil)
				defer it.Close()
				out := outcome{}
				opts := scanMergedCollectionIndexIDOptions{DedupeDocumentID: true, StableDocumentIDTies: stable, LogicalIndexKey: bsonIndexKeyValuePrefixV2, Inspected: &out.inspected, admitTemporary: admit}
				var err error
				out.truncated, err = scanMergedCollectionIndexIDsWithOptionsAndDirectionWorkCap(nil, it, IndexValueBSONOrderedV2, limit, false, 2*rows, opts, func(id []byte) (bool, error) { out.ids = append(out.ids, string(id)); return true, nil })
				if err != nil {
					t.Fatal(err)
				}
				return out
			}
			want := run(nil)
			var charged uint64
			got := run(func(n uint64) error { charged += n; return nil })
			if !reflect.DeepEqual(got, want) || len(got.ids) != limit || got.truncated != (limit < rows) || charged == 0 {
				t.Fatalf("stable=%v limit=%d scanner parity: sizes %d/%d inspected %d/%d truncated %v/%v", stable, limit, len(got.ids), len(want.ids), got.inspected, want.inspected, got.truncated, want.truncated)
			}
		}
	}
	// Allocating BSON escapes must refuse before decoder allocation/output.
	component, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString("x")})
	if err != nil {
		t.Fatal(err)
	}
	key, err := bsonIndexEntryKeyV2(component, []byte("x\x00y"))
	if err != nil {
		t.Fatal(err)
	}
	if id, err := indexKeyDocumentIDAdmitted(IndexValueBSONOrderedV2, key, func(uint64) error { return memtable.ErrCOWCapacity }); !errors.Is(err, memtable.ErrCOWCapacity) || id != nil {
		t.Fatal("escape decoding refused too late", id, err)
	}
	id, err := indexKeyDocumentIDAdmitted(IndexValueBSONOrderedV2, key, nil)
	if err != nil || !bytes.Equal(id, []byte("x\x00y")) {
		t.Fatal("escaped ID parity", id, err)
	}
}
