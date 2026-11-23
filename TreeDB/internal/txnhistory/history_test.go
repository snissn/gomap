package txnhistory

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/batch"
	"math/rand"
	"slices"
	"testing"
	"unsafe"
)

type countedHistoryLease struct{ closes int }

func (l *countedHistoryLease) Close() { l.closes++ }
func TestHistoryPreparedCopiesCannotActOnSuccessorsAndLastSlotOwnsLease(t *testing.T) {
	var leases []*countedHistoryLease
	j, err := New(8, 1<<20, func(a, b, c, d, e uint64) (Lease, error) {
		l := new(countedHistoryLease)
		leases = append(leases, l)
		return l, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := j.Prepare([]batch.Entry{{Key: []byte("a")}, {Key: []byte("b")}, {Key: []byte("c")}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	old := p
	if !j.Install(p, 1) {
		t.Fatal("live install failed")
	}
	old.Cancel()
	if j.Install(old, 99) {
		t.Fatal("consumed capability installed twice")
	}
	next, err := j.Prepare([]batch.Entry{{Key: bytes.Repeat([]byte("unaccepted"), 8)}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	old.Cancel()
	if j.Install(old, 100) || !next.Valid() {
		t.Fatal("stale capability affected successor")
	}
	next.Cancel()
	next.Cancel()
	if leases[2].closes != 1 || leases[1].closes != 0 {
		t.Fatalf("cancel/install custody: %+v", leases)
	}
	for n := 0; n < 2; n++ {
		j.Evict()
		if leases[1].closes != 0 {
			t.Fatal("prefix eviction released full key arena")
		}
	}
	j.Evict()
	if leases[1].closes != 1 {
		t.Fatal("final slot did not release key arena")
	}
	j.Close()
	j.Close()
	if leases[0].closes != 1 {
		t.Fatal("control double release")
	}
}

func TestHistoryShortPointPreparationUsesOriginalSlotWithoutAllocation(t *testing.T) {
	j, _ := New(8, 1<<20, nil)
	defer j.Close()
	entry := []batch.Entry{{Key: []byte("owned-key")}}
	seq := uint64(0)
	if got := testing.AllocsPerRun(100, func() {
		p, err := j.Prepare(entry, nil, false)
		if err != nil {
			panic(err)
		}
		seq++
		if !j.Install(p, seq) {
			panic("install")
		}
	}); got != 0 {
		t.Fatalf("short point preparation allocated %g, want original FIFO-slot custody", got)
	}
}

func TestHistoryInlinePointCustodySurvivesCancelWrapAndCallerReuse(t *testing.T) {
	var leases []*countedHistoryLease
	j, _ := New(8, 1<<20, func(a, b, c, d, e uint64) (Lease, error) {
		l := new(countedHistoryLease)
		leases = append(leases, l)
		return l, nil
	})
	defer j.Close()
	want := make(map[uint64]string)
	var stale Prepared
	for seq := uint64(1); seq <= 64; seq++ {
		// Cancel at the same vacant slot that the next accepted key will use.
		canceled, err := j.Prepare([]batch.Entry{{Key: []byte("canceled")}}, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		canceled.Cancel()
		key := []byte(fmt.Sprintf("key-%d", seq%3))
		want[seq] = string(key)
		p, err := j.Prepare([]batch.Entry{{Key: key}}, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		clear(key) // The prepared command already owns its input.
		canceled.Cancel()
		stale.Cancel()
		if !p.Valid() || !j.Install(p, seq) || j.Install(canceled, seq+100) {
			t.Fatal("stale preparation changed current custody")
		}
		stale = p
		for k := 0; k < j.count; k++ {
			r := &j.records[(j.head+k)%len(j.records)]
			if r.key != want[r.seq] {
				t.Fatalf("record %d key=%q want=%q", r.seq, r.key, want[r.seq])
			}
		}
		if got := j.index[j.slot(want[seq])]; got.key != want[seq] || got.seq != seq {
			t.Fatalf("latest index lost accepted key: %+v", got)
		}
	}
	if len(leases) != 1 {
		t.Fatalf("inline keys allocated dynamic leases: %d", len(leases))
	}
	j.Close()
	if leases[0].closes != 1 {
		t.Fatal("original ring charge did not drain")
	}
}

func TestHistoryRefusedOrCanceledPreparationPreservesAcceptedHistory(t *testing.T) {
	for _, capacity := range []string{"slots", "bytes"} {
		for _, refusal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/refusal=%t", capacity, refusal), func(t *testing.T) {
				limit, accepted := uint64(1<<20), 8
				if capacity == "bytes" {
					limit, accepted = 1024, 2
				}
				denied := errors.New("history staging admission refused")
				deny := false
				var leases []*countedHistoryLease
				j, err := New(8, limit, func(a, b, c, d, e uint64) (Lease, error) {
					if deny {
						return nil, denied
					}
					lease := new(countedHistoryLease)
					leases = append(leases, lease)
					return lease, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				defer j.Close()
				keys := make([][]byte, accepted)
				for i := range keys {
					keys[i] = bytes.Repeat([]byte{byte('a' + i)}, 64)
					p, err := j.Prepare([]batch.Entry{{Key: keys[i]}}, nil, false)
					if err != nil || !j.Install(p, uint64(i+1)) {
						t.Fatalf("seed history %d: %v", i, err)
					}
				}
				count, used, floor := j.State()
				if floor != 0 {
					t.Fatalf("fixture already expired: %d", floor)
				}
				deny = refusal
				key := []byte("private inline key")
				if refusal || capacity == "bytes" {
					key = bytes.Repeat([]byte("z"), 64)
				}
				p, err := j.Prepare([]batch.Entry{{Key: key}}, nil, false)
				if refusal {
					if !errors.Is(err, denied) || p.Valid() {
						t.Fatalf("refused preparation=%+v err=%v", p, err)
					}
				} else if err != nil || !p.Valid() {
					t.Fatalf("cancellable preparation=%+v err=%v", p, err)
				}
				for _, beforeCancel := range []bool{true, false} {
					if !beforeCancel {
						p.Cancel()
					}
					if n, b, f := j.State(); n != count || b != used || f != floor {
						t.Fatalf("unaccepted preparation changed history: %d/%d/%d want %d/%d/%d", n, b, f, count, used, floor)
					}
					if err := j.Check(0); err != nil {
						t.Fatalf("unaccepted preparation expired existing reader: %v", err)
					}
					for _, key := range keys {
						if !j.PointChanged(0, key) {
							t.Fatalf("accepted conflict evidence lost for %q", key)
						}
					}
					for _, lease := range leases[:accepted+1] {
						if lease.closes != 0 {
							t.Fatal("unaccepted preparation released accepted custody")
						}
					}
				}
				deny = false
				p, err = j.Prepare([]batch.Entry{{Key: key}}, nil, false)
				if err != nil || !j.Install(p, uint64(accepted+1)) {
					t.Fatalf("accepted replacement: %v", err)
				}
				if !errors.Is(j.Check(0), ErrTooOld) {
					t.Fatal("accepted replacement did not advance bounded-history floor")
				}
			})
		}
	}
}

func TestHistoryBarrierReasonsOnlyAdvanceOnInstall(t *testing.T) {
	j, _ := New(8, 4096, nil)
	defer j.Close()
	entries := make([]batch.Entry, 9)
	p, err := j.Prepare(entries, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	p.Cancel()
	if got := j.BarrierStats(); got != (BarrierStats{}) {
		t.Fatalf("canceled barrier=%+v", got)
	}
	p, err = j.Prepare(entries, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	j.Install(p, 1)
	if !j.BarrierConflict(0) {
		t.Fatal("accepted capacity barrier omitted")
	}
	if got := j.BarrierStats(); got.EffectCapacity != 1 || got.Conflicts != 1 || !got.Active {
		t.Fatalf("accepted barrier=%+v", got)
	}
	p, err = j.Prepare([]batch.Entry{{Key: make([]byte, 2049)}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	j.Install(p, 2)
	p, err = j.Prepare(nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	j.Install(p, 3)
	if got := j.BarrierStats(); got.ByteCapacity != 1 || got.LogicalReplacement != 1 || got.EffectCapacity != 1 || got.Conflicts != 1 {
		t.Fatalf("reasons=%+v", got)
	}
}

func TestHistorySortedChunkQueriesMatchLinearReferenceAcrossWrap(t *testing.T) {
	j, _ := New(32, 1<<20, nil)
	defer j.Close()
	for seq := uint64(1); seq <= 16; seq++ {
		entries := []batch.Entry{{Key: []byte("z")}, {Key: []byte("a")}, {Key: []byte("m")}, {Key: []byte("d")}}
		p, err := j.Prepare(entries, []batch.DeleteRange{{Start: []byte("g"), End: []byte("j")}}, false)
		if err != nil {
			t.Fatal(err)
		}
		j.Install(p, seq)
		for start := j.floor; start <= seq+1; start++ {
			for _, bounds := range []Range{{nil, nil}, {nil, []byte{}}, {[]byte("a"), []byte("b")}, {[]byte("b"), []byte("d")}, {[]byte("h"), []byte("i")}, {[]byte("z"), nil}, {[]byte("zz"), nil}} {
				want := false
				if !batch.IsDeleteRangeNoop(bounds.Start, bounds.End) {
					for k := 0; k < j.count; k++ {
						e := j.records[(j.head+k)%len(j.records)]
						if e.seq <= start {
							continue
						}
						if e.rangeEffect {
							want = want || (bounds.End == nil || e.key < string(bounds.End)) && (!e.endBounded || string(bounds.Start) < e.end)
						} else {
							want = want || (bounds.Start == nil || bytes.Compare([]byte(e.key), bounds.Start) >= 0) && (bounds.End == nil || bytes.Compare([]byte(e.key), bounds.End) < 0)
						}
					}
				}
				if got := j.RangeChanged(start, bounds.Start, bounds.End); got != want {
					t.Fatalf("seq%d floor%d start%d [%q,%q): got%v want%v", seq, j.floor, start, bounds.Start, bounds.End, got, want)
				}
			}
		}
	}
}

func historyCommit(t *testing.T, j *History, seq uint64, keys ...string) {
	t.Helper()
	entries := make([]batch.Entry, len(keys))
	for i, k := range keys {
		entries[i] = batch.Entry{Type: batch.OpPut, Key: []byte(k)}
	}
	p, err := j.Prepare(entries, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	j.Install(p, seq)
}
func TestTxnJournalPartialEvictionFloor(t *testing.T) {
	j, _ := New(8, 1<<20, nil)
	defer j.Close()
	historyCommit(t, j, 1, "a", "b", "c")
	historyCommit(t, j, 2, "a")
	j.Evict()
	if err := j.Check(0); !errors.Is(err, ErrTooOld) {
		t.Fatalf("partially evicted commit remained eligible: %v", err)
	}
	if err := j.Check(1); err != nil {
		t.Fatal(err)
	}
	if !j.PointChanged(1, []byte("a")) {
		t.Fatal("evicting older key erased newer overwrite")
	}
	j.Evict()
	j.Evict()
	if !j.PointChanged(1, []byte("a")) {
		t.Fatal("newer overwrite lost after full old commit eviction")
	}
}

func TestHistoryNewerSuffixAcrossWrapAndSharedSequences(t *testing.T) {
	j, _ := New(8, 1<<20, nil)
	defer j.Close()
	for seq := uint64(1); seq <= 6; seq++ {
		historyCommit(t, j, seq, "old")
	}
	for n := 0; n < 4; n++ {
		j.Evict()
	}
	historyCommit(t, j, 7, "a", "b", "c")
	historyCommit(t, j, 8, "z")
	for _, tc := range []struct {
		start uint64
		want  int
	}{{0, 0}, {4, 0}, {5, 1}, {6, 2}, {7, 5}, {8, 6}, {99, 6}} {
		if got := j.firstNewer(tc.start); got != tc.want {
			t.Fatalf("firstNewer(%d)=%d want%d", tc.start, got, tc.want)
		}
	}
	if j.RangeChanged(7, []byte("a"), []byte("d")) || !j.RangeChanged(6, []byte("b"), []byte("c")) {
		t.Fatal("suffix scan crossed shared sequence boundary")
	}
}
func TestHistoryExactRangeABAOwnedKeysAndFutureBarrier(t *testing.T) {
	j, _ := New(8, 1<<20, nil)
	defer j.Close()
	key := []byte("b")
	p, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: key}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	j.Install(p, 1)
	key[0] = 'z'
	if !j.RangeChanged(0, []byte("a"), []byte("c")) || j.RangeChanged(0, []byte("c"), []byte("d")) {
		t.Fatal("incorrect owned key/range intersection")
	}
	if j.RangeChanged(0, nil, []byte{}) {
		t.Fatal("empty exclusive end became unbounded")
	}
	p, err = j.Prepare(nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if j.PointChanged(1, []byte("any")) {
		t.Fatal("unaccepted future barrier invalidated start")
	}
	j.Install(p, 2)
	if !j.PointChanged(1, []byte("any")) {
		t.Fatal("accepted barrier omitted")
	}
	historyCommit(t, j, 3, "b")
	historyCommit(t, j, 4, "b")
	if !j.PointChanged(3, []byte("b")) {
		t.Fatal("ABA effect absent")
	}
}
func TestHistoryNativeRangeAndBoundedRetention(t *testing.T) {
	j, _ := New(8, 4096, nil)
	defer j.Close()
	p, err := j.Prepare(nil, []batch.DeleteRange{{Start: []byte("b"), End: []byte("d")}}, false)
	if err != nil {
		t.Fatal(err)
	}
	j.Install(p, 1)
	if !j.PointChanged(0, []byte("b")) || j.PointChanged(0, []byte("d")) {
		t.Fatal("native range boundaries")
	}
	for n := uint64(2); n < 1000; n++ {
		historyCommit(t, j, n, "x")
	}
	count, bytes, floor := j.State()
	if count > 8 || bytes > 4096 || floor == 0 {
		t.Fatalf("unbounded history %d %d %d", count, bytes, floor)
	}
}

func verifyHistoryInvariants(t *testing.T, j *History) {
	t.Helper()
	recordMask := len(j.records) - 1
	indexMask := uint32(len(j.index) - 1)

	live := make([]bool, len(j.records))
	liveRanges := 0
	for k := 0; k < j.count; k++ {
		slot := (j.head + k) & recordMask
		live[slot] = true
		rec := &j.records[slot]
		if rec.seq == 0 {
			t.Fatalf("live record slot %d has zero seq", slot)
		}
		if rec.rangeEffect {
			liveRanges++
		}
		if rec.rangeEffect || rec.barrier {
			if rec.indexSlot != -1 {
				t.Fatalf("non-point record slot %d has indexSlot=%d want -1", slot, rec.indexSlot)
			}
		} else if rec.indexSlot >= 0 {
			idx := int(rec.indexSlot)
			if idx >= len(j.index) {
				t.Fatalf("record slot %d indexSlot=%d out of range", slot, idx)
			}
			ent := j.index[idx]
			if ent.seq != rec.seq || ent.namespace != rec.namespace || ent.key != rec.key || ent.recordSlot != int32(slot) {
				t.Fatalf("record slot %d -> index[%d] mismatch: rec=%+v ent=%+v", slot, idx, *rec, ent)
			}
		} else if rec.indexSlot == -1 {
			idx := j.slotIn(rec.namespace, rec.key)
			if j.index[idx].seq <= rec.seq {
				t.Fatalf("superseded record slot %d (seq=%d key=%q) lacks newer index entry: got %+v", slot, rec.seq, rec.key, j.index[idx])
			}
		} else {
			t.Fatalf("record slot %d has invalid indexSlot=%d", slot, rec.indexSlot)
		}
	}
	if liveRanges != j.ranges {
		t.Fatalf("ranges count=%d want %d", j.ranges, liveRanges)
	}
	for slot := range j.records {
		if !live[slot] && j.records[slot] != (mutation{}) {
			t.Fatalf("vacant record slot %d not zeroed: %+v", slot, j.records[slot])
		}
	}

	occupied := 0
	for idx, ent := range j.index {
		if ent.seq == 0 {
			if ent != (latest{}) {
				t.Fatalf("empty index slot %d not zeroed: %+v", idx, ent)
			}
			continue
		}
		occupied++
		wantHome := uint32(j.hashKey(ent.namespace, ent.key)) & indexMask
		if ent.home != wantHome {
			t.Fatalf("index[%d].home=%d want %d", idx, ent.home, wantHome)
		}
		if got := j.slotIn(ent.namespace, ent.key); got != idx {
			t.Fatalf("slotIn(%+v, %q)=%d want %d (cluster broken after backward shift)", ent.namespace, ent.key, got, idx)
		}
		if ent.recordSlot < 0 || int(ent.recordSlot) >= len(j.records) || !live[ent.recordSlot] {
			t.Fatalf("index[%d].recordSlot=%d is not live", idx, ent.recordSlot)
		}
		rec := &j.records[ent.recordSlot]
		if rec.indexSlot != int32(idx) || rec.seq != ent.seq || rec.namespace != ent.namespace || rec.key != ent.key {
			t.Fatalf("index[%d] -> record[%d] mismatch: ent=%+v rec=%+v", idx, ent.recordSlot, ent, *rec)
		}
	}
	if occupied != j.points {
		t.Fatalf("points count=%d want %d", j.points, occupied)
	}
}

func TestHistoryBackwardShiftWrapAround(t *testing.T) {
	j, err := New(8, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	indexMask := uint32(len(j.index) - 1) // 15
	findKeysForHome := func(target uint32, want int) []string {
		var keys []string
		for i := 0; len(keys) < want && i < 100000; i++ {
			k := fmt.Sprintf("wrap-%d-%d", target, i)
			if uint32(j.hashKey(Namespace{}, k))&indexMask == target {
				keys = append(keys, k)
			}
		}
		if len(keys) != want {
			t.Fatalf("failed to find %d keys for home %d", want, target)
		}
		return keys
	}

	h14 := findKeysForHome(14, 3)
	h15 := findKeysForHome(15, 2)
	h0 := findKeysForHome(0, 2)
	h1 := findKeysForHome(1, 1)

	// Build a contiguous cluster wrapping across 14 -> 15 -> 0 -> 1 -> 2 -> 3 -> 4 -> 5.
	// Interleave homes so backward-shift encounters both entries that must shift across
	// the 15 -> 0 boundary and entries whose home is at or after the hole.
	ordered := []string{h14[0], h15[0], h0[0], h14[1], h15[1], h1[0], h14[2], h0[1]}
	seq := uint64(0)
	for _, k := range ordered {
		seq++
		historyCommit(t, j, seq, k)
		verifyHistoryInvariants(t, j)
	}

	// Verify cluster actually spans the wrap boundary (slots 14, 15, 0, 1, ...).
	for _, s := range []int{14, 15, 0, 1, 2, 3, 4, 5} {
		if j.index[s].seq == 0 {
			t.Fatalf("expected wrapped cluster slot %d to be occupied", s)
		}
	}

	// Overwrite a key in the middle of the wrapped cluster, forcing eviction of h14[0]
	// and superseding h14[1], then continue rotating through all slots.
	for _, k := range []string{h14[1], h0[0], h15[0], h14[2], h1[0], h0[1], h15[1], h14[0]} {
		seq++
		historyCommit(t, j, seq, k)
		verifyHistoryInvariants(t, j)
		if !j.PointChanged(seq-1, []byte(k)) {
			t.Fatalf("installed key %q missing from index", k)
		}
	}

	// Drain completely via Evict(), checking invariants after every shift.
	for j.count > 0 {
		j.Evict()
		verifyHistoryInvariants(t, j)
	}
	if pts, rngs, bar := j.Counts(); pts != 0 || rngs != 0 || bar {
		t.Fatalf("post-drain counts=%d/%d/%v want 0/0/false", pts, rngs, bar)
	}
}

func TestHistoryInlineFastPathEdgeCases(t *testing.T) {
	t.Run("empty-key-and-zero-alloc", func(t *testing.T) {
		j, err := New(8, 1<<20, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()

		entryNil := []batch.Entry{{Type: batch.OpPut, Key: nil}}
		entryEmpty := []batch.Entry{{Type: batch.OpPut, Key: []byte{}}}
		seq := uint64(0)
		if got := testing.AllocsPerRun(50, func() {
			seq++
			e := entryNil
			if seq%2 == 0 {
				e = entryEmpty
			}
			p, err := j.Prepare(e, nil, false)
			if err != nil || !j.Install(p, seq) {
				panic("empty key install failed")
			}
		}); got != 0 {
			t.Fatalf("empty key inline fast path allocated %g, want 0", got)
		}
		verifyHistoryInvariants(t, j)
		if !j.PointChanged(seq-1, nil) || !j.PointChanged(seq-1, []byte{}) {
			t.Fatal("empty key not found in latest index")
		}
		if !j.RangeChanged(seq-1, nil, []byte{0}) || j.RangeChanged(seq-1, []byte{0}, nil) {
			t.Fatal("empty key range lookup mismatch")
		}
		for j.count > 0 {
			j.Evict()
			verifyHistoryInvariants(t, j)
		}
	})

	t.Run("inline-32-vs-33-boundary", func(t *testing.T) {
		var dynamicAdmits int
		j, err := New(8, 1<<20, func(a, b, c, d, e uint64) (Lease, error) {
			dynamicAdmits++
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()
		dynamicAdmits = 0

		k32 := bytes.Repeat([]byte("a"), 32)
		entry32 := []batch.Entry{{Type: batch.OpPut, Key: k32}}
		seq := uint64(0)
		if got := testing.AllocsPerRun(50, func() {
			seq++
			p, err := j.Prepare(entry32, nil, false)
			if err != nil || !j.Install(p, seq) {
				panic("32-byte key install failed")
			}
		}); got != 0 {
			t.Fatalf("32-byte inline key allocated %g, want 0", got)
		}
		if dynamicAdmits != 0 {
			t.Fatalf("32-byte inline key triggered dynamic admit %d times", dynamicAdmits)
		}
		verifyHistoryInvariants(t, j)

		k33 := bytes.Repeat([]byte("b"), 33)
		p, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: k33}}, nil, false)
		if err != nil || !j.Install(p, seq+1) {
			t.Fatalf("33-byte key install failed: %v", err)
		}
		if dynamicAdmits != 1 {
			t.Fatalf("33-byte non-inline key dynamic admits=%d want 1", dynamicAdmits)
		}
		verifyHistoryInvariants(t, j)
	})

	t.Run("tight-maxBytes-barrier-collapse-and-capacity-error", func(t *testing.T) {
		baseCharge := uint64(unsafe.Sizeof(effect{})) + 256
		// Large enough for a collapsed barrier (baseCharge) and 10-byte inline key,
		// but too tight for a 12-byte inline key -> must fall through and collapse to barrier.
		j, err := New(8, baseCharge+10, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()

		p, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: bytes.Repeat([]byte("x"), 10)}}, nil, false)
		if err != nil || !j.Install(p, 1) {
			t.Fatalf("10-byte inline key at exact limit failed: %v", err)
		}
		if s := j.BarrierStats(); s.ByteCapacity != 0 || s.Active {
			t.Fatalf("exact-fit inline key unexpectedly collapsed: %+v", s)
		}
		verifyHistoryInvariants(t, j)

		p, err = j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: bytes.Repeat([]byte("y"), 12)}}, nil, false)
		if err != nil || !j.Install(p, 2) {
			t.Fatalf("12-byte key collapse failed: %v", err)
		}
		if s := j.BarrierStats(); s.ByteCapacity != 1 || !s.Active {
			t.Fatalf("12-byte key did not collapse to ByteCapacity barrier: %+v", s)
		}
		verifyHistoryInvariants(t, j)

		// maxBytes smaller than baseCharge must return ErrEffectCapacity even for empty/short keys.
		jTiny, err := New(8, baseCharge-1, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer jTiny.Close()
		if _, err := jTiny.Prepare([]batch.Entry{{Type: batch.OpPut, Key: []byte("a")}}, nil, false); !errors.Is(err, ErrEffectCapacity) {
			t.Fatalf("tiny maxBytes err=%v want ErrEffectCapacity", err)
		}
		if _, err := jTiny.Prepare([]batch.Entry{{Type: batch.OpPut, Key: nil}}, nil, false); !errors.Is(err, ErrEffectCapacity) {
			t.Fatalf("tiny maxBytes empty key err=%v want ErrEffectCapacity", err)
		}
	})

	t.Run("caller-key-mutation-and-delete-range-and-close-pending", func(t *testing.T) {
		j, err := New(8, 1<<20, nil)
		if err != nil {
			t.Fatal(err)
		}
		key := []byte("original-key")
		p, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: key}}, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		// Mutate caller slice BEFORE Install.
		copy(key, "MUTATED!!key")
		if !j.Install(p, 1) {
			t.Fatal("install failed")
		}
		if !j.PointChanged(0, []byte("original-key")) || j.PointChanged(0, []byte("MUTATED!!key")) {
			t.Fatal("caller key mutation before Install corrupted inline staging")
		}
		verifyHistoryInvariants(t, j)

		// Single OpDeleteRange entry with short key/value must NOT take the inline point fast path.
		p, err = j.Prepare([]batch.Entry{{Type: batch.OpDeleteRange, Key: []byte("a"), Value: []byte("d")}}, nil, false)
		if err != nil || !j.Install(p, 2) {
			t.Fatalf("single OpDeleteRange failed: %v", err)
		}
		if !j.PointChanged(1, []byte("b")) || j.PointChanged(1, []byte("d")) {
			t.Fatal("single OpDeleteRange did not record range effect")
		}
		verifyHistoryInvariants(t, j)

		// Active inline pending preparation must clean up cleanly on Close().
		if _, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: []byte("pending")}}, nil, false); err != nil {
			t.Fatal(err)
		}
		j.Close()
		if n, b, _ := j.State(); n != 0 || b != 0 {
			t.Fatalf("post-close state=%d/%d want 0/0", n, b)
		}
	})
}

func TestHistoryModelCheck(t *testing.T) {
	type refEffect struct {
		ns          Namespace
		key, end    string
		rangeEffect bool
		barrier     bool
		endBounded  bool
		seq         uint64
	}
	namespaces := []Namespace{
		{},
		{Kind: CatalogNamespace},
		{Kind: CollectionNamespace, Collection: 1, Domain: 1, Interpretation: 1},
		{Kind: CollectionNamespace, Collection: 2, Domain: 1, Interpretation: 1},
	}
	pointPool := []string{
		"",
		"a", "b", "c", "d", "e", "f", "g", "h",
		"key-00", "key-01", "key-02", "key-03", "key-04",
		string(bytes.Repeat([]byte("m"), 32)),
		string(bytes.Repeat([]byte("L"), 33)),
		string(bytes.Repeat([]byte("X"), 48)),
	}

	for _, ringSize := range []int{8, 16} {
		t.Run(fmt.Sprintf("ring=%d", ringSize), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(42 + ringSize)))
			j, err := New(ringSize, 1<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()

			var ref []refEffect
			refFloor := uint64(0)
			refBarrierSeq := uint64(0)
			seq := uint64(0)

			syncRefEvict := func() {
				for len(ref) > j.count {
					ev := ref[0]
					ref = ref[1:]
					if ev.seq > refFloor {
						refFloor = ev.seq
					}
				}
			}

			refPointChanged := func(start uint64, ns Namespace, key string) bool {
				if start < refFloor || (ns == (Namespace{}) && refBarrierSeq > start) {
					return true
				}
				for _, e := range ref {
					if e.seq <= start || e.ns != ns {
						continue
					}
					if e.barrier {
						return true
					}
					if e.rangeEffect {
						if key >= e.key && (!e.endBounded || key < e.end) {
							return true
						}
					} else if e.key == key {
						return true
					}
				}
				return false
			}

			const steps = 1500
			for step := 0; step < steps; step++ {
				op := rng.Intn(100)
				switch {
				case op < 10 && j.count > 0:
					// Explicit Evict.
					j.Evict()
					syncRefEvict()
				case op < 18:
					// Canceled inline or non-inline preparation; must have zero observable effect.
					k := []byte(pointPool[rng.Intn(len(pointPool))])
					p, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: k}}, nil, false)
					if err != nil {
						t.Fatalf("step %d cancel prepare: %v", step, err)
					}
					p.Cancel()
				case op < 62:
					// Single raw point (mostly inline fast path, sometimes 33+/48 bytes).
					orig := pointPool[rng.Intn(len(pointPool))]
					buf := append([]byte(nil), orig...)
					p, err := j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: buf}}, nil, false)
					if err != nil {
						t.Fatalf("step %d single prepare: %v", step, err)
					}
					for idx := range buf {
						buf[idx] ^= 0xff
					}
					seq++
					if !j.Install(p, seq) {
						t.Fatalf("step %d single install failed", step)
					}
					ref = append(ref, refEffect{ns: Namespace{}, key: orig, seq: seq})
					syncRefEvict()
				case op < 76:
					// Multi-entry raw batch with possible duplicate keys.
					cnt := 2 + rng.Intn(3)
					entries := make([]batch.Entry, cnt)
					seen := make(map[string]bool)
					var unique []string
					for i := 0; i < cnt; i++ {
						k := pointPool[rng.Intn(len(pointPool))]
						entries[i] = batch.Entry{Type: batch.OpPut, Key: []byte(k)}
						if !seen[k] {
							seen[k] = true
							unique = append(unique, k)
						}
					}
					p, err := j.Prepare(entries, nil, false)
					if err != nil {
						t.Fatalf("step %d batch prepare: %v", step, err)
					}
					seq++
					if !j.Install(p, seq) {
						t.Fatalf("step %d batch install failed", step)
					}
					slices.Sort(unique)
					for _, k := range unique {
						ref = append(ref, refEffect{ns: Namespace{}, key: k, seq: seq})
					}
					syncRefEvict()
				case op < 90:
					// Native semantic point or range or barrier.
					ns := namespaces[1+rng.Intn(len(namespaces)-1)]
					kindChoice := rng.Intn(10)
					var eff Effect
					var re refEffect
					if kindChoice < 7 {
						k := pointPool[rng.Intn(len(pointPool))]
						eff = Effect{Namespace: ns, Kind: PointEffect, Key: []byte(k)}
						re = refEffect{ns: ns, key: k}
					} else if kindChoice < 9 {
						lo := string([]byte{byte('a' + rng.Intn(6))})
						hi := string([]byte{byte(int(lo[0]) + 1 + rng.Intn(3))})
						eff = Effect{Namespace: ns, Kind: RangeEffect, Key: []byte(lo), End: []byte(hi)}
						re = refEffect{ns: ns, key: lo, end: hi, rangeEffect: true, endBounded: true}
					} else {
						eff = Effect{Namespace: ns, Kind: BarrierEffect}
						re = refEffect{ns: ns, barrier: true}
					}
					p, err := j.PrepareEffects(nil, nil, false, []Effect{eff})
					if err != nil {
						t.Fatalf("step %d native prepare: %v", step, err)
					}
					seq++
					if !j.Install(p, seq) {
						t.Fatalf("step %d native install failed", step)
					}
					re.seq = seq
					ref = append(ref, re)
					syncRefEvict()
				default:
					// Raw range or raw barrier.
					if rng.Intn(2) == 0 {
						lo := string([]byte{byte('a' + rng.Intn(5))})
						hi := string([]byte{byte(int(lo[0]) + 1 + rng.Intn(4))})
						p, err := j.Prepare(nil, []batch.DeleteRange{{Start: []byte(lo), End: []byte(hi)}}, false)
						if err != nil {
							t.Fatalf("step %d range prepare: %v", step, err)
						}
						seq++
						if !j.Install(p, seq) {
							t.Fatalf("step %d range install failed", step)
						}
						ref = append(ref, refEffect{ns: Namespace{}, key: lo, end: hi, rangeEffect: true, endBounded: true, seq: seq})
						syncRefEvict()
					} else {
						p, err := j.Prepare(nil, nil, true)
						if err != nil {
							t.Fatalf("step %d barrier prepare: %v", step, err)
						}
						seq++
						if !j.Install(p, seq) {
							t.Fatalf("step %d barrier install failed", step)
						}
						refBarrierSeq = seq
						ref = append(ref, refEffect{ns: Namespace{}, barrier: true, seq: seq})
						syncRefEvict()
					}
				}

				if ringSize == 8 || step%10 == 0 {
					verifyHistoryInvariants(t, j)
					_, _, gotFloor := j.State()
					if gotFloor != refFloor {
						t.Fatalf("step %d floor=%d want %d", step, gotFloor, refFloor)
					}
					starts := []uint64{refFloor, seq}
					if refFloor > 0 {
						starts = append(starts, refFloor-1)
					}
					if seq > refFloor+1 {
						starts = append(starts, refFloor+(seq-refFloor)/2)
					}
					for _, st := range starts {
						for _, ns := range namespaces {
							for _, k := range pointPool {
								got := j.PointChangedIn(st, ns, []byte(k))
								want := refPointChanged(st, ns, k)
								if got != want {
									t.Fatalf("step %d PointChangedIn(start=%d, ns=%+v, key=%q)=%v want %v (floor=%d)",
										step, st, ns, k, got, want, gotFloor)
								}
							}
						}
					}
				}
			}

			j.Close()
			if count, used, _ := j.State(); count != 0 || used != 0 {
				t.Fatalf("post-close state=%d/%d want 0/0", count, used)
			}
			if pts, rngs, bar := j.Counts(); pts != 0 || rngs != 0 || bar {
				t.Fatalf("post-close counts=%d/%d/%v want 0/0/false", pts, rngs, bar)
			}
		})
	}
}

func BenchmarkHistorySinglePointSteady(b *testing.B) {
	const ringSize = 4096
	for _, tc := range []struct {
		name    string
		keyMask int
	}{
		{name: "DistinctKeys", keyMask: (1 << 16) - 1},
		{name: "RepeatedOverwrites", keyMask: 64 - 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			keys := make([][]byte, tc.keyMask+1)
			for i := range keys {
				k := make([]byte, 16)
				copy(k, "txnhist-")
				binary.BigEndian.PutUint64(k[8:], uint64(i))
				keys[i] = k
			}
			j, err := New(ringSize, 64<<20, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer j.Close()

			entries := make([]batch.Entry, 1)
			seq := uint64(0)
			for i := 0; i < ringSize; i++ {
				seq++
				entries[0] = batch.Entry{Type: batch.OpPut, Key: keys[i&tc.keyMask]}
				p, err := j.Prepare(entries, nil, false)
				if err != nil || !j.Install(p, seq) {
					b.Fatalf("prefill %d failed: %v", i, err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seq++
				entries[0] = batch.Entry{Type: batch.OpPut, Key: keys[(ringSize+i)&tc.keyMask]}
				p, err := j.Prepare(entries, nil, false)
				if err != nil || !j.Install(p, seq) {
					b.Fatalf("steady %d failed: %v", i, err)
				}
			}
		})
	}
}
