package txnhistory

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/batch"
	"testing"
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
