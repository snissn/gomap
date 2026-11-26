package txnhistory

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestHistoryBulkPointChunkWrapAroundAndOracle(t *testing.T) {
	const ringSize = 256
	j, err := New(ringSize, 4<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	type refEntry struct {
		ns          Namespace
		key, end    string
		rangeEffect bool
		barrier     bool
		endBounded  bool
		seq         uint64
	}
	var ref []refEntry
	var refFloor, refBarrierSeq uint64

	syncFloor := func() {
		_, _, floor := j.State()
		refFloor = floor
		kept := ref[:0]
		for _, e := range ref {
			if e.seq > refFloor {
				kept = append(kept, e)
			}
		}
		ref = kept
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

	namespaces := []Namespace{
		{},
		{Kind: CatalogNamespace},
		{Kind: CollectionNamespace, Collection: 1, Domain: 0, Interpretation: 1},
		{Kind: CollectionNamespace, Collection: 1, Domain: 1, Interpretation: 1},
		{Kind: CollectionNamespace, Collection: 2, Domain: 0, Interpretation: 1},
	}

	rng := rand.New(rand.NewSource(16301))
	seq := uint64(0)

	// Run mixed bulk chunks (>= 64 points), small point commits, mixed bulk+range
	// commits, and manual single-slot Evict calls across many ring wrap-arounds.
	for step := 0; step < 60; step++ {
		switch step % 5 {
		case 0, 1:
			// Multi-namespace bulk point chunk (72..96 points).
			count := 72 + rng.Intn(25)
			effects := make([]Effect, 0, count)
			for i := 0; i < count; i++ {
				ns := namespaces[2+(i%3)]
				k := fmt.Sprintf("k-%04d", (step*37+i*7)%300)
				effects = append(effects, Effect{Namespace: ns, Kind: PointEffect, Key: []byte(k)})
			}
			p, err := j.PrepareEffects(nil, nil, false, effects)
			if err != nil {
				t.Fatalf("step %d bulk prepare: %v", step, err)
			}
			seq++
			if !j.Install(p, seq) {
				t.Fatalf("step %d bulk install failed", step)
			}
			for _, e := range effects {
				ref = append(ref, refEntry{ns: e.Namespace, key: string(e.Key), seq: seq})
			}
			syncFloor()
			if j.bulkCount == 0 || j.bulkPoints == 0 {
				t.Fatalf("step %d expected active bulk chunk, got count=%d points=%d", step, j.bulkCount, j.bulkPoints)
			}
		case 2:
			// Single-namespace bulk chunk + trailing range effect.
			ns := namespaces[2]
			effects := make([]Effect, 0, 70)
			for i := 0; i < 68; i++ {
				k := fmt.Sprintf("k-%04d", (step*13+i)%200)
				effects = append(effects, Effect{Namespace: ns, Kind: PointEffect, Key: []byte(k)})
			}
			effects = append(effects, Effect{Namespace: ns, Kind: RangeEffect, Key: []byte("k-0050"), End: []byte("k-0060")})
			p, err := j.PrepareEffects(nil, nil, false, effects)
			if err != nil {
				t.Fatalf("step %d bulk+range prepare: %v", step, err)
			}
			seq++
			if !j.Install(p, seq) {
				t.Fatalf("step %d bulk+range install failed", step)
			}
			for _, e := range effects {
				if e.Kind == RangeEffect {
					ref = append(ref, refEntry{ns: e.Namespace, key: string(e.Key), end: string(e.End), rangeEffect: true, endBounded: true, seq: seq})
				} else {
					ref = append(ref, refEntry{ns: e.Namespace, key: string(e.Key), seq: seq})
				}
			}
			syncFloor()
		case 3:
			// Small catalog / raw / collection singleton updates mixed with bulk chunks.
			for _, ns := range []Namespace{namespaces[0], namespaces[1], namespaces[2]} {
				k := fmt.Sprintf("k-%04d", rng.Intn(300))
				var p Prepared
				if ns == (Namespace{}) {
					p, err = j.Prepare([]batch.Entry{{Type: batch.OpPut, Key: []byte(k)}}, nil, false)
				} else {
					p, err = j.PrepareEffects(nil, nil, false, []Effect{{Namespace: ns, Kind: PointEffect, Key: []byte(k)}})
				}
				if err != nil {
					t.Fatalf("step %d small prepare: %v", step, err)
				}
				seq++
				if !j.Install(p, seq) {
					t.Fatalf("step %d small install failed", step)
				}
				ref = append(ref, refEntry{ns: ns, key: k, seq: seq})
				syncFloor()
			}
		case 4:
			// Partial manual Evict() into the head chunk (tests mixed manual + bulk eviction).
			for i := 0; i < 15 && j.count > 0; i++ {
				j.Evict()
			}
			syncFloor()
		}

		// Verify point conflict oracle across all namespaces and starts.
		starts := []uint64{refFloor, seq}
		if refFloor > 0 {
			starts = append(starts, refFloor-1)
		}
		if seq > refFloor {
			starts = append(starts, seq-1)
		}
		if seq > refFloor+2 {
			starts = append(starts, seq-2)
		}
		for _, st := range starts {
			for _, ns := range namespaces {
				for _, k := range []string{"k-0000", "k-0055", "k-0100", "k-0199", "k-0299", "missing"} {
					got := j.PointChangedIn(st, ns, []byte(k))
					want := refPointChanged(st, ns, k)
					if got != want {
						t.Fatalf("step %d PointChangedIn(start=%d, ns=%+v, key=%q)=%v want %v (floor=%d)",
							step, st, ns, k, got, want, refFloor)
					}
				}
			}
		}
	}

	// Verify OnlyPointChanged detects a bulk-chunk point write accurately.
	ns := namespaces[2]
	bulk := make([]Effect, 72)
	for i := range bulk {
		bulk[i] = Effect{Namespace: ns, Kind: PointEffect, Key: []byte(fmt.Sprintf("only-%04d", i))}
	}
	startSeq := seq
	p, err := j.PrepareEffects(nil, nil, false, bulk)
	if err != nil {
		t.Fatal(err)
	}
	seq++
	if !j.Install(p, seq) {
		t.Fatal("install failed")
	}
	target := Dependency{Namespace: ns, Kind: PointEffect, Key: []byte("only-0042")}
	other := Dependency{Namespace: ns, Kind: PointEffect, Key: []byte("untouched")}
	if !j.OnlyPointChanged(startSeq, target, []Dependency{target, other}, nil) {
		t.Fatal("OnlyPointChanged missed point installed in bulk chunk")
	}
	if j.OnlyPointChanged(startSeq, other, []Dependency{target, other}, nil) {
		t.Fatal("OnlyPointChanged unexpectedly matched untouched key")
	}

	// Drain via Close and confirm zero retained points/ranges/chunks.
	j.Close()
	if count, used, _ := j.State(); count != 0 || used != 0 {
		t.Fatalf("post-close state=%d/%d want 0/0", count, used)
	}
	if pts, rngs, bar := j.Counts(); pts != 0 || rngs != 0 || bar {
		t.Fatalf("post-close counts=%d/%d/%v want 0/0/false", pts, rngs, bar)
	}
}

func BenchmarkHistoryBulkChunk3000Steady(b *testing.B) {
	const ringSize = 65536
	j, err := New(ringSize, 64<<20, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()

	ns0 := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 0, Interpretation: 1}
	ns1 := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 1, Interpretation: 1}
	ns2 := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 2, Interpretation: 1}

	const batches = 32
	preparedEffects := make([][]Effect, batches)
	for batchIdx := 0; batchIdx < batches; batchIdx++ {
		effects := make([]Effect, 0, 3000)
		for _, ns := range []Namespace{ns0, ns1, ns2} {
			for i := 0; i < 1000; i++ {
				effects = append(effects, Effect{
					Namespace: ns,
					Kind:      PointEffect,
					Key:       []byte(fmt.Sprintf("b%02d-k%06d", batchIdx, i)),
				})
			}
		}
		preparedEffects[batchIdx] = effects
	}

	// Pre-fill ring so every iteration exercises steady-state wrap-around eviction.
	seq := uint64(0)
	for i := 0; i < batches; i++ {
		p, err := j.PrepareEffects(nil, nil, false, preparedEffects[i])
		if err != nil {
			b.Fatal(err)
		}
		seq++
		j.Install(p, seq)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := j.PrepareEffects(nil, nil, false, preparedEffects[i%batches])
		if err != nil {
			b.Fatal(err)
		}
		seq++
		if !j.Install(p, seq) {
			b.Fatal("Install failed")
		}
	}
}
