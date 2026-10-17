package txnhistory

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func installEffects(t *testing.T, h *History, seq uint64, effects ...Effect) {
	t.Helper()
	p, err := h.PrepareEffects(nil, nil, false, effects)
	if err != nil || !h.Install(p, seq) {
		t.Fatalf("install seq=%d err=%v", seq, err)
	}
}

func TestHistorySemanticNamespacesCannotCollideWithArbitraryRawBytes(t *testing.T) {
	h, _ := New(32, 1<<20, nil)
	defer h.Close()
	key := []byte{0, 1, 0xff, 'c', 'a', 't'}
	catalog := Namespace{Kind: CatalogNamespace}
	a := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 2, Interpretation: 3}
	b := a
	b.Collection++
	installEffects(t, h, 1, Effect{Namespace: a, Kind: PointEffect, Key: key})
	if !h.PointChangedIn(0, a, key) || h.PointChanged(0, key) || h.PointChangedIn(0, b, key) || h.PointChangedIn(0, catalog, key) {
		t.Fatal("namespace collision or missing native provenance")
	}
	p, err := h.Prepare([]batch.Entry{{Key: key}}, nil, false)
	if err != nil || !h.Install(p, 2) {
		t.Fatal(err)
	}
	if !h.PointChanged(1, key) || h.PointChangedIn(1, a, key) {
		t.Fatal("raw publication affected collection namespace")
	}
}

func TestHistoryCatalogAbsenceListingRecreateAndUniquePrefixABA(t *testing.T) {
	h, _ := New(32, 1<<20, nil)
	defer h.Close()
	catalog := Namespace{Kind: CatalogNamespace}
	old := Namespace{Kind: CollectionNamespace, Collection: 4, Domain: 2, Interpretation: 1}
	newCollection := old
	newCollection.Collection = 5
	deps := []Dependency{{Namespace: catalog, Kind: PointEffect, Key: []byte("missing")}, {Namespace: old, Kind: RangeEffect, Key: []byte("red/"), End: []byte("red0")}}
	installEffects(t, h, 1, Effect{Namespace: catalog, Kind: PointEffect, Key: []byte("missing")}, Effect{Namespace: old, Kind: PointEffect, Key: []byte("red/row")})
	installEffects(t, h, 2, Effect{Namespace: catalog, Kind: PointEffect, Key: []byte("missing")}, Effect{Namespace: old, Kind: PointEffect, Key: []byte("red/row")})
	if changed, err := h.DependenciesChanged(0, deps); err != nil || !changed {
		t.Fatalf("ABA disappeared: %t %v", changed, err)
	}
	if !h.RangeChangedIn(0, catalog, []byte("m"), []byte("n")) {
		t.Fatal("listing missed catalog phantom")
	}
	if h.RangeChangedIn(0, newCollection, []byte("red/"), []byte("red0")) {
		t.Fatal("recreate reused old incarnation")
	}
	installEffects(t, h, 3, Effect{Namespace: newCollection, Kind: PointEffect, Key: []byte("blue/row")})
	if h.PointChangedIn(2, old, []byte("blue/row")) {
		t.Fatal("new incarnation changed old domain")
	}
}

func TestHistorySemanticRangeBarrierAndPhysicalDrift(t *testing.T) {
	h, _ := New(32, 1<<20, nil)
	defer h.Close()
	a := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 1, Interpretation: 1}
	b := a
	b.Domain++
	installEffects(t, h, 1, Effect{Namespace: a, Kind: RangeEffect, Key: []byte("b"), End: []byte("d")})
	if !h.PointChangedIn(0, a, []byte("b")) || h.PointChangedIn(0, a, []byte("d")) || h.RangeChangedIn(0, a, []byte("d"), nil) || h.RangeChangedIn(0, b, nil, nil) {
		t.Fatal("range boundary or domain isolation failed")
	}
	// Equivalent physical maintenance supplies no logical effect and cannot
	// invent provenance merely because a descriptor/root address changed.
	p, err := h.PrepareEffects(nil, nil, false, nil)
	if err != nil || p.Valid() {
		t.Fatalf("physical drift journaled: %v %+v", err, p)
	}
	installEffects(t, h, 2, Effect{Namespace: a, Kind: BarrierEffect})
	if !h.PointChangedIn(1, a, []byte("outside")) || h.PointChangedIn(1, b, []byte("outside")) || h.PointChanged(1, []byte("outside")) {
		t.Fatal("domain barrier became global")
	}
}

func TestHistorySemanticOwnedBackingWrapAndFloor(t *testing.T) {
	h, _ := New(8, 1<<20, nil)
	defer h.Close()
	ns := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 1}
	for seq := uint64(1); seq <= 40; seq++ {
		key := bytes.Repeat([]byte{byte(seq)}, 16)
		p, err := h.PrepareEffects(nil, nil, false, []Effect{{Namespace: ns, Kind: PointEffect, Key: key}})
		if err != nil {
			t.Fatal(err)
		}
		clear(key)
		if !h.Install(p, seq) || !h.PointChangedIn(seq-1, ns, bytes.Repeat([]byte{byte(seq)}, 16)) {
			t.Fatal("caller backing or latest-key index lost")
		}
	}
	if !errors.Is(h.Check(0), ErrTooOld) {
		t.Fatal("retired provenance did not raise floor")
	}
	if changed, err := h.DependenciesChanged(0, nil); changed || !errors.Is(err, ErrTooOld) {
		t.Fatalf("old writer passed: %t %v", changed, err)
	}
}

func TestHistoryOversizedSemanticEffectsUseOnlyAffectedDomainBarriers(t *testing.T) {
	h, _ := New(8, 1<<20, nil)
	defer h.Close()
	a := Namespace{Kind: CollectionNamespace, Collection: 1, Domain: 1}
	b := a
	b.Collection++
	effects := make([]Effect, 9)
	for i := range effects {
		effects[i] = Effect{Namespace: a, Kind: PointEffect, Key: []byte{byte(i)}}
	}
	p, err := h.PrepareEffects(nil, nil, false, effects)
	if err != nil {
		t.Fatal(err)
	}
	p.Cancel()
	if h.PointChangedIn(0, a, []byte("other")) {
		t.Fatal("canceled barrier became logical mutation")
	}
	installEffects(t, h, 1, effects...)
	if !h.PointChangedIn(0, a, []byte("other")) || h.PointChangedIn(0, b, []byte("other")) || h.PointChanged(0, []byte("other")) {
		t.Fatal("oversize fallback lost namespace")
	}
}
