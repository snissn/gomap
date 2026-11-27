package memtable

import (
	"fmt"
	"testing"
)

func TestCOWRootMinKey(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatalf("NewCOWBudget: %v", err)
	}
	w, err := NewCOWWriter(b)
	if err != nil {
		t.Fatalf("NewCOWWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	p0, err := w.Prepare(nil, COWPrepareOptions{})
	if err != nil {
		t.Fatalf("Prepare empty: %v", err)
	}
	r0 := p0.Publish()
	defer r0.Release()
	if k, ok := r0.MinKey(); ok || k != "" {
		t.Fatalf("empty MinKey=(%q, %v), want (\"\", false)", k, ok)
	}

	entries := make([]COWMutation, 64)
	for i := range entries {
		entries[i] = COWMutation{
			Key:   []byte(fmt.Sprintf("k-%04d", i+10)),
			Value: []byte("val"),
		}
	}
	p1, err := w.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatalf("Prepare batch: %v", err)
	}
	r1 := p1.Publish()
	defer r1.Release()
	if k, ok := r1.MinKey(); !ok || k != "k-0010" {
		t.Fatalf("MinKey=(%q, %v), want (\"k-0010\", true)", k, ok)
	}
	if k, ok := r1.MaxKey(); !ok || k != "k-0073" {
		t.Fatalf("MaxKey=(%q, %v), want (\"k-0073\", true)", k, ok)
	}
}

func TestCOWWriterPrepareProbeSkipAndTightBudgetFallback(t *testing.T) {
	// Populate a writer with 128 existing keys, then overwrite those exact 128
	// keys under a budget that is too tight for worst-case insert node bounds
	// (probe-free path) yet fits the exact replace-only estimate (probe fallback).
	const n = 128
	entries := make([]COWMutation, n)
	for i := range entries {
		entries[i] = COWMutation{
			Key:   []byte(fmt.Sprintf("key-%05d", i)),
			Value: []byte("initial-value-payload"),
		}
	}

	// First measure exact replace-only charge on a budget where budgetCap
	// (MaxInFlightBytes/4) is below the conservative insert node bound so
	// preferSkipProbes=false actually probes existing keys, while
	// preferSkipProbes=true skips probes.
	probeLimits := DefaultCOWLimits()
	probeLimits.MaxInFlightBytes = 256 << 10
	generous, err := NewCOWBudget(probeLimits)
	if err != nil {
		t.Fatalf("NewCOWBudget generous: %v", err)
	}
	gw, err := NewCOWWriter(generous)
	if err != nil {
		t.Fatalf("NewCOWWriter generous: %v", err)
	}
	for off := 0; off < n; off += 16 {
		gp, err := gw.Prepare(entries[off:off+16], COWPrepareOptions{})
		if err != nil {
			t.Fatalf("generous initial Prepare[%d:%d]: %v", off, off+16, err)
		}
		gp.Publish().Release()
	}

	// Exact vs probe-free estimate on overwrite of existing keys.
	exactCharge, _, err := gw.estimateWithProbePolicy(entries, COWPrepareOptions{}, false)
	if err != nil {
		t.Fatalf("exact estimate: %v", err)
	}
	skipCharge, skippedProbes, err := gw.estimateWithProbePolicy(entries, COWPrepareOptions{}, true)
	if err != nil {
		t.Fatalf("skip estimate: %v", err)
	}
	if !skippedProbes {
		t.Fatalf("expected preferSkipProbes=true to skip probes on %d non-delete entries", n)
	}
	if skipCharge.Total() <= exactCharge.Total() {
		t.Fatalf("expected conservative skipCharge (%d) > exactCharge (%d) for full overwrite", skipCharge.Total(), exactCharge.Total())
	}
	_ = gw.Close()

	// Now create a budget whose MaxInFlightBytes sits strictly between exactCharge
	// and skipCharge so Prepare hits the probe-skip refusal and falls back to exact.
	midCap := exactCharge.Total() + (skipCharge.Total()-exactCharge.Total())/2
	tightLimits := DefaultCOWLimits()
	tightLimits.MaxInFlightBytes = midCap
	tight, err := NewCOWBudget(tightLimits)
	if err != nil {
		t.Fatalf("NewCOWBudget tight: %v", err)
	}
	tw, err := NewCOWWriter(tight)
	if err != nil {
		t.Fatalf("NewCOWWriter tight: %v", err)
	}
	defer func() { _ = tw.Close() }()

	// Seed keys in smaller chunks that easily fit under midCap.
	for off := 0; off < n; off += 16 {
		p, err := tw.Prepare(entries[off:off+16], COWPrepareOptions{})
		if err != nil {
			t.Fatalf("seed Prepare[%d:%d]: %v", off, off+16, err)
		}
		p.Publish().Release()
	}

	// Overwrite all 128 keys in one batch: probe-skip fails MaxInFlightBytes,
	// exact-probe fallback succeeds!
	updated := make([]COWMutation, n)
	for i := range updated {
		updated[i] = COWMutation{
			Key:   entries[i].Key,
			Value: []byte("updated-value-payload"),
		}
	}
	p2, err := tw.Prepare(updated, COWPrepareOptions{})
	if err != nil {
		t.Fatalf("tight-budget overwrite Prepare failed (expected fallback to exact probes): %v", err)
	}
	r2 := p2.Publish()
	defer r2.Release()
	if got := r2.Len(); got != n {
		t.Fatalf("Len=%d want %d", got, n)
	}
	for i := range updated {
		val, ok := r2.Get(updated[i].Key)
		if !ok || string(val.Value) != "updated-value-payload" {
			t.Fatalf("Get(%s)=(%q, %v), want updated-value-payload", updated[i].Key, val.Value, ok)
		}
	}
}
