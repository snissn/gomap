//go:build treedb_test

package mvcc

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// primaryReplacementOptions isolates this narrow publication measurement from
// the default periodic vacuum worker. The same maintained harness is overlaid onto
// baseline and candidate; the actual durable CommitAt path remains unchanged.
func primaryReplacementOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.IndexPrimaryDirectory = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.ValueLog.PointerThreshold = 4096
	return opts
}

// PRIMARY-only replacement advances the actual public frontier while retaining
// the allocator generation. Successor metadata retirement is deferred bookkeeping,
// whereas materializing new DATA must still publish a new allocator generation.
func TestCommitAtPrimaryOnlyRetainsActualDataGeneration(t *testing.T) {
	dir := t.TempDir()
	opts := primaryReplacementOptions(dir)
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()
	if got := d.Stats()["treedb.bg_vacuum.enabled"]; got != "false" {
		_ = d.Close()
		t.Fatal("PRIMARY fixture has background vacuum enabled: " + got)
	}
	st := New(d)
	commit := func(ts uint64, ms []Mutation) {
		t.Helper()
		if err := st.CommitAt(ts, ms, CommitDurable); err != nil {
			t.Fatal(err)
		}
	}
	number := func(key string) uint64 {
		t.Helper()
		value, err := strconv.ParseUint(d.Stats()[key], 10, 64)
		if err != nil || value == 0 {
			t.Fatalf("%s missing or invalid: %v", key, err)
		}
		return value
	}
	commit(1, []Mutation{{Key: []byte("k000"), Value: []byte("old")}})
	commit(2, []Mutation{{Key: []byte("k000"), Value: []byte("keep")}})
	ms := make([]Mutation, 31)
	for i := range ms {
		ms[i] = Mutation{Key: []byte(fmt.Sprintf("k%03d", i+1)), Value: []byte("before")}
	}
	commit(100, ms)
	bootstrapGeneration := number("treedb.durable_root.freelist.generation")
	// Actual DATA materialization creates the successor whose inherited metadata
	// bookkeeping is under test; the 33-entry seed alone can stay PRIMARY-only.
	seed := make([]Mutation, 64)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("seed%03d", i)), Value: []byte("seed")}
	}
	commit(101, seed)
	if got := number("treedb.durable_root.freelist.generation"); got <= bootstrapGeneration {
		t.Fatalf("setup did not create actual DATA generation: %d -> %d", bootstrapGeneration, got)
	}
	generation := number("treedb.durable_root.freelist.generation")
	sequence := number("treedb.durable_root.commit_seq")
	for i := 0; i < 4; i++ {
		commit(100, []Mutation{{Key: []byte("k031"), Value: []byte("after")}})
		next := number("treedb.durable_root.commit_seq")
		got := number("treedb.durable_root.freelist.generation")
		if next <= sequence {
			t.Fatal("public replacement did not advance durable frontier")
		}
		if got != generation {
			t.Fatalf("bookkeeping-only PRIMARY replacement rebuilt DATA generation: before=%d after=%d replacement=%d", generation, got, i)
		}
		sequence = next
	}
	// Exceed the maintained PRIMARY directory so this actual public commit must
	// materialize DATA and cannot use the bookkeeping-only route.
	ms = make([]Mutation, 64)
	for i := range ms {
		ms[i] = Mutation{Key: []byte(fmt.Sprintf("new%03d", i)), Value: []byte("materialized")}
	}
	commit(101, ms)
	if got := number("treedb.durable_root.freelist.generation"); got <= generation {
		t.Fatalf("real DATA materialization reused generation %d (before %d)", got, generation)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	d, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed = false
	st = New(d)
	for _, want := range []struct {
		key   string
		ts    uint64
		value string
	}{
		{"k000", 2, "keep"}, {"k031", 100, "after"}, {"new063", 101, "materialized"},
	} {
		value, err := st.GetAt([]byte(want.key), want.ts)
		if err != nil || !bytes.Equal(value.Value, []byte(want.value)) {
			t.Fatalf("reopen %s: %q %v", want.key, value.Value, err)
		}
	}
}

// BenchmarkCommitAtPrimaryOnlyAfterDataGeneration measures the actual durable
// public replacement route after genuine DATA materialization. Both versions
// report generation advances; the baseline may churn and must remain runnable.
func BenchmarkCommitAtPrimaryOnlyAfterDataGeneration(b *testing.B) {
	opts := primaryReplacementOptions(b.TempDir())
	d, err := treedb.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	if got := d.Stats()["treedb.bg_vacuum.enabled"]; got != "false" {
		_ = d.Close()
		b.Fatal("PRIMARY fixture has background vacuum enabled: " + got)
	}
	st := New(d)
	commit := func(ts uint64, ms []Mutation) {
		b.Helper()
		if err := st.CommitAt(ts, ms, CommitDurable); err != nil {
			b.Fatal(err)
		}
	}
	generation := func() uint64 {
		b.Helper()
		n, err := strconv.ParseUint(d.Stats()["treedb.durable_root.freelist.generation"], 10, 64)
		if err != nil || n == 0 {
			b.Fatal("missing actual durable DATA generation", err)
		}
		return n
	}
	commit(1, []Mutation{{Key: []byte("k000"), Value: []byte("old")}})
	commit(2, []Mutation{{Key: []byte("k000"), Value: []byte("keep")}})
	seed := make([]Mutation, 31)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("k%03d", i+1)), Value: []byte("before")}
	}
	commit(100, seed)
	bootstrap := generation()
	seed = make([]Mutation, 64)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("seed%03d", i)), Value: []byte("seed")}
	}
	commit(101, seed)
	before := generation()
	if before <= bootstrap {
		b.Fatal("fixture did not materialize DATA")
	}
	mutation := []Mutation{{Key: []byte("k031"), Value: []byte("after")}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.CommitAt(100, mutation, CommitDurable); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	after := generation()
	if after < before {
		b.Fatal("DATA generation regressed")
	}
	b.ReportMetric(float64(after-before)/float64(b.N), "generation_advances/op")
}
