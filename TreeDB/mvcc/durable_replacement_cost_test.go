//go:build treedb_test

package mvcc

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// The cost driver overlays only this role and the marked option assignment for
// an exact baseline that still implements PRIMARY. Ordinary current tests use DATA.
const durableReplacementAuthority = "DATA"

func durableReplacementOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	// MVCC_COST_PRIMARY_SETUP
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 4096
	return opts
}

func durableReplacementNumber(t testing.TB, d *treedb.DB, key string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(d.Stats()[key], 10, 64)
	if err != nil || n == 0 {
		t.Fatalf("missing actual %s: %q (%v)", key, d.Stats()[key], err)
	}
	return n
}

// This is outside each benchmark's timed boundary. File presence alone does not
// establish routing: bind the public snapshot's physical root as well.
func durableReplacementObserve(t testing.TB, d *treedb.DB, dir string) {
	t.Helper()
	stats := d.Stats()
	if stats["treedb.profile.resolved"] != string(treedb.ProfileNoWALFast) || stats["treedb.write_path.redo_log"] != "off" {
		t.Fatalf("NoWAL profile/redo mismatch: %+v", stats)
	}
	snapshot := d.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("missing public authority snapshot")
	}
	state := snapshot.State()
	if state == nil {
		_ = snapshot.Close()
		t.Fatal("missing public authority state")
	}
	root, commit := state.RootPageID, state.CommitSeq
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := os.Stat(filepath.Join(dir, "index.db.primary"))
	primaryFile := err == nil
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// Bit 62 remains reserved after retirement; do not import removed PRIMARY types.
	const primaryNamespace = uint64(1) << 62
	if durableReplacementAuthority == "PRIMARY" {
		if !primaryFile || root < primaryNamespace || root >= 2*primaryNamespace {
			t.Fatalf("expected PRIMARY authority: file=%t root=%d", primaryFile, root)
		}
	} else if durableReplacementAuthority == "DATA" {
		if primaryFile || root < 2 || root >= primaryNamespace {
			t.Fatalf("expected DATA authority: PRIMARY file=%t root=%d", primaryFile, root)
		}
		if _, err := os.Stat(filepath.Join(dir, "index.db.primary.new")); !os.IsNotExist(err) {
			t.Fatalf("unexpected PRIMARY construction file: %v", err)
		}
	} else {
		t.Fatalf("unrecognized authority %q", durableReplacementAuthority)
	}
	t.Logf("MVCC_COST_AUTHORITY authority=%s root=%d commit=%d profile=%s mode=%s redo=%s", durableReplacementAuthority, root, commit, stats["treedb.profile.resolved"], stats["treedb.cache.memtable_mode"], stats["treedb.write_path.redo_log"])
}

// Decode both actual selectable DATA slots and their immediate parent identities;
// generation advancement is recorded separately, not used as a PRIMARY proxy.
func durableReplacementDATASlots(t testing.TB, dir string) [2]uint64 {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var commits [2]uint64
	for slot := range commits {
		image := make([]byte, page.PageSize)
		if _, err := f.ReadAt(image, int64(slot*page.PageSize)); err != nil {
			t.Fatal(err)
		}
		if !page.VerifyChecksumNonMutating(image) {
			t.Fatalf("DATA meta checksum: slot=%d", slot)
		}
		meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.ReadAt(image, int64(meta.RootRecordPageID)*int64(page.PageSize)); err != nil {
			t.Fatal(err)
		}
		record, err := rootpublication.DecodeDurableRootRecordV1(image, meta.RootRecordPageID, meta.RootRecordDigest)
		if err != nil || record.CommitSeq != meta.CommitSeq || record.DurableSeq != meta.DurableSeq || record.MetaProjectionDigest != meta.MetaProjectionDigest || record.UserRootPageID < 2 || record.UserRootPageID >= record.TotalPages || record.ParentRecordPageID == 0 {
			t.Fatalf("unbound DATA slot=%d meta=%+v record=%+v err=%v", slot, meta, record, err)
		}
		if _, err := f.ReadAt(image, int64(record.ParentRecordPageID)*int64(page.PageSize)); err != nil {
			t.Fatal(err)
		}
		parent, err := rootpublication.DecodeDurableRootRecordV1(image, record.ParentRecordPageID, record.ParentRecordDigest)
		if err != nil || parent.CommitSeq != record.ParentCommitSeq || parent.DurableSeq+1 != record.DurableSeq {
			t.Fatalf("unbound DATA parent: slot=%d parent=%+v record=%+v err=%v", slot, parent, record, err)
		}
		commits[slot] = record.CommitSeq
		t.Logf("MVCC_COST_DATA_SLOT slot=%d commit=%d record=%d digest=%x parent=%d parent_commit=%d parent_digest=%x", slot, meta.CommitSeq, meta.RootRecordPageID, meta.RootRecordDigest, record.ParentRecordPageID, record.ParentCommitSeq, record.ParentRecordDigest)
	}
	if commits[0] == commits[1] {
		t.Fatalf("DATA slots did not retain distinct commits: %v", commits)
	}
	return commits
}

func TestCommitAtDurableReplacementLifecycle(t *testing.T) {
	dir := t.TempDir()
	opts := durableReplacementOptions(dir)
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	st := New(d)
	commit := func(ts uint64, ms []Mutation) {
		t.Helper()
		if err := st.CommitAt(ts, ms, CommitDurable); err != nil {
			t.Fatal(err)
		}
	}
	commit(1, []Mutation{{Key: []byte("k000"), Value: []byte("old")}})
	commit(2, []Mutation{{Key: []byte("k000"), Value: []byte("keep")}})
	ms := make([]Mutation, 31)
	for i := range ms {
		ms[i] = Mutation{Key: []byte(fmt.Sprintf("k%03d", i+1)), Value: []byte("before")}
	}
	commit(100, ms)
	bootstrap := durableReplacementNumber(t, d, "treedb.durable_root.freelist.generation")
	seed := make([]Mutation, 64)
	for i := range seed {
		seed[i] = Mutation{Key: []byte(fmt.Sprintf("seed%03d", i)), Value: []byte("seed")}
	}
	commit(101, seed)
	generation := durableReplacementNumber(t, d, "treedb.durable_root.freelist.generation")
	if generation <= bootstrap {
		t.Fatal("fixture did not materialize DATA")
	}
	durableReplacementObserve(t, d, dir)
	var beforeSlots [2]uint64
	if durableReplacementAuthority == "DATA" {
		beforeSlots = durableReplacementDATASlots(t, dir)
	}
	sequence := durableReplacementNumber(t, d, "treedb.durable_root.commit_seq")
	for i := 0; i < 4; i++ {
		commit(100, []Mutation{{Key: []byte("k031"), Value: []byte("after")}})
		next := durableReplacementNumber(t, d, "treedb.durable_root.commit_seq")
		got := durableReplacementNumber(t, d, "treedb.durable_root.freelist.generation")
		if next <= sequence || got < generation {
			t.Fatal("replacement did not advance frontier or regressed generation")
		}
		if durableReplacementAuthority == "PRIMARY" && got != generation {
			t.Fatal("bookkeeping-only PRIMARY replacement rebuilt DATA generation")
		}
		sequence = next
	}
	if durableReplacementAuthority == "DATA" {
		after := durableReplacementDATASlots(t, dir)
		for slot := range after {
			if after[slot] <= beforeSlots[slot] {
				t.Fatalf("DATA slot did not advance: before=%v after=%v", beforeSlots, after)
			}
		}
	}
	ms = make([]Mutation, 64)
	for i := range ms {
		ms[i] = Mutation{Key: []byte(fmt.Sprintf("new%03d", i)), Value: []byte("materialized")}
	}
	commit(101, ms)
	if durableReplacementNumber(t, d, "treedb.durable_root.freelist.generation") <= generation {
		t.Fatal("real DATA materialization reused generation")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	st = New(d)
	for _, want := range []struct {
		key   string
		ts    uint64
		value string
	}{{"k000", 2, "keep"}, {"k031", 100, "after"}, {"new063", 101, "materialized"}} {
		value, err := st.GetAt([]byte(want.key), want.ts)
		if err != nil || !bytes.Equal(value.Value, []byte(want.value)) {
			t.Fatalf("reopen %s: %q %v", want.key, value.Value, err)
		}
	}
	durableReplacementObserve(t, d, dir)
	if durableReplacementAuthority == "DATA" {
		durableReplacementDATASlots(t, dir)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	t.Logf("MVCC_COST_LIFECYCLE authority=%s", durableReplacementAuthority)
}

// The seed, durable write API, replacement trace and timed boundary are identical
// to BenchmarkCommitAtPrimaryOnlyAfterDataGeneration before PRIMARY retirement.
func BenchmarkCommitAtDurableReplacementAfterDataGeneration(b *testing.B) {
	dir := b.TempDir()
	opts := durableReplacementOptions(dir)
	d, err := treedb.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	st := New(d)
	commit := func(ts uint64, ms []Mutation) {
		b.Helper()
		if err := st.CommitAt(ts, ms, CommitDurable); err != nil {
			b.Fatal(err)
		}
	}
	generation := func() uint64 {
		b.Helper()
		return durableReplacementNumber(b, d, "treedb.durable_root.freelist.generation")
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
	durableReplacementObserve(b, d, dir)
}
