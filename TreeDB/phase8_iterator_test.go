package treedb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"treedb/internal/iterator"
)

func collectKeys(t *testing.T, it iterator.UnsafeIterator) [][]byte {
	t.Helper()
	var out [][]byte
	for ; it.Valid(); it.Next() {
		out = append(out, append([]byte(nil), it.UnsafeKey()...))
	}
	// UnsafeIterator doesn't have Error(), but treedb.Iterator does.
	// We can cast or ignore for test.
	// If UnsafeIterator has Error() (which I added), use it.
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return out
}

func TestIteratorForwardReverseSemantics(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, k := range [][]byte{[]byte("A"), []byte("C"), []byte("E")} {
		if err := db.SetSync(k, []byte("v"+string(k))); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	it, err := db.Iterator([]byte("A"), []byte("Z"))
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	got := collectKeys(t, it)
	_ = it.Close()
	want := [][]byte{[]byte("A"), []byte("C"), []byte("E")}
	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("forward mismatch at %d: %q != %q", i, got[i], want[i])
		}
	}

	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	gotRev := collectKeys(t, rit)
	_ = rit.Close()
	wantRev := [][]byte{[]byte("E"), []byte("C"), []byte("A")}
	for i := range wantRev {
		if !bytes.Equal(gotRev[i], wantRev[i]) {
			t.Fatalf("reverse mismatch at %d: %q != %q", i, gotRev[i], wantRev[i])
		}
	}
}

func TestIteratorEndExclusiveAndTombstones(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, k := range [][]byte{[]byte("A"), []byte("B"), []byte("C")} {
		if err := db.SetSync(k, []byte("v"+string(k))); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	rit, err := db.ReverseIterator(nil, []byte("C"))
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	gotRev := collectKeys(t, rit)
	_ = rit.Close()
	wantRev := [][]byte{[]byte("B"), []byte("A")}
	if len(gotRev) != len(wantRev) {
		t.Fatalf("expected reverse [B A], got %q", gotRev)
	}
	for i := range wantRev {
		if !bytes.Equal(gotRev[i], wantRev[i]) {
			t.Fatalf("expected reverse [B A], got %q", gotRev)
		}
	}

	it, err := db.Iterator([]byte("B"), []byte("C"))
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	gotF := collectKeys(t, it)
	_ = it.Close()
	if len(gotF) != 1 || !bytes.Equal(gotF[0], []byte("B")) {
		t.Fatalf("expected forward [B], got %q", gotF)
	}

	if err := db.DeleteSync([]byte("B")); err != nil {
		t.Fatalf("delete B: %v", err)
	}
	all, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator nil bounds: %v", err)
	}
	gotAll := collectKeys(t, all)
	_ = all.Close()
	wantAll := [][]byte{[]byte("A"), []byte("C")}
	if len(gotAll) != len(wantAll) || !bytes.Equal(gotAll[0], wantAll[0]) || !bytes.Equal(gotAll[1], wantAll[1]) {
		t.Fatalf("expected tombstone-skipping [A C], got %q", gotAll)
	}
}

func TestReverseIteratorCosmosSemantics(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, k := range [][]byte{[]byte("A"), []byte("B"), []byte("C"), []byte("D"), []byte("E")} {
		if err := db.SetSync(k, []byte("v"+string(k))); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	rit, err := db.ReverseIterator([]byte("B"), []byte("D"))
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	got := collectKeys(t, rit)
	_ = rit.Close()
	want := [][]byte{[]byte("C"), []byte("B")}
	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("reverse mismatch at %d: %q != %q", i, got[i], want[i])
		}
	}

	rit2, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator nil bounds: %v", err)
	}
	got2 := collectKeys(t, rit2)
	_ = rit2.Close()
	want2 := [][]byte{[]byte("E"), []byte("D"), []byte("C"), []byte("B"), []byte("A")}
	for i := range want2 {
		if !bytes.Equal(got2[i], want2[i]) {
			t.Fatalf("reverse nil mismatch at %d: %q != %q", i, got2[i], want2[i])
		}
	}
}

func TestIteratorSnapshotStabilityUnderCommits(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var keys [][]byte
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("k%02d", i))
		keys = append(keys, k)
		if err := db.SetSync(k, []byte("v0")); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}

	for i := 0; i < 10; i++ {
		_ = db.DeleteSync(keys[i])
	}
	for i := 10; i < 20; i++ {
		_ = db.SetSync(keys[i], []byte("v1"))
	}

	got := collectKeys(t, it)
	_ = it.Close()
	if len(got) != len(keys) {
		t.Fatalf("expected %d keys from snapshot, got %d", len(keys), len(got))
	}
	for i := 0; i < len(keys); i++ {
		want := keys[len(keys)-1-i]
		if !bytes.Equal(got[i], want) {
			t.Fatalf("snapshot order mismatch at %d: %q != %q", i, got[i], want)
		}
	}
}

func TestIteratorsStableUnderAggressivePruning(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, KeepRecent: 0})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var keys [][]byte
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		keys = append(keys, k)
		if err := db.SetSync(k, []byte("v0")); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	fit, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			k := keys[i%len(keys)]
			_ = db.SetSync(k, []byte(fmt.Sprintf("v%d", i)))
			st := db.state.Load()
			if st != nil {
				_ = db.pruner.Prune(st.CommitSeq)
			}
		}
	}()

	gotF := collectKeys(t, fit)
	gotR := collectKeys(t, rit)
	_ = fit.Close()
	_ = rit.Close()
	wg.Wait()

	if len(gotF) != len(keys) {
		t.Fatalf("forward expected %d keys, got %d", len(keys), len(gotF))
	}
	for i := range keys {
		if !bytes.Equal(gotF[i], keys[i]) {
			t.Fatalf("forward snapshot mismatch at %d: %q != %q", i, gotF[i], keys[i])
		}
	}
	for i := range keys {
		want := keys[len(keys)-1-i]
		if !bytes.Equal(gotR[i], want) {
			t.Fatalf("reverse snapshot mismatch at %d: %q != %q", i, gotR[i], want)
		}
	}
}