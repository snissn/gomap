package treedb_test

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestCompactIndexOuterLeavesRemainWritableAndReopen(t *testing.T) {
	opts := treedb.Options{
		Dir:                        t.TempDir(),
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	}
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	const keyCount = 512
	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("compact/%04d", i)) }
	valueFor := func(i int) []byte { return bytes.Repeat([]byte{byte(i)}, 96) }
	seed := database.NewBatch()
	defer seed.Close()
	for i := 0; i < keyCount; i++ {
		if err := seed.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	old := database.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing original snapshot")
	}
	defer old.Close()

	for rebuild := 0; rebuild < 3; rebuild++ {
		if err := database.CompactIndex(); err != nil {
			t.Fatalf("compact %d: %v", rebuild, err)
		}
	}
	updated := bytes.Repeat([]byte("u"), 96)
	// Touch one leaf while the other rebuilt leaves remain unchanged. A
	// rebuild that silently switches to pager leaves fails on this write.
	if err := database.SetSync(keyFor(0), updated); err != nil {
		t.Fatalf("write after compact: %v", err)
	}
	for i := 0; i < keyCount; i++ {
		if got, err := old.Get(keyFor(i)); err != nil || !bytes.Equal(got, valueFor(i)) {
			t.Fatalf("original snapshot key %d: %x %v", i, got, err)
		}
		want := valueFor(i)
		if i == 0 {
			want = updated
		}
		if got, err := database.Get(keyFor(i)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("current key %d: %x %v", i, got, err)
		}
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < keyCount; i++ {
		want := valueFor(i)
		if i == 0 {
			want = updated
		}
		if got, err := database.Get(keyFor(i)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopened key %d: %x %v", i, got, err)
		}
	}
}
