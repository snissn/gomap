package zipper_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Exercise the public caller that creates an allocator zipper clone per Write.
// Scratch selection itself is checked in TestAllocatorCloneReusesApplyScratch;
// this test covers ownership of published and retained results across reuse.
func TestAllocatorClonePublicBatchWriteRetainedSnapshot(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	key := func(i int) []byte { return []byte(fmt.Sprintf("clone-%06d", i)) }
	write := func(round int) {
		t.Helper()
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < 1200; i++ {
			if round > 0 && round%2 == 1 && i%3 == 0 {
				err = b.Delete(key(i))
			} else {
				err = b.Set(key(i), []byte(fmt.Sprintf("round-%d", round)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatal(err)
		}
	}
	write(0)
	snapshot := d.AcquireSnapshot()
	defer snapshot.Close()
	retained, err := snapshot.Get(key(1))
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 4; round++ {
		write(round)
		got, err := d.Get(key(1))
		if err != nil || string(got) != fmt.Sprintf("round-%d", round) {
			t.Fatalf("public overwrite: got=%q err=%v", got, err)
		}
		got, err = d.Get(key(0))
		if round%2 == 1 {
			if err != nil || got != nil {
				t.Fatalf("public delete: got=%q err=%v", got, err)
			}
		} else if err != nil || string(got) != fmt.Sprintf("round-%d", round) {
			t.Fatalf("public restore: got=%q err=%v", got, err)
		}
		old, err := snapshot.Get(key(0))
		if err != nil || string(old) != "round-0" {
			t.Fatalf("retained snapshot: got=%q err=%v", old, err)
		}
		if !bytes.Equal(retained, []byte("round-0")) {
			t.Fatalf("later write mutated owned Get result: %q", retained)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
}
