package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"treedb/internal/compaction"
	"treedb/internal/slab"
	"treedb/internal/tree"
)

func TestCompactBlocksAndReducesDeadBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write 10 large values into the initial active slab.
	baseVal := bytes.Repeat([]byte("a"), 128)
	for i := 0; i < 10; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("k%02d", i)), baseVal); err != nil {
			t.Fatalf("set base %d: %v", i, err)
		}
	}

	coldID := db.slabs.ActiveID()

	// Force a rotation so coldID becomes cold and a new slab is active.
	if _, err := db.slabs.ForceRotate(); err != nil {
		t.Fatalf("force rotate: %v", err)
	}

	// Overwrite 7 keys, leaving 3 live in the cold slab.
	newVal := bytes.Repeat([]byte("b"), 128)
	for i := 0; i < 7; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("k%02d", i)), newVal); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}

	// Ensure cold slab has high dead ratio.
	set := db.slabs.SlabSet()
	cold, ok := set.Get(coldID)
	if !ok || cold == nil {
		t.Fatalf("expected cold slab %d present", coldID)
	}
	stats := cold.Stats()
	if stats.TotalBytes == 0 || float64(stats.DeadBytes)/float64(stats.TotalBytes) <= 0.5 {
		t.Fatalf("expected dead ratio > 0.5, got %+v", stats)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Live keys (7..9) should retain old value, overwritten keys retain new value.
	for i := 0; i < 10; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k%02d", i)))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		want := baseVal
		if i < 7 {
			want = newVal
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for k%02d", i)
		}
	}

	// Cold slab removed from active SlabSet.
	set = db.slabs.SlabSet()
	if _, ok := set.Get(coldID); ok {
		t.Fatalf("expected slab %d removed after compaction", coldID)
	}

	// Dead bytes among remaining slabs should be near zero.
	var deadSum uint64
	for _, id := range set.IDs() {
		f, _ := set.Get(id)
		if f == nil {
			continue
		}
		deadSum += f.Stats().DeadBytes
	}
	if deadSum != 0 {
		t.Fatalf("expected dead bytes 0 after compaction, got %d", deadSum)
	}
}

func TestCompactionZombieLifeSupport(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("x"), 128)
	for i := 0; i < 5; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("z%02d", i)), val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	coldID := db.slabs.ActiveID()
	if _, err := db.slabs.ForceRotate(); err != nil {
		t.Fatalf("force rotate: %v", err)
	}
	// Overwrite 4 keys so cold slab dead ratio high, leave z04 live in cold slab.
	for i := 0; i < 4; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("z%02d", i)), bytes.Repeat([]byte("y"), 128)); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}

	// Hold an iterator open to pin the cold slab.
	it, err := db.Iterator([]byte("z04"), nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("expected iterator valid")
	}
	before := it.Value()

	done := make(chan error, 1)
	go func() {
		done <- db.Compact()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("compact: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("compact did not complete")
	}

	// Iterator should still read the old value even though cold slab is zombie.
	after := it.Value()
	if !bytes.Equal(before, after) {
		t.Fatalf("iterator value changed during compaction")
	}

	// Close iterator to drop slab pins; cold slab should delete.
	_ = it.Close()
	path := filepath.Join(dir, fmt.Sprintf("data-%04d.slab", coldID))
	var gone bool
	for i := 0; i < 50; i++ {
		_, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			gone = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !gone {
		t.Fatalf("expected zombie slab file removed: %s", path)
	}
}

func TestCompactionResurrectionRaceUserWins(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < 6; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("r%02d", i)), val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	coldID := db.slabs.ActiveID()
	if _, err := db.slabs.ForceRotate(); err != nil {
		t.Fatalf("force rotate: %v", err)
	}
	// Overwrite 4 keys so cold slab candidate, leave r05 live.
	for i := 0; i < 4; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("r%02d", i)), bytes.Repeat([]byte("w"), 128)); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	liveKey := []byte("r05")
	userVal := bytes.Repeat([]byte("U"), 128)

	var once sync.Once
	hooks := &compaction.Hooks{
		AfterCopy: func(_ uint32, _ []compaction.Update) {
			once.Do(func() {
				_ = db.SetSync(liveKey, userVal)
			})
		},
	}

	comp := compaction.New(db.pager, db.slabs, db.state, db.grave, db.pruner, &db.writerMu).WithHooks(hooks)
	if err := comp.CompactAll(); err != nil {
		t.Fatalf("compactall: %v", err)
	}

	if set := db.slabs.SlabSet(); set != nil {
		if _, ok := set.Get(coldID); ok {
			t.Fatalf("expected cold slab %d removed", coldID)
		}
	}

	got, err := db.Get(liveKey)
	if err != nil {
		t.Fatalf("get live: %v", err)
	}
	if !bytes.Equal(got, userVal) {
		t.Fatalf("expected user write to win")
	}
}

func TestTornCompactionRecoverySkipped(t *testing.T) {
	t.Skip("kill/torn-compaction recovery is long-running; covered by crash tests")
}

func TestCompactionConcurrentWritesSmoke(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("s"), 128)
	for i := 0; i < 3; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("c%02d", i)), val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	coldID := db.slabs.ActiveID()
	if _, err := db.slabs.ForceRotate(); err != nil {
		t.Fatalf("force rotate: %v", err)
	}
	// Overwrite to make cold slab candidate.
	for i := 0; i < 2; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("c%02d", i)), bytes.Repeat([]byte("t"), 128)); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}

	errCh := make(chan error, 1)
	go func() { errCh <- db.Compact() }()

	// Concurrent writes should not deadlock.
	for i := 0; i < 5; i++ {
		if err := db.Set([]byte(fmt.Sprintf("cw%02d", i)), val); err != nil {
			t.Fatalf("concurrent set: %v", err)
		}
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("compact: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("compact did not complete under concurrent writes")
	}

	if set := db.slabs.SlabSet(); set != nil {
		if _, ok := set.Get(coldID); ok {
			t.Fatalf("expected cold slab %d removed", coldID)
		}
	}

	// Sanity: ensure stats key for active slab exists post-compaction.
	snap, err := db.state.AcquireSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	st := snap.State()
	sys := tree.NewSystemTree(db.pager, st.SystemRootPageID)
	if _, err := sys.GetRaw(slab.StatsKey(db.slabs.ActiveID())); err != nil {
		t.Fatalf("expected active stats key: %v", err)
	}
}
