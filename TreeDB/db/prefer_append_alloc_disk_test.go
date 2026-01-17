package db

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestPreferAppendAlloc_Churn_DoesNotBalloonWhenDisabled(t *testing.T) {
	type result struct {
		pageCount uint64
		fileSize  int64
		userPages uint64
	}

	run := func(t *testing.T, preferAppend bool) result {
		t.Helper()

		dir := t.TempDir()
		chunkSize := int64(64 * 1024)

		d, err := Open(Options{
			Dir:                           dir,
			ChunkSize:                     chunkSize,
			KeepRecent:                    1,
			PreferAppendAlloc:             preferAppend,
			DisableBackgroundPrune:        true,
			BackgroundIndexVacuumInterval: -1, // avoid background noise/auto-vacuum
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = d.Close() }()

		const keysPerSet = 8000
		keys := make([][]byte, keysPerSet*2)
		for i := range keys {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i))
			keys[i] = k
		}

		valA := bytes.Repeat([]byte("a"), 200) // inline (DefaultInlineThreshold=256)
		valB := bytes.Repeat([]byte("b"), 200)

		writeSet := func(start int, val []byte) {
			b := d.NewBatch().(*Batch)
			for i := start; i < start+keysPerSet; i++ {
				if err := b.SetView(keys[i], val); err != nil {
					t.Fatalf("set: %v", err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = b.Close()
		}

		deleteSet := func(start int) {
			b := d.NewBatch().(*Batch)
			for i := start; i < start+keysPerSet; i++ {
				if err := b.DeleteView(keys[i]); err != nil {
					t.Fatalf("del: %v", err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("del write: %v", err)
			}
			_ = b.Close()
		}

		// Seed a stable live set (A).
		writeSet(0, valA)
		_ = d.SetSync([]byte("zz-seed"), valA)
		d.Prune()

		// Churn: swap between set A and set B. This creates lots of retired pages.
		const rounds = 10
		for i := 0; i < rounds; i++ {
			deleteSet(0)
			writeSet(keysPerSet, valB)

			deleteSet(keysPerSet)
			writeSet(0, valA)
		}

		// Advance commit seq enough for KeepRecent=1 pruning to reclaim retired pages.
		_ = d.SetSync([]byte("zz-prune-1"), valA)
		_ = d.SetSync([]byte("zz-prune-2"), valA)
		d.Prune()

		indexPath := filepath.Join(dir, indexFileName)
		info, err := os.Stat(indexPath)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}

		rep, err := d.FragmentationReport()
		if err != nil {
			t.Fatalf("FragmentationReport: %v", err)
		}
		userPagesStr := rep["treedb.user.pages"]
		if userPagesStr == "" {
			t.Fatalf("missing treedb.user.pages")
		}
		userPages, err := strconv.ParseUint(userPagesStr, 10, 64)
		if err != nil {
			t.Fatalf("parse treedb.user.pages: %v", err)
		}

		return result{
			pageCount: d.Pager().PageCount(),
			fileSize:  info.Size(),
			userPages: userPages,
		}
	}

	reuse := run(t, false)
	// "Not 4x bigger" guard: if PreferAppendAlloc is disabled, the allocator
	// should heavily reuse the freelist under churn instead of ballooning the file.
	//
	// Bound against live/user pages rather than a baseline stat: this catches the
	// class of regressions where pages are being freed but not reused.
	if reuse.userPages == 0 {
		t.Fatalf("unexpected userPages=0")
	}
	if reuse.pageCount > reuse.userPages*4 {
		t.Fatalf("PreferAppendAlloc=false ballooned index pages: userPages=%d filePages=%d", reuse.userPages, reuse.pageCount)
	}
	if reuse.fileSize > int64(reuse.userPages*4)*int64(page.PageSize) {
		t.Fatalf("PreferAppendAlloc=false ballooned index.db: userPages=%d fileSize=%d", reuse.userPages, reuse.fileSize)
	}

	prefer := run(t, true)
	// Sanity-check the knob has a material effect under this workload.
	// (If this ever fails, the test may no longer exercise enough churn.)
	if prefer.pageCount <= reuse.pageCount {
		t.Fatalf("expected PreferAppendAlloc=true to grow file more than reuse: reusePages=%d preferPages=%d", reuse.pageCount, prefer.pageCount)
	}
}
