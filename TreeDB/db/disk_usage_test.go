package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestDiskUsage_StableKeyset_DoesNotExplode(t *testing.T) {
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:                    dir,
		ChunkSize:              chunkSize,
		KeepRecent:             1,
		PreferAppendAlloc:      false,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	const keys = 10_000
	valA := bytes.Repeat([]byte("a"), 32)
	valB := bytes.Repeat([]byte("b"), 32)

	writeRange := func(val []byte) {
		const batchSize = 1024
		for base := 0; base < keys; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := base + batchSize
			if limit > keys {
				limit = keys
			}
			for i := base; i < limit; i++ {
				k := []byte(fmt.Sprintf("k%06d", i))
				if err := b.Set(k, val); err != nil {
					t.Fatalf("set: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = b.Close()
		}
	}

	// Seed baseline data.
	writeRange(valA)

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}
	beforePages := d.Pager().PageCount()

	// Repeatedly overwrite the same keyset to induce churn.
	for round := 0; round < 6; round++ {
		writeRange(valB)
		writeRange(valA)
	}

	// Advance commit seq for KeepRecent=1 and prune synchronously.
	_ = d.SetSync([]byte("zz"), valA)
	d.Prune()

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	afterPages := d.Pager().PageCount()

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	userPagesStr := rep["treedb.user.pages"]
	if userPagesStr == "" {
		t.Fatalf("missing treedb.user.pages")
	}
	userPages, err := strconv.ParseInt(userPagesStr, 10, 64)
	if err != nil {
		t.Fatalf("parse treedb.user.pages: %v", err)
	}

	// Bound growth loosely: the file should not balloon on stable key churn.
	if afterPages > beforePages*2 {
		t.Fatalf("pages grew too much: before=%d after=%d", beforePages, afterPages)
	}
	if afterInfo.Size() > beforeInfo.Size()*2 {
		t.Fatalf("index.db grew too much: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := VacuumIndexOffline(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	afterVacuumInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after vacuum: %v", err)
	}

	expectedMin := userPages * int64(page.PageSize)
	slack := expectedMin / 4
	if slack < 1024*int64(page.PageSize) {
		slack = 1024 * int64(page.PageSize)
	}
	expectedMax := expectedMin + slack
	if afterVacuumInfo.Size() < expectedMin {
		t.Fatalf("vacuumed index.db smaller than expected: got=%d min=%d", afterVacuumInfo.Size(), expectedMin)
	}
	if afterVacuumInfo.Size() > expectedMax {
		t.Fatalf("vacuumed index.db larger than expected: got=%d max=%d", afterVacuumInfo.Size(), expectedMax)
	}
}
