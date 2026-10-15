package db

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDurableRootMetadataReuseLowPlacementAndFallback4627(t *testing.T) {
	dir := t.TempDir()
	options := Options{Dir: dir, DisableBackgroundPrune: true}
	database, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	if err := database.SetSync([]byte("retained"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	old := database.AcquireSnapshot()
	defer old.Close()
	oldRef := database.durableRoot.record.Freelist
	if err := database.SetSync([]byte("retained"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		publishNeutralMetadata4627(t, database)
	}
	if got, err := old.Get([]byte("retained")); err != nil || string(got) != "old" {
		t.Fatalf("held old snapshot=%q %v", got, err)
	}
	if _, err := freelist.LoadGenerationV1(database.idx.Load().pager, oldRef); err != nil {
		t.Fatalf("held old generation: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	var warmHigh uint64
	var lowMetadata, lowAux bool
	for i := 0; i < 64; i++ {
		before := database.idx.Load().pager.PageCount()
		publishNeutralMetadata4627(t, database)
		record := database.durableRoot.record
		lowMetadata = lowMetadata || record.Freelist.HeaderPageID < before
		lowAux = lowAux || (record.Manifest.FirstPageID < before && database.durableRoot.meta.RootRecordPageID < before)
		if i == 31 {
			warmHigh = record.TotalPages
		}
	}
	final := database.durableRoot.record.TotalPages
	t.Logf("after32=%d after64=%d lowMetadata=%v lowAux=%v", warmHigh, final, lowMetadata, lowAux)
	if !lowMetadata || !lowAux || final != warmHigh {
		t.Fatalf("missing steady reused metadata/auxiliary placement: warm=%d final=%d low=%v/%v", warmHigh, final, lowMetadata, lowAux)
	}
	validateBothMetadataSlots4627(t, database)
	active := database.metaPageID
	fallback := database.durableRoot.slotRecord[1-active]
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	validateBothMetadataSlots4627(t, reopened)
	if got, err := reopened.Get([]byte("retained")); err != nil || string(got) != "new" {
		t.Fatalf("latest reopen=%q %v", got, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	corruptIndexPageByte(t, dir, active)
	recovered, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.State().CommitSeq != fallback.CommitSeq || recovered.State().AppliedCommandLSN != fallback.AppliedCommandLSN {
		t.Fatalf("not exact fallback: state=%+v want commit=%d lsn=%d", recovered.State(), fallback.CommitSeq, fallback.AppliedCommandLSN)
	}
	if got, err := recovered.Get([]byte("retained")); err != nil || string(got) != "new" {
		t.Fatalf("fallback value=%q %v", got, err)
	}
	if _, err := freelist.LoadGenerationV1(recovered.idx.Load().pager, fallback.Freelist); err != nil {
		t.Fatal(err)
	}
}

func publishNeutralMetadata4627(t testing.TB, database *DB) {
	t.Helper()
	state := database.State()
	lsn := state.AppliedCommandLSN + 1
	if err := database.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, lsn, []CommandWALLSNRange{{First: lsn, Last: lsn}}, true); err != nil {
		t.Fatal(err)
	}
}

func validateBothMetadataSlots4627(t testing.TB, database *DB) {
	t.Helper()
	for slot := uint64(0); slot < 2; slot++ {
		record := database.durableRoot.slotRecord[slot]
		if record.CommitSeq == 0 {
			t.Fatalf("slot%d absent", slot)
		}
		generation, err := freelist.LoadGenerationV1(database.idx.Load().pager, record.Freelist)
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := rootpublication.LoadDependencyManifestV1(database.idx.Load().pager, record.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		if manifest.PageCount() != record.Manifest.PageCount {
			t.Fatal("manifest page count mismatch")
		}
		aux, err := durableRootSlotAuxiliaryPagesV1(database.durableRoot.slotMeta[slot], record)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range aux {
			if generation.Allocatable(id) {
				t.Fatalf("slot%d live auxiliary%d is free", slot, id)
			}
		}
	}
}

func TestDurableRootMetadataReuseMultipageManifest4627(t *testing.T) {
	dir := t.TempDir()
	options := Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}}
	database, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var pointers []page.ValuePtr
	for i := 0; i < 24; i++ {
		pointers = append(pointers, appendPointersInNewSegment(t, dir, 0, uint32(i+1), uint64(10000+i), 1, func(int) []byte { return []byte("immutable dependency payload") })[0])
	}
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	for i, ptr := range pointers {
		if err := batch.SetPointer([]byte(fmt.Sprintf("external-%02d", i)), ptr); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if database.durableRoot.record.Manifest.PageCount < 2 {
		t.Fatal("fixture did not build multipage manifest")
	}
	low := false
	for i := 0; i < 48; i++ {
		before := database.idx.Load().pager.PageCount()
		publishNeutralMetadata4627(t, database)
		ref := database.durableRoot.record.Manifest
		if ref.PageCount < 2 {
			t.Fatal("manifest lost dependencies")
		}
		low = low || ref.FirstPageID+uint64(ref.PageCount) < before
	}
	if !low {
		t.Fatal("multipage manifest never reused a low interval")
	}
	validateBothMetadataSlots4627(t, database)
	t.Logf("manifest pages=%d entries=%d totalpages=%d", database.durableRoot.record.Manifest.PageCount, database.durableRoot.record.Manifest.EntryCount, database.durableRoot.record.TotalPages)
	active := database.metaPageID
	fallbackCommit := database.durableRoot.slotRecord[1-active].CommitSeq
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	validateBothMetadataSlots4627(t, reopened)
	for i := range pointers {
		got, err := reopened.Get([]byte(fmt.Sprintf("external-%02d", i)))
		if err != nil || string(got) != "immutable dependency payload" {
			t.Fatalf("external%d=%q %v", i, got, err)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	corruptIndexPageByte(t, dir, active)
	fallback, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer fallback.Close()
	if fallback.State().CommitSeq != fallbackCommit || fallback.durableRoot.record.Manifest.PageCount < 2 {
		t.Fatal("multipage fallback authority not retained")
	}
	for i := range pointers {
		got, err := fallback.Get([]byte(fmt.Sprintf("external-%02d", i)))
		if err != nil || string(got) != "immutable dependency payload" {
			t.Fatalf("fallback external%d=%q %v", i, got, err)
		}
	}
}

func TestDurableRootMetadataReuseProcessCut4627(t *testing.T) {
	if mode := os.Getenv("TREEDB_METADATA_REUSE_CUT4627"); mode != "" {
		database, err := Open(Options{Dir: os.Getenv("TREEDB_METADATA_REUSE_DIR4627"), DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := database.SetSync([]byte("retained"), []byte("old")); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 64; i++ {
			publishNeutralMetadata4627(t, database)
		}
		if database.State().CommitSeq != 66 {
			t.Fatalf("fixture commit=%d", database.State().CommitSeq)
		}
		before := database.idx.Load().pager.PageCount()
		low := false
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Point == durabilitycut.BeforePublicationSealWrite {
				low = uint64(event.Offset)/page.PageSize < before
				if !low {
					return fmt.Errorf("seal did not reuse low page: offset=%d before=%d", event.Offset, before)
				}
				if mode == "before-seal" {
					os.Exit(73)
				}
			}
			return nil
		})
		defer restore()
		if err := database.SetSync([]byte("retained"), []byte("new")); err != nil {
			t.Fatal(err)
		}
		if mode != "after-ack" || !low {
			t.Fatal("cut not reached")
		}
		os.Exit(73) // Deliberately no Close after acknowledged publication.
	}
	for _, mode := range []string{"before-seal", "after-ack"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDurableRootMetadataReuseProcessCut4627$")
			cmd.Env = append(os.Environ(), "TREEDB_METADATA_REUSE_CUT4627="+mode, "TREEDB_METADATA_REUSE_DIR4627="+dir)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("child did not hit cut: %v\n%s", err, output)
			}
			reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			want, commit := "old", uint64(66)
			if mode == "after-ack" {
				want, commit = "new", 67
			}
			if got, err := reopened.Get([]byte("retained")); err != nil || string(got) != want {
				t.Fatalf("recovered value=%q err=%v want%s", got, err, want)
			}
			if reopened.State().CommitSeq != commit {
				t.Fatalf("recovered commit=%d want%d", reopened.State().CommitSeq, commit)
			}
			validateBothMetadataSlots4627(t, reopened)
		})
	}
}
