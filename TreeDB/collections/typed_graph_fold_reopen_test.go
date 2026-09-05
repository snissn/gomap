package collections

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestTypedGraphFoldProcessCut(t *testing.T) {
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"user"}}, {Name: "path", Strings: []string{"path"}}}
	ids, retained := [][]byte{[]byte("row")}, [][]byte{[]byte(`{"id":"row"}`)}
	if dir := os.Getenv("GOMAP_TYPED_FOLD_CRASH_DIR"); dir != "" {
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		columns[1].Strings[0] = "captured"
		if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("FOLD_CAPTURE_LSN=%d\n", state.manifest.AppliedCommandLSN)
		capturedLSN := state.manifest.AppliedCommandLSN
		closeState()
		mode := os.Getenv("GOMAP_TYPED_FOLD_CUT")
		entered, blocked := make(chan struct{}), make(chan struct{})
		var fired atomic.Bool
		run := func() error {
			return col.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 128, func() error {
				if mode == "unsealed_suffix" {
					durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Root == dir && event.Resource == durabilitycut.ResourceSeal && event.Point == durabilitycut.BeforePublicationSealWrite && fired.CompareAndSwap(false, true) {
							close(entered)
							<-blocked
						}
						return nil
					})
				}
				columns[1].Strings[0] = "after-capture"
				if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
					return err
				}
				current, closeCurrent, err := col.loadColumnStoreCompactionState(context.Background())
				if err != nil {
					return err
				}
				fmt.Printf("FOLD_SUFFIX_LSN=%d\n", current.manifest.AppliedCommandLSN)
				closeCurrent()
				if mode != "unsealed_suffix" {
					if err := db.Checkpoint(); err != nil {
						return err
					}
				}
				if mode == "before_seal" {
					durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Root == dir && event.Resource == durabilitycut.ResourceSeal && event.Point == durabilitycut.BeforePublicationSealWrite {
							os.Exit(73)
						}
						return nil
					})
				}
				return nil
			})
		}
		if mode == "unsealed_suffix" {
			done := make(chan error, 1)
			go func() { done <- run() }()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("fold returned before blocked suffix seal: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("suffix seal not reached")
			}
			deadline := time.After(10 * time.Second)
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					snap := db.AcquireSnapshot()
					catalog, err := loadCollectionCatalog(snap, "minima")
					_ = snap.Close()
					if err != nil {
						t.Fatal(err)
					}
					if catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN == capturedLSN {
						os.Exit(75)
					}
				case err := <-done:
					t.Fatalf("fold returned with seal blocked: %v", err)
				case <-deadline:
					t.Fatal("fold candidate did not become visible behind unsealed suffix")
				}
			}
		}
		err = run()
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(74) // successful fold has completed its explicit checkpoint
	}
	for _, mode := range []string{"before_seal", "after_seal", "unsealed_suffix"} {
		t.Run(mode, func(t *testing.T) {
			dir, db, col := openTypedMinimaCollection(t)
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			oldBaseLSN := state.catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN
			closeState()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedGraphFoldProcessCut$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "GOMAP_TYPED_FOLD_CRASH_DIR="+dir, "GOMAP_TYPED_FOLD_CUT="+mode)
			out, err := cmd.CombinedOutput()
			code := 74
			if mode == "before_seal" {
				code = 73
			}
			if mode == "unsealed_suffix" {
				code = 75
			}
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != code {
				t.Fatalf("child expected exit%d: %v\n%s", code, err, out)
			}
			var capturedLSN, suffixLSN uint64
			for _, line := range strings.Split(string(out), "\n") {
				if strings.HasPrefix(line, "FOLD_CAPTURE_LSN=") {
					_, _ = fmt.Sscanf(line, "FOLD_CAPTURE_LSN=%d", &capturedLSN)
				}
				if strings.HasPrefix(line, "FOLD_SUFFIX_LSN=") {
					_, _ = fmt.Sscanf(line, "FOLD_SUFFIX_LSN=%d", &suffixLSN)
				}
			}
			if capturedLSN <= oldBaseLSN {
				t.Fatalf("no advanced capture: %s", out)
			}
			var jsonScans atomic.Uint64
			restore := setColumnVectorGraphCanonicalRowsTestHook(func() { jsonScans.Add(1) })
			defer restore()
			db = openTypedMinimaDB(t, dir)
			defer db.Close()
			col, err = NewCollectionManager(db).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			state, closeState, err = col.loadColumnStoreCompactionState(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer closeState()
			want := capturedLSN
			if mode != "after_seal" {
				want = oldBaseLSN
			}
			if suffixLSN <= capturedLSN || state.manifest.AppliedCommandLSN != suffixLSN {
				t.Fatalf("recovered frontier=%d want acknowledged%d > captured%d", state.manifest.AppliedCommandLSN, suffixLSN, capturedLSN)
			}
			if state.catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN != want {
				t.Fatalf("base frontier=%d want%d", state.catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN, want)
			}
			limits := typedGraphOverlapLimits()
			if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
				t.Fatal(err)
			}
			owner, err := col.openTypedGraphReadOwner(limits)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			var buffer VectorIndexSearchBuffer
			results, _, err := owner.overlay.search(columns[0].Float32Vectors[0], 1, 16, 128, &buffer)
			if err != nil || len(results) != 1 || results[0].Score < .999 {
				t.Fatalf("results=%+v err=%v", results, err)
			}
			fetched, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
			if err != nil || !bytes.Contains(fetched.Results[0].Document, []byte(`"content":"after-capture"`)) {
				t.Fatalf("document=%+v err=%v", fetched, err)
			}
			if jsonScans.Load() != 0 {
				t.Fatal("fold/reopen extracted indexed JSON")
			}
		})
	}
}
