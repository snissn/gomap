package treedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// Leave actual typed and kind201 commands interleaved after a physical basis.
// Startup materializes preceding typed members before replaying each drop, so
// its physical manifest can differ from the one in the captured DDL payload.
func TestNativeTypedStringIndexDropUnappliedReplay(t *testing.T) {
	dir := os.Getenv("GOMAP_TYPED_INDEX_DROP_CRASH_DIR")
	child := dir != ""
	if child {
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	} else {
		dir = t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeTypedStringIndexDropUnappliedReplay$", "-test.v")
		cmd.Env = append(os.Environ(), "GOMAP_TYPED_INDEX_DROP_CRASH_DIR="+dir)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 97 || !bytes.Contains(output, []byte("TYPED DROP UNAPPLIED basis=2 accepted=10")) || bytes.Contains(output, []byte("DATA RACE")) {
			t.Fatalf("typed drop child: %v\n%s", err, output)
		}
		t.Logf("child:\n%s", output)
	}
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	before := workstats.Read().Replay
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	after := workstats.Read().Replay
	var manager *collections.CollectionManager
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	a, b, c := nativeTypedOpsRows("string")
	afterA := nativeTypedOpsChanged(a)
	afterB := b
	afterB.text = afterA.text
	d := c
	d.id, d.text, d.payload, d.retained = "d", afterA.text, "new-d", []byte(`{"id":"d", "payload":"new-d"}`)
	if child {
		fixture := nativeColumnPendingCaseFor(t, "string")
		fixture.typedSeed = &collections.TypedColumnBatch{}
		fixture.column.Owner = collections.TypedStorageOwnerRowAsset
		meta := nativeColumnPendingMeta(fixture)
		meta.Indexes = []collections.IndexDefinition{
			{Name: "label_lookup", Field: "label", ValueType: collections.IndexValueString},
			{Name: "label_unique", Field: "label", ValueType: collections.IndexValueString, Unique: true},
			{Name: "label_keep", Field: "label", ValueType: collections.IndexValueString},
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			t.Fatal(err)
		}
		col, err := manager.OpenCollection("events")
		if err != nil {
			t.Fatal(err)
		}
		insert := func(rows ...nativeTypedOpsRow) {
			t.Helper()
			ids, residual, columns := nativeTypedOpsInput("string", rows...)
			if got, _, err := col.InsertTypedBatchWithStats(ids, residual, columns); err != nil || len(got) != len(rows) {
				t.Fatalf("typed drop replay insert: ids=%q err=%v", got, err)
			}
		}
		replace := func(row nativeTypedOpsRow) {
			t.Helper()
			ids, residual, columns := nativeTypedOpsInput("string", row)
			if got, err := col.ReplaceTypedBatch(ids, residual, columns); err != nil || len(got) != 1 || !got[0].Modified {
				t.Fatalf("typed drop replay replace: results=%+v err=%v", got, err)
			}
		}
		insert(a, b)
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if applied := indexedCRUDAppliedLSN(t, handle); applied != 2 {
			t.Fatalf("typed drop seed applied=%d want2", applied)
		}
		insert(c)                                                // 3
		if _, err := col.DropIndex("label_lookup"); err != nil { // 4
			t.Fatal(err)
		}
		replace(afterA)                                                      // 5
		if _, err := col.DropIndexes([]string{"label_unique"}); err != nil { // 6
			t.Fatal(err)
		}
		replace(afterB)                                 // 7: duplicate scalar becomes legal.
		if _, err := col.DropAllIndexes(); err != nil { // 8
			t.Fatal(err)
		}
		if count, err := col.DeleteBatch([][]byte{[]byte("c")}); err != nil || count != 1 { // 9
			t.Fatalf("typed drop replay delete: count=%d err=%v", count, err)
		}
		insert(d) // 10
		checkNativeTypedOpsCurrent(t, col, "string", []nativeTypedOpsRow{afterA, afterB, d}, []string{"c"})
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if indexedCRUDAppliedLSN(t, handle) != 2 || handle.CommandWALNextLSN() != 11 {
			t.Fatalf("typed drop unapplied basis=%d next=%d", indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN())
		}
		layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
		if err != nil {
			t.Fatal(err)
		}
		paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[uint64]bool)
		for _, path := range paths {
			reader, err := commitlog.NewReader(path)
			if err != nil {
				t.Fatal(err)
			}
			for {
				frame, err := reader.ReadCommandFrame()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					_ = reader.Close()
					t.Fatal(err)
				}
				var want []string
				switch frame.LSN {
				case 4:
					want = []string{"label_keep", "label_unique"}
				case 6:
					want = []string{"label_keep"}
				case 8:
					want = []string{}
				default:
					continue
				}
				if seen[frame.LSN] || frame.Kind != commitlog.CommandKindCatalogIndexMutation || frame.PayloadFormat != commitlog.PayloadFormatCatalogIndexMutationV1 {
					t.Fatalf("typed drop canonical frame LSN%d kind=%d format=%d", frame.LSN, frame.Kind, frame.PayloadFormat)
				}
				payload, err := commitlog.DecodeCatalogIndexMutationPayload(frame.Payload)
				if err != nil || payload.Collection != "events" || payload.Incarnation == 0 {
					t.Fatalf("typed drop canonical payload: %+v err=%v", payload, err)
				}
				var prior, current collections.CollectionMeta
				if err := json.Unmarshal(payload.BeforeMetadata, &prior); err != nil {
					t.Fatalf("typed drop LSN%d before metadata decode: %v", frame.LSN, err)
				}
				if err := json.Unmarshal(payload.Metadata, &current); err != nil {
					t.Fatalf("typed drop LSN%d after metadata decode: %v", frame.LSN, err)
				}
				if current.Options.ColumnStore == nil {
					t.Fatalf("typed drop LSN%d removed original column configuration", frame.LSN)
				}
				expectedOptions := prior.Options
				if len(current.Indexes) == 0 {
					// Final-index removal preserves the entire original config,
					// while the maintained indexed-buffer policy clears exactly
					// these three derived fields when no declarations remain.
					expectedOptions.BufferedIndexedWrites = false
					expectedOptions.BufferedIndexedAsyncFlush = false
					expectedOptions.BufferedIndexedAsyncFlushMaxQueuedUnits = 0
				}
				if !reflect.DeepEqual(expectedOptions, current.Options) {
					t.Fatalf("typed drop LSN%d changed options beyond the exact indexed-buffer policy", frame.LSN)
				}
				got := make([]string, 0, len(current.Indexes))
				for _, index := range current.Indexes {
					got = append(got, index.Name)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("typed drop LSN%d schema=%q want=%q", frame.LSN, got, want)
				}
				seen[frame.LSN] = true
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if len(seen) != 3 {
			t.Fatalf("observed typed kind201 frames=%v want LSN4/6/8", seen)
		}
		t.Log("TYPED DROP UNAPPLIED basis=2 accepted=10")
		os.Exit(97)
	}
	if after.FramesApplied-before.FramesApplied != 8 || after.FrameErrors != before.FrameErrors {
		t.Fatalf("actual typed/drop replay: %+v -> %+v", before, after)
	}
	for reopen := 0; reopen < 2; reopen++ {
		col, err := manager.OpenCollection("events")
		if err != nil {
			t.Fatal(err)
		}
		checkNativeTypedOpsCurrent(t, col, "string", []nativeTypedOpsRow{afterA, afterB, d}, []string{"c"})
		if len(col.Meta().Indexes) != 0 || indexedCRUDAppliedLSN(t, handle) != 10 || handle.CommandWALNextLSN() != 11 {
			t.Fatal("typed drop replay lost schema or canonical identity")
		}
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("typed drop replay final registry: %+v", stats)
		}
		if reopen == 0 {
			before := workstats.Read().Replay
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if after := workstats.Read().Replay; after.FramesApplied != before.FramesApplied || after.FrameErrors != before.FrameErrors {
				t.Fatalf("typed drop replay repeated: %+v -> %+v", before, after)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
		}
	}
}
