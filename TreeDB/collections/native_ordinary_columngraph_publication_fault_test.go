package collections

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Ordinary ColumnGraph publishes retained JSON and authoritative vector columns
// in the native transaction. Its ambiguity boundary is command assignment, not
// the retired ordinary runtime's post-insert catalog reload.
func TestNativeOrdinaryColumnGraphInsertBatchPublicationCuts(t *testing.T) {
	const dirEnv = "GOMAP_NATIVE_ORDINARY_PUBLICATION_FAULT_DIR"
	const cutEnv = "GOMAP_NATIVE_ORDINARY_PUBLICATION_FAULT_CUT"
	const causeEnv = "GOMAP_NATIVE_ORDINARY_PUBLICATION_FAULT_CAUSE"
	const marker = "ORIGINAL ORDINARY COLUMNGRAPH PUBLICATION CUT VERIFIED"
	type walCut struct {
		path string
		lsn  uint64
	}
	ids := [][]byte{[]byte("u1"), []byte("u2")}
	docs := [][]byte{[]byte(`{"name":"ada","embedding":[1,0]}`), []byte(`{"name":"grace","embedding":[0,1]}`)}
	vectors := [][]float32{{1, 0}, {0, 1}}
	if dir := os.Getenv(dirEnv); dir != "" {
		_, backend, _, col, _ := openNativeOrdinaryPublicationFaultCollection(t, dir, false)
		before, ok := backend.StateToken()
		if !ok {
			t.Fatal("missing initial state token")
		}
		next := backend.CommandWALNextLSN()
		// The public owner directory contains maindb; the scanner takes the
		// backend directory and reads its physical WAL segments directly.
		frames := collectionCommandWALFrames(t, backend.Dir())
		held, err := col.nativeJSON.Capture("users")
		if err != nil {
			t.Fatal(err)
		}
		point := durabilitycut.BeforeDependencyAppend
		assigned := os.Getenv(cutEnv) == "after_sync"
		if assigned {
			point = durabilitycut.AfterDependencyFileSync
		}
		var injected error = io.EOF
		if os.Getenv(causeEnv) == "unexpected_eof" {
			injected = io.ErrUnexpectedEOF
		}
		var attempts, appends, cuts, faults atomic.Int32
		var appendCut, syncCut atomic.Pointer[walCut]
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Resource != durabilitycut.ResourceCommandWAL {
				return nil
			}
			if event.Point == durabilitycut.BeforeDependencyAppend {
				attempts.Add(1)
			}
			if event.Point == durabilitycut.AfterDependencyAppend {
				appends.Add(1)
				appendCut.Store(&walCut{path: event.Path, lsn: event.LSN})
			}
			if event.Point == point {
				cuts.Add(1)
				if assigned {
					// Copy diagnostic identity only; never retain StablePath.
					syncCut.Store(&walCut{path: event.Path, lsn: event.LSN})
				}
				if faults.CompareAndSwap(0, 1) {
					return injected
				}
			}
			return nil
		})
		result, err := col.InsertBatch(ids, docs)
		restore()
		if !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) != assigned || result != nil {
			t.Fatalf("InsertBatch ids=%q err=%v assigned=%t cause=%v", result, err, assigned, injected)
		}
		if assigned && !strings.Contains(err.Error(), "native command publication completion") {
			t.Fatalf("missing native publication context: %v", err)
		}
		if attempts.Load() != 1 || cuts.Load() != 1 || faults.Load() != 1 || isRetriableCollectionMutationError(err) {
			t.Fatalf("fault/retry boundary: attempts=%d cuts=%d faults=%d err=%v", attempts.Load(), cuts.Load(), faults.Load(), err)
		}
		wantAppends := int32(0)
		if assigned {
			wantAppends = 1
		}
		if appends.Load() != wantAppends || backend.CommandWALNextLSN() != next+uint64(wantAppends) {
			t.Fatalf("assignment repeated: appends=%d next=%d initial=%d", appends.Load(), backend.CommandWALNextLSN(), next)
		}
		afterFrames := collectionCommandWALFrames(t, backend.Dir())
		if len(afterFrames) != len(frames)+int(wantAppends) {
			t.Fatalf("WAL frames=%d initial=%d assigned=%d", len(afterFrames), len(frames), wantAppends)
		}
		for i, original := range frames {
			frame := afterFrames[i]
			if frame.LSN != original.LSN || frame.Kind != original.Kind || frame.PayloadFormat != original.PayloadFormat || !bytes.Equal(frame.Payload, original.Payload) {
				t.Fatalf("publication changed prior WAL frame %d: %+v", i, frame)
			}
		}
		if assigned {
			appendEvent, syncEvent := appendCut.Load(), syncCut.Load()
			if appendEvent == nil || syncEvent == nil || appendEvent.lsn != next || syncEvent.lsn != next || appendEvent.path != syncEvent.path || filepath.Dir(appendEvent.path) != backenddb.WALDirPath(backend.Dir()) || afterFrames[len(frames)].LSN != next {
				t.Fatalf("lost original durable identity: append=%+v sync=%+v frame=%d want=%d", appendEvent, syncEvent, afterFrames[len(frames)].LSN, next)
			}
		}
		if now, ok := backend.StateToken(); !ok || now != before {
			t.Fatalf("failed publication changed checkpoint authority: before=%+v after=%+v", before, now)
		}
		// Public native completion syncs after installation. The failed ACK can
		// therefore leave the complete new logical cut visible, while the held
		// opening cut still owns neither attempted document nor vector row.
		assertNativeOrdinaryPublicationFaultRows(t, held, ids, docs, vectors, false)
		fresh, err := col.nativeJSON.Capture("users")
		if err != nil {
			t.Fatal(err)
		}
		wantSequence := held.capture.LogicalSequence() + uint64(wantAppends)
		if fresh.capture.LogicalSequence() != wantSequence {
			t.Fatalf("logical mutation repeated: sequence=%d want=%d", fresh.capture.LogicalSequence(), wantSequence)
		}
		assertNativeOrdinaryPublicationFaultRows(t, fresh, ids, docs, vectors, assigned)
		if err := fresh.Close(); err != nil {
			t.Fatal(err)
		}
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
		if assigned {
			if _, e := col.InsertBatch([][]byte{[]byte("forbidden")}, [][]byte{docs[0]}); !errors.Is(e, backenddb.ErrRecoveryRequired) || backend.CommandWALNextLSN() != next+1 {
				t.Fatalf("assigned failure bypassed recovery fence: err=%v next=%d", e, backend.CommandWALNextLSN())
			}
		}
		_, _ = os.Stdout.WriteString(marker + "\n")
		os.Exit(0) // No Close, Flush or checkpoint can rescue the failed publication.
	}
	for _, cut := range []string{"before_append", "after_sync"} {
		for _, cause := range []string{"eof", "unexpected_eof"} {
			t.Run(cut+"/"+cause, func(t *testing.T) {
				dir := t.TempDir()
				owner, backend, _, _, closeAll := openNativeOrdinaryPublicationFaultCollection(t, dir, true)
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				next := backend.CommandWALNextLSN()
				closeAll()
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeOrdinaryColumnGraphInsertBatchPublicationCuts$")
				cmd.Env = append(os.Environ(), dirEnv+"="+dir, cutEnv+"="+cut, causeEnv+"="+cause)
				output, err := cmd.CombinedOutput()
				if err != nil || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("publication cut child: %v\n%s", err, output)
				}
				_, recovered, _, col, _ := openNativeOrdinaryPublicationFaultCollection(t, dir, false)
				wantNext := next
				if cut == "after_sync" {
					wantNext++
				}
				if recovered.CommandWALNextLSN() != wantNext {
					t.Fatalf("recovery reassigned command: next=%d want=%d", recovered.CommandWALNextLSN(), wantNext)
				}
				read, err := col.nativeJSON.Capture("users")
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()
				assertNativeOrdinaryPublicationFaultRows(t, read, ids, docs, vectors, cut == "after_sync")
				assertNativeOrdinaryPublicationFaultRows(t, read, [][]byte{[]byte("forbidden")}, docs[:1], vectors[:1], false)
			})
		}
	}
}

func openNativeOrdinaryPublicationFaultCollection(t *testing.T, dir string, create bool) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection, func()) {
	t.Helper()
	owner, err := treedb.Open(nativeTypedGroupOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	var manager *CollectionManager
	closeAll := func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if owner != nil {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
			owner = nil
		}
	}
	t.Cleanup(closeAll)
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	if create {
		_, err := manager.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
		}}, VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if col.nativeJSON == nil || col.db != nil {
		t.Fatal("publication fixture did not use the public native collection facade")
	}
	return owner, backend, manager, col, closeAll
}

func assertNativeOrdinaryPublicationFaultRows(t *testing.T, read *NativeJSONRead, ids, docs [][]byte, vectors [][]float32, present bool) {
	t.Helper()
	for i, id := range ids {
		got, err := read.Get(id)
		if present {
			if err != nil || !bytes.Equal(got, docs[i]) {
				t.Fatalf("document %q=%q err=%v want=%q", id, got, err, docs[i])
			}
		} else if !errors.Is(err, tree.ErrKeyNotFound) {
			t.Fatalf("uncommitted document %q=%q err=%v", id, got, err)
		}
		found, equal, err := read.typedRowEqual(id, docs[i], []columnDeclaredValue{{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vectors[i]}})
		if err != nil || found != present || equal != present {
			t.Fatalf("vector %q: found=%t equal=%t err=%v want present=%t", id, found, equal, err, present)
		}
	}
}
