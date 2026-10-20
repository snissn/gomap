package treedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// Useful cohorts must fit the ordinary default admission envelope, not merely
// a tiny family smoke. Each batch is one ordinary public insert command.
func TestNativeExistingColumnUsefulCohort(t *testing.T) {
	for _, family := range []string{"string", "float32_vector"} {
		t.Run(family, func(t *testing.T) {
			const batchRows, batches = 32, 3
			fixture := nativeColumnPendingCaseFor(t, family)
			if family == "float32_vector" {
				fixture.column.VectorDims = 384
			}
			ids, expected := make([][]byte, batchRows*batches), make([][]byte, batchRows*batches)
			for row := range ids {
				ids[row] = []byte(fmt.Sprintf("row-%03d", row))
				document := map[string]any{"payload": string(ids[row])}
				if family == "string" {
					// About 2 KiB of declared string, with JSON escaping required.
					document[fixture.column.Path] = fmt.Sprintf("label-%03d:", row) + strings.Repeat("quote\"\\\t\n", 224)
				} else {
					values := make([]float32, fixture.column.VectorDims)
					for dim := range values {
						values[dim] = float32((row*7+dim)%257-128) / 8
					}
					document[fixture.column.Path] = values
				}
				var err error
				expected[row], err = json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
			}
			opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			var manager *collections.CollectionManager
			var held *collections.CollectionReadView
			t.Cleanup(func() {
				if held != nil {
					if err := held.Close(); err != nil {
						t.Error(err)
					}
				}
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
			if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
				t.Fatal(err)
			}
			collection, err := manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			checkRows := func(count int) {
				t.Helper()
				for row := 0; row < count; row++ {
					got, err := collection.Get(ids[row])
					if err != nil {
						t.Fatalf("Get row %d: %v", row, err)
					}
					requireNativeColumnJSON(t, got, expected[row])
				}
			}
			checkHeld := func() {
				t.Helper()
				if held == nil {
					return
				}
				result, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
				if err != nil || len(result.Results) != len(ids) {
					t.Fatalf("held cohort results=%d: %v", len(result.Results), err)
				}
				for row, item := range result.Results {
					if item.Found != (row < batchRows) {
						t.Fatalf("held row %d found=%t", row, item.Found)
					}
					if item.Found {
						requireNativeColumnJSON(t, item.Document, expected[row])
						item.Document[0] = '!'
					}
				}
			}
			for batch := 0; batch < batches; batch++ {
				start, end := batch*batchRows, (batch+1)*batchRows
				documents := make([][]byte, batchRows)
				for i := range documents {
					documents[i] = bytes.Clone(expected[start+i])
				}
				beforeLSN := handle.CommandWALNextLSN()
				beforeSeq, err := strconv.ParseUint(database.Stats()["treedb.cache.cow.txn.logical_seq"], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				inserted, err := collection.InsertBatch(ids[start:end], documents)
				if err != nil {
					t.Fatalf("batch %d ordinary InsertBatch: %v", batch, err)
				}
				if len(inserted) != batchRows {
					t.Fatalf("batch %d inserted=%d", batch, len(inserted))
				}
				for i, id := range inserted {
					if !bytes.Equal(id, ids[start+i]) {
						t.Fatalf("batch %d returned ID %d=%q", batch, i, id)
					}
					documents[i][0] = '!'
				}
				if handle.CommandWALNextLSN() != beforeLSN+1 || database.Stats()["treedb.cache.cow.txn.logical_seq"] != strconv.FormatUint(beforeSeq+1, 10) {
					t.Fatalf("batch %d did not install one canonical command and successor", batch)
				}
				requireNativeUsefulCohortFrame(t, opts, beforeLSN, ids[start:end], expected[start:end])
				got, err := collection.Get(ids[start])
				if err != nil {
					t.Fatalf("batch %d pending Get: %v", batch, err)
				}
				requireNativeColumnJSON(t, got, expected[start])
				got[0] = '!'
				checkRows(end) // The second read must not observe caller mutation.
				checkHeld()
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("batch %d checkpoint: %v", batch, err)
				}
				if applied := indexedCRUDAppliedLSN(t, handle); applied != beforeLSN {
					t.Fatalf("batch %d applied=%d want=%d", batch, applied, beforeLSN)
				}
				checkRows(end)
				if batch == 0 {
					held, err = collection.OpenCollectionReadView()
					if err != nil {
						t.Fatal(err)
					}
				}
				checkHeld()
			}
			checkRows(len(ids))
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			closeDatabase := func() {
				t.Helper()
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
					t.Fatalf("cohort final registry %+v", stats)
				}
			}
			closeDatabase()
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			collection, err = manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			checkRows(len(ids))
			closeDatabase()
		})
	}
}

func requireNativeUsefulCohortFrame(t *testing.T, opts Options, lsn uint64, ids, documents [][]byte) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("cohort WAL paths=%d: %v", len(paths), err)
	}
	found := 0
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
			if frame.LSN != lsn {
				continue
			}
			found++
			if frame.Kind != commitlog.CommandKindCompositeMutation || frame.PayloadFormat != commitlog.PayloadFormatCompositeMutationV1 {
				_ = reader.Close()
				t.Fatalf("cohort LSN %d kind=%v format=%v", lsn, frame.Kind, frame.PayloadFormat)
			}
			members, err := commitlog.DecodeCompositeMutationPayloadView(frame.Payload)
			if err != nil || len(members) != 1 || members[0].Incarnation == 0 || members[0].Kind != commitlog.CommandKindCollectionInsertBatchByID || members[0].PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
				_ = reader.Close()
				t.Fatalf("cohort LSN %d exact insert member: %+v %v", lsn, members, err)
			}
			payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(members[0].Payload)
			if err != nil || payload.Collection != "events" || len(payload.Documents) != len(ids) {
				_ = reader.Close()
				t.Fatalf("cohort LSN %d collection=%q rows=%d: %v", lsn, payload.Collection, len(payload.Documents), err)
			}
			for i, row := range payload.Documents {
				if !bytes.Equal(row.ID, ids[i]) || !bytes.Equal(row.Document, documents[i]) {
					_ = reader.Close()
					t.Fatalf("cohort LSN %d canonical row %d differs", lsn, i)
				}
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if found != 1 {
		t.Fatalf("cohort LSN %d frames=%d", lsn, found)
	}
}
