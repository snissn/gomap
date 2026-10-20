package collections

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Exercise the registered startup replay executor with a real persisted schema
// and incarnation. Framing validation alone cannot reject an erased bad row.
func TestNativeJSONCompositeReplayValidatesErasedDocuments(t *testing.T) {
	for _, operation := range []string{"insert", "update"} {
		for _, variant := range []struct {
			name, document, wantError string
			indexed                   bool
			bsonFormat                bool
		}{
			{"unindexed-opaque-cancellation", `{"email":`, "", false, false},
			{"indexed-invalid-json", `{"email":`, "invalid JSON", true, false},
			{"indexed-invalid-scalar", `{"email":17}`, "must be string", true, false},
			// This temporarily collides with u1, but the subsequent delete removes
			// that intermediate owner. Uniqueness is deliberately final-state only.
			{"indexed-valid-cancellation", `{"email":"a@example.com"}`, "", true, false},
			{"unindexed-invalid-bson", "", "invalid BSON", false, true},
			{"indexed-invalid-bson", "", "invalid BSON", true, true},
			{"bson-indexed-invalid-scalar", "", "must be string", true, true},
			{"bson-indexed-valid-cancellation", "", "", true, true},
		} {
			t.Run(operation+"/"+variant.name, func(t *testing.T) {
				meta := CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}
				if variant.indexed {
					meta.Indexes = []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}}
				}
				seed := [][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)}
				document := []byte(variant.document)
				if variant.bsonFormat {
					meta.Options.DocumentFormat = DocumentFormatBSON
					encode := func(value any) []byte {
						t.Helper()
						out, err := bson.Marshal(bson.D{{Key: "email", Value: value}})
						if err != nil {
							t.Fatal(err)
						}
						return out
					}
					seed = [][]byte{encode("a@example.com"), encode("b@example.com")}
					document = encode("a@example.com")
					if strings.Contains(variant.name, "invalid-bson") {
						document = document[:len(document)-1]
					}
					if strings.Contains(variant.name, "invalid-scalar") {
						document = encode(int32(17))
					}
				}
				dir := prepareCollectionCommandWALDir(t, meta, collectionCommandWALSetupInsert{
					ids: [][]byte{[]byte("u1"), []byte("u2")}, docs: seed,
				})
				// Read the identity actually published by CreateCollection; no test
				// identity, native state or replay token is fabricated.
				d := openCollectionCommandWALDB(t, dir)
				snapshot := d.AcquireSnapshot()
				if snapshot == nil {
					_ = d.Close()
					t.Fatal("setup snapshot unavailable")
				}
				token, ok := snapshot.StateToken()
				if !ok {
					_ = snapshot.Close()
					_ = d.Close()
					t.Fatal("setup snapshot token unavailable")
				}
				identity, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte(nativeCatalogIncarnationPrefix+meta.Name))
				if err != nil {
					_ = snapshot.Close()
					_ = d.Close()
					t.Fatal(err)
				}
				incarnation, identityErr := nativeCatalogIdentityValue(identity, false)
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				if identityErr != nil {
					t.Fatal(identityErr)
				}

				id := []byte("u3")
				kind, format := commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1
				docs := []commitlog.CollectionDocument{{ID: id, Document: document}}
				payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(meta.Name, docs)
				if operation == "update" {
					id = []byte("u2")
					docs[0].ID = id
					kind, format = commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1
					payload, err = commitlog.EncodeCollectionUpdateBatchByIDPayload(meta.Name, docs)
				}
				if err != nil {
					t.Fatal(err)
				}
				deletion, err := commitlog.EncodeCollectionDeleteBatchByIDPayload(meta.Name, [][]byte{id})
				if err != nil {
					t.Fatal(err)
				}
				members := []commitlog.CompositeMutationMember{
					{Kind: kind, PayloadFormat: format, Incarnation: incarnation, Payload: payload},
					{Kind: commitlog.CommandKindCollectionDeleteBatchByID, PayloadFormat: commitlog.PayloadFormatCollectionDeleteBatchByIDV1, Incarnation: incarnation, Payload: deletion},
				}
				composite, err := commitlog.EncodeCompositeMutationPayloadTo(nil, members)
				if err != nil {
					t.Fatal(err)
				}
				walDir := backenddb.WALDirPath(dir)
				if err := os.MkdirAll(walDir, 0o755); err != nil {
					t.Fatal(err)
				}
				writer, err := commitlog.NewWriter(filepath.Join(walDir, commitlog.CommandSegmentName(0, 1)))
				if err != nil {
					t.Fatal(err)
				}
				appendErr := writer.AppendCommand(commitlog.CommandEnvelope{Version: commitlog.CommandFrameVersionV2, LSN: 1,
					Kind: commitlog.CommandKindCompositeMutation, Scope: commitlog.CommandScopeSystem,
					DurabilityClass: commitlog.CommandDurabilityDurable, PayloadFormat: commitlog.PayloadFormatCompositeMutationV1, Payload: composite})
				closeErr := writer.Close()
				if appendErr != nil || closeErr != nil {
					t.Fatalf("write actual frame: append=%v close=%v", appendErr, closeErr)
				}
				before := workstats.Read().Replay
				reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
				after := workstats.Read().Replay
				if variant.wantError != "" {
					if reopened != nil {
						if closeErr := reopened.Close(); closeErr != nil {
							t.Error(closeErr)
						}
					}
					if err == nil || !strings.Contains(err.Error(), variant.wantError) {
						t.Fatalf("erased invalid document replay err=%v, want %q", err, variant.wantError)
					}
					if after.FramesAttempted-before.FramesAttempted != 1 || after.FramesApplied != before.FramesApplied || after.FrameErrors-before.FrameErrors != 1 {
						t.Fatalf("expected actual replay refusal before=%+v after=%+v", before, after)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if reopened != nil {
						if err := reopened.Close(); err != nil {
							t.Error(err)
						}
					}
				})
				if after.FramesAttempted-before.FramesAttempted != 1 || after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
					t.Fatalf("expected actual cancellation replay before=%+v after=%+v", before, after)
				}
				manager := NewCollectionManager(reopened)
				t.Cleanup(func() {
					if manager != nil {
						if err := manager.Close(); err != nil {
							t.Error(err)
						}
					}
				})
				col, err := manager.OpenCollection(meta.Name)
				if err != nil {
					t.Fatal(err)
				}
				if variant.bsonFormat {
					if got, err := col.Get([]byte("u1")); err != nil || !bytes.Equal(got, seed[0]) {
						t.Fatalf("original row changed: got=%x err=%v", got, err)
					}
				} else {
					assertCollectionDocument(t, col, "u1", `{"email":"a@example.com"}`)
				}
				if got, err := col.Get(id); err != nil || got != nil {
					t.Fatalf("canceled row=%s err=%v", got, err)
				}
				if variant.indexed {
					owners, err := col.FindByIndexValue("email", "a@example.com")
					if err != nil || len(owners) != 1 || string(owners[0]) != "u1" {
						t.Fatalf("final unique owner=%q err=%v", owners, err)
					}
				}
				if reopened.State().AppliedCommandLSN != 1 {
					t.Fatal("cancellation did not apply exactly one frame")
				}
				closeManager := manager
				manager = nil
				if err := closeManager.Close(); err != nil {
					t.Fatal(err)
				}
				closeDB := reopened
				reopened = nil
				if err := closeDB.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
