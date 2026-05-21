package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionCommandWALInsertBatchByIDPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		[]byte(`{"name":"Ada"}`),
		[]byte(`{"name":"Grace"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionDocument(t, reopened, "u1", `{"name":"Ada"}`)
	assertCollectionDocument(t, reopened, "u2", `{"name":"Grace"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertByIDPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"Ada"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCollectionInsertBatchByID {
		_ = d.Close()
		t.Fatalf("command WAL frames=%+v, want one collection insert frame", frames)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionDocument(t, reopened, "u1", `{"name":"Ada"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDReplayRecoversUnappliedFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u2"), Document: []byte(`{"name":"Grace"}`)},
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	assertCollectionDocument(t, col, "u2", `{"name":"Grace"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != 1 {
		t.Fatalf("command WAL frames after replay=%d, want original frame only", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDReplayTemplateV1StoredDocument(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection setup: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	encoder := &TemplateV1Encoder{}
	seed := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "HNL"})
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("seed")}, [][]byte{seed}, encoder); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatchWithTemplateV1Encoder setup: %v", err)
	}
	stored, err := encoder.EncodeDocument([]string{"name", "city"}, []any{"Grace", "NYC"})
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeDocument stored: %v", err)
	}
	if !bytes.HasPrefix(stored, []byte(templateV1StoredMagic)) {
		_ = d.Close()
		t.Fatalf("stored prefix=%q, want %q", stored[:min(len(stored), len(templateV1StoredMagic))], templateV1StoredMagic)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u1"), Document: stored},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Grace"`)) || !bytes.Contains(jsonDoc, []byte(`"NYC"`)) {
		t.Fatalf("materialized template doc=%s, want Grace/NYC", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDTemplateV1NewShapeReplay(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}
	doc := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "SEA"})

	updateDir := prepareCollectionCommandWALDir(t, meta)
	updateDB := openCollectionCommandWALDB(t, updateDir)
	updateCol, err := NewCollectionManager(updateDB).OpenCollection("users")
	if err != nil {
		_ = updateDB.Close()
		t.Fatalf("OpenCollection update: %v", err)
	}
	if _, err := updateCol.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		_ = updateDB.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := updateDB.Close(); err != nil {
		t.Fatalf("Close update DB: %v", err)
	}

	frames := collectionCommandWALFrames(t, updateDir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	frame := frames[0]
	if frame.Kind != commitlog.CommandKindCollectionInsertBatchByID {
		t.Fatalf("frame kind=%d, want insert batch", frame.Kind)
	}
	payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(frame.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
	if len(payload.Documents) != 1 {
		t.Fatalf("payload documents=%d, want 1", len(payload.Documents))
	}
	if bytes.HasPrefix(payload.Documents[0].Document, []byte(templateV1StoredMagic)) {
		t.Fatalf("TemplateV1 insert WAL payload used stored document bytes; replay needs deterministic input envelope")
	}

	replayDir := prepareCollectionCommandWALDir(t, meta)
	writeCollectionCommandWALFrame(t, replayDir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, frame.Payload)
	reopen := openCollectionCommandWALDB(t, replayDir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replay: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Ada"`)) || !bytes.Contains(jsonDoc, []byte(`"SEA"`)) {
		t.Fatalf("materialized template doc=%s, want Ada/SEA", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALInsertBatchByIDReplayAdvancesEmptyFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", nil)
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDReplayIgnoresMissingIDs(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
			[]byte(`{"name":"Grace"}`),
		},
	})
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("u1"), []byte("missing")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if got, err := col.Get([]byte("u1")); err != nil || got != nil {
		t.Fatalf("u1=%q err=%v, want missing", got, err)
	}
	assertCollectionDocument(t, col, "u2", `{"name":"Grace"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDReplayAdvancesMissingOnlyFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
		},
	})
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDReplayAdvancesRootlessNoopFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteBatchByIDPublishesMissingOnlyNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	deleted, err := col.DeleteBatch([][]byte{[]byte("missing")})
	if err != nil {
		t.Fatalf("DeleteBatch missing only: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted=%d, want 0", deleted)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALDeleteDocumentByIDPublishesMissingNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	deleted, err := col.DeleteDocument([]byte("missing"))
	if err != nil {
		t.Fatalf("DeleteDocument missing: %v", err)
	}
	if deleted {
		t.Fatal("deleted=true, want false")
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","active":false}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	callbacks := 0
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		callbacks++
		if !bytes.Contains(current, []byte(`"active":false`)) {
			t.Fatalf("callback current=%s, want inactive", current)
		}
		return []byte(`{"name":"Ada","active":true}`), true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified || callbacks != 1 {
		t.Fatalf("Update matched=%t modified=%t callbacks=%d, want true/true/1", matched, modified, callbacks)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada","active":true}`)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionDocument(t, reopened, "u1", `{"name":"Ada","active":true}`)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDReplayRecoversUnappliedFrame(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","city":"hnl"}`),
			[]byte(`{"name":"Grace","city":"nyc"}`),
		},
	})
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada","city":"sea"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada","city":"sea"}`)
	assertCollectionDocument(t, col, "u2", `{"name":"Grace","city":"nyc"}`)
}

func TestCollectionCommandWALUpdateByIDReplayTemplateV1StoredDocument(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection setup: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	encoder := &TemplateV1Encoder{}
	seed := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "HNL"})
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{seed}, encoder); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatchWithTemplateV1Encoder setup: %v", err)
	}
	stored, err := encoder.EncodeDocument([]string{"name", "city"}, []any{"Ada", "SEA"})
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeDocument stored: %v", err)
	}
	if !bytes.HasPrefix(stored, []byte(templateV1StoredMagic)) {
		_ = d.Close()
		t.Fatalf("stored prefix=%q, want %q", stored[:min(len(stored), len(templateV1StoredMagic))], templateV1StoredMagic)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("u1"), Document: stored},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Ada"`)) || !bytes.Contains(jsonDoc, []byte(`"SEA"`)) {
		t.Fatalf("materialized template doc=%s, want Ada/SEA", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDTemplateV1NewShapeReplay(t *testing.T) {
	baseMeta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}
	baseInsert := collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustTemplateV1Document(t, []string{"name"}, []any{"Ada"}),
		},
	}
	replacement := mustTemplateV1Document(t, []string{"name", "city"}, []any{"Ada", "SEA"})

	updateDir := prepareCollectionCommandWALDir(t, baseMeta, baseInsert)
	updateDB := openCollectionCommandWALDB(t, updateDir)
	updateCol, err := NewCollectionManager(updateDB).OpenCollection("users")
	if err != nil {
		_ = updateDB.Close()
		t.Fatalf("OpenCollection update: %v", err)
	}
	matched, modified, err := updateCol.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		_ = updateDB.Close()
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		_ = updateDB.Close()
		t.Fatalf("Update matched=%t modified=%t, want true/true", matched, modified)
	}
	if err := updateDB.Close(); err != nil {
		t.Fatalf("Close update DB: %v", err)
	}

	frames := collectionCommandWALFrames(t, updateDir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	frame := frames[0]
	if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID {
		t.Fatalf("frame kind=%d, want update batch", frame.Kind)
	}
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	if len(payload.Documents) != 1 {
		t.Fatalf("payload documents=%d, want 1", len(payload.Documents))
	}
	if bytes.HasPrefix(payload.Documents[0].Document, []byte(templateV1StoredMagic)) {
		t.Fatalf("TemplateV1 update WAL payload used stored document bytes; replay needs deterministic input envelope")
	}

	replayDir := prepareCollectionCommandWALDir(t, baseMeta, baseInsert)
	writeCollectionCommandWALFrame(t, replayDir, 1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, frame.Payload)
	reopen := openCollectionCommandWALDB(t, replayDir)
	defer func() { _ = reopen.Close() }()
	replayed, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection replay: %v", err)
	}
	got, err := replayed.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get replayed template doc: %v", err)
	}
	jsonDoc, err := replayed.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"Ada"`)) || !bytes.Contains(jsonDoc, []byte(`"SEA"`)) {
		t.Fatalf("materialized template doc=%s, want Ada/SEA", jsonDoc)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDIndexedPublishesSecondaryRoots(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","city":"hnl"}`),
			[]byte(`{"name":"Grace","city":"nyc"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"name":"Ada","city":"sea"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v, want one matched modified", results)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada","city":"sea"}`)
	assertCollectionIndexIDs(t, col, "city", "hnl")
	assertCollectionIndexIDs(t, col, "city", "sea", "u1")
	assertCollectionIndexIDs(t, col, "city", "nyc", "u2")
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDPublishesMissingNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.Update([]byte("missing"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"name":"Ada"}`), true, nil
	})
	if err != nil {
		t.Fatalf("Update missing: %v", err)
	}
	if matched || modified {
		t.Fatalf("Update missing matched=%t modified=%t, want false/false", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCollectionUpdateBatchByID {
		t.Fatalf("command WAL frames=%+v, want one collection update noop frame", frames)
	}
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frames[0].Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	if len(payload.Documents) != 0 {
		t.Fatalf("noop update payload documents=%d, want 0", len(payload.Documents))
	}
}

func TestCollectionCommandWALUpdateByIDPublishesCallbackNoop(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}, collectionCommandWALSetupInsert{
		ids:  [][]byte{[]byte("u1")},
		docs: [][]byte{[]byte(`{"name":"Ada"}`)},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return nil, false, nil
	})
	if err != nil {
		t.Fatalf("Update callback noop: %v", err)
	}
	if !matched || modified {
		t.Fatalf("Update callback noop matched=%t modified=%t, want true/false", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	assertCollectionDocument(t, col, "u1", `{"name":"Ada"}`)
}

func TestCollectionCommandWALUpdateBSONSetUniqueIndexChangePublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString, Unique: true}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Grace"}, {Key: "city", Value: "nyc"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	assertCollectionIndexIDs(t, col, "city", "hnl")
	assertCollectionIndexIDs(t, col, "city", "sea", "u1")
	assertCollectionIndexIDs(t, col, "city", "nyc", "u2")
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetIndexedUnchangedIndexPublishesAppliedLSN(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "email", Value: "ada@example.test"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get doc: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
	assertCollectionIndexIDs(t, col, "email", "ada@example.test", "u1")
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetNoIndexStagesAfterWALAppend(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		_ = d.Close()
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		_ = d.Close()
		t.Fatalf("UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get staged doc: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		_ = d.Close()
		t.Fatalf("staged city=%q want sea", got)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after staged update=%d, want 0 before flush", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCollectionUpdateBatchByID {
		_ = d.Close()
		t.Fatalf("command WAL frames=%+v, want one update frame before flush", frames)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatalf("Flush: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after flush=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	doc, err = reopened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get reopened doc: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("reopened city=%q want sea", got)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetNoIndexDrainsBeforeRawKVCommandWAL(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		_ = d.Close()
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		_ = d.Close()
		t.Fatalf("UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after staged update=%d, want 0 before raw write", got)
	}
	if err := d.Set([]byte("raw"), []byte("v")); err != nil {
		_ = d.Close()
		t.Fatalf("raw Set after staged collection update: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after raw write=%d, want staged update and raw frame applied", got)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get staged doc after raw write: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		_ = d.Close()
		t.Fatalf("city after raw write=%q want sea", got)
	}
	raw, err := d.Get([]byte("raw"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get raw key: %v", err)
	}
	if !bytes.Equal(raw, []byte("v")) {
		_ = d.Close()
		t.Fatalf("raw key=%q, want v", raw)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	doc, err = reopened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get reopened doc: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("reopened city=%q want sea", got)
	}
	raw, err = reopen.Get([]byte("raw"))
	if err != nil {
		t.Fatalf("Get reopened raw key: %v", err)
	}
	if !bytes.Equal(raw, []byte("v")) {
		t.Fatalf("reopened raw key=%q, want v", raw)
	}
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 2", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetNoIndexWaitsForRawKVCommandWALPublish(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	attempting := make(chan struct{})
	stagingAttempted := make(chan struct{})
	var signalStagingAttempt sync.Once
	restoreStageLockHook := setTestBeforeCommandWALBufferedUpdateStageLockForTest(func() {
		signalStagingAttempt.Do(func() {
			close(stagingAttempted)
		})
	})
	defer restoreStageLockHook()
	done := make(chan error, 1)
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
		go func() {
			close(attempting)
			matched, modified, updateErr := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
				Key:   "city",
				Value: mustBSONRawValue(t, "sea"),
			}})
			if updateErr == nil && (!matched || !modified) {
				updateErr = errors.New("UpdateBSONSet matched/modified false")
			}
			done <- updateErr
		}()
		<-attempting
		select {
		case <-stagingAttempted:
		case updateErr := <-done:
			if updateErr != nil {
				return updateErr
			}
			return errors.New("collection update completed before command WAL staging lock")
		case <-time.After(2 * time.Second):
			return errors.New("collection update did not reach command WAL staging lock")
		}
		select {
		case updateErr := <-done:
			if updateErr != nil {
				return updateErr
			}
			return errors.New("collection update completed while raw command WAL publish was open")
		default:
			return nil
		}
	})
	defer unregister()

	if err := d.Set([]byte("raw"), []byte("v")); err != nil {
		t.Fatalf("raw Set while collection update is waiting: %v", err)
	}
	select {
	case updateErr := <-done:
		if updateErr != nil {
			t.Fatalf("UpdateBSONSet after raw publish: %v", updateErr)
		}
	case <-time.After(time.Second):
		t.Fatalf("UpdateBSONSet did not complete after raw publish")
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after raw publish and staged update=%d, want 1", got)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get updated doc: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city after update=%q want sea", got)
	}
	raw, err := d.Get([]byte("raw"))
	if err != nil {
		t.Fatalf("Get raw key: %v", err)
	}
	if !bytes.Equal(raw, []byte("v")) {
		t.Fatalf("raw key=%q, want v", raw)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush staged update: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after flush=%d, want 2", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetNoIndexDoesNotDeadlockRawPublish(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "city", Value: "hnl"}, {Key: "state", Value: "hi"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	closeDB := true
	defer func() {
		if closeDB {
			_ = d.Close()
		}
	}()

	var launchSecond atomic.Bool
	var launchedSecond atomic.Bool
	secondStarted := make(chan struct{})
	secondStagingAttempted := make(chan struct{})
	secondDone := make(chan error, 1)
	var signalSecondStaging sync.Once
	var col *Collection
	restoreStageLockHook := setTestBeforeCommandWALBufferedUpdateStageLockForTest(func() {
		if launchedSecond.Load() {
			signalSecondStaging.Do(func() {
				close(secondStagingAttempted)
			})
		}
	})
	defer restoreStageLockHook()
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
		if !launchSecond.Load() || !launchedSecond.CompareAndSwap(false, true) {
			return nil
		}
		go func() {
			close(secondStarted)
			_, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
				Key:   "state",
				Value: mustBSONRawValue(t, "wa"),
			}})
			secondDone <- err
		}()
		<-secondStarted
		select {
		case <-secondStagingAttempted:
			return nil
		case err := <-secondDone:
			if err != nil {
				return err
			}
			return errors.New("second collection update completed before command WAL staging lock")
		case <-time.After(2 * time.Second):
			return errors.New("second collection update did not reach command WAL staging lock")
		}
	})
	defer unregister()

	mgr := NewCollectionManager(d)
	var err error
	col, err = mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("first UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("first UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after first update=%d, want 0", got)
	}

	launchSecond.Store(true)
	rawDone := make(chan error, 1)
	go func() {
		rawDone <- d.Set([]byte("raw"), []byte("v"))
	}()
	select {
	case err := <-rawDone:
		if err != nil {
			t.Fatalf("raw Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		closeDB = false
		t.Fatalf("raw Set did not complete")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second UpdateBSONSet: %v", err)
		}
	case <-time.After(2 * time.Second):
		closeDB = false
		t.Fatalf("second UpdateBSONSet did not complete")
	}
	raw, err := d.Get([]byte("raw"))
	if err != nil {
		t.Fatalf("Get raw key: %v", err)
	}
	if string(raw) != "v" {
		t.Fatalf("raw key=%q, want v", raw)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q, want sea", got)
	}
	if got := bson.Raw(doc).Lookup("state").StringValue(); got != "wa" {
		t.Fatalf("state=%q, want wa", got)
	}
}

func TestCollectionCommandWALCoordinatorNotStoredAfterCloseHooksDrained(t *testing.T) {
	d := &backenddb.DB{}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if coord := collectionCommandWALCoordinatorForDB(d); coord != nil {
		t.Fatalf("collectionCommandWALCoordinatorForDB after close hooks drained returned non-nil coordinator")
	}
	if _, ok := collectionCommandWALCoordinators.Load(d); ok {
		t.Fatalf("collection command WAL coordinator stored after close hooks drained")
	}
}

func TestCollectionCommandWALUpdateBSONSetNoIndexDoesNotReserveBeforeMutation(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "city", Value: "hnl"}, {Key: "state", Value: "hi"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	unlockMutation := col.lockMutation()
	mutationLocked := true
	releaseMutation := func() {
		if mutationLocked {
			unlockMutation.Unlock()
			mutationLocked = false
		}
	}
	defer releaseMutation()

	updateDone := make(chan error, 1)
	updateStarted := make(chan struct{})
	go func() {
		close(updateStarted)
		_, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
			Key:   "city",
			Value: mustBSONRawValue(t, "sea"),
		}})
		updateDone <- err
	}()
	<-updateStarted

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case err := <-updateDone:
			t.Fatalf("UpdateBSONSet completed while mutation lock was held: %v", err)
		default:
		}
		if collectionCommandWALDomainStageReserved(col.writeDomain) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	intent, err := col.newCollectionUpdateCommandWALIntent(nil, nil)
	if err != nil {
		t.Fatalf("newCollectionUpdateCommandWALIntent: %v", err)
	}
	publishDone := make(chan error, 1)
	publishStarted := make(chan struct{})
	go func() {
		close(publishStarted)
		publishDone <- col.publishCommandWALNoop(intent, false)
	}()
	<-publishStarted
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish no-op while mutation lock held: %v", err)
		}
	case <-time.After(time.Second):
		releaseMutation()
		t.Fatalf("publish no-op waited on a pre-mutation stage reservation")
	}

	releaseMutation()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateBSONSet after mutation release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("UpdateBSONSet did not complete after mutation release")
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN before flushing staged update=%d, want 1", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush staged update: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after flushing staged update=%d, want 2", got)
	}
}

func TestCollectionCommandWALUpdateBSONSetNoopFlushesStagedUpdate(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet staged: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("staged UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after staged update=%d, want 0", got)
	}
	matched, modified, err = col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet noop: %v", err)
	}
	if !matched || modified {
		t.Fatalf("noop UpdateBSONSet matched=%t modified=%t, want true/false", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after noop=%d, want 2", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 2 {
		t.Fatalf("command WAL frame count=%d, want 2", len(frames))
	}
}

func TestCollectionCommandWALUpdateBSONSetNoIndexInterleavedCollectionsDrainOwner(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "a",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("a1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "b",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection b: %v", err)
	}
	a, err := mgr.OpenCollection("a")
	if err != nil {
		t.Fatalf("OpenCollection a: %v", err)
	}
	b, err := mgr.OpenCollection("b")
	if err != nil {
		t.Fatalf("OpenCollection b: %v", err)
	}
	if _, err := b.Insert([]byte("b1"), mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Grace"}, {Key: "city", Value: "nyc"}})); err != nil {
		t.Fatalf("Insert b1: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after setup=%d, want 2", got)
	}
	if _, _, err := a.UpdateBSONSet([]byte("a1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}}); err != nil {
		t.Fatalf("UpdateBSONSet a1 first: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after staged a=%d, want 2", got)
	}
	if _, _, err := b.UpdateBSONSet([]byte("b1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "bos"),
	}}); err != nil {
		t.Fatalf("UpdateBSONSet b1: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 3 {
		t.Fatalf("AppliedCommandLSN after switching to b=%d, want 3", got)
	}
	if _, _, err := a.UpdateBSONSet([]byte("a1"), []BSONSetField{{
		Key:   "state",
		Value: mustBSONRawValue(t, "wa"),
	}}); err != nil {
		t.Fatalf("UpdateBSONSet a1 second: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 4 {
		t.Fatalf("AppliedCommandLSN after switching back to a=%d, want 4", got)
	}
	if err := a.Flush(); err != nil {
		t.Fatalf("Flush a: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 5 {
		t.Fatalf("AppliedCommandLSN after final flush=%d, want 5", got)
	}
	doc, err := a.Get([]byte("a1"))
	if err != nil {
		t.Fatalf("Get a1: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("a.city=%q want sea", got)
	}
	if got := bson.Raw(doc).Lookup("state").StringValue(); got != "wa" {
		t.Fatalf("a.state=%q want wa", got)
	}
	doc, err = b.Get([]byte("b1"))
	if err != nil {
		t.Fatalf("Get b1: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "bos" {
		t.Fatalf("b.city=%q want bos", got)
	}
}

func TestCollectionCommandWALPublishCoordinatorDoesNotHoldCoordinatorWhileDrainingOwner(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}}); err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after staged update=%d, want 0", got)
	}

	coord := mgr.commandWALCoordinator
	if coord == nil {
		t.Fatalf("missing command WAL coordinator")
	}
	coord.mu.Lock()
	if coord.owner != col.writeDomain {
		coord.mu.Unlock()
		t.Fatalf("coordinator owner=%p, want collection write domain %p", coord.owner, col.writeDomain)
	}
	coord.mu.Unlock()

	col.writeDomain.mu.Lock()
	drainDone := make(chan error, 1)
	drainStarted := make(chan struct{})
	go func() {
		close(drainStarted)
		unlock, lockErr := mgr.lockCommandWALPublishCoordinator()
		if lockErr == nil {
			unlock()
		}
		drainDone <- lockErr
	}()
	<-drainStarted
	select {
	case err := <-drainDone:
		col.writeDomain.mu.Unlock()
		t.Fatalf("owner drain completed while owner domain lock was held: %v", err)
	default:
	}

	coordAvailable := make(chan struct{})
	go func() {
		coord.mu.Lock()
		_ = coord.owner
		coord.mu.Unlock()
		close(coordAvailable)
	}()
	select {
	case <-coordAvailable:
	case <-time.After(250 * time.Millisecond):
		col.writeDomain.mu.Unlock()
		t.Fatalf("publish coordinator stayed locked while waiting for owner domain")
	}

	col.writeDomain.mu.Unlock()
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("owner drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("owner drain did not complete after owner domain unlocked")
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after owner drain=%d, want 1", got)
	}
}

func TestCollectionCommandWALThresholdFlushClearsCoordinatorOwner(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatBSON,
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1")},
		docs: [][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "email", Value: "ada@example.test"}, {Key: "city", Value: "hnl"}}),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%t modified=%t, want true/true", matched, modified)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after threshold flush=%d, want 1", got)
	}
	coord := mgr.commandWALCoordinator
	if coord == nil {
		t.Fatalf("missing command WAL coordinator")
	}
	coord.mu.Lock()
	owner := coord.owner
	coord.mu.Unlock()
	if owner != nil {
		t.Fatalf("coordinator owner after threshold flush=%p, want nil", owner)
	}
}

func TestCollectionCommandWALStageCoordinatorPinsFallbackToDomain(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	domain := &collectionWriteDomain{}
	col := &Collection{db: d, writeDomain: domain}

	unlock, err := col.lockCommandWALStageCoordinator()
	if err != nil {
		t.Fatalf("lockCommandWALStageCoordinator: %v", err)
	}
	coord := domain.commandWALCoordinator.Load()
	if coord == nil {
		t.Fatalf("fallback coordinator was not pinned to domain")
	}
	if got := domain.commandWALStageReservations.Load(); got != 1 {
		t.Fatalf("stage reservations before unlock=%d, want 1", got)
	}
	unlock()
	if got := domain.commandWALStageReservations.Load(); got != 0 {
		t.Fatalf("stage reservations after unlock=%d, want 0", got)
	}
	coord.mu.Lock()
	owner := coord.owner
	coord.mu.Unlock()
	if owner != nil {
		t.Fatalf("coordinator owner after unlock=%p, want nil", owner)
	}
}

func TestCollectionCommandWALPublishWaitsForStageReservation(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	unlockStage, err := col.lockCommandWALStageCoordinator()
	if err != nil {
		t.Fatalf("lockCommandWALStageCoordinator: %v", err)
	}
	releaseStage := func() {
		if unlockStage != nil {
			unlockStage()
			unlockStage = nil
		}
	}
	defer releaseStage()

	intent, err := col.newCollectionUpdateCommandWALIntent(nil, nil)
	if err != nil {
		t.Fatalf("newCollectionUpdateCommandWALIntent: %v", err)
	}
	publishDone := make(chan error, 1)
	publishStarted := make(chan struct{})
	go func() {
		close(publishStarted)
		publishDone <- col.publishCommandWALNoop(intent, false)
	}()
	<-publishStarted
	select {
	case err := <-publishDone:
		t.Fatalf("publish completed while staged owner had no pending LSN: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN before releasing stage reservation=%d, want 0", got)
	}

	releaseStage()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish after stage release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("publish did not complete after stage reservation release")
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after publish=%d, want 1", got)
	}
}

func TestCollectionCommandWALPendingFirstLSNRequiresAppliedNext(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	domain := &collectionWriteDomain{}
	if err := domain.recordPendingCommandWALLSNLocked(d, 2); !errors.Is(err, backenddb.ErrCommandWALAppliedLSNNonContig) {
		t.Fatalf("recordPendingCommandWALLSNLocked error=%v, want ErrCommandWALAppliedLSNNonContig", err)
	}
}

func TestCollectionCommandWALUpdateBSONSetUniqueIndexReopenRecovery(t *testing.T) {
	doc1 := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Ada"}, {Key: "city", Value: "hnl"}})
	doc2 := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "Grace"}, {Key: "city", Value: "nyc"}})
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString, Unique: true}},
	}, collectionCommandWALSetupInsert{
		ids:  [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{doc1, doc2},
	})
	d := openCollectionCommandWALDB(t, dir)
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}}); err != nil {
		_ = d.Close()
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col2, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertCollectionIndexIDs(t, col2, "city", "hnl")
	assertCollectionIndexIDs(t, col2, "city", "sea", "u1")
	assertCollectionIndexIDs(t, col2, "city", "nyc", "u2")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after reopen=%d, want 1", got)
	}
}

func TestCollectionCommandWALUpdateByIDPreflightReplansStaleIndexedPlan(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}, collectionCommandWALSetupInsert{
		ids: [][]byte{[]byte("u1"), []byte("u2")},
		docs: [][]byte{
			[]byte(`{"name":"Ada","city":"hnl"}`),
			[]byte(`{"name":"Grace","city":"nyc"}`),
		},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	colA, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection A: %v", err)
	}
	colB, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection B: %v", err)
	}

	readyA := make(chan struct{}, 2)
	readyB := make(chan struct{}, 2)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	var callsA atomic.Int32
	var callsB atomic.Int32
	go func() {
		_, _, err := colA.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
			callsA.Add(1)
			readyA <- struct{}{}
			<-releaseA
			return []byte(`{"name":"Ada","city":"sea"}`), true, nil
		})
		doneA <- err
	}()
	go func() {
		_, _, err := colB.Update([]byte("u2"), func([]byte) ([]byte, bool, error) {
			call := callsB.Add(1)
			readyB <- struct{}{}
			<-releaseB
			if call == 1 {
				return []byte(`{"name":"Grace","city":"bos"}`), true, nil
			}
			return []byte(`{"name":"Grace","city":"lax"}`), true, nil
		})
		doneB <- err
	}()
	waitCollectionCommandWALSignal(t, readyA, "update A planned")
	waitCollectionCommandWALSignal(t, readyB, "update B planned")
	close(releaseA)
	if err := waitCollectionCommandWALErr(t, doneA, "update A"); err != nil {
		t.Fatalf("update A error: %v", err)
	}
	close(releaseB)
	if err := waitCollectionCommandWALErr(t, doneB, "update B"); err != nil {
		t.Fatalf("update B error: %v", err)
	}
	if got := callsB.Load(); got < 2 {
		t.Fatalf("update B callbacks=%d, want retry after stale command-WAL preflight", got)
	}
	assertCollectionDocument(t, colA, "u1", `{"name":"Ada","city":"sea"}`)
	assertCollectionDocument(t, colA, "u2", `{"name":"Grace","city":"lax"}`)
	assertCollectionIndexIDs(t, colA, "city", "sea", "u1")
	assertCollectionIndexIDs(t, colA, "city", "lax", "u2")
	if got := d.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
	if got := callsA.Load(); got != 1 {
		t.Fatalf("update A callbacks=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	var foundRetryPayload bool
	for _, frame := range frames {
		if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID {
			continue
		}
		payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
		if err != nil {
			t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
		}
		for _, doc := range payload.Documents {
			if bytes.Equal(doc.ID, []byte("u2")) {
				if bytes.Contains(doc.Document, []byte(`"city":"bos"`)) {
					t.Fatalf("stale retry command WAL payload recorded first callback result: %s", doc.Document)
				}
				if bytes.Contains(doc.Document, []byte(`"city":"lax"`)) {
					foundRetryPayload = true
				}
			}
		}
	}
	if !foundRetryPayload {
		t.Fatalf("command WAL frames did not record retry result for u2: %+v", frames)
	}
}

func TestCollectionCommandWALCreateCollectionPublishesAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != 1 || frames[0].Kind != commitlog.CommandKindCatalogCreateCollection || frames[0].Scope != commitlog.CommandScopeCatalog {
		_ = d.Close()
		t.Fatalf("catalog create frames=%+v, want one catalog create frame", frames)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if _, err := NewCollectionManager(reopen).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection after reopen: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("reopen AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALReplayManagerDoesNotRegisterBackendHooks(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	replayManager := newCommandWALReplayCollectionManager(d)
	if replayManager.commandWALCoordinator == nil {
		t.Fatalf("replay manager missing command WAL coordinator")
	}
	if replayManager.commandWALRawUnregister != nil {
		t.Fatalf("replay manager registered raw publish barrier")
	}
	if replayManager.closeUnregister != nil {
		t.Fatalf("replay manager registered close hook")
	}

	liveManager := NewCollectionManager(d)
	if liveManager.commandWALRawUnregister == nil {
		t.Fatalf("live manager missing raw publish barrier")
	}
	if liveManager.closeUnregister == nil {
		t.Fatalf("live manager missing close hook")
	}
}

func TestCollectionCommandWALCreateCollectionReplayRecoversUnappliedFrame(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	payload := catalogCreateCollectionPayload(t, meta)
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, payload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	if _, err := NewCollectionManager(reopen).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection after replay: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALCreateCollectionReplaySameMetadataIdempotent(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	dir := prepareCollectionCommandWALDir(t, meta)
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, meta))

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	col, err := NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection after idempotent replay: %v", err)
	}
	if normalizedDocumentFormat(col.meta.Options.DocumentFormat) != DocumentFormatJSON {
		t.Fatalf("document format=%q, want json", col.meta.Options.DocumentFormat)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCollectionCommandWALCreateCollectionReplayIncompatibleMetadataFailsClosed(t *testing.T) {
	existing := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	dir := prepareCollectionCommandWALDir(t, existing)
	incompatible := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, incompatible))

	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err == nil {
		_ = d.Close()
		t.Fatalf("Open with incompatible catalog replay succeeded, want fail-closed error")
	}
	if !errors.Is(err, backenddb.ErrCommandWALUnsupported) && !bytes.Contains([]byte(err.Error()), []byte("incompatible")) {
		t.Fatalf("Open error=%v, want incompatible catalog replay failure", err)
	}

	if err := os.RemoveAll(backenddb.WALDirPath(dir)); err != nil {
		t.Fatalf("RemoveAll wal: %v", err)
	}
	inspect := openCollectionCommandWALDB(t, dir)
	defer func() { _ = inspect.Close() }()
	col, err := NewCollectionManager(inspect).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection after removing failed frame: %v", err)
	}
	if normalizedDocumentFormat(col.meta.Options.DocumentFormat) != DocumentFormatJSON {
		t.Fatalf("document format after failed replay=%q, want original json", col.meta.Options.DocumentFormat)
	}
	if got := inspect.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after failed replay=%d, want original 0", got)
	}
}

func TestCollectionCommandWALCreateCollectionDrainsRecoveredLowerLSN(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	rawPayload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindRawKVBatch, commitlog.PayloadFormatRawKVBatchV1, rawPayload)

	d := openCollectionCommandWALDB(t, dir)
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after lower LSN recovery=%d, want 1", got)
	}
	if _, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection after lower LSN recovery: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 2 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN after catalog create=%d, want 2", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCollectionCommandWALCreateCollectionExistingNoop(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	dir := prepareCollectionCommandWALDir(t, meta)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	got, err := NewCollectionManager(d).CreateCollection(&meta)
	if err != nil {
		t.Fatalf("CreateCollection existing: %v", err)
	}
	if got.Name != meta.Name {
		t.Fatalf("existing metadata=%+v want collection name %q", got, meta.Name)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 for existing no-op", got)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != 0 {
		t.Fatalf("command WAL frames=%d, want 0 for existing no-op", got)
	}
}

func TestCollectionCommandWALRejectsCatalogIndexMutations(t *testing.T) {
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	})
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("CreateIndex error=%v, want ErrCommandWALRejected", err)
	}
	if _, err := col.DropIndex("city"); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("DropIndex error=%v, want ErrCommandWALRejected", err)
	}
	if _, err := col.DropIndexes([]string{"city"}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("DropIndexes error=%v, want ErrCommandWALRejected", err)
	}
	if _, err := col.DropAllIndexes(); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("DropAllIndexes error=%v, want ErrCommandWALRejected", err)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after rejected index DDL=%d, want 0", got)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != 0 {
		t.Fatalf("command WAL frames after rejected index DDL=%d, want 0", got)
	}
}

type collectionCommandWALSetupInsert struct {
	ids  [][]byte
	docs [][]byte
}

func prepareCollectionCommandWALDir(t *testing.T, meta CollectionMeta, inserts ...collectionCommandWALSetupInsert) string {
	t.Helper()
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open setup DB: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&meta); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection setup: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection setup: %v", err)
	}
	for _, insert := range inserts {
		if _, err := col.InsertBatch(insert.ids, insert.docs); err != nil {
			_ = d.Close()
			t.Fatalf("InsertBatch setup: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint setup DB: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	return dir
}

func openCollectionCommandWALDB(t *testing.T, dir string) *backenddb.DB {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL DB: %v", err)
	}
	return d
}

func writeCollectionCommandWALFrame(t *testing.T, dir string, lsn uint64, kind commitlog.CommandKind, format commitlog.PayloadFormat, payload []byte) {
	t.Helper()
	walDir := backenddb.WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(commitlog.CommandEnvelope{
		LSN:           lsn,
		Kind:          kind,
		Scope:         commandWALScopeForKind(kind),
		PayloadFormat: format,
		Payload:       payload,
	}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func commandWALScopeForKind(kind commitlog.CommandKind) commitlog.CommandScope {
	switch kind {
	case commitlog.CommandKindRawKVBatch:
		return commitlog.CommandScopeRawKV
	case commitlog.CommandKindCatalogCreateCollection:
		return commitlog.CommandScopeCatalog
	default:
		return commitlog.CommandScopeCollection
	}
}

func catalogCreateCollectionPayload(t *testing.T, meta CollectionMeta) []byte {
	t.Helper()
	encoded, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatalf("encodeCollectionMeta: %v", err)
	}
	payload, err := commitlog.EncodeCatalogCreateCollectionPayload(meta.Name, encoded)
	if err != nil {
		t.Fatalf("EncodeCatalogCreateCollectionPayload: %v", err)
	}
	return payload
}

func countCollectionCommandWALFrames(t *testing.T, dir string) int {
	t.Helper()
	return len(collectionCommandWALFrames(t, dir))
}

func collectionCommandWALFrames(t *testing.T, dir string) []commitlog.CommandEnvelope {
	t.Helper()
	walDir := backenddb.WALDirPath(dir)
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("ReadDir wal: %v", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		paths = append(paths, filepath.Join(walDir, entry.Name()))
	}
	frames, err := commitlog.ScanCommandFrameSegments(paths, commitlog.Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrameSegments: %v", err)
	}
	return frames
}

func assertCollectionDocument(t *testing.T, col *Collection, id string, want string) {
	t.Helper()
	got, err := col.Get([]byte(id))
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%q)=%q, want %q", id, got, want)
	}
}

func waitCollectionCommandWALSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
	}
}

func waitCollectionCommandWALErr(t *testing.T, ch <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
		return nil
	}
}

func assertCollectionIndexIDs(t *testing.T, col *Collection, indexName string, value any, want ...string) {
	t.Helper()
	got, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		t.Fatalf("FindByIndexValue(%q,%v): %v", indexName, value, err)
	}
	if len(got) != len(want) {
		t.Fatalf("FindByIndexValue(%q,%v)=%q, want %q", indexName, value, got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("FindByIndexValue(%q,%v)[%d]=%q, want %q (all got %q)", indexName, value, i, got[i], want[i], got)
		}
	}
}
