package commandwalapply

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestRecoverabilityStatusNames(t *testing.T) {
	if StatusLocallyApplied != "locally_applied" {
		t.Fatalf("StatusLocallyApplied=%q", StatusLocallyApplied)
	}
	if StatusLocallyWALRecoverable != "locally_wal_recoverable" {
		t.Fatalf("StatusLocallyWALRecoverable=%q", StatusLocallyWALRecoverable)
	}
	if StatusLocallyRootRecoverable != "locally_root_recoverable" {
		t.Fatalf("StatusLocallyRootRecoverable=%q", StatusLocallyRootRecoverable)
	}
}

func TestAppendAndFinalizeNoopFrame(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          dir,
		CommandWAL:                   true,
		DisableBackgroundPrune:       true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	frame, err := TestNoopFrame()
	if err != nil {
		_ = db.Close()
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append: %v", err)
	}
	if appendResult.Status != StatusLocallyWALRecoverable {
		_ = db.Close()
		t.Fatalf("append status=%q, want %q", appendResult.Status, StatusLocallyWALRecoverable)
	}
	if appendResult.LSN == 0 || handle.LSN() != appendResult.LSN {
		_ = db.Close()
		t.Fatalf("append lsn=%d handle=%d, want non-zero match", appendResult.LSN, handle.LSN())
	}
	if appendResult.AppliedCommandLSN != 0 || db.State().AppliedCommandLSN != 0 {
		_ = db.Close()
		t.Fatalf("AppliedCommandLSN after append result=%d state=%d, want 0", appendResult.AppliedCommandLSN, db.State().AppliedCommandLSN)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		_ = db.Close()
		t.Fatalf("command WAL frames after append=%d, want 1", len(frames))
	}
	assertNoopFrame(t, frames[0], appendResult.LSN)

	finalResult, err := Finalize(db, handle, ApplyMetadata{}, Options{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Finalize: %v", err)
	}
	if finalResult.Status != StatusLocallyRootRecoverable {
		_ = db.Close()
		t.Fatalf("final status=%q, want %q", finalResult.Status, StatusLocallyRootRecoverable)
	}
	if finalResult.LSN != appendResult.LSN || finalResult.AppliedCommandLSN != appendResult.LSN {
		_ = db.Close()
		t.Fatalf("final result=%+v, want lsn and applied %d", finalResult, appendResult.LSN)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 1 {
		_ = db.Close()
		t.Fatalf("command WAL frames after finalize=%d, want no duplicate append", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, CommandWAL: true})
	if err != nil {
		t.Fatalf("read-only reopen after finalize: %v", err)
	}
	if got := ro.State().AppliedCommandLSN; got != appendResult.LSN {
		_ = ro.Close()
		t.Fatalf("reopen AppliedCommandLSN=%d, want %d", got, appendResult.LSN)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
}

func TestAppendCatalogCreateCollectionFrame(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          dir,
		CommandWAL:                   true,
		DisableBackgroundPrune:       true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	payload, err := commitlog.EncodeCatalogCreateCollectionPayload("users", []byte(`{"version":5,"name":"users"}`))
	if err != nil {
		t.Fatalf("EncodeCatalogCreateCollectionPayload: %v", err)
	}
	frame, err := CatalogCreateCollectionFrame(payload)
	if err != nil {
		t.Fatalf("CatalogCreateCollectionFrame: %v", err)
	}
	handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	defer Abort(db, handle)
	if appendResult.Status != StatusLocallyWALRecoverable {
		t.Fatalf("append status=%q, want %q", appendResult.Status, StatusLocallyWALRecoverable)
	}
	if appendResult.LSN == 0 || handle.LSN() != appendResult.LSN || handle.CommandWALIntent() == nil {
		t.Fatalf("append result=%+v handle lsn=%d intent nil=%v", appendResult, handle.LSN(), handle.CommandWALIntent() == nil)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after catalog append=%d, want 0 before executor finalize", got)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames after append=%d, want 1", len(frames))
	}
	if frames[0].LSN != appendResult.LSN ||
		frames[0].Kind != commitlog.CommandKindCatalogCreateCollection ||
		frames[0].Scope != commitlog.CommandScopeCatalog ||
		frames[0].PayloadFormat != commitlog.PayloadFormatCatalogCreateCollectionV1 {
		t.Fatalf("catalog create frame=%+v, want lsn=%d CatalogCreateCollection/Catalog/V1", frames[0], appendResult.LSN)
	}
}

func TestAppendCollectionMutationFrames(t *testing.T) {
	insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{
		ID:       []byte("u1"),
		Document: []byte(`{"name":"ada"}`),
	}})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	deletePayload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("u1")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	updatePayload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{{
		ID:       []byte("u1"),
		Document: []byte(`{"name":"grace"}`),
	}})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}

	cases := []struct {
		name          string
		payload       []byte
		build         func([]byte) (LoweredFrame, error)
		kind          commitlog.CommandKind
		payloadFormat commitlog.PayloadFormat
	}{
		{
			name:          "insert",
			payload:       insertPayload,
			build:         CollectionInsertBatchByIDFrame,
			kind:          commitlog.CommandKindCollectionInsertBatchByID,
			payloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
		},
		{
			name:          "delete",
			payload:       deletePayload,
			build:         CollectionDeleteBatchByIDFrame,
			kind:          commitlog.CommandKindCollectionDeleteBatchByID,
			payloadFormat: commitlog.PayloadFormatCollectionDeleteBatchByIDV1,
		},
		{
			name:          "update",
			payload:       updatePayload,
			build:         CollectionUpdateBatchByIDFrame,
			kind:          commitlog.CommandKindCollectionUpdateBatchByID,
			payloadFormat: commitlog.PayloadFormatCollectionUpdateBatchByIDV1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := backenddb.Open(backenddb.Options{
				Dir:                          dir,
				CommandWAL:                   true,
				DisableBackgroundPrune:       true,
				CommandWALStatsScan:          true,
				CommandWALSegmentTargetBytes: 1 << 20,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			frame, err := tc.build(tc.payload)
			if err != nil {
				t.Fatalf("build frame: %v", err)
			}
			handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			defer Abort(db, handle)
			if appendResult.Status != StatusLocallyWALRecoverable || appendResult.LSN == 0 || handle.LSN() != appendResult.LSN || handle.CommandWALIntent() == nil {
				t.Fatalf("append result=%+v handle lsn=%d intent nil=%v", appendResult, handle.LSN(), handle.CommandWALIntent() == nil)
			}
			frames := readCommandWALFrames(t, dir)
			if len(frames) != 1 {
				t.Fatalf("command WAL frames=%d, want 1", len(frames))
			}
			got := frames[0]
			if got.LSN != appendResult.LSN ||
				got.Kind != tc.kind ||
				got.Scope != commitlog.CommandScopeCollection ||
				got.PayloadFormat != tc.payloadFormat {
				t.Fatalf("frame=%+v, want lsn=%d kind=%d collection format=%d", got, appendResult.LSN, tc.kind, tc.payloadFormat)
			}
		})
	}
}

func TestAppendRejectsOutstandingNoopHandle(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if _, _, err := Append(db, frame, ApplyMetadata{}, Options{}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("Append second outstanding error=%v, want ErrCommandWALRejected", err)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 1 {
		t.Fatalf("command WAL frames after rejected outstanding append=%d, want 1", got)
	}

	if _, err := Finalize(db, handle, ApplyMetadata{}, Options{}); err != nil {
		t.Fatalf("Finalize first: %v", err)
	}
	next, _, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append after finalize: %v", err)
	}
	if next.LSN() <= appendResult.LSN {
		t.Fatalf("next append lsn=%d, want greater than first lsn=%d", next.LSN(), appendResult.LSN)
	}
	if _, err := Finalize(db, next, ApplyMetadata{}, Options{}); err != nil {
		t.Fatalf("Finalize second: %v", err)
	}
}

func TestOutstandingApplyHandleBlocksRawCommandWALPublish(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		b := db.NewBatch()
		if err := b.Set([]byte("raw/blocked"), []byte("value")); err != nil {
			writeDone <- err
			return
		}
		close(writeStarted)
		writeDone <- b.Write()
	}()

	select {
	case <-writeStarted:
	case err := <-writeDone:
		t.Fatalf("raw write completed before starting Write: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("raw write goroutine did not start")
	}

	select {
	case err := <-writeDone:
		t.Fatalf("raw write completed while apply handle was outstanding: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if _, err := Finalize(db, handle, ApplyMetadata{}, Options{}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("raw write after finalize: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("raw write remained blocked after finalize")
	}
	if got := db.State().AppliedCommandLSN; got != appendResult.LSN+1 {
		t.Fatalf("AppliedCommandLSN after raw write=%d, want %d", got, appendResult.LSN+1)
	}
}

func TestOutstandingApplyHandleBlocksPublicCommandWALNoopPublish(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewTrustedCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewTrustedCommandWALIntent: %v", err)
	}

	publishStarted := make(chan struct{})
	publishDone := make(chan error, 1)
	go func() {
		close(publishStarted)
		publishDone <- db.PublishCommandWALNoop(intent, false)
	}()

	select {
	case <-publishStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("public publish goroutine did not start")
	}
	select {
	case err := <-publishDone:
		t.Fatalf("public no-op publish completed while apply handle was outstanding: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if _, err := Finalize(db, handle, ApplyMetadata{}, Options{}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("public no-op publish after finalize: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("public no-op publish remained blocked after finalize")
	}
	if got := db.State().AppliedCommandLSN; got != appendResult.LSN+1 {
		t.Fatalf("AppliedCommandLSN after public no-op publish=%d, want %d", got, appendResult.LSN+1)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 2 {
		t.Fatalf("command WAL frames after public no-op publish=%d, want 2", got)
	}
}

func TestAbortOutstandingApplyHandleReleasesPublicPublishAndPoisons(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, appendResult, err := Append(db, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewTrustedCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewTrustedCommandWALIntent: %v", err)
	}

	publishStarted := make(chan struct{})
	publishDone := make(chan error, 1)
	go func() {
		close(publishStarted)
		publishDone <- db.PublishCommandWALNoop(intent, false)
	}()

	select {
	case <-publishStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("public publish goroutine did not start")
	}
	select {
	case err := <-publishDone:
		t.Fatalf("public no-op publish completed while apply handle was outstanding: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	Abort(db, handle)
	select {
	case err := <-publishDone:
		if !errors.Is(err, backenddb.ErrRecoveryRequired) {
			t.Fatalf("public no-op publish after abort error=%v, want ErrRecoveryRequired", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("public no-op publish remained blocked after abort")
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after abort=%d, want 0", got)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 1 {
		t.Fatalf("command WAL frames after abort=%d, want only abandoned apply frame", got)
	}
	if got := readCommandWALFrames(t, dir)[0].LSN; got != appendResult.LSN {
		t.Fatalf("abandoned frame lsn=%d, want %d", got, appendResult.LSN)
	}
}

func TestFinalizeRejectsHandleFromDifferentDB(t *testing.T) {
	sourceDir := t.TempDir()
	source, err := backenddb.Open(backenddb.Options{
		Dir:                    sourceDir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open source: %v", err)
	}
	defer func() { _ = source.Close() }()

	targetDir := t.TempDir()
	target, err := backenddb.Open(backenddb.Options{
		Dir:                    targetDir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, _, err := Append(source, frame, ApplyMetadata{}, Options{})
	if err != nil {
		t.Fatalf("Append source: %v", err)
	}

	if _, err := Finalize(target, handle, ApplyMetadata{}, Options{}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("Finalize target error=%v, want ErrCommandWALRejected", err)
	}
	if got := target.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("target AppliedCommandLSN=%d, want 0", got)
	}
	if got := len(readCommandWALFrames(t, targetDir)); got != 0 {
		t.Fatalf("target command WAL frames=%d, want 0", got)
	}
	if _, err := Finalize(source, handle, ApplyMetadata{}, Options{}); err != nil {
		t.Fatalf("Finalize source after rejected target finalize: %v", err)
	}
}

func TestRejectedInputFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	nonEmptyPayload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte("k"),
		Value: []byte("v"),
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	tests := []struct {
		name    string
		frame   LoweredFrame
		wantErr error
		wantMsg string
	}{
		{
			name: "unsupported class",
			frame: LoweredFrame{
				Class:         0,
				Kind:          commitlog.CommandKindRawKVBatch,
				Scope:         commitlog.CommandScopeRawKV,
				PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
				Payload:       frame.Payload,
			},
			wantErr: backenddb.ErrCommandWALUnsupported,
			wantMsg: "not accepted",
		},
		{
			name: "unsupported identity",
			frame: LoweredFrame{
				Class:         LoweredFrameClassTestNoop,
				Kind:          commitlog.CommandKindCollectionInsertBatchByID,
				Scope:         commitlog.CommandScopeCollection,
				PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
				Payload:       frame.Payload,
			},
			wantErr: backenddb.ErrCommandWALUnsupported,
			wantMsg: "unsupported identity",
		},
		{
			name: "malformed payload",
			frame: LoweredFrame{
				Class:         LoweredFrameClassTestNoop,
				Kind:          commitlog.CommandKindRawKVBatch,
				Scope:         commitlog.CommandScopeRawKV,
				PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
				Payload:       []byte{0x01},
			},
			wantErr: backenddb.ErrCommandWALRejected,
			wantMsg: "malformed",
		},
		{
			name: "non empty raw kv payload",
			frame: LoweredFrame{
				Class:         LoweredFrameClassTestNoop,
				Kind:          commitlog.CommandKindRawKVBatch,
				Scope:         commitlog.CommandScopeRawKV,
				PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
				Payload:       nonEmptyPayload,
			},
			wantErr: backenddb.ErrCommandWALUnsupported,
			wantMsg: "empty RawKVBatch",
		},
		{
			name: "non canonical empty raw kv payload",
			frame: LoweredFrame{
				Class:         LoweredFrameClassTestNoop,
				Kind:          commitlog.CommandKindRawKVBatch,
				Scope:         commitlog.CommandScopeRawKV,
				PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
				Payload:       nonCanonicalEmptyRawKVBatchPayload(),
			},
			wantErr: backenddb.ErrCommandWALRejected,
			wantMsg: "canonical empty RawKVBatch",
		},
		{
			name: "malformed catalog create payload",
			frame: LoweredFrame{
				Class:         LoweredFrameClassCatalogCreateCollection,
				Kind:          commitlog.CommandKindCatalogCreateCollection,
				Scope:         commitlog.CommandScopeCatalog,
				PayloadFormat: commitlog.PayloadFormatCatalogCreateCollectionV1,
				Payload:       []byte{0x01},
			},
			wantErr: backenddb.ErrCommandWALRejected,
			wantMsg: "malformed",
		},
		{
			name: "malformed collection insert payload",
			frame: LoweredFrame{
				Class:         LoweredFrameClassCollectionInsertBatchByID,
				Kind:          commitlog.CommandKindCollectionInsertBatchByID,
				Scope:         commitlog.CommandScopeCollection,
				PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
				Payload:       []byte{0x01},
			},
			wantErr: backenddb.ErrCommandWALRejected,
			wantMsg: "malformed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(readCommandWALFrames(t, dir))
			_, _, err := Append(db, tt.frame, ApplyMetadata{}, Options{})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Append error=%v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Append error=%v, want message containing %q", err, tt.wantMsg)
			}
			after := len(readCommandWALFrames(t, dir))
			if after != before {
				t.Fatalf("command WAL frames after rejected input=%d, want %d", after, before)
			}
		})
	}
}

func TestApplyRejectsReadOnlyDB(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, CommandWAL: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer func() { _ = ro.Close() }()
	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	if _, _, err := Append(ro, frame, ApplyMetadata{}, Options{}); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("Append read-only error=%v, want ErrReadOnly", err)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames after read-only apply=%d, want 0", got)
	}
}

func TestApplyRejectsCommandWALDisabledDB(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open WAL-off DB: %v", err)
	}
	defer func() { _ = db.Close() }()
	frame, err := TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	if _, _, err := Append(db, frame, ApplyMetadata{}, Options{}); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("Append command-WAL-disabled error=%v, want ErrCommandWALUnsupported", err)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames after disabled apply=%d, want 0", got)
	}
}

func readCommandWALFrames(t *testing.T, dir string) []commitlog.CommandEnvelope {
	t.Helper()
	walDir := backenddb.WALDirPath(dir)
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("ReadDir %s: %v", walDir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && commitlog.IsCommandSegmentName(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	var frames []commitlog.CommandEnvelope
	for _, name := range names {
		path := filepath.Join(walDir, name)
		r, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatalf("NewReader %s: %v", name, err)
		}
		for {
			env, err := r.ReadCommandFrame()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				_ = r.Close()
				t.Fatalf("ReadCommandFrame %s: %v", name, err)
			}
			frames = append(frames, env)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close reader %s: %v", name, err)
		}
	}
	return frames
}

func assertNoopFrame(t *testing.T, env commitlog.CommandEnvelope, wantLSN uint64) {
	t.Helper()
	if env.LSN != wantLSN ||
		env.Kind != commitlog.CommandKindRawKVBatch ||
		env.Scope != commitlog.CommandScopeRawKV ||
		env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		t.Fatalf("noop frame identity=%+v, want lsn=%d RawKVBatch/RawKV/V1", env, wantLSN)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("noop frame ops=%+v, want empty", ops)
	}
}

func nonCanonicalEmptyRawKVBatchPayload() []byte {
	payload := make([]byte, 10)
	binary.LittleEndian.PutUint16(payload[0:2], 2)
	binary.LittleEndian.PutUint32(payload[2:6], 0)
	binary.LittleEndian.PutUint32(payload[6:10], 1)
	return payload
}
