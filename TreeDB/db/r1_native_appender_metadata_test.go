package db

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// The private 16 KiB estimate produces several real native rotations without
// changing the production cap or the short-lived rewrite-writer history contract.
func TestR1NativeAppenderCreationMetadataBoundedAcrossRotations(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	opts := leafGenerationPackPublicationTestOptions(t.TempDir())
	opts.CommandWAL, opts.Durability = true, DurabilityDurable
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			closeNoErr(t, database)
		}
	}()
	appender, ok := database.currentValueLogAppender().(*replayInlineAppender)
	if !ok {
		t.Fatalf("native appender=%T", database.currentValueLogAppender())
	}
	appender.mu.Lock()
	appender.writer.maxSize = 16 << 10
	appender.mu.Unlock()
	primaryIDs, leafIDs := make(map[uint32]bool), make(map[uint32]bool)
	want := make(map[string][]byte)
	maxIDs, maxSegments := 0, 0
	write := func(i, round int) {
		key := fmt.Sprintf("metadata-%02d", i)
		v := make([]byte, 256)
		for j := range v {
			v[j] = byte((i*17 + round*29 + j*37) % 251)
		}
		ptrs, err := database.AppendValueLogValues([][]byte{v})
		if err != nil {
			t.Fatal(err)
		}
		primaryIDs[ptrs[0].FileID] = true
		if _, ok := database.valueLogManager.StableSegmentIdentity(ptrs[0].FileID); !ok {
			t.Fatal("pointer returned before exact manager handoff")
		}
		batch := database.NewBatch()
		pointerBatch, ok := batch.(interface {
			SetPointer([]byte, page.ValuePtr) error
		})
		if !ok {
			closeNoErr(t, batch)
			t.Fatal("native pointer batch unavailable")
		}
		if err := pointerBatch.SetPointer([]byte(key), ptrs[0]); err != nil {
			closeNoErr(t, batch)
			t.Fatal(err)
		}
		if err := batch.WriteSync(); err != nil {
			closeNoErr(t, batch)
			t.Fatal(err)
		}
		closeNoErr(t, batch)
		want[key] = v
		primaryPath, primaryID, primaryOK := appender.CurrentValueLogSegment()
		leafPath, leafID, leafOK := appender.currentLeafValueLogSegment()
		if !primaryOK || primaryPath != database.valueLogManager.SegmentPath(primaryID) {
			t.Fatal("current primary segment lost after metadata handoff")
		}
		if !leafOK || leafPath != database.valueLogManager.SegmentPath(leafID) {
			t.Fatal("current leaf segment lost after metadata handoff")
		}
		if lane, _ := valuelog.DecodeFileID(leafID); lane != valuelog.ReservedLeafLogLaneID {
			t.Fatal("leaf adapter reported wrong lane")
		}
		leafIDs[leafID] = true
		appender.mu.Lock()
		maxIDs = max(maxIDs, len(appender.writer.createdIDs))
		maxSegments = max(maxSegments, len(appender.writer.createdSegments))
		appender.mu.Unlock()
	}
	for i := 0; i < 32; i++ {
		write(i, 0)
	}
	oldWant := make(map[string][]byte)
	for k, v := range want {
		oldWant[k] = bytes.Clone(v)
	}
	held := database.AcquireSnapshot()
	defer held.Close()
	for round := 1; round <= 16; round++ {
		for i := 16; i < 32; i++ {
			write(i, round)
		}
	}
	oracle := func(label string, get func([]byte) ([]byte, error), expected map[string][]byte) {
		t.Helper()
		for k, v := range expected {
			got, err := get([]byte(k))
			if err != nil || !bytes.Equal(got, v) {
				t.Fatalf("%s row %s differs: %v", label, k, err)
			}
		}
	}
	oracle("held", held.Get, oldWant)
	oracle("current cold/hot", database.Get, want)
	t.Logf("natural rotations primary files=%d leaf files=%d metadata peaks IDs=%d segments=%d", len(primaryIDs), len(leafIDs), maxIDs, maxSegments)
	if len(primaryIDs) < 4 || len(leafIDs) < 4 {
		t.Fatalf("insufficient natural rotations primary=%d leaf=%d", len(primaryIDs), len(leafIDs))
	}
	if maxIDs != 0 || maxSegments != 0 {
		t.Errorf("handed-off native metadata retained: IDs=%d segments=%d", maxIDs, maxSegments)
	}
	appender.mu.Lock()
	if cap(appender.writer.createdIDs) != 0 || cap(appender.writer.createdSegments) != 0 || appender.writer.createdSegmentsPublishIdx != 0 {
		t.Error("native metadata backing storage or publish cursor retained after handoff")
	}
	appender.mu.Unlock()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	beforeLSN, beforeNext := database.State().AppliedCommandLSN, database.CommandWALNextLSN()
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if database.State().AppliedCommandLSN != beforeLSN || database.CommandWALNextLSN() != beforeNext {
		t.Fatal("fallback refresh changed command coverage")
	}
	newestSlot := database.metaPageID
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	corruptIndexPageByte(t, opts.Dir, newestSlot)
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, reopened)
	oracle("converged fallback reopen", reopened.Get, want)
}

// A real failed manager registration must keep the old creation record even
// when a later natural rotation hands a different file off successfully.
func TestR1NativeAppenderCreationMetadataRetainsFailedAndAmbiguousHandoffs(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, database)
	appender, err := newReplayInlineAppender(database, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := appender.close(); err != nil {
			t.Error(err)
		}
	}()
	appender.writer.maxSize = 16 << 10
	appender.writer.blockCompression = false
	value := bytes.Repeat([]byte("failed-handoff|"), 900)
	appender.db = &DB{dir: filepath.Join(t.TempDir(), "missing-root"), valueLogManager: database.valueLogManager}
	if _, err := appender.append(value); err == nil {
		t.Fatal("registration against a missing namespace succeeded")
	}
	if len(appender.writer.createdSegments) != 1 || len(appender.writer.createdIDs) != 1 {
		t.Fatal("failed registration lost its creation evidence")
	}
	failed := appender.writer.createdSegments[0]
	appender.db = database
	ptr, err := appender.append(value)
	if err != nil {
		t.Fatal(err)
	}
	if ptr.FileID == failed.fileID {
		t.Fatal("fixture did not naturally rotate")
	}
	if len(appender.writer.createdSegments) != 1 || appender.writer.createdSegments[0] != failed || len(appender.writer.createdIDs) != 1 || appender.writer.createdIDs[0] != failed.fileID {
		t.Fatal("successful new-file handoff erased unrelated failed creation evidence")
	}
	if err := appender.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := appender.ReadValueLogRecordAppend(ptr, nil)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("registered pointer differs: %v", err)
	}
	// Registration succeeds, but a mismatched creation witness cannot be retired.
	appender.mu.Lock()
	appender.writer.createdSegments[0].identity.ObjectID[0] ^= 1
	err = appender.registerProducedPointerLocked(page.ValuePtr{FileID: failed.fileID})
	retained := len(appender.writer.createdSegments) == 1 && len(appender.writer.createdIDs) == 1
	appender.writer.createdSegments[0] = failed
	appender.mu.Unlock()
	if err != nil || !retained {
		t.Fatalf("ambiguous authority lost evidence: retained=%v err=%v", retained, err)
	}
	appender.mu.Lock()
	err = appender.registerProducedPointerLocked(page.ValuePtr{FileID: failed.fileID})
	retained = cap(appender.writer.createdSegments) != 0 || cap(appender.writer.createdIDs) != 0
	appender.mu.Unlock()
	if err != nil || retained {
		t.Fatalf("exact completed handoff retained evidence: retained=%v err=%v", retained, err)
	}
}
