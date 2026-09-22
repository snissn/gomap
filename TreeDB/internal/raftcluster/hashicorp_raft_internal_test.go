package raftcluster

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestHashicorpRaftApplyTimeoutIsCommitAmbiguous(t *testing.T) {
	err := mapHashicorpRaftApplyError(context.DeadlineExceeded)
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("mapHashicorpRaftApplyError=%v want ErrCommitAmbiguous", err)
	}
	if errors.Is(err, ErrHashicorpRaftUnavailable) {
		t.Fatalf("mapHashicorpRaftApplyError=%v should not be ErrHashicorpRaftUnavailable", err)
	}
}

func TestHashicorpRaftLeadershipLostIsCommitAmbiguous(t *testing.T) {
	err := mapHashicorpRaftApplyError(hraft.ErrLeadershipLost)
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("mapHashicorpRaftApplyError=%v want ErrCommitAmbiguous", err)
	}
	if errors.Is(err, ErrAdmissionUnavailable) {
		t.Fatalf("mapHashicorpRaftApplyError=%v should not be ErrAdmissionUnavailable", err)
	}
}

func TestHashicorpRaftConfigPreservesConfiguredSnapshots(t *testing.T) {
	src := hraft.DefaultConfig()
	src.SnapshotInterval = time.Millisecond
	src.SnapshotThreshold = 1
	src.TrailingLogs = 3

	conf := hashicorpRaftConfig("node-a", src)
	if conf.SnapshotInterval < 5*time.Millisecond {
		t.Fatalf("SnapshotInterval=%s is invalid for hashicorp raft", conf.SnapshotInterval)
	}
	if conf.SnapshotThreshold != src.SnapshotThreshold {
		t.Fatalf("SnapshotThreshold=%d want configured %d", conf.SnapshotThreshold, src.SnapshotThreshold)
	}
	if conf.TrailingLogs != src.TrailingLogs {
		t.Fatalf("TrailingLogs=%d want configured %d", conf.TrailingLogs, src.TrailingLogs)
	}
	if err := hraft.ValidateConfig(conf); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
}

func TestHashicorpRaftConfigDefaultsAvoidAutomaticSnapshots(t *testing.T) {
	conf := hashicorpRaftConfig("node-a", nil)
	if conf.SnapshotThreshold != ^uint64(0) {
		t.Fatalf("default SnapshotThreshold=%d want max uint64", conf.SnapshotThreshold)
	}
	if err := hraft.ValidateConfig(conf); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
}

func TestHashicorpRaftConfigFloorsTrailingLogsForReadIndex(t *testing.T) {
	src := hraft.DefaultConfig()
	src.TrailingLogs = 0

	conf := hashicorpRaftConfig("node-a", src)
	if conf.TrailingLogs != hashicorpRaftMinTrailingLogs {
		t.Fatalf("TrailingLogs=%d want %d", conf.TrailingLogs, hashicorpRaftMinTrailingLogs)
	}
	if err := hraft.ValidateConfig(conf); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
}

func TestHashicorpRaftSnapshotPersistRejectsMismatchedBoundary(t *testing.T) {
	manifest := validSnapshotManifestV1()
	payload := validRaftSnapshotArchivePayloadV1(t, manifest)
	snapshot := hashicorpRaftSnapshotV1{
		snapshot: RaftSnapshotV1{
			Manifest: manifest,
			Payload:  payload,
		},
	}
	sink := &boundaryTestSnapshotSink{
		boundary: hashicorpRaftSnapshotBoundaryV1{
			Term:  manifest.LastIncludedTerm,
			Index: manifest.LastIncludedIndex + 1,
		},
	}

	err := snapshot.Persist(sink)
	if !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("Persist err=%v want ErrInvalidSnapshotManifest", err)
	}
	if !sink.canceled {
		t.Fatal("Persist did not cancel sink after boundary mismatch")
	}
	if sink.closed {
		t.Fatal("Persist closed sink after boundary mismatch")
	}
	if sink.Len() != 0 {
		t.Fatalf("Persist wrote %d bytes before rejecting boundary", sink.Len())
	}
}

func TestHashicorpRaftSnapshotPersistAcceptsMatchingBoundary(t *testing.T) {
	manifest := validSnapshotManifestV1()
	payload := validRaftSnapshotArchivePayloadV1(t, manifest)
	snapshot := hashicorpRaftSnapshotV1{
		snapshot: RaftSnapshotV1{
			Manifest: manifest,
			Payload:  payload,
		},
	}
	sink := &boundaryTestSnapshotSink{
		boundary: hashicorpRaftSnapshotBoundaryV1{
			Term:  manifest.LastIncludedTerm,
			Index: manifest.LastIncludedIndex,
		},
	}

	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if !sink.closed {
		t.Fatal("Persist did not close sink")
	}
	if sink.canceled {
		t.Fatal("Persist canceled sink after successful write")
	}
	if !bytes.Equal(sink.Bytes(), payload) {
		t.Fatalf("Persist wrote %d bytes, want payload %d bytes", sink.Len(), len(payload))
	}
}

func TestHashicorpRaftSnapshotPersistStreamsArchivePathInChunks(t *testing.T) {
	manifest := validSnapshotManifestV1()
	payload := validRaftSnapshotArchivePayloadWithBodyV1(t, manifest, hashicorpRaftSnapshotCopyBuffer*3)
	archivePath := writeRaftSnapshotArchiveFileForTest(t, payload)
	snapshot := hashicorpRaftSnapshotV1{
		snapshot: RaftSnapshotV1{
			Manifest:    manifest,
			ArchivePath: archivePath,
		},
	}
	sink := &chunkRejectingSnapshotSink{maxWrite: hashicorpRaftSnapshotCopyBuffer}

	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if !sink.closed {
		t.Fatal("Persist did not close sink")
	}
	if sink.canceled {
		t.Fatal("Persist canceled sink after successful write")
	}
	if sink.writeCount < 2 {
		t.Fatalf("Persist used %d writes, want chunked streaming", sink.writeCount)
	}
	if sink.maxSeen > hashicorpRaftSnapshotCopyBuffer {
		t.Fatalf("Persist max write=%d want <= %d", sink.maxSeen, hashicorpRaftSnapshotCopyBuffer)
	}
	if !bytes.Equal(sink.Bytes(), payload) {
		t.Fatalf("Persist wrote %d bytes, want archive %d bytes", sink.Len(), len(payload))
	}
}

func TestHashicorpRaftSnapshotPersistWriteFailureCancelsNotCloses(t *testing.T) {
	manifest := validSnapshotManifestV1()
	payload := validRaftSnapshotArchivePayloadWithBodyV1(t, manifest, hashicorpRaftSnapshotCopyBuffer*2)
	archivePath := writeRaftSnapshotArchiveFileForTest(t, payload)
	snapshot := hashicorpRaftSnapshotV1{
		snapshot: RaftSnapshotV1{
			Manifest:    manifest,
			ArchivePath: archivePath,
		},
	}
	writeErr := errors.New("sink write failed")
	sink := &chunkRejectingSnapshotSink{
		maxWrite: hashicorpRaftSnapshotCopyBuffer,
		writeErr: writeErr,
	}

	err := snapshot.Persist(sink)
	if !errors.Is(err, writeErr) {
		t.Fatalf("Persist err=%v want write failure", err)
	}
	if !sink.canceled {
		t.Fatal("Persist did not cancel sink after write failure")
	}
	if sink.closed {
		t.Fatal("Persist closed sink after write failure")
	}
}

func TestHashicorpRaftSnapshotReleaseRemovesArchivePath(t *testing.T) {
	manifest := validSnapshotManifestV1()
	archivePath := writeRaftSnapshotArchiveFileForTest(t, validRaftSnapshotArchivePayloadV1(t, manifest))
	snapshot := hashicorpRaftSnapshotV1{
		snapshot: RaftSnapshotV1{
			Manifest:    manifest,
			ArchivePath: archivePath,
		},
	}

	snapshot.Release()
	if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
		t.Fatalf("Release did not remove staged archive: stat err=%v", err)
	}
	snapshot.Release()
}

func TestHashicorpRaftSnapshotPersistFileSourceAllocationGuard(t *testing.T) {
	manifest := validSnapshotManifestV1()
	payload := validRaftSnapshotArchivePayloadWithBodyV1(t, manifest, 4<<20)
	archivePath := writeRaftSnapshotArchiveFileForTest(t, payload)
	snapshot := hashicorpRaftSnapshotV1{
		snapshot: RaftSnapshotV1{
			Manifest:    manifest,
			ArchivePath: archivePath,
		},
	}
	sink := &discardSnapshotSink{}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("streamed %d byte archive with %d bytes allocated during Persist", len(payload), allocated)
	if allocated > uint64(len(payload)/2) {
		t.Fatalf("Persist allocated %d bytes for %d byte archive; want streaming allocation below half archive size", allocated, len(payload))
	}
	if !sink.closed || sink.canceled {
		t.Fatalf("sink closed=%v canceled=%v, want close-only success", sink.closed, sink.canceled)
	}
}

func TestHashicorpRaftSnapshotPersistCancelsOnInvalidArchivePathSource(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, archivePath string, manifest SnapshotManifestV1)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, archivePath string, _ SnapshotManifestV1) {
				t.Helper()
				if err := os.Remove(archivePath); err != nil {
					t.Fatalf("Remove archive path: %v", err)
				}
			},
		},
		{
			name: "replaced manifest",
			mutate: func(t *testing.T, archivePath string, manifest SnapshotManifestV1) {
				t.Helper()
				replacement := manifest
				replacement.LastIncludedIndex++
				if err := os.WriteFile(archivePath, validRaftSnapshotArchivePayloadV1(t, replacement), 0o600); err != nil {
					t.Fatalf("Replace archive path: %v", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifest := validSnapshotManifestV1()
			archivePath := writeRaftSnapshotArchiveFileForTest(t, validRaftSnapshotArchivePayloadV1(t, manifest))
			tc.mutate(t, archivePath, manifest)
			snapshot := hashicorpRaftSnapshotV1{
				snapshot: RaftSnapshotV1{
					Manifest:    manifest,
					ArchivePath: archivePath,
				},
			}
			sink := &boundaryTestSnapshotSink{}

			err := snapshot.Persist(sink)
			if !errors.Is(err, ErrInvalidSnapshotManifest) {
				t.Fatalf("Persist err=%v want ErrInvalidSnapshotManifest", err)
			}
			if !sink.canceled {
				t.Fatal("Persist did not cancel sink after invalid archive path source")
			}
			if sink.closed {
				t.Fatal("Persist closed sink after invalid archive path source")
			}
			if sink.Len() != 0 {
				t.Fatalf("Persist wrote %d bytes before rejecting archive path source", sink.Len())
			}
		})
	}
}

func TestHashicorpRaftFileSnapshotStoreListIgnoresStagedSiblingDirectory(t *testing.T) {
	baseDir := t.TempDir()
	store, err := hraft.NewFileSnapshotStore(baseDir, 2, io.Discard)
	if err != nil {
		t.Fatalf("NewFileSnapshotStore: %v", err)
	}
	stagedDir := filepath.Join(baseDir, "staged")
	if err := os.MkdirAll(stagedDir, 0o700); err != nil {
		t.Fatalf("Mkdir staged dir: %v", err)
	}
	orphanPath := filepath.Join(stagedDir, "treedb-snapshot-orphan.tar")
	if err := os.WriteFile(orphanPath, []byte("orphaned staged archive"), 0o600); err != nil {
		t.Fatalf("Write staged archive: %v", err)
	}

	sink, err := store.Create(hraft.SnapshotVersionMax, 11, 7, hraft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatalf("Create snapshot: %v", err)
	}
	if _, err := sink.Write(validRaftSnapshotArchivePayloadV1(t, validSnapshotManifestV1())); err != nil {
		_ = sink.Cancel()
		t.Fatalf("Write snapshot: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close snapshot: %v", err)
	}

	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("List snapshots with staged sibling dir: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("List returned %d snapshots, want 1", len(snapshots))
	}
	if snapshots[0].Index != 11 || snapshots[0].Term != 7 {
		t.Fatalf("snapshot meta index=%d term=%d, want index=11 term=7", snapshots[0].Index, snapshots[0].Term)
	}
	if _, err := os.Stat(orphanPath); err != nil {
		t.Fatalf("staged sibling archive should be outside FileSnapshotStore retention/listing path: %v", err)
	}
}

func TestHashicorpRaftSnapshotStoreWrapsCreateBoundary(t *testing.T) {
	store := wrapHashicorpRaftSnapshotStoreV1(hraft.NewInmemSnapshotStore())
	sink, err := store.Create(hraft.SnapshotVersionMax, 11, 7, hraft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer func() { _ = sink.Cancel() }()

	boundary, ok := hashicorpRaftSnapshotBoundaryFromSinkV1(sink)
	if !ok {
		t.Fatal("wrapped snapshot sink did not expose boundary")
	}
	if boundary.Term != 7 || boundary.Index != 11 {
		t.Fatalf("boundary={Term:%d Index:%d} want {Term:7 Index:11}", boundary.Term, boundary.Index)
	}
}

func TestHashicorpRaftReadIndexGapHasNoCommands(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 1, Term: 1, Type: hraft.LogNoop},
		{Index: 2, Term: 1, Type: hraft.LogConfiguration},
		{Index: 3, Term: 1, Type: hraft.LogBarrier},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(1, 3)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if !noCommands {
		t.Fatalf("readIndexGapHasNoCommands=false, want true for non-command gap")
	}
	if firstCmd != 0 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 0 for non-command gap", firstCmd)
	}
}

func TestHashicorpRaftReadIndexGapDetectsCommand(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 1, Term: 1, Type: hraft.LogNoop},
		{Index: 2, Term: 1, Type: hraft.LogCommand, Data: []byte("entry")},
		{Index: 3, Term: 1, Type: hraft.LogBarrier},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(1, 3)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if noCommands {
		t.Fatalf("readIndexGapHasNoCommands=true, want false for command gap")
	}
	if firstCmd != 2 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 2", firstCmd)
	}
}

func TestHashicorpRaftReadIndexGapStopsAtFirstCommand(t *testing.T) {
	inner := hraft.NewInmemStore()
	if err := inner.StoreLogs([]*hraft.Log{
		{Index: 2, Term: 1, Type: hraft.LogNoop},
		{Index: 3, Term: 1, Type: hraft.LogConfiguration},
		{Index: 4, Term: 1, Type: hraft.LogCommand, Data: []byte("first")},
		{Index: 5, Term: 1, Type: hraft.LogCommand, Data: []byte("second")},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	store := &countingReadIndexLogStore{LogStore: inner}
	provider := &HashicorpRaftProvider{logStore: store}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(2, 5)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if noCommands {
		t.Fatalf("readIndexGapHasNoCommands=true, want false for command gap")
	}
	if firstCmd != 4 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 4", firstCmd)
	}
	if store.getLogCount != 3 {
		t.Fatalf("GetLog count=%d, want scan through first command only", store.getLogCount)
	}
}

func TestHashicorpRaftReadIndexGapMissingLogFailsClosed(t *testing.T) {
	provider := &HashicorpRaftProvider{logStore: hraft.NewInmemStore()}

	_, _, err := provider.readIndexGapHasNoCommands(1, 1)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexGapHasNoCommands err=%v want ErrReadBarrierNotSatisfied", err)
	}
}

func TestHashicorpRaftReadIndexGapMaxIndexNoOverflow(t *testing.T) {
	store := hraft.NewInmemStore()
	lastIndex := uint64(math.MaxUint64)
	if err := store.StoreLog(&hraft.Log{Index: lastIndex, Term: 1, Type: hraft.LogNoop}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	counting := &countingReadIndexLogStore{LogStore: store}
	provider := &HashicorpRaftProvider{logStore: counting}

	noCommands, firstCmd, err := provider.readIndexGapHasNoCommands(lastIndex, lastIndex)
	if err != nil {
		t.Fatalf("readIndexGapHasNoCommands: %v", err)
	}
	if !noCommands {
		t.Fatalf("readIndexGapHasNoCommands=false, want true")
	}
	if firstCmd != 0 {
		t.Fatalf("readIndexGapHasNoCommands firstCmd=%d, want 0", firstCmd)
	}
	if counting.getLogCount != 1 {
		t.Fatalf("GetLog count=%d, want 1", counting.getLogCount)
	}
}

func TestHashicorpRaftReadIndexGapFirstIndexAfterAppliedMaxFailsClosed(t *testing.T) {
	_, err := readIndexGapFirstIndexAfterApplied(uint64(math.MaxUint64))
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexGapFirstIndexAfterApplied err=%v want ErrReadBarrierNotSatisfied", err)
	}

	firstIndex, err := readIndexGapFirstIndexAfterApplied(41)
	if err != nil {
		t.Fatalf("readIndexGapFirstIndexAfterApplied: %v", err)
	}
	if firstIndex != 42 {
		t.Fatalf("firstIndex=%d want 42", firstIndex)
	}
}

func TestHashicorpRaftReadIndexCommitIndexHasCurrentTerm(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLogs([]*hraft.Log{
		{Index: 1, Term: 1, Type: hraft.LogCommand},
		{Index: 2, Term: 2, Type: hraft.LogNoop},
	}); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	ok, err := provider.readIndexCommitIndexHasCurrentTerm(2, 2)
	if err != nil {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm current term: %v", err)
	}
	if !ok {
		t.Fatal("readIndexCommitIndexHasCurrentTerm=false, want true for current-term committed entry")
	}
}

func TestHashicorpRaftReadIndexCommitIndexPreviousTermNotEnough(t *testing.T) {
	store := hraft.NewInmemStore()
	if err := store.StoreLog(&hraft.Log{Index: 1, Term: 1, Type: hraft.LogCommand}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	provider := &HashicorpRaftProvider{logStore: store}

	ok, err := provider.readIndexCommitIndexHasCurrentTerm(1, 2)
	if err != nil {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm previous term: %v", err)
	}
	if ok {
		t.Fatal("readIndexCommitIndexHasCurrentTerm=true, want false for previous-term committed prefix")
	}
}

func TestHashicorpRaftReadIndexCommitIndexMissingLogFailsClosed(t *testing.T) {
	provider := &HashicorpRaftProvider{logStore: hraft.NewInmemStore()}

	_, err := provider.readIndexCommitIndexHasCurrentTerm(2, 2)
	if !errors.Is(err, ErrReadBarrierNotSatisfied) {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm err=%v want ErrReadBarrierNotSatisfied", err)
	}
	if !errors.Is(err, hraft.ErrLogNotFound) {
		t.Fatalf("readIndexCommitIndexHasCurrentTerm err=%v want raft ErrLogNotFound", err)
	}
}

func TestHashicorpRaftFSMApplyFailureDefaultPanics(t *testing.T) {
	applyErr := errors.New("command WAL fsync failed")
	fsm := hashicorpRaftFSM{
		groupID: "group-a",
		applier: CommittedCommandApplierFunc(func(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
			return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusRecoveryRequired}, applyErr
		}),
	}
	payload, err := encodeHashicorpRaftCommandEntryV1(CommitCommandEntryV1Request{
		GroupID:    "group-a",
		NodeID:     "node-a",
		EntryBytes: []byte("deterministic-entry"),
	}, ResolvedConfig{GroupID: "group-a", NodeID: "node-a"})
	if err != nil {
		t.Fatalf("encodeHashicorpRaftCommandEntryV1: %v", err)
	}

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("Apply did not panic")
		}
		err, ok := recovered.(error)
		if !ok {
			t.Fatalf("panic=%T %[1]v, want error", recovered)
		}
		if !errors.Is(err, ErrHashicorpRaftLogEntry) || !errors.Is(err, applyErr) {
			t.Fatalf("panic error=%v want ErrHashicorpRaftLogEntry and apply error", err)
		}
	}()

	_ = fsm.Apply(&hraft.Log{Type: hraft.LogCommand, Term: 1, Index: 1, Data: payload})
}

type countingReadIndexLogStore struct {
	hraft.LogStore
	getLogCount int
}

func (s *countingReadIndexLogStore) GetLog(index uint64, log *hraft.Log) error {
	s.getLogCount++
	return s.LogStore.GetLog(index, log)
}

type boundaryTestSnapshotSink struct {
	bytes.Buffer
	id       string
	boundary hashicorpRaftSnapshotBoundaryV1
	closed   bool
	canceled bool
}

func (s *boundaryTestSnapshotSink) ID() string {
	if s.id != "" {
		return s.id
	}
	return "boundary-test"
}

func (s *boundaryTestSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *boundaryTestSnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}

func (s *boundaryTestSnapshotSink) hashicorpRaftSnapshotBoundaryV1() (hashicorpRaftSnapshotBoundaryV1, bool) {
	return s.boundary, true
}

type chunkRejectingSnapshotSink struct {
	buf        bytes.Buffer
	id         string
	maxWrite   int
	writeErr   error
	writeCount int
	maxSeen    int
	closed     bool
	canceled   bool
}

func (s *chunkRejectingSnapshotSink) ID() string {
	if s.id != "" {
		return s.id
	}
	return "chunk-rejecting-test"
}

func (s *chunkRejectingSnapshotSink) Write(p []byte) (int, error) {
	s.writeCount++
	if len(p) > s.maxSeen {
		s.maxSeen = len(p)
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	if s.maxWrite > 0 && len(p) > s.maxWrite {
		return 0, fmt.Errorf("write size %d exceeds max %d", len(p), s.maxWrite)
	}
	return s.buf.Write(p)
}

func (s *chunkRejectingSnapshotSink) Bytes() []byte {
	return s.buf.Bytes()
}

func (s *chunkRejectingSnapshotSink) Len() int {
	return s.buf.Len()
}

func (s *chunkRejectingSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *chunkRejectingSnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}

type discardSnapshotSink struct {
	closed   bool
	canceled bool
}

func (s *discardSnapshotSink) ID() string { return "discard-test" }

func (s *discardSnapshotSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *discardSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *discardSnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}

func validRaftSnapshotArchivePayloadWithBodyV1(t *testing.T, manifest SnapshotManifestV1, bodySize int) []byte {
	t.Helper()
	headerBytes, err := EncodeRaftSnapshotArchiveHeaderV1(NewRaftSnapshotArchiveHeaderV1(manifest))
	if err != nil {
		t.Fatalf("EncodeRaftSnapshotArchiveHeaderV1: %v", err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: RaftSnapshotArchiveManifestPathV1,
		Mode: 0o600,
		Size: int64(len(headerBytes)),
	}); err != nil {
		t.Fatalf("WriteHeader manifest: %v", err)
	}
	if _, err := tw.Write(headerBytes); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: "db/index.db",
		Mode: 0o600,
		Size: int64(bodySize),
	}); err != nil {
		t.Fatalf("WriteHeader body: %v", err)
	}
	if _, err := io.CopyN(tw, zeroReader{}, int64(bodySize)); err != nil {
		t.Fatalf("Write body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}
	return buf.Bytes()
}

func writeRaftSnapshotArchiveFileForTest(t *testing.T, payload []byte) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "snapshot-*.tar")
	if err != nil {
		t.Fatalf("CreateTemp archive: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatalf("Write archive: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}
	return file.Name()
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
