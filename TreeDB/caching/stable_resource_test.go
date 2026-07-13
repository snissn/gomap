//go:build linux

package caching

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication/osadapter"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type blockingStableValueWriter struct {
	*valuelog.Writer
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *blockingStableValueWriter) Flush() error {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return w.Writer.Flush()
}

func TestAppendValuesAndRegisterStableResourcesCapturesBeforeCompetingRotation(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, valueLogName(1, 1))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	releaseFlush := make(chan struct{})
	blocking := &blockingStableValueWriter{
		Writer:  writer,
		entered: make(chan struct{}),
		release: releaseFlush,
	}
	db := &DB{
		dir:                     dir,
		valueLogDir:             dir,
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
		stableResourcePins:      rootpublication.NewIdentityPinRegistry(),
	}
	lane := &lane{id: 1, vlog: blocking, vlogPath: path, vlogSeq: 1}
	appender := &cachingValueLogAppender{db: db, lane: lane}

	type appendResult struct {
		ptrs []page.ValuePtr
		set  *rootpublication.StableResourceSet
		err  error
	}
	appendDone := make(chan appendResult, 1)
	go func() {
		ptrs, set, appendErr := appender.AppendValuesAndRegisterStableResources(
			[][]byte{[]byte("stable-before-rotation")},
			rootpublication.StableResourceSpec{ReachabilityField: "prepared_root.value_ptr"},
		)
		appendDone <- appendResult{ptrs: ptrs, set: set, err: appendErr}
	}()
	<-blocking.entered

	rotateAttempted := make(chan struct{})
	rotateDone := make(chan error, 1)
	go func() {
		close(rotateAttempted)
		rotateDone <- db.rotateValueLogLocked(lane)
	}()
	<-rotateAttempted
	close(releaseFlush)

	result := <-appendDone
	if result.err != nil {
		t.Fatalf("AppendValuesAndRegisterStableResources: %v", result.err)
	}
	if err := <-rotateDone; err != nil {
		t.Fatalf("competing rotation: %v", err)
	}
	defer func() { _ = result.set.Release() }()
	defer func() { _ = writer.Close() }()
	if len(result.ptrs) != 1 || result.ptrs[0].FileID != fileID {
		t.Fatalf("returned pointers = %+v, want original file id %d", result.ptrs, fileID)
	}
	tokens := result.set.Tokens()
	if len(tokens) != 1 {
		t.Fatalf("stable token count = %d, want 1", len(tokens))
	}
	oldFile, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity, err := osadapter.ResourceIdentity(oldFile)
	_ = oldFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Identity() != oldIdentity {
		t.Fatalf("captured identity = %+v, want original inode %+v", tokens[0].Identity(), oldIdentity)
	}
	if got := blocking.FileID(); got == fileID {
		t.Fatalf("competing rotation did not replace writer file id %d", got)
	}
}

func TestAppendValuesAndRegisterStableResourcesPreservesMaxSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, valueLogName(2, 1))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, bytes.Repeat([]byte("s"), 192)); err != nil {
		t.Fatal(err)
	}
	db := &DB{
		dir:                     dir,
		valueLogDir:             dir,
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
		valueLogMaxSegmentBytes: 256,
		stableResourcePins:      rootpublication.NewIdentityPinRegistry(),
	}
	lane := &lane{id: 2, vlog: writer, vlogPath: path, vlogSeq: 1}
	ptrs, set, err := (&cachingValueLogAppender{db: db, lane: lane}).AppendValuesAndRegisterStableResources(
		[][]byte{bytes.Repeat([]byte("a"), 32), bytes.Repeat([]byte("b"), 32)},
		rootpublication.StableResourceSpec{ReachabilityField: "prepared_root.value_ptr"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if lane.vlogSeq != 2 {
		t.Fatalf("value-log sequence = %d, want preflight rotation to 2", lane.vlogSeq)
	}
	if len(ptrs) != 2 || ptrs[0].FileID == fileID || ptrs[1].FileID != ptrs[0].FileID {
		t.Fatalf("rotated pointers = %+v, want one new segment", ptrs)
	}
	tokens := set.Tokens()
	if len(tokens) != 1 || tokens[0].Kind() != rootpublication.ResourceValueLogSegment {
		t.Fatalf("rotated stable tokens = %+v", tokens)
	}
}

func TestAppendValuesAndRegisterStableResourcesReturnsSetAcrossInternalRotation(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, valueLogName(3, 1))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	db := &DB{
		dir:                     dir,
		valueLogDir:             dir,
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionBlock),
		valueLogMaxSegmentBytes: 256,
		relaxedSync:             true,
		stableResourcePins:      rootpublication.NewIdentityPinRegistry(),
	}
	lane := &lane{id: 3, vlog: writer, vlogPath: path, vlogSeq: 1}
	values := make([][]byte, 512)
	for i := range values {
		value := make([]byte, 128)
		for j := range value {
			value[j] = byte(i*31 + j*17 + i*j)
		}
		values[i] = value
	}
	ptrs, set, err := (&cachingValueLogAppender{db: db, lane: lane}).AppendValuesAndRegisterStableResources(
		values,
		rootpublication.StableResourceSpec{ReachabilityField: "prepared_root.value_ptr"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	fileIDs := make(map[uint32]struct{})
	for _, ptr := range ptrs {
		fileIDs[ptr.FileID] = struct{}{}
	}
	if len(fileIDs) < 2 {
		t.Fatalf("internal max-segment rotation produced %d segment(s), want at least 2", len(fileIDs))
	}
	if got := len(set.Tokens()); got != len(fileIDs) {
		t.Fatalf("stable token count = %d, want one for each of %d referenced segments", got, len(fileIDs))
	}
}

func TestAppendLeafPagesAndRegisterStableResourcesPreservesCompaction(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(leafDir, valueLogName(leafLogLaneID, 1))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	db := &DB{
		dir:                        dir,
		leafLogDir:                 leafDir,
		closeCh:                    make(chan struct{}),
		indexOuterLeavesInValueLog: true,
		valueLogCompressionMode:    uint8(vlogCompressionOff),
		stableResourcePins:         rootpublication.NewIdentityPinRegistry(),
	}
	db.leafLog = lane{id: leafLogLaneID, vlog: writer, vlogPath: path, vlogSeq: 1}
	leafLog := &cachingLeafPageLog{db: db, lane: &db.leafLog}
	pages := [][]byte{
		buildSparseLeafPageForLeafLogTestWithTag(t, 'x'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 'y'),
	}
	for i, leafPage := range pages {
		if _, compacted, err := valuelog.MaybeCompactLeafLogPayload(leafPage); err != nil || !compacted {
			t.Fatalf("leaf page %d compactability = %v, %v", i, compacted, err)
		}
	}
	refs, set, err := leafLog.AppendLeafPagesAndRegisterStableResources(
		pages,
		rootpublication.StableResourceSpec{ReachabilityField: "prepared_root.outer_leaf"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := valuelog.NewReader(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i, want := range pages {
		_, got, gotPtr, err := reader.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("compacted outer leaf %d did not round-trip", i)
		}
		gotRef, err := page.LeafLogPtrFromValuePtr(gotPtr)
		if err != nil || gotRef != refs[i] {
			t.Fatalf("outer leaf ref %d = %+v, %v; want %+v", i, gotRef, err, refs[i])
		}
	}
	tokens := set.Tokens()
	if len(tokens) != 1 || tokens[0].Kind() != rootpublication.ResourceOuterLeafSegment {
		t.Fatalf("outer-leaf stable tokens = %+v", tokens)
	}
}

func TestStableValueLogRegistrationRetainsExactWriterAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l1-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ptr, err := w.Append(0, nil, 1, []byte("stable value"))
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir, stableResourcePins: rootpublication.NewIdentityPinRegistry()}
	l := &lane{id: 1, vlog: w, vlogPath: path, vlogSeq: 1}
	a := &cachingValueLogAppender{db: db, lane: l}
	token, err := a.RegisterStableValueLogSegment([]page.ValuePtr{ptr}, rootpublication.StableResourceSpec{
		ReachabilityField: "meta.value_ptr",
	})
	if err != nil {
		t.Fatalf("RegisterStableValueLogSegment: %v", err)
	}
	wantFrontier := ptr.Offset + uint64(page.ValuePtrRecordLength(ptr))
	if token.Kind() != rootpublication.ResourceValueLogSegment || token.RequiredFrontier() != wantFrontier {
		t.Fatalf("token kind/frontier = %s/%d, want %s/%d", token.Kind(), token.RequiredFrontier(), rootpublication.ResourceValueLogSegment, wantFrontier)
	}
	oldInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	nextID, err := valuelog.EncodeFileID(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	nextPath := filepath.Join(dir, "value-l1-000002.log")
	if err := w.RotateTo(nextPath, nextID); err != nil {
		t.Fatal(err)
	}
	l.vlogPath = nextPath
	l.vlogSeq = 2
	if err := token.FlushThrough(); err != nil {
		t.Fatalf("FlushThrough retained writer: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough retained writer: %v", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(oldInfo, afterInfo) {
		t.Fatal("retained token stopped naming original writer inode")
	}
	if err := token.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := a.RegisterStableValueLogSegment([]page.ValuePtr{ptr}, rootpublication.StableResourceSpec{
		ReachabilityField: "meta.stale_value_ptr",
	}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("registration after rotation = %v, want ErrResourceConflict", err)
	}
}

func TestStableOuterLeafRegistrationUsesExactLogRecordFrontier(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 7)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "outer-leaf.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ptr, err := w.Append(0, nil, 1, []byte("outer leaf"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir, stableResourcePins: rootpublication.NewIdentityPinRegistry()}
	lane := &lane{id: valuelog.ReservedLeafLogLaneID, vlog: w, vlogPath: path, vlogSeq: 7}
	leafLog := &cachingLeafPageLog{db: db, lane: lane}
	token, err := leafLog.RegisterStableOuterLeafSegment([]page.LogRecordRef{ref}, rootpublication.StableResourceSpec{
		ReachabilityField: "tree.child.log",
	})
	if err != nil {
		t.Fatalf("RegisterStableOuterLeafSegment: %v", err)
	}
	wantFrontier := ref.Offset + uint64(ref.RecordLength())
	if token.Kind() != rootpublication.ResourceOuterLeafSegment || token.RequiredFrontier() != wantFrontier {
		t.Fatalf("token kind/frontier = %s/%d, want %s/%d", token.Kind(), token.RequiredFrontier(), rootpublication.ResourceOuterLeafSegment, wantFrontier)
	}
	if token.Identity() == (rootpublication.StableIdentity{}) {
		t.Fatal("outer-leaf token has empty writer identity")
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStableWriterTokenBlocksManagerDeletion(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l2-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ptr, err := w.Append(0, nil, 1, []byte("delete gate"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	manager, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	registry := rootpublication.NewIdentityPinRegistry()
	if err := manager.SetStableResourcePinRegistry(registry); err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir, stableResourcePins: registry}
	lane := &lane{id: 2, vlog: w, vlogPath: path, vlogSeq: 1}
	token, err := (&cachingValueLogAppender{db: db, lane: lane}).RegisterStableValueLogSegment(
		[]page.ValuePtr{ptr},
		rootpublication.StableResourceSpec{ReachabilityField: "meta.value_ptr"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveSegment(fileID); !errors.Is(err, valuelog.ErrFilePinned) {
		t.Fatalf("RemoveSegment while writer token pinned = %v, want ErrFilePinned", err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveSegment(fileID); err != nil {
		t.Fatalf("RemoveSegment after release: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment still exists after release/delete: %v", err)
	}
}

func TestExactStableFrontiersRejectIncompleteOrMixedReferences(t *testing.T) {
	fileID, err := valuelog.EncodeFileID(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := valuelog.EncodeFileID(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := exactValuePtrFrontier([]page.ValuePtr{{FileID: fileID, Offset: 1}}); !errors.Is(err, rootpublication.ErrInvalidStableResource) {
		t.Fatalf("zero record hint = %v, want ErrInvalidStableResource", err)
	}
	if _, _, err := exactValuePtrFrontier([]page.ValuePtr{{FileID: fileID, Offset: 1, Length: 2}, {FileID: otherID, Offset: 3, Length: 4}}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("mixed file ids = %v, want ErrResourceConflict", err)
	}
	if _, _, err := exactLogRecordRefFrontier([]page.LogRecordRef{{FileID: page.ValueLogSegmentID(fileID), Offset: 1}}); !errors.Is(err, rootpublication.ErrInvalidStableResource) {
		t.Fatalf("zero outer-leaf record hint = %v, want ErrInvalidStableResource", err)
	}
}

type stableRegistryRecordingBackend struct {
	*MockBackend
	registry *rootpublication.IdentityPinRegistry
}

func (b *stableRegistryRecordingBackend) SetValueLogStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	b.registry = registry
	return nil
}

func TestOpenInjectsOneStableRegistryIntoReaderAndBackend(t *testing.T) {
	backend := &stableRegistryRecordingBackend{MockBackend: NewMockBackend()}
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.stableResourcePins == nil || backend.registry != db.stableResourcePins {
		t.Fatalf("registry injection backend=%p db=%p", backend.registry, db.stableResourcePins)
	}
}

func TestBackendCandidateBoundaryReachesExactCachingProducer(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
		JournalLanes:   1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer cached.Close()
	if backend.StableResourcePinRegistry() == nil {
		t.Fatal("direct backend DB did not own a stable resource registry")
	}
	if cached.stableResourcePins != backend.StableResourcePinRegistry() {
		t.Fatalf("cached reader registry %p differs from backend-owned registry %p", cached.stableResourcePins, backend.StableResourcePinRegistry())
	}
	ptrs, err := backend.AppendValueLogValues([][]byte{[]byte("candidate-bound value")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := backend.RegisterStableValueLogSegment(ptrs, rootpublication.StableResourceSpec{
		ReachabilityField: "prepared_root.value_ptr",
	})
	if err != nil {
		t.Fatalf("backend RegisterStableValueLogSegment: %v", err)
	}
	if token.Kind() != rootpublication.ResourceValueLogSegment {
		t.Fatalf("backend token kind = %s", token.Kind())
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	atomicPtrs, set, err := backend.AppendValuesAndRegisterStableResources(
		[][]byte{[]byte("atomic-candidate-bound value")},
		rootpublication.StableResourceSpec{ReachabilityField: "prepared_root.atomic_value_ptr"},
	)
	if err != nil {
		t.Fatalf("backend AppendValuesAndRegisterStableResources: %v", err)
	}
	if len(atomicPtrs) != 1 || set == nil || len(set.Tokens()) != 1 {
		t.Fatalf("backend atomic producer returned ptrs=%+v set=%v", atomicPtrs, set)
	}
	if err := set.Release(); err != nil {
		t.Fatal(err)
	}
}
