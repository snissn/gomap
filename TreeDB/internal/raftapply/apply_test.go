package raftapply

import (
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestUnsupportedDeterministicEntryRejectsBeforeAppendAndStores(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/insert_batch_entry.hex")

	beforeLSN := db.State().AppliedCommandLSN
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedUnsupported, raftentry.ErrorUnsupportedCommandV1)
	if result.CommandDigest == (raftentry.CommandDigestV1{}) {
		t.Fatal("unsupported deterministic entry should still report a digest")
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("command-WAL seam calls append=%d finalize=%d, want 0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames=%d, want 0", got)
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, beforeLSN)
	}
	if progress.Len() != 0 {
		t.Fatalf("progress records=%d, want 0", progress.Len())
	}
	if results.Len() != 0 {
		t.Fatalf("result records=%d, want 0", results.Len())
	}
}

func TestCreateCollectionDecodedButNotLoweredDoesNotMutateCatalog(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore:       NewMemoryApplyProgressStore(8, 8),
		ResultStore:         NewMemoryApplyResultStore(8),
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedUnsupported, raftentry.ErrorUnsupportedCommandV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("create_collection rejection reached command WAL seam/files append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users error=%v, want ErrCollectionNotFound", openErr)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0", got)
	}
}

func TestMalformedBytesReturnDeterministicErrorWithoutAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	result, err := ApplyCommittedEntryV1(db, []byte("bad"), applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedMalformed, raftentry.ErrorMalformedEntryV1)
	if result.CommandDigest != (raftentry.CommandDigestV1{}) {
		t.Fatalf("malformed digest=%s, want zero digest", result.CommandDigest.Hex())
	}
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("malformed input reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
}

func TestHarnessApplyAPIAcceptsDeterministicBytesOnly(t *testing.T) {
	method := reflect.TypeOf((*Harness).ApplyCommittedEntryV1)
	if method.NumIn() != 3 {
		t.Fatalf("Harness.ApplyCommittedEntryV1 inputs=%d, want receiver+bytes+metadata", method.NumIn())
	}
	bytesType := method.In(1)
	if bytesType.Kind() != reflect.Slice || bytesType.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("ApplyCommittedEntryV1 input=%s, want []byte deterministic entry", bytesType)
	}
	if got := method.In(2); got != reflect.TypeOf(ApplyMetadataV1{}) {
		t.Fatalf("ApplyCommittedEntryV1 metadata input=%s, want ApplyMetadataV1", got)
	}
}

func TestProgressBoundOverflowFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore:       NewMemoryApplyProgressStore(8, 1),
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorResourceExhaustedV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("overflow reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after overflow error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestProgressGapFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore:       NewMemoryApplyProgressStore(8, 8),
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("gap reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after gap=%d, want 0", got)
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after gap error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestReadOnlyDBApplyFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close writable DB: %v", err)
	}
	ro, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open read-only DB: %v", err)
	}
	defer func() { _ = ro.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, applyErr := ApplyCommittedEntryV1(ro, raw, applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, applyErr, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorReadOnlyV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("read-only apply reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	_, openErr := collections.NewCollectionManager(ro).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after read-only apply error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestUnsafeDurabilityFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	meta := applyMeta(1, 1)
	meta.LocalDurabilityBoundary = LocalDurabilityBoundaryV1("local-visible-only-v1")
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, meta, Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorUnsafeDurabilityModeV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("unsafe durability reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
}

func TestSameApplyEntryIDDifferentDigestConflictsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	id := raftentry.ApplyEntryID{Term: 1, Index: 1}
	results := NewMemoryApplyResultStore(8)
	var otherDigest raftentry.CommandDigestV1
	otherDigest[0] = 1
	if err := results.RecordApplyResult(ApplyResultRecordV1{
		EntryID:       id,
		CommandDigest: otherDigest,
		Result: raftentry.ApplyResultV1{
			Status:                 raftentry.ApplyStatusApplied,
			CommandDigest:          otherDigest,
			DeterministicErrorCode: raftentry.ErrorNoneV1,
		},
	}); err != nil {
		t.Fatalf("seed result record: %v", err)
	}

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("conflict reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if results.Len() != 1 {
		t.Fatalf("result records after conflict=%d, want seeded record only", results.Len())
	}
}

func TestMemoryStoresAreBounded(t *testing.T) {
	id1 := raftentry.ApplyEntryID{Term: 1, Index: 1}
	id2 := raftentry.ApplyEntryID{Term: 1, Index: 2}
	var digest raftentry.CommandDigestV1
	digest[0] = 7

	results := NewMemoryApplyResultStore(1)
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id1, CommandDigest: digest}); err != nil {
		t.Fatalf("RecordApplyResult id1: %v", err)
	}
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id2, CommandDigest: digest}); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("RecordApplyResult id2 error=%v code=%s, want resource exhausted", err, codeOf(err))
	}

	progress := NewMemoryApplyProgressStore(1, 8)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id1, CommandDigest: digest}); err != nil {
		t.Fatalf("RecordApplied id1: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id2, CommandDigest: digest}); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("RecordApplied id2 error=%v code=%s, want resource exhausted", err, codeOf(err))
	}

	progress = NewMemoryApplyProgressStore(8, 8)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id1, CommandDigest: digest}); err != nil {
		t.Fatalf("RecordApplied lower-index setup: %v", err)
	}
	if err := progress.CheckCanApply(id1); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("CheckCanApply duplicate/lower error=%v code=%s, want conflict", err, codeOf(err))
	}
	id3 := raftentry.ApplyEntryID{Term: 1, Index: 3}
	if err := progress.CheckCanApply(id3); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("CheckCanApply gap error=%v code=%s, want conflict", err, codeOf(err))
	}
}

func openApplyHarnessDB(t *testing.T, dir string) *backenddb.DB {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          dir,
		CommandWAL:                   true,
		DisableBackgroundPrune:       true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	return db
}

func applyMeta(term, index uint64) ApplyMetadataV1 {
	return ApplyMetadataV1{
		EntryID:                 raftentry.ApplyEntryID{Term: term, Index: index},
		LocalDurabilityBoundary: LocalDurabilityCommandWALV1,
	}
}

func assertRejected(t *testing.T, result raftentry.ApplyResultV1, err error, wantStatus raftentry.ApplyStatusV1, wantCode raftentry.DeterministicErrorCodeV1) {
	t.Helper()
	if err == nil {
		t.Fatalf("ApplyCommittedEntryV1 returned nil error for rejected result %+v", result)
	}
	if result.Status != wantStatus || result.DeterministicErrorCode != wantCode {
		t.Fatalf("result status/code=%s/%s, want %s/%s (err=%v)", result.Status, result.DeterministicErrorCode, wantStatus, wantCode, err)
	}
	if got := codeOf(err); got != wantCode {
		t.Fatalf("error code=%s, want %s (err=%v)", got, wantCode, err)
	}
}

func codeOf(err error) raftentry.DeterministicErrorCodeV1 {
	code, _ := ErrorCodeOf(err)
	return code
}

func readHexFixture(t *testing.T, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(rel)
	if err != nil {
		t.Fatalf("read fixture %s: %v", rel, err)
	}
	hexText := strings.Join(strings.Fields(string(raw)), "")
	out, err := hex.DecodeString(hexText)
	if err != nil {
		t.Fatalf("decode fixture %s: %v", rel, err)
	}
	return out
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

type countingCommandWALApplySeam struct {
	appendCalls   int
	finalizeCalls int
}

func (s *countingCommandWALApplySeam) Append(db *backenddb.DB, frame commandwalapply.LoweredFrame, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error) {
	s.appendCalls++
	return commandwalapply.Handle{}, commandwalapply.Result{}, errors.New("unexpected append")
}

func (s *countingCommandWALApplySeam) Finalize(db *backenddb.DB, handle commandwalapply.Handle, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Result, error) {
	s.finalizeCalls++
	return commandwalapply.Result{}, errors.New("unexpected finalize")
}
