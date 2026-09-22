package db

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendValueLogValuesEmitsExactDependencyPath(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	appender, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = appender.close() }()
	db.SetValueLogAppender(appender)
	defer db.SetValueLogAppender(nil)
	var paths []string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.AfterDependencyAppend {
			paths = append(paths, event.Paths...)
		}
		return nil
	})
	defer restore()
	if _, err := db.AppendValueLogValues([][]byte{bytes.Repeat([]byte("value"), 512)}); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 || !strings.Contains(filepath.ToSlash(paths[0]), "/value_vlog/") {
		t.Fatalf("after-append exact paths=%v", paths)
	}
}

func TestAppendValueLogValuesUnavailableReturnsSentinel(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.AppendValueLogValues([][]byte{[]byte("large value")})
	if !errors.Is(err, ErrValueLogAppenderUnavailable) {
		t.Fatalf("AppendValueLogValues err=%v want ErrValueLogAppenderUnavailable", err)
	}
}

func TestAppendValueLogValuesPublishSystemRootReleasesPendingPin(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	appender, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = appender.close() }()
	db.SetValueLogAppender(appender)
	defer db.SetValueLogAppender(nil)

	ptrs, err := db.AppendValueLogValues([][]byte{bytes.Repeat([]byte("native-root-value|"), 32)})
	if err != nil {
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("AppendValueLogValues returned %d ptrs, want 1", len(ptrs))
	}

	db.pendingValueLogAppendMu.Lock()
	pendingBefore := len(db.pendingValueLogAppendPtrRefs)
	db.pendingValueLogAppendMu.Unlock()
	if pendingBefore != 1 {
		t.Fatalf("pending ptr refs before publish = %d, want 1", pendingBefore)
	}

	table := memtable.NewAppendOnlyWithEntryCapacity(1)
	table.SetEntry([]byte("doc/p"), nil, ptrs[0], node.FlagPointer)
	table.Freeze()
	root, err := db.PublishSystemRootIterator(table.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishSystemRootIterator: %v", err)
	}
	if root == 0 {
		t.Fatalf("PublishSystemRootIterator returned root 0")
	}

	db.pendingValueLogAppendMu.Lock()
	pendingPtrs := len(db.pendingValueLogAppendPtrRefs)
	pendingFiles := len(db.pendingValueLogAppendFileIDRefs)
	db.pendingValueLogAppendMu.Unlock()
	if pendingPtrs != 0 || pendingFiles != 0 {
		t.Fatalf("pending refs after publish ptrs=%d files=%d, want zero", pendingPtrs, pendingFiles)
	}
}

func TestReleasePendingValueLogAppendFileIDsFromEntriesDecrementsDuplicateRefs(t *testing.T) {
	fileID := page.ValueLogFileID(1)
	ptrA := page.ValuePtr{FileID: fileID, Offset: 11, Length: 3}
	ptrB := page.ValuePtr{FileID: fileID, Offset: 22, Length: 4}
	db := &DB{
		pendingValueLogAppendFileIDRefs: map[uint32]int{fileID: 3},
		pendingValueLogAppendPtrRefs: map[page.ValuePtr]int{
			ptrA: 2,
			ptrB: 1,
		},
	}

	db.releasePendingValueLogAppendFileIDsFromEntries([]batch.Entry{
		{Type: batch.OpPut, IsPtr: true, ValuePtr: ptrA},
		{Type: batch.OpPut, IsPtr: true, ValuePtr: ptrA},
		{Type: batch.OpPut, IsPtr: true, ValuePtr: ptrB},
		{Type: batch.OpPut, Value: []byte("inline")},
		{Type: batch.OpDelete, Key: []byte("deleted")},
	})

	if db.pendingValueLogAppendPtrRefs != nil {
		t.Fatalf("pending ptr refs after release = %v, want nil", db.pendingValueLogAppendPtrRefs)
	}
	if db.pendingValueLogAppendFileIDRefs != nil {
		t.Fatalf("pending file refs after release = %v, want nil", db.pendingValueLogAppendFileIDRefs)
	}
}
