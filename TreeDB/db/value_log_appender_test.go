package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

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

func TestAppendValueLogValuesPublishOrderedRootReleasesPendingPin(t *testing.T) {
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
	root, err := db.PublishOrderedRootIterator(0, table.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if root == 0 {
		t.Fatalf("PublishOrderedRootIterator returned root 0")
	}

	db.pendingValueLogAppendMu.Lock()
	pendingPtrs := len(db.pendingValueLogAppendPtrRefs)
	pendingFiles := len(db.pendingValueLogAppendFileIDRefs)
	db.pendingValueLogAppendMu.Unlock()
	if pendingPtrs != 0 || pendingFiles != 0 {
		t.Fatalf("pending refs after publish ptrs=%d files=%d, want zero", pendingPtrs, pendingFiles)
	}
}
