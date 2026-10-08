package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func registrationCellFixture(t *testing.T) (*Manager, uint32, string) {
	t.Helper()
	dir := t.TempDir()
	id := mustEncodeFileID(t, 0, 1)
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, nil, 1, []byte("cell retained")); err != nil {
		writer.Close()
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager, id, path
}
func registrationCellAccount(t *testing.T) *FiniteStableMetadata {
	t.Helper()
	a, err := NewFiniteStableMetadata(7, 3, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	return a
}
func TestStableRegistrationCellSharesOrdinaryReferences(t *testing.T) {
	manager, id, _ := registrationCellFixture(t)
	account := registrationCellAccount(t)
	f := manager.files[id]
	cell := f.RefCount.cell
	if cell == nil || cell.id != id || cell.generation == 0 || cell.incarnation != manager.registrationIncarnation {
		t.Fatal("constructor cell missing exact registration")
	}
	set := manager.CurrentSetNoRefresh()
	defer manager.Release(set)
	h, err := manager.acquireFiniteSegmentRetention(id, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	if cell.refs.Load() != 2 || f.RefCount.Load() != 2 {
		t.Fatal("finite retention did not use actual ordinary reference counter")
	}
	if err = manager.EvictSegment(id); err == nil {
		t.Fatal("Manager forgot a retained registration")
	}
	before := account.retained
	if err = h.Release(); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatal("missing consumer was not observable", err)
	}
	if cell.refs.Load() != 2 || account.retained != before {
		t.Fatal("refusal mutated actual reference/account")
	}
	if err = manager.ReleaseStableSegmentRetention(h, nil); err != nil {
		t.Fatal(err)
	}
	if cell.refs.Load() != 1 || f.RefCount.Load() != 1 || account.retained != before-1 {
		t.Fatal("terminal release did not preserve ordinary reference")
	}
	if err = manager.ReleaseStableSegmentRetention(h, nil); err != nil || cell.refs.Load() != 1 {
		t.Fatal("terminal release not idempotent")
	}
}
func TestStableRegistrationCellRejectsWrongIncarnationAndUnknownFile(t *testing.T) {
	manager, id, _ := registrationCellFixture(t)
	other, otherID, _ := registrationCellFixture(t)
	if otherID != id {
		t.Fatal("fixture ID mismatch")
	}
	account := registrationCellAccount(t)
	h, err := manager.acquireFiniteSegmentRetention(id, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	cell, before := h.cell, account.retained
	if err = other.ValidateStableSegmentRetention(h); !errors.Is(err, rootpublication.ErrStableTerminalConsumerMismatch) {
		t.Fatal(err)
	}
	if err = other.ReleaseStableSegmentRetention(h, nil); !errors.Is(err, rootpublication.ErrStableTerminalConsumerMismatch) {
		t.Fatal(err)
	}
	if cell.refs.Load() != 1 || account.retained != before {
		t.Fatal("same-ID foreign consumer changed source")
	}
	manager.mu.Lock()
	replacement := File{ID: id, Path: manager.files[id].Path, File: manager.files[id].File}
	replacement.RefCount = fileReferenceCount{}
	actual := manager.files[id]
	manager.files[id] = &replacement
	manager.mu.Unlock()
	if _, err = manager.acquireFiniteSegmentRetention(id, 1, account); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		t.Fatal("unknown File acquired finite provenance", err)
	}
	if err = manager.ValidateStableSegmentRetention(h); !errors.Is(err, rootpublication.ErrStableTerminalConsumerMismatch) {
		t.Fatal("replacement cell accepted", err)
	}
	manager.mu.Lock()
	manager.files[id] = actual
	manager.mu.Unlock()
	if err = manager.ReleaseStableSegmentRetention(h, nil); err != nil {
		t.Fatal(err)
	}
}
func TestStableRegistrationCellCloseJoinAndClosedTerminal(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	h, err := manager.acquireFiniteSegmentRetention(id, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	cell := h.cell
	joined, err := manager.BeginTerminalRelease()
	if err != nil || !joined {
		t.Fatal("terminal admission", err)
	}
	done := make(chan error, 1)
	go func() { done <- manager.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.RLock()
		closing := manager.closing
		manager.mu.RUnlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			manager.EndTerminalRelease(joined)
			t.Fatal("Close did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatal("Close crossed actual terminal join", err)
	default:
	}
	if _, err = manager.BeginTerminalRelease(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatal("closing Manager admitted live consumer", err)
	}
	manager.EndTerminalRelease(joined)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if !cell.closed.Load() || !cell.incarnation.closed.Load() {
		t.Fatal("terminal cells stamped before actual cleanup completion")
	}
	if err = h.Release(); err != nil {
		t.Fatal("exact closed cell required live Manager", err)
	}
	if cell.refs.Load() != 0 || account.retained != 0 {
		t.Fatal("closed terminal did not balance references/account")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("closed terminal unexpectedly deleted/reopened segment", err)
	}
}
