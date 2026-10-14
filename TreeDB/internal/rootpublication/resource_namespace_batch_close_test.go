package rootpublication

import (
	"errors"
	"os"
	"testing"
)

type closeAfterNamespaceSyncAdapter struct {
	nativeNamespaceAdapter
	calls int
}

func (adapter *closeAfterNamespaceSyncAdapter) Sync(file *os.File) error {
	if err := adapter.nativeNamespaceAdapter.Sync(file); err != nil {
		return err
	}
	adapter.calls++
	return file.Close()
}
func TestStableNamespaceBatchActualFinalCloseRefusal(t *testing.T) {
	if !StableRelativeNamespaceSupported() {
		t.Skip("parent namespace persistence unsupported")
	}
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(dir + "/child")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	generation, err := StableNamespaceParentGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &closeAfterNamespaceSyncAdapter{}
	tokens, err := newStableNamespaceBatchTokensWithDuplicate(StableNamespaceBatchSpec{
		Registrations: []StableNamespaceSpec{{Parent: parent, LinkedResource: child, ParentGeneration: generation, Operation: NamespaceCreate, NewName: "child", DiagnosticPath: "child"}},
	}, adapter, duplicateStableFile)
	if !errors.Is(err, os.ErrClosed) || tokens != nil || adapter.calls != 1 {
		t.Fatalf("actual final Close: err=%v tokens=%d syncs=%d; require refusal before stable return", err, len(tokens), adapter.calls)
	}
}
