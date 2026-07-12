package valuelog

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestRemoveSegmentFileOnceEmitsUnlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	if err := os.WriteFile(path, []byte("value"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace != "" {
			events = append(events, event)
		}
		return nil
	})
	defer restore()

	if err := removeSegmentFileOnce(path); err != nil {
		t.Fatalf("removeSegmentFileOnce: %v", err)
	}
	want := []durabilitycut.Event{{
		Resource:  durabilitycut.ResourceValueLog,
		Root:      dir,
		Namespace: durabilitycut.NamespaceUnlink,
		OldPath:   path,
	}}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("namespace events = %#v, want %#v", events, want)
	}
}

func TestWriterSyncEmitsExactValueLogFileBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := NewWriter(path, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	var events []durabilitycut.Event
	syncCalled := false
	writer.syncFn = func(*os.File) error {
		if len(events) != 1 || events[0].Point != durabilitycut.BeforeDependencyFileSync {
			t.Fatalf("sync called before before-boundary: events=%#v", events)
		}
		syncCalled = true
		return nil
	}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Point != "" {
			events = append(events, event)
		}
		return nil
	})
	defer restore()

	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if !syncCalled {
		t.Fatal("Sync did not call file sync")
	}
	want := []durabilitycut.Event{
		{Point: durabilitycut.BeforeDependencyFileSync, Resource: durabilitycut.ResourceValueLog, Root: dir, Path: path},
		{Point: durabilitycut.AfterDependencyFileSync, Resource: durabilitycut.ResourceValueLog, Root: dir, Path: path},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("sync events = %#v, want %#v", events, want)
	}
}
