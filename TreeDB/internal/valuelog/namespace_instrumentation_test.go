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
