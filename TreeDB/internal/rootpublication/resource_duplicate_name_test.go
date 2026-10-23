//go:build darwin || linux || freebsd || netbsd || openbsd || windows

package rootpublication

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestStableDuplicateRetainsDiagnosticProvenanceAndIndependentHandle(t *testing.T) {
	syncSuffix := "#stable-pin"
	if runtime.GOOS == "windows" {
		syncSuffix = "#stable-sync-pin"
	}
	for _, tc := range []struct {
		name      string
		duplicate func(*os.File) (*os.File, error)
		suffix    string
	}{
		{"file", duplicateStableFile, "#stable-pin"},
		{"sync", duplicateStableSyncFile, syncSuffix},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "original.bin")
			if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
				t.Fatal(err)
			}
			source, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			first, err := tc.duplicate(source)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			second, err := tc.duplicate(first)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			if source.Name() != path || first.Name() != path+tc.suffix || second.Name() != first.Name() {
				t.Fatalf("diagnostic provenance: source=%q first=%q second=%q", source.Name(), first.Name(), second.Name())
			}
			want, err := StableIdentityFromFile(source)
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := StableIdentityFromFile(second)
			if err != nil || !SamePhysicalIdentity(want, got) {
				t.Fatalf("retained duplicate identity=%+v want=%+v err=%v", got, want, err)
			}
			var data [8]byte
			if _, err := second.ReadAt(data[:], 0); err != nil || string(data[:]) != "retained" {
				t.Fatalf("retained duplicate read=%q err=%v", data, err)
			}
		})
	}
}
