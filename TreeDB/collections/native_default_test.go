package collections

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCollectionsRuntimeHasNoOracleOrTranslationSelectors(t *testing.T) {
	var dir string
	if _, file, _, ok := runtime.Caller(0); ok && filepath.IsAbs(file) {
		if _, err := os.Stat(file); err == nil {
			dir = filepath.Dir(file)
		}
	}
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		dir = wd
	}
	for _, name := range []string{"api.go", "planner.go"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		src := string(data)
		for _, forbidden := range []string{
			"oracle",
			"native-fastpath",
			"TREEDB_COLLECTION_PATH_LABEL",
			"executionPath",
			"detached",
			"replay",
		} {
			if strings.Contains(src, forbidden) {
				t.Fatalf("%s contains forbidden runtime selector/translation token %q", name, forbidden)
			}
		}
	}
}
