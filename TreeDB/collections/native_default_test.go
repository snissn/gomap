package collections

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCollectionsRuntimeHasNoOracleOrTranslationSelectors(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
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
