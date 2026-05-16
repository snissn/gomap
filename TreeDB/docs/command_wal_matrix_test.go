package docs_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type commandWALSupportMatrix struct {
	Version  int                      `json:"version"`
	Owner    string                   `json:"owner"`
	Tracker  string                   `json:"tracker"`
	Statuses []string                 `json:"statuses"`
	Entries  []commandWALSupportEntry `json:"entries"`
}

type commandWALSupportEntry struct {
	Surface    string   `json:"surface"`
	EntryPoint string   `json:"entry_point"`
	Command    string   `json:"command"`
	Status     string   `json:"status"`
	FirstPR    string   `json:"first_pr"`
	Tests      []string `json:"tests"`
}

func TestCommandWALSupportMatrixIsWellFormed(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	if matrix.Version != 1 {
		t.Fatalf("matrix version=%d, want 1", matrix.Version)
	}
	if matrix.Owner == "" || matrix.Tracker == "" {
		t.Fatalf("matrix missing owner/tracker: %+v", matrix)
	}
	allowedStatus := make(map[string]struct{}, len(matrix.Statuses))
	for _, status := range matrix.Statuses {
		allowedStatus[status] = struct{}{}
	}
	seen := make(map[string]struct{}, len(matrix.Entries))
	for _, entry := range matrix.Entries {
		key := entry.Surface + "\x00" + entry.EntryPoint
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate matrix entry %q/%q", entry.Surface, entry.EntryPoint)
		}
		seen[key] = struct{}{}
		if entry.Surface == "" || entry.EntryPoint == "" || entry.Command == "" || entry.Status == "" || entry.FirstPR == "" {
			t.Fatalf("incomplete matrix entry: %+v", entry)
		}
		if _, ok := allowedStatus[entry.Status]; !ok {
			t.Fatalf("%s/%s has unknown status %q", entry.Surface, entry.EntryPoint, entry.Status)
		}
		if len(entry.Tests) == 0 && entry.Status != "future" {
			t.Fatalf("%s/%s status %s needs test evidence", entry.Surface, entry.EntryPoint, entry.Status)
		}
	}
}

func TestCommandWALSupportMatrixCoversCollectionMutators(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, entryPoint := range []string{
		"CollectionManager.CreateCollection",
		"Collection.CreateIndex",
		"Collection.DropIndex",
		"Collection.DropIndexes",
		"Collection.DropAllIndexes",
		"Collection.Insert",
		"Collection.InsertBatch",
		"Collection.InsertBatchWithTemplateV1Encoder",
		"Collection.InsertBatchValidatedBSON",
		"Collection.Delete",
		"Collection.DeleteDocument",
		"Collection.DeleteBatch",
		"Collection.Update",
		"Collection.UpdateBatch",
		"Collection.UpdateBatchIfNoSecondaryUniqueIndexes",
		"Collection.UpdateBatchIfNoSecondaryUniqueIndexChanges",
		"Collection.UpdateBSONSet",
		"Collection.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges",
	} {
		requireMatrixEntry(t, matrix, "collections", entryPoint)
	}
}

func TestCommandWALSupportMatrixCoversMongoMutationHandlers(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, command := range []string{"create", "insert", "update", "delete", "createIndexes", "dropIndexes"} {
		requireMatrixEntry(t, matrix, "mongo_gateway", command)
	}
}

func TestCommandWALSupportMatrixCoversNativeWireMutationCommands(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, command := range []string{
		"CommandCreateCollection",
		"CommandCreateIndex",
		"CommandDropIndex",
		"CommandInsertBatch",
		"CommandReplaceBatch",
		"CommandDeleteBatch",
	} {
		requireMatrixEntry(t, matrix, "nativewire", command)
	}
}

func TestCommandWALSupportMatrixDocumentsRejectedCommandsWithPublicError(t *testing.T) {
	matrix := loadCommandWALSupportMatrix(t)
	for _, entry := range matrix.Entries {
		if entry.Status != "WAL-rejected" {
			continue
		}
		if !strings.Contains(strings.Join(entry.Tests, " "), "Reject") && !strings.Contains(entry.Command, "future") && entry.Command != "none" {
			t.Fatalf("%s/%s WAL-rejected entry lacks rejection evidence: %+v", entry.Surface, entry.EntryPoint, entry)
		}
	}
	assertFileContains(t, filepath.Join(repoRootForDocsTest(t), "TreeDB", "errors.go"), "ErrCommandWALRejected")
}

func TestCommandWALNoActiveCollectionWALImplementationDrift(t *testing.T) {
	_, repoRoot := repoRoots(t)
	allowed := map[string]bool{
		filepath.Join(repoRoot, "TreeDB", "internal", "collectionwal"): true,
		filepath.Join(repoRoot, "TreeDB", "docs"):                      true,
	}
	err := filepath.WalkDir(filepath.Join(repoRoot, "TreeDB"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if allowed[path] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if strings.Contains(text, "collection-l*.log") || strings.Contains(text, "collection-l0") {
			t.Fatalf("%s references collection WAL segment names outside deprecated implementation", path)
		}
		if strings.Contains(text, "internal/collectionwal") &&
			!strings.Contains(text, "legacy collection WAL") &&
			!strings.Contains(text, "RequireClean") &&
			!strings.Contains(text, "ErrRecoveryRequired = collectionwal.ErrCollectionWALRecoveryRequired") {
			t.Fatalf("%s imports internal/collectionwal without legacy-clean guard language", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk TreeDB: %v", err)
	}
}

func loadCommandWALSupportMatrix(t *testing.T) commandWALSupportMatrix {
	t.Helper()
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "command-wal-support-matrix.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read support matrix: %v", err)
	}
	var matrix commandWALSupportMatrix
	if err := json.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("decode support matrix: %v", err)
	}
	return matrix
}

func requireMatrixEntry(t *testing.T, matrix commandWALSupportMatrix, surface, entryPoint string) commandWALSupportEntry {
	t.Helper()
	for _, entry := range matrix.Entries {
		if entry.Surface == surface && entry.EntryPoint == entryPoint {
			return entry
		}
	}
	t.Fatalf("matrix missing %s/%s", surface, entryPoint)
	return commandWALSupportEntry{}
}

func repoRootForDocsTest(t *testing.T) string {
	t.Helper()
	_, repoRoot := repoRoots(t)
	return repoRoot
}

func assertFileContains(t *testing.T, path, substr string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(raw), substr) {
		t.Fatalf("%s missing %q", path, substr)
	}
}
