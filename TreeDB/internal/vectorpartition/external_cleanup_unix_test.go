//go:build unix

package vectorpartition

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExternalBackendRootExitKillsProcessGroupMember(t *testing.T) {
	root := t.TempDir()
	pidDir := t.TempDir()
	pidFile := filepath.Join(pidDir, "child.pid")
	t.Setenv("TMPDIR", root)
	t.Setenv("TREE_DB_CHILD_PID", pidFile)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := RunExternalJSONForSource(ctx, []string{"sh", "-c", "printf '{' > \"$1\"; (sleep 5) & echo $! > \"$TREE_DB_CHILD_PID\"; exit 0"}, []byte("{}"), 1024, Source{SourceID: "expected", Checksum: strings.Repeat("0", 64), Vectors: 1, Dimensions: 1, Metric: "cosine"})
	if err == nil {
		t.Fatal("root-exited backend accepted")
	}
	raw, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if parseErr != nil || pid < 1 {
		t.Fatalf("invalid child pid %q: %v", raw, parseErr)
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		err = syscall.Kill(pid, 0)
		if err == syscall.ESRCH {
			return
		}
		if err != nil {
			t.Fatalf("probe child pid %d: %v", pid, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("root-exited descendant %d still alive after process-group cleanup", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
