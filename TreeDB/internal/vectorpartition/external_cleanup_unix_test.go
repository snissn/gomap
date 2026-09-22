//go:build unix

package vectorpartition

import (
	"context"
	"os"
	"os/exec"
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
	request, buildErr := Build(fixture(), config())
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	requestRaw, marshalErr := CanonicalJSON(request)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	_, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", "printf '{' > \"$1\"; (sleep 5) & echo $! > \"$TREE_DB_CHILD_PID\"; exit 0"}, requestRaw, ExternalJSONLimits{MaxInput: len(requestRaw), MaxOutput: 1024}, request)
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
		stopped, probeErr := processStoppedOrZombie(pid)
		if probeErr != nil {
			t.Fatalf("probe child pid %d: %v", pid, probeErr)
		}
		if stopped {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("root-exited descendant %d still running after process-group cleanup", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// processStoppedOrZombie distinguishes an executing orphan from a process
// already stopped by SIGKILL but awaiting reaping by PID 1. kill(pid, 0) alone
// reports both as extant. POSIX ps exposes the process state across supported
// Unix test targets; a Z state proves the descendant is no longer running.
func processStoppedOrZombie(pid int) (bool, error) {
	if err := syscall.Kill(pid, 0); err != nil {
		if err == syscall.ESRCH {
			return true, nil
		}
		return false, err
	}
	out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return true, nil
		}
		return false, err
	}
	state := strings.TrimSpace(string(out))
	return state == "" || strings.HasPrefix(state, "Z"), nil
}
