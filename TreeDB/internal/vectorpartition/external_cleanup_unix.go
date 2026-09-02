//go:build unix

package vectorpartition

import (
	"os/exec"
	"syscall"
)

// cleanupExternalCommand is intentionally called after Wait on every result.
// A child may still hold an inherited pipe even after the root process exits.
// A process that deliberately calls setsid escapes this process-group boundary;
// Go's portable exec API cannot contain that hostile case.
func cleanupExternalCommand(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
