//go:build unix

package vectorpartition

import (
	"os/exec"
	"syscall"
	"time"
)

// configureExternalCommand places the backend in its own process group. A
// context timeout terminates members of that group which retain inherited I/O
// pipes, while WaitDelay bounds any remaining pipe-drain wait.
func configureExternalCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 100 * time.Millisecond
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
