//go:build !unix

package vectorpartition

import (
	"os/exec"
	"time"
)

// Windows and other platforms retain CommandContext's portable process kill;
// WaitDelay still bounds descendant-held pipe cleanup.
func configureExternalCommand(cmd *exec.Cmd) { cmd.WaitDelay = 100 * time.Millisecond }
