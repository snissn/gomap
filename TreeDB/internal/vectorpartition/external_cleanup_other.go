//go:build !unix

package vectorpartition

import "os/exec"

// Non-Unix platforms retain CommandContext's portable root-process handling.
func cleanupExternalCommand(cmd *exec.Cmd) {}
