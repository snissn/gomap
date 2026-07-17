//go:build linux

package powerlosscert

import (
	"os"
	"syscall"
)

func peakRSSBytes(state *os.ProcessState) (uint64, bool) {
	usage, ok := state.SysUsage().(*syscall.Rusage)
	if !ok || usage.Maxrss <= 0 {
		return 0, false
	}
	return uint64(usage.Maxrss) * 1024, true
}
