//go:build !linux && !darwin

package powerlosscert

import "os"

func peakRSSBytes(*os.ProcessState) (uint64, bool) { return 0, false }
