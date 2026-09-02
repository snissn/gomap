//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"
)

func checkpointBenchSignal() (os.Signal, bool) {
	return syscall.SIGUSR1, true
}

func shutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

func getRusageSnapshot() rusageSnapshot {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return rusageSnapshot{}
	}
	return rusageSnapshot{
		Minflt: ru.Minflt,
		Majflt: ru.Majflt,
		Nvcsw:  ru.Nvcsw,
		Nivcsw: ru.Nivcsw,
	}
}

func formatRusageDelta(before, after rusageSnapshot) string {
	return fmt.Sprintf(
		"minflt +%d majflt +%d nvcsw +%d nivcsw +%d",
		after.Minflt-before.Minflt,
		after.Majflt-before.Majflt,
		after.Nvcsw-before.Nvcsw,
		after.Nivcsw-before.Nivcsw,
	)
}
