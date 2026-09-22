//go:build !unix

package main

import "os"

func checkpointBenchSignal() (os.Signal, bool) {
	return nil, false
}

func shutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}

func getRusageSnapshot() rusageSnapshot {
	return rusageSnapshot{}
}

func formatRusageDelta(before, after rusageSnapshot) string {
	return ""
}
