//go:build !unix

package main

// CPU accounting is intentionally unavailable rather than represented as a
// measured zero on platforms without the Unix getrusage contract.
func cpuNanos() (int64, bool) { return 0, false }
