//go:build !linux

package main

// Peak RSS is unavailable outside the Linux /proc VmHWM contract.
func peakRSS() (int64, bool) { return 0, false }
