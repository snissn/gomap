//go:build !linux

package main

func m3PeakRSS() (int64, bool)       { return 0, false }
func m3ResidentBytes() (int64, bool) { return 0, false }
