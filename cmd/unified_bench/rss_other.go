//go:build !linux

package main

func currentRSSBytes() (uint64, bool, error) {
	return 0, false, nil
}
