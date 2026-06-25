//go:build !darwin && !linux

package db

func detectPhysicalCoreCount() int { return 0 }
