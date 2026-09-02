//go:build !darwin && !linux && !windows

package main

func hostCPUModel() string { return "" }

func hostMachineModel() string { return "" }

func hostMemBytes() uint64 { return 0 }
