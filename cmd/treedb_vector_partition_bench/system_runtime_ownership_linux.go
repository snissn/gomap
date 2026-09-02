//go:build linux

package main

import (
	"errors"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

func applyVectorPartitionSystemRuntimeOwnershipPlatformV1(cpus []int) error {
	var want unix.CPUSet
	for _, cpu := range cpus {
		want.Set(cpu)
	}
	for attempt := 0; attempt < 8; attempt++ {
		entries, err := os.ReadDir("/proc/self/task")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			tid, err := strconv.Atoi(entry.Name())
			if err != nil {
				continue
			}
			if err := unix.SchedSetaffinity(tid, &want); err != nil && !errors.Is(err, unix.ESRCH) {
				return err
			}
		}
		verified := true
		entries, err = os.ReadDir("/proc/self/task")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			tid, err := strconv.Atoi(entry.Name())
			if err != nil {
				continue
			}
			var got unix.CPUSet
			if err := unix.SchedGetaffinity(tid, &got); err != nil {
				if errors.Is(err, unix.ESRCH) {
					verified = false
					continue
				}
				return err
			}
			if got.Count() != len(cpus) {
				verified = false
				continue
			}
			for _, cpu := range cpus {
				if !got.IsSet(cpu) {
					verified = false
					break
				}
			}
		}
		if verified {
			return nil
		}
	}
	return errors.New("runtime ownership could not bind every process thread")
}
