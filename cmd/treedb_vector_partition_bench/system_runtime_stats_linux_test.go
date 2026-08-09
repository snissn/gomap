//go:build linux

package main

import (
	"runtime"
	"sync"
	"testing"
)

func TestVectorPartitionSystemKernelRuntimeStatsCoverWorkerThreadsV1(t *testing.T) {
	beforeCPU, _, _, beforeVoluntary, beforeNonvoluntary := vectorPartitionSystemKernelRuntimeStatsV1()
	var wait sync.WaitGroup
	wait.Add(4)
	for worker := 0; worker < 4; worker++ {
		go func(seed uint64) {
			defer wait.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			for index := uint64(0); index < 20_000_000; index++ {
				seed = seed*6364136223846793005 + index
			}
			if seed == 0 {
				t.Error("unexpected zero worker result")
			}
		}(uint64(worker + 1))
	}
	wait.Wait()
	afterCPU, _, _, afterVoluntary, afterNonvoluntary := vectorPartitionSystemKernelRuntimeStatsV1()
	if afterCPU <= beforeCPU || afterVoluntary < beforeVoluntary || afterNonvoluntary < beforeNonvoluntary {
		t.Fatalf("process runtime counters did not advance monotonically: before=(%d,%d,%d) after=(%d,%d,%d)", beforeCPU, beforeVoluntary, beforeNonvoluntary, afterCPU, afterVoluntary, afterNonvoluntary)
	}
}
