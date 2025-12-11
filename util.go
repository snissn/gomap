package gomap

import (
	"fmt"
	"time"

	"os"
	"runtime"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

func getCPUNumber() int {
	return runtime.NumCPU()
}

func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
func doesFileExist(fileName string) bool {
	_, error := os.Stat(fileName)
	// check if error is "file not exists"
	if os.IsNotExist(error) {
		return false
	} else {
		return true
	}
}

func NtoBytesHashmap(N uint64) int64 {
	i := Key{}
	return int64(unsafe.Sizeof(i)) * int64(N)
}

func getSlabOffset(slabMap mmap.MMap) *SlabOffset {
	cap := (*SlabOffset)(unsafe.Pointer(&slabMap[0]))
	return cap
}

func getCount(slabMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&slabMap[8]))
}

func getRunTime(startTime time.Time) time.Duration {
	endTime := time.Now()
	return endTime.Sub(startTime)
}

func printTotalRunTime(startTime time.Time) {
	endTime := time.Now()
	totalRunTime := endTime.Sub(startTime)
	fmt.Printf("Total run time: %s\n", totalRunTime)
}
