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

type Mapper func([]byte) (uint64, bool)
type Result struct {
	index uint64
	hkey  uint64
	isnew bool
}

func ConcurrentMap(inputs []Item, mapper Mapper) ([]uint64, uint64) {
	ch := make(chan Result)
	results := make([]uint64, len(inputs))
	totalNewKey := uint64(0)

	for i, input := range inputs {
		go func(i uint64, input Item, ch chan<- Result) {
			hkey, isnew := mapper(input.Key)
			result := Result{index: i, hkey: hkey, isnew: isnew}
			ch <- result
		}(uint64(i), input, ch)
	}
	for _ = range inputs {
		result := <-ch
		results[result.index] = result.hkey
		if result.isnew {
			totalNewKey += 1
		}
	}

	return results, totalNewKey
}

type Set struct {
	data map[uint64]bool
}

func NewSet() *Set {
	return &Set{data: make(map[uint64]bool)}
}

func (s *Set) Add(value uint64) bool {
	alreadyExists := s.data[value]
	s.data[value] = true
	return alreadyExists
}
