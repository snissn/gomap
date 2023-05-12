package gomap

import (
	"fmt"
	"sync"
	"time"

	"os"
	"runtime"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/segmentio/fasthash/fnv1"
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

func hash(key []byte) Hash {
	return Hash(fnv1.HashBytes64(key))
}

func NtoBytesHashmap(N uint64) int64 {
	i := Hash(0)
	return int64(unsafe.Sizeof(i)) * int64(N)
}

func NtoBytesHashmapOffsetIndex(N uint64) int64 {
	i := SlabOffset(0)

	return (int64(unsafe.Sizeof(i))) * int64(N)
}

func getSlabOffset(slabMap mmap.MMap) *SlabOffset {
	cap := (*SlabOffset)(unsafe.Pointer(&slabMap[0]))
	return cap
}

func getCount(slabMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&slabMap[8]))
}

func printTotalRunTime(startTime time.Time) {
	endTime := time.Now()
	totalRunTime := endTime.Sub(startTime)
	fmt.Printf("Total run time: %s\n", totalRunTime)
}

type Mapper func([]byte) (uint64, bool)

func ConcurrentMap(inputs []Item, mapper Mapper) ([]uint64, []bool) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]uint64, len(inputs))
	isnew := make([]bool, len(inputs))

	for i, input := range inputs {
		wg.Add(1)
		go func(i int, input Item) {
			defer wg.Done()

			mu.Lock()
			results[i], isnew[i] = mapper(input.Key)
			mu.Unlock()
		}(i, input)
	}

	wg.Wait()

	return results, isnew
}
