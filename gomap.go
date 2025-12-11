package gomap

import (
	"bytes"
	"fmt"
	"syscall"
	"time"

	"log"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(32 * 1024)

func getRunTime(startTime time.Time) time.Duration {
	endTime := time.Now()
	return endTime.Sub(startTime)
}

func printTotalRunTime(startTime time.Time) {
	endTime := time.Now()
	totalRunTime := endTime.Sub(startTime)
	fmt.Printf("Total run time: %s\n", totalRunTime)
}

func (h *Hashmap) closeFPs() error {
	if err := h.hashMapFile.Close(); err != nil {
		return err
	}
	if err := h.hashMap.Unmap(); err != nil {
		return err
	}
	return nil
}

// Get retrieves the value for a given key.
// It returns nil, nil if the key is not found.
func (h *Hashmap) Get(key []byte) ([]byte, error) {

	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity

		mybucket := (*h.Keys)[myKeyIndex]

		if mybucket.slabOffset == 0 {
			return nil, nil
		}

		if mybucket.hash == myhash {
			item, err := h.unmarshalItemFromSlab(mybucket)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(item.Key, key) {
				return item.Value, nil
			}
		}
		count++
	}

	return nil, nil
}

// AddMany inserts multiple items in a batch.
// It is not thread-safe.
func (h *Hashmap) AddMany(items []Item) error {

	startTime := time.Now()
	slabOffsets, err := h.addManySlabs(items)
	if err != nil {
		return err
	}
	slabTime := getRunTime(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	for i, item := range items {
		if err := h.addBucket(item.Key, slabOffsets[i]); err != nil {
			return err
		}
	}
	hashTime := getRunTime(startTime)
	h.hashTime += hashTime
	return nil
}

// Add inserts a single key-value pair.
// It is not thread-safe.
func (h *Hashmap) Add(key []byte, value []byte) error {
	item := Item{Key: key, Value: value}
	startTime := time.Now()
	slabOffset, err := h.addSlab(item)
	if err != nil {
		return err
	}
	slabTime := getRunTime(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	err = h.addBucket(key, slabOffset)
	hashTime := getRunTime(startTime)
	h.hashTime += hashTime
	return err
}

// mlock locks the data in memory to prevent it from being swapped to disk.
func (h *Hashmap) mlock(data mmap.MMap) {
	_, _, errno := syscall.Syscall(syscall.SYS_MLOCK, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), 0)
	if errno != 0 {
		// If the syscall fails, it could be because the user does not have
		// sufficient privileges to lock memory. To fix this, edit the
		// /etc/security/limits.conf file and add the following line:
		//
		// <username> soft memlock unlimited
		//
		// where <username> is the name of the user running the program.
		// Then, log out and log back in for the changes to take effect.
		//
		// Alternatively, you can run the program with sudo privileges to
		// bypass this error.
		log.Fatalf("syscall.Syscall(SYS_MLOCK) failed: %v\n"+
			"To fix this, edit the /etc/security/limits.conf file and add the following line:\n"+
			"<username> soft memlock unlimited\n"+
			"where <username> is the name of the user running the program.\n"+
			"Then, log out and log back in for the changes to take effect.\n"+
			"Alternatively, you can run the program with sudo privileges to bypass this error.", errno)
	}
}

// New initializes a Hashmap in the given folder.
func (h *Hashmap) New(folder string) error {
	h.Folder = folder
	N, err := h.readCapacity()
	if err != nil {
		return err
	}
	return h.initN(folder, N)
}

/*

Example usage:
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	obj.Add("key", "value")

*/
