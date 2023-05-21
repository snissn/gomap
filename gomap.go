package gomap

import (
	"bytes"
	"syscall"
	"time"

	"log"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(32 * 1024)
var DEFAULTSLABSIZE int64 = int64(1024 * DEFAULTMAPSIZE)

func (h *Hashmap) closeFPs() {
	err := h.hashMapFile.Close()
	handleError(err)
	err = h.hashMap.Unmap()
	handleError(err)
}

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
			item := h.unmarshalItemFromSlab(mybucket)
			if bytes.Equal(item.Key, key) {
				return item.Value, nil
			}
		}
		count++
	}

	return nil, nil
}

func (h *Hashmap) AddMany(items []Item) {

	startTime := time.Now()
	slabOffsets := h.addManySlabs(items)
	slabTime := getRunTime(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	for i, item := range items {
		h.addBucket(item.Key, slabOffsets[i])
	}
	hashTime := getRunTime(startTime)
	h.hashTime += hashTime
}

func (h *Hashmap) Add(key []byte, value []byte) {
	item := Item{Key: key, Value: value}
	startTime := time.Now()
	slabOffset := h.addSlab(item)
	slabTime := getRunTime(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	h.addBucket(key, slabOffset)
	hashTime := getRunTime(startTime)
	h.hashTime += hashTime
}

// Mlock locks the data in memory to prevent it from being swapped to disk.
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

func (h *Hashmap) New(folder string) {
	h.Folder = folder
	N, slabSize := h.readCapacity()
	h.initN(folder, N, slabSize)
}

/*

Example usage:
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	obj.Add("key", "value")

*/
