package gomap

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-errors/errors"

	"log"
	"os"
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

func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool) {
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		hkey := ((uint64(myhash) % (h.Capacity)) + count) % h.Capacity
		mybucket := (*h.Keys)[hkey]
		if mybucket.slabOffset == 0 {
			return hkey, true
		} else {
			if mybucket.hash == myhash {
				item := h.unmarshalItemFromSlab(mybucket)
				if bytes.Equal(item.Key, key) {
					return hkey, false
				}
			}
			count++
		}
	}
	panic("why")
}

func (h *Hashmap) addKey(key []byte, slabOffset Key) {
	h.mapLk.Lock()
	defer h.mapLk.Unlock()

	hkey, newKey := h.getKeyOffsetToAdd(key)
	(*h.Keys)[hkey] = slabOffset
	if newKey {
		*h.Count += 1
	}
}

func (h *Hashmap) getFromMap(key []byte, keys *[]Key) ([]byte, error) {
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity

		mybucket := (*keys)[myKeyIndex]

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
func (h *Hashmap) Get(key []byte) ([]byte, error) {
	h.mapLk.RLock()
	defer h.mapLk.RUnlock()

	res, err := h.getFromMap(key, h.Keys)
	//err found return err
	if err != nil {
		return res, err
	}

	//result found from new hashmap so use that
	if res != nil {
		return res, err
	}

	//no result found, return result from old keys
	return h.getFromMap(key, h.oldKeys)

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

func (h *Hashmap) addManyBuckets(items []Item, slabOffsets []Key) {
	if h.checkResize() {
		h.resize()
	}

	h.addManyKeys(items, slabOffsets)
}

func (h *Hashmap) addManyKeys(items []Item, slabOffsets []Key) {
	h.mapLk.Lock()
	defer h.mapLk.Unlock()

	var wg sync.WaitGroup
	seenSet := NewSet()
	extra_items := make([]Item, 0)
	extra_slabOffsets := make([]Key, 0)

	hkeys, totalNewKey := ConcurrentMap(items, h.getKeyOffsetToAdd)
	for i, hkey := range hkeys {
		alreadyExists := seenSet.Add(hkey)
		if alreadyExists {
			extra_items = append(extra_items, items[i])
			extra_slabOffsets = append(extra_slabOffsets, slabOffsets[i])
			totalNewKey -= 1
		} else {
			//todo put this in gothing
			wg.Add(1)
			go func(i int, hkey uint64) {
				defer wg.Done()
				(*h.Keys)[hkey] = slabOffsets[i]
			}(i, hkey)
		}
	}
	wg.Wait()
	*h.Count += totalNewKey
	for i, item := range extra_items {
		h.addBucket(item.Key, extra_slabOffsets[i])
	}
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

func (h *Hashmap) addBucket(key []byte, slabOffset Key) {
	if h.checkResize() {
		h.resize()
	}

	h.addKey(key, slabOffset)

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

func (h *Hashmap) createDirectory() {
	err := os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}
}

func (h *Hashmap) getKeys(m mmap.MMap) []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&m[0]))
	ret := unsafe.Slice(tmpkeys, h.Capacity)
	return ret
}

func (h *Hashmap) readCapacity() (uint64, int64) {
	dat, err := os.ReadFile(h.Folder + "/capacity")
	if err != nil {
		return DEFAULTMAPSIZE, DEFAULTSLABSIZE
	}
	capacity, err := strconv.ParseUint(string(dat), 10, 64)
	handleError(err)

	slabdat, err := os.ReadFile(h.Folder + "/slabSize")
	if err != nil {
		return DEFAULTMAPSIZE, DEFAULTSLABSIZE
	}
	slabSize, err := strconv.ParseInt(string(slabdat), 10, 64)
	handleError(err)

	return capacity, slabSize
}

func (h *Hashmap) New(folder string) {
	h.Folder = folder
	N, slabSize := h.readCapacity()
	h.initN(folder, N, slabSize)
}

func (h *Hashmap) writeSlabSize(slabSize int64) error {
	s := strconv.FormatInt(slabSize, 10)
	return os.WriteFile(h.Folder+"/slabSize", []byte(s), 0655)
}

func (h *Hashmap) writeCapacity(N uint64) error {
	s := strconv.FormatUint(N, 10)
	return os.WriteFile(h.Folder+"/capacity", []byte(s), 0655)
}

func (h *Hashmap) initN(folder string, N uint64, slabSize int64) {
	h.Folder = folder
	m, f_map, err := h.openMmapHash(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	slab, f_slab, err := h.openMmapSlab(slabSize)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	err = h.writeCapacity(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}
	err = h.writeSlabSize(slabSize)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	h.hashMap = m
	h.hashMapFile = f_map

	h.slabMap = slab
	h.slabFILE = f_slab
	h.slabSize = slabSize

	//todo
	h.slabOffset = getSlabOffset(h.slabMap)
	//xxx

	if *h.slabOffset == 0 {
		sentinel := []byte("offset")
		h.writeSlab(sentinel)
		*h.slabOffset = SlabOffset(len(sentinel))
	}

	h.Capacity = N
	h.Count = getCount(h.slabMap)
	keys := h.getKeys(m)
	h.Keys = &keys

	h.resizeOffset = getResizeOffset(h.slabMap)

	if *h.resizeOffset != 0 {
		fmt.Println("Last restarted during resize")
		old_slab, _, err := h.openMmapSlab(slabSize) //todo leaking old file
		if err != nil {
			log.Fatal(errors.Wrap(err, 1))
			keys = h.getKeys(old_slab)
			h.oldKeys = &keys
			h.oldCapacity = N / 2
			h.slabMapOld = old_slab
			go h.copyToNewMap()

		}

	}

}

/*

Example usage:
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	obj.Add("key", "value")

*/
