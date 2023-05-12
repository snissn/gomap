package gomap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-errors/errors"
	"golang.org/x/sys/unix"

	"log"
	"os"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(32 * 1024)
var DEFAULTSLABSIZE int64 = int64(1024 * DEFAULTMAPSIZE)

func (h *Hashmap) checkResize() bool {
	return *h.Count*100 > h.Capacity*55
}

func (h *Hashmap) closeFPs() {
	err := h.hashMapFile.Close()
	handleError(err)
	err = h.hashMap.Unmap()
	handleError(err)
}
func (h *Hashmap) replaceHashmap(newH Hashmap) {
	//TODO close and delete old file, can be async
	// see closeFPs

	h.hashMap = newH.hashMap
	h.hashMapFile = newH.hashMapFile
	h.Capacity = newH.Capacity
	h.Count = newH.Count
	h.Keys = newH.Keys

	h.slabMap = newH.slabMap
}
func (h *Hashmap) resize2() {
	fmt.Println("Resizing")
	fmt.Println("Count: ", *h.Count)
	fmt.Println("Capacity: ", h.Capacity)

	startTime := time.Now()
	defer printTotalRunTime(startTime)
	BATCH_SIZE := 32 * 1024

	slabOffsets := make([]Key, BATCH_SIZE)
	items := make([]Item, BATCH_SIZE)
	batch_index := 0

	var newH Hashmap
	//todo create a new init function that doesn't take a slabSize and doesn't resize the slab
	newH.initN(h.Folder, 2*(h.Capacity), (h.slabSize))

	index := uint64(0)
	for index < h.Capacity {
		mykey := (*h.Keys)[index]
		if mykey != 0 {
			item := h.unmarshalItemFromSlab(mykey)
			slabOffsets[batch_index] = mykey
			items[batch_index] = item
			batch_index += 1
			if batch_index == BATCH_SIZE {
				newH.addManyBuckets(items, slabOffsets)
				batch_index = 0
			}
		}
		index += 1
	}
	newH.addManyBuckets(items[:batch_index], slabOffsets[:batch_index])

	h.replaceHashmap(newH)
}
func (h *Hashmap) resize() {
	startTime := time.Now()
	defer printTotalRunTime(startTime)

	var newH Hashmap
	fmt.Println("Resizing")
	fmt.Println("Count: ", *h.Count)
	fmt.Println("Capacity: ", h.Capacity)
	fmt.Println("Hash Time: ", h.hashTime)
	fmt.Println("Slab Time: ", h.slabTime)
	//todo create a new init function that doesn't take a slabSize and doesn't resize the slab
	newH.initN(h.Folder, 2*(h.Capacity), (h.slabSize))

	index := uint64(0)
	for index < h.Capacity {
		mykey := (*h.Keys)[index]
		if mykey != 0 {
			item := h.unmarshalItemFromSlab(mykey)
			newH.addKey(item.Key, mykey)
		}
		index += 1
	}

	h.replaceHashmap(newH)
}

func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool) {
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		hkey := ((uint64(myhash) % (h.Capacity)) + count) % h.Capacity
		mybucket := (*h.Keys)[hkey]
		if mybucket == 0 {
			return hkey, true
		} else {
			item := h.unmarshalItemFromSlab(mybucket)
			if bytes.Equal(item.Key, key) {
				return hkey, false
			} else {
				count++
			}
		}
	}
	panic("why")
}

func (h *Hashmap) addKey(key []byte, slabOffset Key) {
	hkey, newKey := h.getKeyOffsetToAdd(key)
	(*h.Keys)[hkey] = slabOffset
	if newKey {
		*h.Count += 1
	}
}

func decodeLEB128(input []byte) (uint64, int) {
	var result uint64
	var shift uint
	var length int
	for {
		b := input[length]
		length++
		result |= (uint64(b&0x7F) << shift)
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result, length
}

func decodeuint64(input []byte) (uint64, int) {
	return binary.LittleEndian.Uint64(input), 8
}

func encodeuint64(slab []byte, input uint64) int {
	binary.LittleEndian.PutUint64(slab, input)
	return 8
}

func encodeLEB128(slab []byte, input uint64) int {
	var i int
	for input >= 0x80 {
		slab[i] = byte(input&0x7F | 0x80)
		input >>= 7
		i++
	}
	slab[i] = byte(input)
	return i + 1
}
func (h *Hashmap) unmarshalItemFromSlab(slabValues Key) Item {
	var ret Item

	rawBytes := h.slabMap[slabValues:]

	keyLength, n := decodeuint64(rawBytes)
	valueLength, m := decodeuint64(rawBytes[n:])

	ret.Key = rawBytes[n+m : n+m+int(keyLength)]
	ret.Value = rawBytes[n+m+int(keyLength) : n+m+int(keyLength)+int(valueLength)]

	return ret
}

func (h *Hashmap) addSlab(item Item) Key {
	keyBytes := item.Key
	valueBytes := item.Value

	totalLength := len(keyBytes) + len(valueBytes) + 16 // 10 is the maximum length of LEB128 encoded uint64

	offset := *h.slabOffset

	// Make sure that offset + totalLength is within h.slabSize
	if uint64(offset)+uint64(totalLength) > uint64(h.slabSize) {
		err := h.doubleSlab()
		if err != nil {
			panic(err)
		}
	}

	slab := unsafe.Slice((*byte)(unsafe.Pointer(&h.slabMap[offset])), totalLength)

	// Write key length
	keyLength := encodeuint64(slab, uint64(len(keyBytes)))
	// Write value length
	valueLength := encodeuint64(slab[keyLength:], uint64(len(valueBytes)))
	// Write key
	copy(slab[keyLength+valueLength:], keyBytes)
	// Write value
	copy(slab[keyLength+valueLength+len(keyBytes):], valueBytes)

	actualTotalLength := keyLength + valueLength + len(keyBytes) + len(valueBytes)
	*h.slabOffset += SlabOffset(actualTotalLength)
	return Key(offset)
}
func (h *Hashmap) Get(key []byte) ([]byte, error) {
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity

		mybucket := (*h.Keys)[myKeyIndex]

		if mybucket == 0 {
			return nil, nil
		}

		item := h.unmarshalItemFromSlab(mybucket)
		if bytes.Equal(item.Key, key) {
			return item.Value, nil
		} else {
			count++
		}
	}

	return nil, nil
}

func (h *Hashmap) AddMany(items []Item) {

	slabOffsets := make([]Key, len(items))
	for i, item := range items {
		slabOffsets[i] = h.addSlab(item)
	}
	h.addManyBuckets(items, slabOffsets)
}

func (h *Hashmap) addManyBuckets(items []Item, slabOffsets []Key) {
	if h.checkResize() {
		h.resize()
	}

	h.addManyKeys(items, slabOffsets)
}

func (h *Hashmap) addManyKeys(items []Item, slabOffsets []Key) {
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
		h.addKey(item.Key, extra_slabOffsets[i])
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

func (h *Hashmap) openMmapSlab(slabSize int64) (mmap.MMap, *os.File, error) {
	var f *os.File
	var err error

	err = os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}
	filename := h.Folder + "/slab"
	if !doesFileExist(filename) {
		f, err = os.Create(filename)

		if err != nil {
			log.Fatal("2", errors.Wrap(err, 1))
		}
		f.Seek(slabSize-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
		f.Close()
	}
	f, err = os.OpenFile(filename, os.O_RDWR, 0655)
	//todo test:
	//    f, err = os.OpenFile(filename, os.O_RDWR|os.O_SYNC, 0655)
	if err != nil {
		log.Fatal("3", errors.Wrap(err, 1))
	}

	fi, err := f.Stat()
	if err != nil {
		log.Fatal("4", errors.Wrap(err, 1))
	}
	if slabSize > fi.Size() { // need to expand file
		f.Seek(slabSize-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
	}

	// Advise the kernel that we intend to access the file sequentially.
	// This will enable the kernel to do read-ahead and improve write performance.
	if err := unix.Fadvise(int(f.Fd()), 0, int64(fi.Size()), unix.FADV_SEQUENTIAL); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to advise kernel for file %s: %w", filename, err)
	}

	// mmap the whole file into memory with read-write permissions.
	// As the file is larger than memory, it won't be fully loaded into memory.
	// Instead, the kernel will load and unload parts of the file as needed.
	ret, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	// Advise the kernel that the mapped memory will be accessed soon.
	// This will help to reduce the number of page faults in the beginning of the processing.
	if err := unix.Madvise(ret, unix.MADV_WILLNEED); err != nil {
		unix.Munmap(ret)
		f.Close()
		return nil, nil, fmt.Errorf("failed to advise kernel for file %s: %w", filename, err)
	}

	return ret, f, err
}

func (h *Hashmap) doubleSlab() error {
	f := h.slabFILE
	f.Seek(2*h.slabSize-1, 0)
	f.Write([]byte("\x00"))
	f.Seek(0, 0)
	f.Sync()
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	h.slabSize *= 2
	h.slabMap = m
	return nil
}
func (h *Hashmap) createDirectory() {
	err := os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}
}

func (h *Hashmap) createFile(filename string, bytes int64) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("2", errors.Wrap(err, 1))
	}
	f.Seek(bytes-1, 0)
	f.Write([]byte("\x00"))
	f.Seek(0, 0)
	f.Sync()
	f.Close()
}

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.hashMap[0]))
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
		*h.slabOffset = 8 * 3
	}

	h.Capacity = N
	h.Count = getCount(h.slabMap)
	keys := h.getKeys()
	h.Keys = &keys

}

/*

Example usage:
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	obj.Add("key", "value")

*/
