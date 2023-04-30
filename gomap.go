package gomap

import (
	"fmt"
	"strconv"

	"github.com/go-errors/errors"

	"log"
	"os"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(32 * 1024 * 64)
var DEFAULTSLABSIZE int64 = int64(1024 * DEFAULTMAPSIZE)

func (h *Hashmap) checkResize() bool {
	return *h.Count*14 > h.Capacity*10
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

	h.hashMapSlabValue = newH.hashMapSlabValue
	h.SlabValues = newH.SlabValues
	h.slabMap = newH.slabMap
}
func (h *Hashmap) resize() {
	var newH Hashmap
	fmt.Println("Resize", h.Capacity)
	//todo create a new init function that doesn't take a slabSize and doesn't resize the slab
	newH.initN(h.Folder, 2*(h.Capacity), (h.slabSize))

	for index, mykey := range *h.Keys {
		if mykey.hash != 0 {
			slabValues := (*h.SlabValues)[index]
			newH.addKey(mykey, slabValues.slabOffset)
		}
	}

	h.replaceHashmap(newH)
}

func (h *Hashmap) addKey(key Key, slabOffset SlabOffset) {
	slabValues := SlabValues{slabOffset: slabOffset}
	count := uint64(0)
	myhash := key.hash
	for count < h.Capacity {
		hkey := ((uint64(myhash) % (h.Capacity)) + count) % h.Capacity
		mybucket := (*h.Keys)[hkey]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				*h.Count += 1
			}
			(*h.Keys)[hkey] = key
			(*h.SlabValues)[hkey] = slabValues
			return
		} else {
			count++
		}
	}
	panic("why")
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

func (h *Hashmap) unmarshalItemFromSlab(slabValues SlabValues) Item {
	var ret Item

	// Get slice from slabMap
	rawBytes := h.slabMap[slabValues.slabOffset:]

	// Decode key length and value length
	keyLength, n := decodeLEB128(rawBytes)
	valueLength, m := decodeLEB128(rawBytes[n:])

	// Decode key and value
	ret.Key = string(rawBytes[n+m : n+m+int(keyLength)])
	ret.Value = string(rawBytes[n+m+int(keyLength) : n+m+int(keyLength)+int(valueLength)])

	return ret
}

func (h *Hashmap) addSlab(item Item) SlabOffset {
	keyBytes := []byte(item.Key)
	valueBytes := []byte(item.Value)

	totalLength := len(keyBytes) + len(valueBytes) + 10 // 10 is the maximum length of LEB128 encoded uint64

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
	keyLength := encodeLEB128(slab, uint64(len(keyBytes)))
	// Write value length
	valueLength := encodeLEB128(slab[keyLength:], uint64(len(valueBytes)))
	// Write key
	copy(slab[keyLength+valueLength:], keyBytes)
	// Write value
	copy(slab[keyLength+valueLength+len(keyBytes):], valueBytes)

	actualTotalLength := keyLength + valueLength + len(keyBytes) + len(valueBytes)
	*h.slabOffset += SlabOffset(actualTotalLength)
	return offset
}

func (h *Hashmap) Get(key string) (string, error) {
	myhash := hash(key)
	count := uint64(0)
	for count != h.Capacity {
		myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity
		mybucket := (*h.Keys)[myKeyIndex]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				//todo return err=notfound
				return "", nil
			}

			//XXX todo confirm the key is a match instead of just relyign on hash matching
			item := h.unmarshalItemFromSlab((*h.SlabValues)[myKeyIndex])

			return item.Value, nil
		} else {
			count++
		}
	}
	//todo return err=notfound
	return "", nil
}

func (h *Hashmap) Add(key string, value string) {

	item := Item{Key: key, Value: value}
	slabOffset := h.addSlab(item)
	h.addBucket(key, slabOffset)
}

func (h *Hashmap) addBucket(key string, slabOffset SlabOffset) {
	if h.checkResize() {
		h.resize()
	}

	myhash := hash(key)
	mykey := Key{hash: myhash}
	h.addKey(mykey, slabOffset)

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
	ret, err := mmap.Map(f, mmap.RDWR, 0)
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

func (h *Hashmap) openMmapFile(filename string) (mmap.MMap, *os.File, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0655)
	if err != nil {
		log.Fatal("3", errors.Wrap(err, 1))
	}

	ret, err := mmap.Map(f, mmap.RDWR, 0)
	return ret, f, err
}

func (h *Hashmap) openMmapHashOffsetIndex(N uint64) (mmap.MMap, *os.File, error) {
	bytes := NtoBytesHashmapOffsetIndex(N)
	h.createDirectory()
	filename := h.Folder + "/hashkeysOffsetIndex-" + fmt.Sprint(N)

	if !doesFileExist(filename) {
		h.createFile(filename, bytes)
	}

	return h.openMmapFile(filename)
}

func (h *Hashmap) openMmapHash(N uint64) (mmap.MMap, *os.File, error) {
	bytes := NtoBytesHashmap(N)
	h.createDirectory()
	filename := h.Folder + "/hashkeys-" + fmt.Sprint(N)

	if !doesFileExist(filename) {
		h.createFile(filename, bytes)
	}

	return h.openMmapFile(filename)
}

func (h *Hashmap) getSlabValues() []SlabValues {
	tmpkeys := (*SlabValues)(unsafe.Pointer(&h.hashMapSlabValue[0]))
	ret := unsafe.Slice(tmpkeys, h.Capacity)
	return ret
}

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.hashMap[size*2]))
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

	offsetIndex, f_mapOffsetIndex, err := h.openMmapHashOffsetIndex(N)
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

	h.hashMapSlabValueFile = f_mapOffsetIndex
	h.hashMapSlabValue = offsetIndex

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
	h.Count = getCount(h.hashMap)
	keys := h.getKeys()
	h.Keys = &keys

	slabValues := h.getSlabValues()
	h.SlabValues = &slabValues
}

/*

Example usage:
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	obj.Add("key", "value")

*/
