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
	"github.com/vmihailenco/msgpack/v5"
)

var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(32 * 1024)
var DEFAULTSLABSIZE int64 = int64(1024 * DEFAULTMAPSIZE)

func (h *Hashmap) checkResize() bool {
	return *h.Count*14 > *h.Capacity*10
}

func (h *Hashmap) closeFPs() {
	err := h.FILE.Close()
	handleError(err)
	err = h.keyMap.Unmap()
	handleError(err)
}
func (h *Hashmap) replaceHashmap(newH Hashmap) {
	//TODO close and delete old file, can be async
	// see closeFPs

	h.keyMap = newH.keyMap
	h.FILE = newH.FILE
	h.Capacity = newH.Capacity
	h.Count = newH.Count
	h.Keys = newH.Keys
}
func (h *Hashmap) resize() {
	var newH Hashmap
	newH.initN(h.Folder, 2*(*h.Capacity), 2*(h.slabSize))

	for _, mykey := range *h.Keys {
		if mykey.hash != 0 {
			newH.addKey(mykey)
		}
	}

	h.replaceHashmap(newH)
}

func (h *Hashmap) addKey(key Key) {
	count := uint64(0)
	myhash := key.hash
	for count < *h.Capacity {
		hkey := ((myhash % (*h.Capacity)) + count) % *h.Capacity
		mybucket := (*h.Keys)[hkey]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				*h.Count += 1
			}
			(*h.Keys)[hkey] = key
			return
		} else {
			count++
		}
	}
	panic("why")
}

func (h *Hashmap) unmarshalItemFromSlab(key Key) Item {
	var ret Item
	valueRawBytes := unsafe.Slice((*byte)(unsafe.Pointer(&h.slabMap[key.slabOffset])), key.slabValueLength)

	err := msgpack.Unmarshal(valueRawBytes, &ret)
	if err != nil {
		panic(err)
	}
	return ret

}

func (h *Hashmap) Get(key string) (string, error) {
	myhash := hash(key)
	count := uint64(0)
	for count != *h.Capacity {
		mybucket := (*h.Keys)[((myhash%*h.Capacity)+count)%*h.Capacity]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				return "", nil
			}
			item := h.unmarshalItemFromSlab(mybucket)
			return item.Value, nil
		} else {
			count++
		}
	}
	return "", nil
}

func (h *Hashmap) Add(key string, value string) {

	item := Item{Key: key, Value: value}
	slabOffset, slabValueLength := h.addSlab(item)
	h.addBucket(key, slabOffset, slabValueLength)
}

func (h *Hashmap) addSlab(item Item) (uint64, uint64) {
	b, err := msgpack.Marshal(&item)
	if err != nil {
		panic(err)
	}

	offset := *h.slabOffset
	//make sure that offset + len(b) is within	h.slabSize
	if offset+uint64(len(b)) > uint64(h.slabSize) {
		err := h.doubleSlab()
		if err != nil {
			panic(err)
		}
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&h.slabMap[offset])), len(b)), b)
	*h.slabOffset += uint64(len(b))
	return offset, uint64((len(b)))
}

func (h *Hashmap) addBucket(key string, slabOffset uint64, slabValueLength uint64) {
	if h.checkResize() {
		h.resize()
	}

	myhash := hash(key)
	mykey := Key{hash: myhash, slabOffset: slabOffset, slabValueLength: slabValueLength}
	h.addKey(mykey)

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
	fmt.Println("new slab size", h.slabSize)
	h.slabMap = m
	return nil
}

func (h *Hashmap) openMmapHash(N uint64) (mmap.MMap, *os.File, error) {
	//make sure you close files!
	var f *os.File
	var err error
	bytes := NtoBytes(N) * 2

	err = os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}
	filename := h.Folder + "/hashkeys-" + fmt.Sprint(N)
	if !doesFileExist(filename) {
		f, err = os.Create(filename)

		if err != nil {
			log.Fatal("2", errors.Wrap(err, 1))
		}
		f.Seek(bytes-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
		f.Close()

	}

	f, err = os.OpenFile(filename, os.O_RDWR, 0655)
	if err != nil {
		log.Fatal("3", errors.Wrap(err, 1))
	}

	ret, err := mmap.Map(f, mmap.RDWR, 0)
	return ret, f, err
}

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.keyMap[size*2]))
	ret := unsafe.Slice(tmpkeys, *h.Capacity)
	return ret

}

func (h *Hashmap) readCapcity() (uint64, int64) {
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

func (h *Hashmap) init(folder string) {
	h.Folder = folder
	N, slabSize := h.readCapcity()
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

	h.keyMap = m
	h.FILE = f_map

	h.slabMap = slab
	h.slabFILE = f_slab
	h.slabSize = slabSize
	h.slabOffset = getSlabOffset(h.slabMap)
	if *h.slabOffset == 0 {
		*h.slabOffset = 8
	}

	h.Capacity = getCapacity(h.keyMap)
	*h.Capacity = N
	h.Count = getCount(h.keyMap)
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
