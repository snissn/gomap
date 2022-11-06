package gomap

import (
	"fmt"
	"strconv"

	"github.com/go-errors/errors"

	"log"
	"math/big"
	"os"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/segmentio/fasthash/fnv1"
)

func doesFileExist(fileName string) bool {
	_, error := os.Stat(fileName)
	// check if error is "file not exists"
	if os.IsNotExist(error) {
		return false
	} else {
		return true
	}
}

type Key struct {
	data uint64
	hash uint64
}
type Hashmap struct {
	Folder   string
	FILE     *os.File
	keyMap   mmap.MMap
  slabMap  mmap.MMap
  slabFILE *os.File
  slabSize int64
	Capacity *uint64
	Count    *uint64
	Keys     *[]Key
}

var LOADFACTOR *big.Float = big.NewFloat(0.7)
var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(8)
var DEFAULTSLABSIZE int64 = int64(8*1024)

func hash(data []byte) uint64 {
	h := fnv1.HashBytes64(data)
	if h == 0 {
		//hash = 0 means it's empty
		h++
	}
	if h == 18446744073709551615 {
		h--
		// MaxInt means it's been deleted
	}
	return h
}

func (h *Hashmap) checkResize() bool {
	return *h.Count*14 > *h.Capacity*10
}

func (h *Hashmap) _checkResize() bool {
	fcap := new(big.Float).SetUint64(*h.Capacity)
	fcount := new(big.Float).SetUint64(*h.Count)
	quo := new(big.Float).Quo(fcount, fcap) // our current load factor
	return quo.Cmp(LOADFACTOR) != -1        // return true if we need to resize https://pkg.go.dev/math/big#Float.Cmp
	//returns -1 if quo is less than load factor
}

func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func (h *Hashmap) closeFPs() {
	err := h.FILE.Close()
	handleError(err)
	err = h.keyMap.Unmap()
	handleError(err)

}
func (h *Hashmap) replaceHashmap(newH Hashmap) {
	//todo maybe make a copy to close easier?
	//go h.closeFPs() // async close fp
	//	fmt.Println("Hashmap resize", h)
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

func (h *Hashmap) get(key []byte) (*Key, error) {
	myhash := hash(key)
	count := uint64(0)
	for count != *h.Capacity {
		mybucket := (*h.Keys)[((myhash%*h.Capacity)+count)%*h.Capacity]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				return nil, nil
			}
			return &mybucket, nil
		} else {
			count++
		}
	}
	return nil, nil
}

func (h *Hashmap) add(key []byte, data uint64) {
	if h.checkResize() {
		h.resize()
	}

	myhash := hash(key)
	mykey := Key{hash: myhash, data: data}
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

func NtoBytes(N uint64) int64 {
	return int64(size*2) * int64(2+N*2)
}

func getCapacity(keyMap mmap.MMap) *uint64 {
	cap := (*uint64)(unsafe.Pointer(&keyMap[0]))
	if *cap == 0 {
		*cap = uint64(DEFAULTMAPSIZE)
	}
	return cap
}

func getCount(keyMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&keyMap[size]))
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
  h.slabFILE =  f_slab
  fmt.Println("slabsize",slabSize)
  h.slabSize = slabSize

	h.Capacity = getCapacity(h.keyMap)
	*h.Capacity = N
	h.Count = getCount(h.keyMap)
	keys := h.getKeys()
	h.Keys = &keys
}

func main() {

	folder := "./folder"

	var obj Hashmap
	obj.init(folder)

	/*
		obj = (*Hashmap)(unsafe.Pointer(&mmap[0]))
		obj.Capacity = N
		obj.Count = 0
	*/

	obj.add([]byte{'w', 'x', 'r', 'l', 'q'}, 69)
	obj.add([]byte{'w', 'x', 'r', 'l', 'b'}, 69)
	obj.add([]byte{'w', 'x', 'r', 'l', 'e'}, 69)
	obj.add([]byte{'w', 'x', 'r', 'l', 'c'}, 69)
	obj.add([]byte{'w', 'x', 'r', 'l', 'k'}, 71)

	//	fmt.Printf("%+v\n\n", obj)
}
