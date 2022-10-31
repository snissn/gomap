package main

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
	Capacity *uint64
	Count    *uint64
	Keys     []Key
}

var LOADFACTOR *big.Float = big.NewFloat(0.7)
var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(2)

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
	fcap := new(big.Float).SetUint64(*h.Capacity)
	fcount := new(big.Float).SetUint64(*h.Count)
	quo := new(big.Float).Quo(fcount, fcap) // our current load factor
	return quo.Cmp(LOADFACTOR) != -1        // return true if we need to resize https://pkg.go.dev/math/big#Float.Cmp
	//returns -1 if quo is less than load factor
}

func (h *Hashmap) addResize(key Key) {
	fmt.Println("resize", key)
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
	go h.closeFPs() // async close fp
	h.keyMap = newH.keyMap
	h.FILE = newH.FILE
	h.Capacity = newH.Capacity
	h.Count = newH.Count
	h.Keys = newH.Keys
	fmt.Println("resze")
}
func (h *Hashmap) resize() {

	var newH Hashmap
	newH.initN(h.Folder, 2*(*h.Capacity))
	for index, mykey := range h.Keys {
		fmt.Println("resze At index", index, "value is", mykey)
		if mykey.hash != 0 {
			newH.addKey(mykey)
		}
	}

	h.replaceHashmap(newH)
}

func (h *Hashmap) addKey(key Key) {
	count := uint64(0)
	myhash := key.hash
	for true {
		mybucket := h.Keys[((myhash%*h.Capacity)+count)%*h.Capacity]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				*h.Count += 1
			}
			h.Keys[myhash%*h.Capacity] = key
			break
		} else {
			count++
		}
	}
}

func (h *Hashmap) get(key []byte) (uint64, error) {
	myhash := hash(key)
	count := uint64(0)
	for count != *h.Capacity {
		mybucket := h.Keys[((myhash%*h.Capacity)+count)%*h.Capacity]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				return 0, nil
			}
			return mybucket.hash, nil
		} else {
			count++
		}
	}
	return 0, nil
}

func (h *Hashmap) add(key []byte, data uint64) {
	//fmt.Println("add - capacity count is: ", *h.Capacity, *h.Count)
	if h.checkResize() {
		h.resize()
	}

	myhash := hash(key)
	mykey := Key{hash: myhash, data: data}
	h.addKey(mykey)
	fmt.Println("add", myhash)

}

func (h *Hashmap) openMmap(N uint64) (mmap.MMap, *os.File, error) {
	//make sure you close files!
	var f *os.File
	var err error
	bytes := NtoBytes(N)

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

	} else {
		f, err = os.OpenFile(filename, os.O_RDWR, 0655)
		if err != nil {
			fmt.Println(filename)
			log.Fatal("3", errors.Wrap(err, 1))
		}

	}

	ret, err := mmap.Map(f, mmap.RDWR, 0)
	return ret, f, err
}

func NtoBytes(N uint64) int64 {
	return int64(size) * int64(2+N*2)
}

func getCapacity(keyMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&keyMap[0]))
}

func getCount(keyMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&keyMap[size]))
}

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.keyMap[size*2]))
	return unsafe.Slice(tmpkeys, *h.Capacity)
}

func (h *Hashmap) readCapcity() uint64 {
	dat, err := os.ReadFile(h.Folder + "/capacity")
	if err != nil {
		return DEFAULTMAPSIZE
	}
	fmt.Print(string(dat))

	ret, err := strconv.ParseUint(string(dat), 10, 64)
	handleError(err)
	return ret

}

func (h *Hashmap) init(folder string) {
	h.Folder = folder
	N := h.readCapcity()
	h.initN(folder, N)
}
func (h *Hashmap) initN(folder string, N uint64) {
	h.Folder = folder
	m, f, err := h.openMmap(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	h.keyMap = m
	h.FILE = f

	h.Capacity = getCapacity(h.keyMap)
	h.Count = getCount(h.keyMap)
	h.Keys = h.getKeys()
	if *h.Capacity == 0 {
		*h.Capacity = N
	}

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
