package main

import (
	"fmt"
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
	Filename string
	FILE     *os.File
	keyMap   mmap.MMap
	Capacity *uint64
	Count    *uint64
	Keys     []Key
}

var LOADFACTOR *big.Float = big.NewFloat(0.7)

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

func (h *Hashmap) resize() {
	// TODO resize
}

func (h *Hashmap) add(key []byte, data uint64) {
	if h.checkResize() {
		h.resize()
	}

	myhash := hash(key)
	count := uint64(0)
	for true {
		mybucket := h.Keys[myhash%*h.Capacity+count]
		fmt.Println("hash", mybucket.hash, myhash)
		if mybucket.hash == 0 || mybucket.hash == myhash {
			if mybucket.hash == 0 {
				*h.Count += 1
			}
			h.Keys[myhash%*h.Capacity] = Key{hash: myhash, data: data}
			break
		} else {
			count++
		}
	}
}

func openMmap(filename string, bytes int64) (mmap.MMap, error) {

	var f *os.File
	var err error
	if !doesFileExist(filename) {
		f, err = os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		f.Seek(bytes-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)

	} else {
		f, _ = os.OpenFile("./file", os.O_RDWR, 0644)
	}
	defer f.Close()
	return mmap.Map(f, mmap.RDWR, 0)
}

func main() {

	filename := "./file"

	var obj Hashmap
	obj.Filename = filename

	N := uint64(2)
	size := reflect.TypeOf(uint64(0)).Size() // 8 bytes

	bytes := int64(size) * int64(2+N*2)
	fmt.Println("byts", bytes)

	mmap, _ := openMmap(filename, bytes)
	defer mmap.Unmap()
	/*
		obj = (*Hashmap)(unsafe.Pointer(&mmap[0]))
		obj.Capacity = N
		obj.Count = 0
	*/
	obj.Capacity = (*uint64)(unsafe.Pointer(&mmap[0]))
	obj.Count = (*uint64)(unsafe.Pointer(&mmap[size]))

	*obj.Capacity = N

	tmpkeys := (*Key)(unsafe.Pointer(&mmap[size*2]))
	obj.Keys = unsafe.Slice(tmpkeys, *obj.Capacity)
	fmt.Printf("%+v\n\n", obj)

	obj.add([]byte{'w', 'x', 'r', 'l', 'd'}, 69)
	fmt.Printf("%+v\n\n", obj)
	obj.add([]byte{'w', 'x', 'r', 'l', 'k'}, 71)
	fmt.Printf("%+v\n\n", obj)

	fmt.Printf("%+v\n\n", obj.Keys[0])
	fmt.Printf("%+v\n\n", obj.Keys[1])

	fmt.Printf("%+v\n\n", obj)

	mmap.Flush()

}
