package main

import (
	"fmt"
	"log"
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

type Bucket struct {
	data uint64
	hash uint64
}
type Hashmap struct {
	Count    uint64
	Capacity uint64
	Values   []Bucket
}

func hash(data []byte) uint64 {
	h := fnv1.HashBytes64(data)
	if h == 0 {
		panic("Hash cannot be zero")
	}
	return h
}
func (h *Hashmap) add(key []byte, data uint64) {
	myhash := hash(key)
	count := uint64(0)
	for true {
		mybucket := h.Values[myhash%h.Capacity+count]
		if mybucket.hash == 0 || mybucket.hash == myhash {
			h.Values[myhash%h.Capacity] = Bucket{hash: myhash, data: data}
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
		f.Seek(bytes, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)

	} else {
		f, _ = os.OpenFile("./file", os.O_RDWR, 0644)
	}
	defer f.Close()
	return mmap.Map(f, mmap.RDWR, 0)
}

func main() {
	N := uint64(4)
	size := reflect.TypeOf(uint64(0)).Size()

	bytes := int64(size) * int64(2+N*2)
	filename := "./file"

	mmap, _ := openMmap(filename, bytes)
	defer mmap.Unmap()

	var obj *Hashmap
	obj = (*Hashmap)(unsafe.Pointer(&mmap[0]))
	obj.Capacity = N
	obj.Count = 0

	pt := uintptr(unsafe.Pointer(&obj.Values))
	var data []Bucket
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data = pt
	sh.Len = int(obj.Capacity)
	sh.Cap = int(obj.Capacity)
	obj.Values = sh

	obj.add([]byte{'w', 'x', 'r', 'l', 'd'}, 69)
	fmt.Printf("%+v\n\n", obj)
	obj.add([]byte{'w', 'x', 'r', 'l', 'd'}, 71)
	fmt.Printf("%+v\n\n", obj)

	mmap.Flush()

}
