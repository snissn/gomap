package gomap

import (
	"fmt"
	"os"
	"strconv"

	"testing"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/stretchr/testify/assert"
)

func f() {
	var a []int
	for i := 0; i < 100; i++ {
		_ = append(a, i*3)
	}
}

var Ntests int = 10

func TestBasic(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
}

func TestAdd1(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
	key := string([]byte{'w', 'x', 'r', 'l', 'q'})
	obj.add(key, 69)
}

func TestAddGet1(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
	key := string([]byte{'w', 'x', 'r', 'l', 'q'})
	obj.add(key, 69)
	res, _ := obj.get(key)
	assert.Equal(t, res.hash, hash(key), "they should be equal")
}

func TestAddGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.init(folder)

	for i := 0; i < Ntests; i++ {
		key := strconv.Itoa(i)
		obj.add(key, uint64(i))
		res, _ := obj.get(key)
		assert.Equal(t, res.hash, hash(key), "they should be equal")

		//	t.Errorf("Find(%v, %d) = %d, expected %d",				e.a, e.x, res, e.exp)

	}
}

func BenchmarkF(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.init(folder)
	fmt.Println(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		obj.add(key, 69)

	}
}

func BenchmarkValue(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.init(folder)
	fmt.Println(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		item := Item{Value: key, Key: key}
		obj.AddValue(key, item)

	}
}

func TestAddValue(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.init(folder)
	fmt.Println(folder)
	value := Item{Key: "key", Value: "bartesttest"}
	obj.AddValue("foo", value)

}

func TestMsgPack(t *testing.T) {

	key := "keyxyz"
	value := "valuezyx"
	item := Item{Key: key, Value: value}

	b, err := msgpack.Marshal(&item)
	if err != nil {
		panic(err)
	}
	var ret Item
	err = msgpack.Unmarshal(b, &ret)
	if err != nil {
		panic(err)
	}
	fmt.Println("final", ret.Value, ret.Key)

}

func TestAddValue1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	fmt.Println("Folder", folder)
	var obj Hashmap
	obj.init(folder)
	key := "keyxyz"
	value := "valuezyx"
	item := Item{Key: key, Value: value}
	obj.AddValue(key, item)
	x, _ := obj.get(key)
	fmt.Println("data", x.data)
	var ret Item
	//	TODO := obj.slabSize
	ptr := unsafe.Slice((*byte)(unsafe.Pointer(&obj.slabMap[x.data])), 27)
	fmt.Println("str should be", string(ptr))
	err := msgpack.Unmarshal(ptr, &ret)
	if err != nil {
		panic(err)
	}
	fmt.Println("final", ret.Value, ret.Key)

}
