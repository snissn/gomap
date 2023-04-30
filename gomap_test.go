package gomap

import (
	"fmt"
	"os"
	"strconv"

	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/stretchr/testify/assert"
)

func f() {
	var a []int
	for i := 0; i < 100; i++ {
		_ = append(a, i*3)
	}
}

var Ntests int = int(400_000_0)

func TestBasic(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
}

func TestAdd1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap

	obj.New(folder)

	key := string([]byte{'w', 'x', 'r', 'l', 'q'})
	value := "awoiljfasdlfj"
	obj.Add(key, value)
}

func TestAddGet1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
	key := string([]byte{'w', 'x', 'r', 'l', 'q'})
	value := "value"
	obj.Add(key, value)
	res, _ := obj.Get(key)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddResizeGet(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
	key := string([]byte{'w', 'x', 'r', 'l', 'q'})
	value := "value"
	obj.Add(key, value)
	obj.resize()
	key = string([]byte{'w', 'x', 'r', 'l', 'x'})
	obj.Add(key, value)
	res, _ := obj.Get(key)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	fmt.Println(folder)

	var obj Hashmap
	obj.New(folder)

	for i := 0; i < Ntests; i++ {
		key := strconv.Itoa(i)
		value := key
		obj.Add(key, value)
		res, _ := obj.Get(key)
		if res != value {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func BenchmarkValue(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.New(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		value := key
		obj.Add(key, value)
	}
}

func TestAddValue(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
	key := "key"
	value := "bartesttesttest"
	obj.Add(key, value)
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

}
