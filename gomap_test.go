package gomap

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"

	"testing"

	"github.com/stretchr/testify/assert"
)

func f() {
	var a []int
	for i := 0; i < 100; i++ {
		_ = append(a, i*3)
	}
}

var Ntests int = int(8_000_00)

func TestBasic(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
}

func TestAdd1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap

	obj.New(folder)

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("awoiljfasdlfj")
	obj.Add(key, value)
}

func TestAddGet1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")
	obj.Add(key, value)
	res, _ := obj.Get(key)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddResizeGet(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")
	obj.Add(key, value)
	obj.resize()
	key = []byte{'w', 'x', 'r', 'l', 'x'}
	obj.Add(key, value)
	res, _ := obj.Get(key)
	assert.Equal(t, value, res, "they should be equal")
	obj.resize()
	obj.resize()
	obj.resize()
	obj.resize()
	obj.resize()

	res, _ = obj.Get(key)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	fmt.Println(folder)

	var obj Hashmap
	obj.New(folder)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		obj.Add(key, value)
		res, _ := obj.Get(key)
		if !bytes.Equal(res, value) {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func TestAddGetN_bigt(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	fmt.Println(folder)

	var obj Hashmap
	obj.New(folder)
	randomBytes := make([]byte, 1024)
	rand.Read(randomBytes)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := randomBytes
		obj.Add(key, value)
		res, _ := obj.Get(key)
		if !bytes.Equal(res, value) {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func TestAddGetN_bigt_batch(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	fmt.Println(folder)

	var obj Hashmap
	obj.New(folder)
	randomBytes := make([]byte, 1024)
	rand.Read(randomBytes)

	items := []Item{}

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := randomBytes
		item := Item{Key: key, Value: value}
		items = append(items, item)
		if len(items) > 100000 {
			obj.AddMany(items)
			items = []Item{}
		}
	}
	obj.AddMany(items)
	//for i := 0; i < Ntests; i++ {
	//key := []byte(strconv.Itoa(i))
	//value := randomBytes
	//res, _ := obj.Get(key)
	//if !bytes.Equal(res, value) {
	//assert.Equal(t, res, value, "they should be equal")
	//}
	//}

}

func BenchmarkValue(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.New(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		obj.Add(key, value)
	}
}

func BenchmarkGoDefaultHashmap(b *testing.B) {
	hashMap := make(map[string][]byte)

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		value := []byte(key)
		hashMap[key] = value
	}
}

func TestAddValue(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	var obj Hashmap
	obj.New(folder)
	key := []byte("key")
	value := []byte("bartesttesttest")
	obj.Add(key, value)
}

func BenchmarkAddManySlabs(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.New(folder)
	N := 100
	items := make([]Item, N)
	for i := 0; i < N; i++ {
		key := []byte(strconv.Itoa(i))
		value := bytes.Repeat([]byte{'a'}, 1024)
		items[i] = Item{Key: key, Value: value}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.addManySlabs(items)
	}
}
