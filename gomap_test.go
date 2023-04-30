package gomap

import (
	"bytes"
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

<<<<<<< HEAD
var Ntests int = int(4_000_000)
=======
var Ntests int = int(400_000_0)
>>>>>>> nostrings

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
