package gomap

import (
	"bytes"
	"crypto/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var Ntests int = int(1_000)

func TestBasic(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)
	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
}

func TestAdd1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)
	var obj Hashmap

	err := obj.New(folder)
	assert.NoError(t, err)

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("awoiljfasdlfj")
	err = obj.Add(key, value)
	assert.NoError(t, err)
}

func TestAddGet1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)
	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")
	err = obj.Add(key, value)
	assert.NoError(t, err)
	res, err := obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddResizeGet(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)
	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")
	err = obj.Add(key, value)
	assert.NoError(t, err)
	obj.resize()
	key = []byte{'w', 'x', 'r', 'l', 'x'}
	err = obj.Add(key, value)
	assert.NoError(t, err)
	res, err := obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res, "they should be equal")
	obj.resize()
	obj.resize()
	obj.resize()
	obj.resize()
	obj.resize()

	res, err = obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		err = obj.Add(key, value)
		assert.NoError(t, err)
		res, err := obj.Get(key)
		assert.NoError(t, err)
		if !bytes.Equal(res, value) {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func TestAddGetN_bigt(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
	randomBytes := make([]byte, 1024)
	rand.Read(randomBytes)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := randomBytes
		err = obj.Add(key, value)
		assert.NoError(t, err)
		res, err := obj.Get(key)
		assert.NoError(t, err)
		if !bytes.Equal(res, value) {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func TestAddGetN_bigt_batch(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
	randomBytes := make([]byte, 1024)
	rand.Read(randomBytes)

	items := []Item{}

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := randomBytes
		item := Item{Key: key, Value: value}
		items = append(items, item)
		if len(items) > 100000 {
			err = obj.AddMany(items)
			assert.NoError(t, err)
			items = []Item{}
		}
	}
	err = obj.AddMany(items)
	assert.NoError(t, err)
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
	defer os.RemoveAll(folder)

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
	defer os.RemoveAll(folder)
	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
	key := []byte("key")
	value := []byte("bartesttesttest")
	err = obj.Add(key, value)
	assert.NoError(t, err)
}

func BenchmarkAddManySlabs(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

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

func BenchmarkAddMany(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj Hashmap
	obj.New(folder)
	N := 1000
	items := make([]Item, N)
	for i := 0; i < N; i++ {
		key := []byte(strconv.Itoa(i))
		value := bytes.Repeat([]byte{'a'}, 1024)
		items[i] = Item{Key: key, Value: value}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.AddMany(items)
	}
}