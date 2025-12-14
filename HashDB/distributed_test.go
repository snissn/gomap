package hashdb

import (
	"bytes"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashDBBasic(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj HashDB
	obj.New(folder)

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("awoiljfasdlfj")
	err := obj.Put(key, value)
	assert.Nil(t, err, "Error should be nil")
}

func TestHashDBPutGet1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj HashDB
	obj.New(folder)

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")

	err := obj.Put(key, value)
	assert.Nil(t, err, "Error should be nil")

	res, err := obj.Get(key)
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, value, res, "they should be equal")
}

func TestHashDBPutGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj HashDB
	obj.New(folder)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := key

		err := obj.Put(key, value)
		assert.Nil(t, err, "Error should be nil")

		res, err := obj.Get(key)
		assert.Nil(t, err, "Error should be nil")
		assert.Equal(t, res, value, "they should be equal")
	}
}
func TestHashDBPutGetNAsync(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj HashDB
	obj.New(folder)

	var wg sync.WaitGroup // create a WaitGroup

	for i := 0; i < Ntests; i++ {
		wg.Add(1)        // increment the WaitGroup counter
		go func(i int) { // capture loop variable
			defer wg.Done() // defer the Done call

			key := []byte(strconv.Itoa(i))
			value := key

			err := obj.Put(key, value)

			assert.Nil(t, err, "Error should be nil")
		}(i) // pass loop variable as argument
	}

	wg.Wait() // wait for all above goroutines to finish

	for i := 0; i < 10; i++ {
		wg.Add(1)        // increment the WaitGroup counter
		go func(i int) { // capture loop variable
			defer wg.Done() // defer the Done call

			key := []byte(strconv.Itoa(i))
			value := key

			res, err := obj.Get(key)

			assert.Nil(t, err, "Error should be nil")
			assert.Equal(t, res, value, "they should be equal")
		}(i) // pass loop variable as argument
	}

	wg.Wait() // wait for all above goroutines to finish
}

func BenchmarkHashDBValue(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj HashDB
	obj.New(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i))
		value := key

		err := obj.Put(key, value)
		assert.Nil(b, err, "Error should be nil")
	}
}

func BenchmarkHashDBPutMany(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj HashDB
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
		obj.PutMany(items)
	}
}
