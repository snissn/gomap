package gomap

import (
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDistributedHashmapBasic(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj HashmapDistributed
	obj.New(folder)

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("awoiljfasdlfj")
	err := obj.Add(key, value)
	assert.Nil(t, err, "Error should be nil")
}

func TestDistributedHashmapAddGet1(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj HashmapDistributed
	obj.New(folder)

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")

	err := obj.Add(key, value)
	assert.Nil(t, err, "Error should be nil")

	res, err := obj.Get(key)
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, value, res, "they should be equal")
}

func TestDistributedHashmapAddGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj HashmapDistributed
	obj.New(folder)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := key

		err := obj.Add(key, value)
		assert.Nil(t, err, "Error should be nil")

		res, err := obj.Get(key)
		assert.Nil(t, err, "Error should be nil")
		assert.Equal(t, res, value, "they should be equal")
	}
}
func TestDistributedHashmapAddGetNAsync(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj HashmapDistributed
	obj.New(folder)

	var wg sync.WaitGroup // create a WaitGroup

	for i := 0; i < Ntests; i++ {
		wg.Add(1)        // increment the WaitGroup counter
		go func(i int) { // capture loop variable
			defer wg.Done() // defer the Done call

			key := []byte(strconv.Itoa(i))
			value := key

			err := obj.Add(key, value)

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

func BenchmarkDistributedHashmapValue(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj HashmapDistributed
	obj.New(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i))
		value := key

		err := obj.Add(key, value)
		assert.Nil(b, err, "Error should be nil")
	}
}
