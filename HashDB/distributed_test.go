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
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(folder)

	var obj HashDB
	if err := obj.NewWithShards(folder, 4); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("awoiljfasdlfj")
	if err := obj.Put(key, value); err != nil {
		t.Fatalf("put: %v", err)
	}
}

func TestHashDBPutGet1(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(folder)

	var obj HashDB
	if err := obj.NewWithShards(folder, 4); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")

	if err := obj.Put(key, value); err != nil {
		t.Fatalf("put: %v", err)
	}

	res, err := obj.Get(key)
	assert.Nil(t, err, "Error should be nil")
	assert.Equal(t, value, res, "they should be equal")
}

func TestHashDBPutGetN(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(folder)

	var obj HashDB
	if err := obj.NewWithShards(folder, 4); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

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
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(folder)

	var obj HashDB
	if err := obj.NewWithShards(folder, 4); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	var wg sync.WaitGroup // create a WaitGroup
	errCh := make(chan error, Ntests)

	for i := 0; i < Ntests; i++ {
		wg.Add(1)        // increment the WaitGroup counter
		go func(i int) { // capture loop variable
			defer wg.Done() // defer the Done call

			key := []byte(strconv.Itoa(i))
			value := key

			if err := obj.Put(key, value); err != nil {
				errCh <- err
			}
		}(i) // pass loop variable as argument
	}

	wg.Wait() // wait for all above goroutines to finish
	close(errCh)
	for err := range errCh {
		t.Fatalf("put: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		res, err := obj.Get(key)
		assert.Nil(t, err, "Error should be nil")
		assert.Equal(t, res, value, "they should be equal")
	}
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
