package gomap

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
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

func TestDelete(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)
	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)

	key := []byte("key")
	value := []byte("value")

	// Add
	err = obj.Add(key, value)
	assert.NoError(t, err)

	// Get
	res, err := obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res)

	// Delete
	err = obj.Delete(key)
	assert.NoError(t, err)

	// Get (should be nil)
	res, err = obj.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// Add again (Reuse tombstone? Test implicitly covers logic if no error)
	err = obj.Add(key, value)
	assert.NoError(t, err)
	
	res, err = obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res)
}

func TestCrashRecovery(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	// Phase 1: Populate and Delete
	{
		var obj Hashmap
		err := obj.New(folder)
		assert.NoError(t, err)

		obj.Add([]byte("keep"), []byte("val1"))
		obj.Add([]byte("delete"), []byte("val2"))
		obj.Delete([]byte("delete"))
		
		// "Crash" -> obj goes out of scope, files remain
		// We don't close cleanly to simulate crash (though New doesn't hold locks on files except flock? no flock used)
	}

	// Phase 2: Recover
	{
		var obj Hashmap
		// New loads metadata. 
		// If metadata says Count=1 (correct), we are good.
		// But let's assume metadata is corrupt or we want to force rebuild.
		// We corrupt metadata file? 
		// Or just call Recover explicitly.
		err := obj.New(folder)
		assert.NoError(t, err)
		
		// Verify state before recovery (should be consistent if closed properly/flushed)
		// But we want to test REPLAY.
		
		err = obj.Recover()
		assert.NoError(t, err)
		
		val, err := obj.Get([]byte("keep"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("val1"), val)
		
		val, err = obj.Get([]byte("delete"))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
}

func TestSegmentRotation(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)
	
	// Reduce limit for test
	originalLimit := MaxSegmentSize
	MaxSegmentSize = 1024 // 1KB
	defer func() { MaxSegmentSize = originalLimit }()
	
	var obj Hashmap
	err := obj.New(folder)
	assert.NoError(t, err)
	
	// Write enough to force rotation
	// 1KB limit. 100 items of 20 bytes ~ 2KB.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte("val")
		err := obj.Add(key, val)
		assert.NoError(t, err)
	}
	
	// Check files
	files, err := os.ReadDir(folder)
	assert.NoError(t, err)
	slabFiles := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "slab-") {
			slabFiles++
		}
	}
	assert.Greater(t, slabFiles, 1, "Should have rotated segments")
	
	// Verify reading works (across segments)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val, err := obj.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, []byte("val"), val)
	}
	
	// Verify Recovery with segments
	err = obj.Recover()
	assert.NoError(t, err)
	
	val, err := obj.Get([]byte("key-0"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("val"), val)
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