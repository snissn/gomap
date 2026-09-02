package hashdb

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

var Ntests int = int(1_000)

func TestBasic(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)
	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })
}

func TestAdd1(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)
	var obj DB

	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("awoiljfasdlfj")
	err = obj.Put(key, value)
	assert.NoError(t, err)
}

func TestAddGet1(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)
	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")
	err = obj.Put(key, value)
	assert.NoError(t, err)
	res, err := obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res, "they should be equal")
}

func TestAddResizeGet(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)
	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	value := []byte("value")
	err = obj.Put(key, value)
	assert.NoError(t, err)
	obj.resize()
	key = []byte{'w', 'x', 'r', 'l', 'x'}
	err = obj.Put(key, value)
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
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)

	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		err = obj.Put(key, value)
		assert.NoError(t, err)
		res, err := obj.Get(key)
		assert.NoError(t, err)
		if !bytes.Equal(res, value) {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func TestAddGetN_bigt(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)

	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })
	randomBytes := make([]byte, 1024)
	rand.Read(randomBytes)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := randomBytes
		err = obj.Put(key, value)
		assert.NoError(t, err)
		res, err := obj.Get(key)
		assert.NoError(t, err)
		if !bytes.Equal(res, value) {
			assert.Equal(t, res, value, "they should be equal")
		}
	}

}

func TestAddGetN_bigt_batch(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)

	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })
	randomBytes := make([]byte, 1024)
	rand.Read(randomBytes)

	items := []Item{}

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
		value := randomBytes
		item := Item{Key: key, Value: value}
		items = append(items, item)
		if len(items) > 100000 {
			err = obj.PutMany(items)
			assert.NoError(t, err)
			items = []Item{}
		}
	}
	err = obj.PutMany(items)
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

	var obj DB
	obj.Open(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		obj.Put(key, value)
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
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)
	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })
	key := []byte("key")
	value := []byte("bartesttesttest")
	err = obj.Put(key, value)
	assert.NoError(t, err)
}

func TestDelete(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)
	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	key := []byte("key")
	value := []byte("value")

	// Add
	err = obj.Put(key, value)
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
	err = obj.Put(key, value)
	assert.NoError(t, err)

	res, err = obj.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, res)
}

func TestCrashRecovery(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)

	runCrashRecoveryWriter(t, folder)

	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	if err := obj.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	val, err := obj.Get([]byte("keep"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)

	val, err = obj.Get([]byte("delete"))
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestSegmentRotation(t *testing.T) {
	folder, err := os.MkdirTemp("", "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)

	// Reduce limit for test
	originalLimit := atomic.LoadInt64(&MaxSegmentSize)
	atomic.StoreInt64(&MaxSegmentSize, 1024) // 1KB
	defer atomic.StoreInt64(&MaxSegmentSize, originalLimit)

	var obj DB
	if err := obj.Open(folder); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = obj.Close() })

	// Write enough to force rotation
	// 1KB limit. 100 items of 20 bytes ~ 2KB.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte("val")
		err := obj.Put(key, val)
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

func runCrashRecoveryWriter(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperCrashRecoveryWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"HASHDB_CRASH_HELPER=1",
		"HASHDB_CRASH_DIR="+dir,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash writer helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperCrashRecoveryWriter(t *testing.T) {
	if os.Getenv("HASHDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("HASHDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing HASHDB_CRASH_DIR")
	}

	var obj DB
	if err := obj.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}

	_ = obj.Put([]byte("keep"), []byte("val1"))
	_ = obj.Put([]byte("delete"), []byte("val2"))
	_ = obj.Delete([]byte("delete"))

	// Simulate a crash by exiting without calling Close() (no defers run, but OS releases locks).
	os.Exit(0)
}

func BenchmarkAddManySlabs(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	defer os.RemoveAll(folder)

	var obj DB
	obj.Open(folder)
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

	var obj DB
	obj.Open(folder)
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
