package main

import (
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

var Ntests int = 10

func TestBasic(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
}
func TestCustom(t *testing.T) {
	folder := "/var/folders/mg/bc5vt66d4dx_c88kt3028dc80000gn/T/hash3180176573/"
	var obj Hashmap
	obj.init(folder)

	fmt.Println(obj.get([]byte("4111")))
	obj.add([]byte("4111"), 69)
	fmt.Println(obj.get([]byte("4111")))
}

func TestAdd1(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	obj.add(key, 69)
}

func TestAddGet1(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
	key := []byte{'w', 'x', 'r', 'l', 'q'}
	obj.add(key, 69)
	res, _ := obj.get(key)
	assert.Equal(t, res.hash, hash(key), "they should be equal")
}

func TestAddGetN(t *testing.T) {
	folder, _ := os.MkdirTemp("", "hash")

	var obj Hashmap
	obj.init(folder)

	for i := 0; i < Ntests; i++ {
		key := []byte(strconv.Itoa(i))
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
		s := strconv.Itoa(i)
		key := []byte(s)
		obj.add(key, 69)

	}
}
