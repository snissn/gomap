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

var N int = 10

func TestBasic(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)
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
	assert.Equal(t, res, hash(key), "they should be equal")
	fmt.Println(res)
}

func TestAddGetN(t *testing.T) {
	folder := os.TempDir()
	var obj Hashmap
	obj.init(folder)

	for i := 0; i < N; i++ {
		key := []byte(strconv.Itoa(i))
		obj.add(key, uint64(i))
		res, _ := obj.get(key)
		assert.Equal(t, res, hash(key), "they should be equal")

		//	t.Errorf("Find(%v, %d) = %d, expected %d",				e.a, e.x, res, e.exp)

	}
}

func BenchmarkF(b *testing.B) {
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)

		b := []byte(s)
		obj.add(b, 69)

	}
}
