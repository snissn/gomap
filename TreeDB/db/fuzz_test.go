package db

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/snissn/gomap/TreeDB/page"
)

// Op represents a database operation
type Op struct {
	Type  int // 0=Set, 1=Delete, 2=Get
	Key   string
	Value []byte
}

func (Op) Generate(rand *rand.Rand, size int) reflect.Value {
	op := Op{
		Type: rand.Intn(3),
		Key:  fmt.Sprintf("key-%d", rand.Intn(100)), // Limit key space to trigger collisions
	}
	if op.Type == 0 { // Set
		valLen := rand.Intn(page.DefaultInlineThreshold + 1)
		op.Value = make([]byte, valLen)
		rand.Read(op.Value)
	}
	return reflect.ValueOf(op)
}

func TestFuzzModel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping fuzz test in short mode")
	}

	f := func(ops []Op) bool {
		dir := t.TempDir()
		db, err := Open(Options{Dir: dir})
		if err != nil {
			t.Logf("Open failed: %v", err)
			return false
		}
		defer db.Close()

		model := make(map[string][]byte)

		for _, op := range ops {
			key := []byte(op.Key)

			switch op.Type {
			case 0: // Set
				err := db.Set(key, op.Value)
				if err != nil {
					t.Logf("Set failed: %v", err)
					return false
				}
				model[op.Key] = op.Value

			case 1: // Delete
				err := db.Delete(key)
				if err != nil {
					t.Logf("Delete failed: %v", err)
					return false
				}
				delete(model, op.Key)

			case 2: // Get
				val, err := db.Get(key)
				expected, exists := model[op.Key]

				if exists {
					if err != nil {
						t.Logf("Get failed for existing key %s: %v", op.Key, err)
						return false
					}
					if !bytes.Equal(val, expected) {
						t.Logf("Get mismatch for key %s. Want len %d, got len %d", op.Key, len(expected), len(val))
						return false
					}
				} else {
					// Expect nil, nil
					if err != nil {
						t.Logf("Get returned error for missing key %s: %v", op.Key, err)
						return false
					}
					if val != nil {
						t.Logf("Get found value for missing key %s", op.Key)
						return false
					}
				}
			}
		}

		// Final Consistency Check via Iterator
		it, err := db.Iterator(nil, nil)
		if err != nil {
			t.Logf("Iterator failed: %v", err)
			return false
		}
		defer it.Close()

		var dbKeys []string
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			v := it.Value()
			dbKeys = append(dbKeys, k)

			expected, ok := model[k]
			if !ok {
				t.Logf("Iterator found phantom key: %s", k)
				return false
			}
			if !bytes.Equal(v, expected) {
				t.Logf("Iterator value mismatch for key: %s", k)
				return false
			}
		}

		if len(dbKeys) != len(model) {
			t.Logf("Count mismatch. DB: %d, Model: %d", len(dbKeys), len(model))
			return false
		}

		// Verify all model keys were found
		sort.Strings(dbKeys)
		// Model keys
		var modelKeys []string
		for k := range model {
			modelKeys = append(modelKeys, k)
		}
		sort.Strings(modelKeys)

		for i, k := range modelKeys {
			if dbKeys[i] != k {
				t.Logf("Key order mismatch at %d: %s vs %s", i, dbKeys[i], k)
				return false
			}
		}

		return true
	}

	config := &quick.Config{
		MaxCount: 50,
	}
	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}
