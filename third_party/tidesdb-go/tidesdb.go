package tidesdb

import (
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

type Pair struct {
	Key   []byte
	Value []byte
}

type DB struct {
	mu   sync.RWMutex
	dir  string
	data map[string][]byte
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d := &DB{dir: dir, data: map[string][]byte{}}
	f, err := os.Open(filepath.Join(dir, "tidesdb.gob"))
	if err == nil {
		defer f.Close()
		_ = gob.NewDecoder(f).Decode(&d.data)
	}
	return d, nil
}

func (d *DB) Close() error {
	d.mu.RLock()
	cp := make(map[string][]byte, len(d.data))
	for k, v := range d.data {
		vv := make([]byte, len(v)); copy(vv, v); cp[k] = vv
	}
	d.mu.RUnlock()
	f, err := os.Create(filepath.Join(d.dir, "tidesdb.gob"))
	if err != nil { return err }
	defer f.Close()
	return gob.NewEncoder(f).Encode(cp)
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock(); defer d.mu.RUnlock()
	v, ok := d.data[string(key)]
	if !ok { return nil, ErrKeyNotFound }
	out := make([]byte, len(v)); copy(out, v)
	return out, nil
}

func (d *DB) Put(key, value []byte) error {
	d.mu.Lock(); defer d.mu.Unlock()
	v := make([]byte, len(value)); copy(v, value)
	d.data[string(key)] = v
	return nil
}

func (d *DB) Delete(key []byte) error {
	d.mu.Lock(); defer d.mu.Unlock()
	if _, ok := d.data[string(key)]; !ok { return ErrKeyNotFound }
	delete(d.data, string(key)); return nil
}

func (d *DB) Scan(start, end []byte) ([]Pair, error) {
	d.mu.RLock(); defer d.mu.RUnlock()
	keys := make([]string, 0, len(d.data))
	for k := range d.data { keys = append(keys, k) }
	sort.Strings(keys)
	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		kb := []byte(k)
		if start != nil && string(kb) < string(start) { continue }
		if end != nil && string(kb) >= string(end) { continue }
		v := d.data[k]
		kc := make([]byte, len(kb)); copy(kc, kb)
		vc := make([]byte, len(v)); copy(vc, v)
		pairs = append(pairs, Pair{Key: kc, Value: vc})
	}
	return pairs, nil
}
