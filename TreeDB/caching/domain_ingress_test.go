package caching

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func statUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}

func TestDomainIngressSetDeleteStats(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:         1 << 20,
		JournalLanes:           1,
		DomainIngressWorkers:   2,
		DomainIngressQueueSize: 64,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	}()

	const groups = 8
	const keysPerGroup = 64
	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < keysPerGroup; i++ {
				key := []byte(fmt.Sprintf("k-%02d-%03d", g, i))
				val := []byte(fmt.Sprintf("v-%02d-%03d", g, i))
				if err := db.Set(key, val); err != nil {
					t.Errorf("set %q: %v", key, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for g := 0; g < groups; g++ {
		for i := 0; i < keysPerGroup; i++ {
			key := []byte(fmt.Sprintf("k-%02d-%03d", g, i))
			val, err := db.Get(key)
			if err != nil {
				t.Fatalf("get %q: %v", key, err)
			}
			want := fmt.Sprintf("v-%02d-%03d", g, i)
			if string(val) != want {
				t.Fatalf("get %q: got %q want %q", key, string(val), want)
			}
		}
	}

	for g := 0; g < groups; g++ {
		for i := 0; i < keysPerGroup; i += 2 {
			key := []byte(fmt.Sprintf("k-%02d-%03d", g, i))
			if err := db.Delete(key); err != nil {
				t.Fatalf("delete %q: %v", key, err)
			}
		}
	}

	for g := 0; g < groups; g++ {
		for i := 0; i < keysPerGroup; i++ {
			key := []byte(fmt.Sprintf("k-%02d-%03d", g, i))
			val, err := db.Get(key)
			if err != nil {
				t.Fatalf("get after delete %q: %v", key, err)
			}
			if i%2 == 0 {
				if val != nil {
					t.Fatalf("expected key %q deleted, got %q", key, string(val))
				}
				continue
			}
			want := fmt.Sprintf("v-%02d-%03d", g, i)
			if string(val) != want {
				t.Fatalf("get after delete %q: got %q want %q", key, string(val), want)
			}
		}
	}

	stats := db.Stats()
	if got := stats["treedb.cache.domain_ingress.enabled"]; got != "true" {
		t.Fatalf("domain ingress enabled stat = %q", got)
	}
	if got := stats["treedb.cache.domain_ingress.workers"]; got != "2" {
		t.Fatalf("domain ingress workers stat = %q", got)
	}
	enqueued := statUint64(t, stats, "treedb.cache.domain_ingress.enqueued")
	processed := statUint64(t, stats, "treedb.cache.domain_ingress.processed")
	if enqueued == 0 {
		t.Fatalf("expected ingress enqueued > 0")
	}
	if processed < enqueued {
		t.Fatalf("expected processed >= enqueued, got processed=%d enqueued=%d", processed, enqueued)
	}
}

func TestDomainIngressDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	}()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.domain_ingress.enabled"]; got != "false" {
		t.Fatalf("domain ingress enabled stat = %q", got)
	}
}
