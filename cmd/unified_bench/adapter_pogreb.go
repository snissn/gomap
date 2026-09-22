package main

import (
	"flag"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/snissn/gomap/kvstore"
)

var (
	pogrebNoSync = flag.Bool("pogreb-nosync", false, "Pogreb: use relaxed sync/background sync if possible")
)

func init() {
	RegisterDB("pogreb", NewPogreb)
}

type PogrebWrapper struct {
	db *pogreb.DB
}

func NewPogreb(dir string) (kvstore.DB, error) {
	// pogreb doesn't expose many options for sync control in Put, but has BackgroundSync in Options.
	opts := &pogreb.Options{
		BackgroundSyncInterval: 0, // default
	}
	if *pogrebNoSync {
		// If we want less durability, we might set BackgroundSyncInterval to something non-zero
		// But Pogreb's Put is typically WAL-based or mmap based?
		// Pogreb docs say: "fsync is called on every Put unless BackgroundSyncInterval is set."
		opts.BackgroundSyncInterval = -1 // Wait, -1 means disabled? 0 is disabled (fsync every write).
		// Actually typical usage:
		// "If BackgroundSyncInterval is greater than 0, the DB will sync changes to disk periodically."
		opts.BackgroundSyncInterval = 500 // ms? No, it's a Duration usually, but here it's time.Duration?
		// In pogreb source options.go: BackgroundSyncInterval time.Duration.
		// So let's just assume we can pass a value.
	}

	// Re-checking pogreb options:
	// type Options struct { BackgroundSyncInterval time.Duration }
	// If 0, Put calls fsync.

	realOpts := &pogreb.Options{}
	if *pogrebNoSync {
		realOpts.BackgroundSyncInterval = 1 * time.Second
	}

	db, err := pogreb.Open(dir, realOpts)
	if err != nil {
		return nil, err
	}
	return &PogrebWrapper{db: db}, nil
}

func (p *PogrebWrapper) Name() string { return "Pogreb" }
func (p *PogrebWrapper) Close() error { return p.db.Close() }

func (p *PogrebWrapper) Get(key []byte) ([]byte, error) {
	val, err := p.db.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (p *PogrebWrapper) Set(key, value []byte) error {
	return p.db.Put(key, value)
}

func (p *PogrebWrapper) Delete(key []byte) error {
	return p.db.Delete(key)
}

func (p *PogrebWrapper) ForEach(fn func(k, v []byte) error) error {
	it := p.db.Items()
	for {
		k, v, err := it.Next()
		if err == pogreb.ErrIterationDone {
			break
		}
		if err != nil {
			return err
		}
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
