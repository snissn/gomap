package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/kvstore"
)

func openBenchDB(factory DBFactory, name, dir string, shards int) (kvstore.DB, func() (kvstore.DB, error), error) {
	if shards <= 1 {
		db, err := factory(dir)
		if err != nil {
			return nil, nil, err
		}
		return db, func() (kvstore.DB, error) {
			return factory(dir)
		}, nil
	}
	db, err := openShardedDB(factory, name, dir, shards)
	if err != nil {
		return nil, nil, err
	}
	return db, func() (kvstore.DB, error) {
		return openShardedDB(factory, name, dir, shards)
	}, nil
}

func openShardedDB(factory DBFactory, name, dir string, shards int) (kvstore.DB, error) {
	if shards <= 1 {
		return factory(dir)
	}
	shardDBs := make([]kvstore.DB, 0, shards)
	for i := 0; i < shards; i++ {
		shardDir := filepath.Join(dir, fmt.Sprintf("shard-%d", i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			closeShards(shardDBs)
			return nil, fmt.Errorf("shard dir %s: %w", shardDir, err)
		}
		db, err := factory(shardDir)
		if err != nil {
			closeShards(shardDBs)
			return nil, err
		}
		shardDBs = append(shardDBs, db)
	}
	return newShardedDB(name, shardDBs), nil
}

func closeShards(shards []kvstore.DB) {
	for _, shard := range shards {
		_ = shard.Close()
	}
}
