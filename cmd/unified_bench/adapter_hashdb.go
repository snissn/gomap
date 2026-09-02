package main

import (
	"flag"

	hashdb "github.com/snissn/gomap/HashDB"
	btreeonhashdb "github.com/snissn/gomap/HashDB/BTreeOnHashDB"
	"github.com/snissn/gomap/kvstore"
	btreeadapter "github.com/snissn/gomap/kvstore/adapters/btreeonhashdb"
	hashdbadapter "github.com/snissn/gomap/kvstore/adapters/hashdb"
)

var (
	hashdbLockControls       = flag.Bool("hashdb-lock-controls", true, "HashDB: best-effort lock (mlock/VirtualLock) the SwissHash control bytes")
	hashdbLockControlsStrict = flag.Bool("hashdb-lock-controls-strict", false, "HashDB: require control-bytes locking to succeed (may require memlock/ulimit changes)")
	hashdbAdviseKeysWillNeed = flag.Bool("hashdb-advise-keys-willneed", true, "HashDB: madvise WILLNEED for hash key map (best-effort)")
	hashdbAdviseKeysRandom   = flag.Bool("hashdb-advise-keys-random", true, "HashDB: madvise RANDOM for hash key map (best-effort)")
)

func init() {
	RegisterDB("hashdb", NewHashDB)
	RegisterAlias("gomap", "hashdb")
	RegisterDB("btree", NewBTree)
}

func openHashDBForBench(dir string) (*hashdb.HashDB, error) {
	opts := hashdb.HashDBOptions{
		IndexMemoryPolicySet: true,
		IndexMemoryPolicy: hashdb.IndexMemoryPolicy{
			LockControls:       *hashdbLockControls,
			LockControlsStrict: *hashdbLockControlsStrict,
			AdviseKeysWillNeed: *hashdbAdviseKeysWillNeed,
			AdviseKeysRandom:   *hashdbAdviseKeysRandom,
		},
	}
	return hashdb.OpenWithOptions(dir, opts)
}

func NewHashDB(dir string) (kvstore.DB, error) {
	m, err := openHashDBForBench(dir)
	if err != nil {
		return nil, err
	}
	return hashdbadapter.WrapNamed(m, "HashDB"), nil
}

func NewBTree(dir string) (kvstore.DB, error) {
	m, err := openHashDBForBench(dir)
	if err != nil {
		return nil, err
	}
	t, err := btreeonhashdb.NewTreeOnHashDB(m, "bench", &btreeonhashdb.Options{CacheSize: 4096})
	if err != nil {
		return nil, err
	}
	return btreeadapter.WrapNamed(m, t, "BTree"), nil
}
