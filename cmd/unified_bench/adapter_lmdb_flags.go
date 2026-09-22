package main

import "flag"

var (
	lmdbMapSize    = flag.Int64("lmdb-map-size", 10<<30, "LMDB: map size in bytes (default 10GB)")
	lmdbNoSync     = flag.Bool("lmdb-nosync", false, "LMDB: use MDB_NOSYNC flag")
	lmdbNoMetaSync = flag.Bool("lmdb-nometasync", false, "LMDB: use MDB_NOMETASYNC flag")
	lmdbWriteMap   = flag.Bool("lmdb-writemap", false, "LMDB: use MDB_WRITEMAP flag (use mmap for writes)")
)
