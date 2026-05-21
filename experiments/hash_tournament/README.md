# Content Hash Tournament

This experiment compares candidate content hash functions for block-sized read
integrity checks. It includes the current TreeDB-style CRC path:

```go
crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
```

The benchmark sizes mirror ClickHouse-like compressed block boundaries that are
useful for reasoning about checksum cost in a read hot path:

- 64 KiB: default lower compression-block threshold.
- 256 KiB: middle case.
- 512 KiB: common one-mark scale for wider rows.
- 1 MiB: default maximum compression block size.

## Run

From this directory:

```sh
go test -run '^$' -bench BenchmarkContentHashTournament -benchtime=1s -count=3
```

For a longer run:

```sh
go test -run '^$' -bench BenchmarkContentHashTournament -benchtime=2s -count=5
```

## Competitors

- `CRC32C_Castagnoli_TreeDB`: Go `hash/crc32` with `crc32.Castagnoli`.
- `CRC32_IEEE`: Go `hash/crc32.ChecksumIEEE`.
- `XXHash64`: `github.com/cespare/xxhash/v2`.
- `XXH3_64`: `github.com/zeebo/xxh3`.
- `FarmHash64`: `github.com/dgryski/go-farm`.
- `MapHash_ProcessLocal`: Go `hash/maphash`.
- `FNV1a_64`: Go `hash/fnv`.
- `SHA256`: Go `crypto/sha256`.

## Initial Apple M3 Read

A short Apple M3 run showed `FarmHash64` as the fastest candidate across the
four sizes, with `XXH3_64` next and `CRC32C_Castagnoli_TreeDB` around 9-10
GB/s on larger buffers.

Representative top-contender pass:

```text
64 KiB:  FarmHash64 ~24.3 GB/s, XXH3 ~15.2 GB/s, CRC32C ~7.6 GB/s
256 KiB: FarmHash64 ~25.8 GB/s, XXH3 ~16.8 GB/s, CRC32C ~9.4 GB/s
512 KiB: FarmHash64 ~23.9 GB/s, XXH3 ~16.1 GB/s, CRC32C ~9.8 GB/s
1 MiB:   FarmHash64 ~20.5 GB/s, XXH3 ~15.6 GB/s, CRC32C ~9.3 GB/s
```

Treat these numbers as local signal only; rerun on target hardware before using
them to choose an on-disk checksum format.
