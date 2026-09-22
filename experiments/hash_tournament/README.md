# Content Hash Tournament

This experiment compares candidate content hash functions for block-sized read
integrity checks. It includes the current TreeDB-style CRC path:

```go
var crc32Table = crc32.MakeTable(crc32.IEEE)

crc32.Checksum(data, crc32Table)
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

- `CRC32_IEEE_Table_TreeDB`: `github.com/snissn/go-crc32-asm` with `crc32.IEEE`.
- `CRC32_IEEE`: `github.com/snissn/go-crc32-asm` `crc32.ChecksumIEEE`.
- `CRC32_Koopman`: Go `hash/crc32` with `crc32.Koopman`.
- `CRC64_ECMA`: Go `hash/crc64` with `crc64.ECMA`.
- `CRC64_ISO`: Go `hash/crc64` with `crc64.ISO`.
- `XXHash64`: `github.com/cespare/xxhash/v2`.
- `XXHash64_Digest`: `github.com/cespare/xxhash/v2` streaming digest.
- `XXHash64_DigestSeeded`: `github.com/cespare/xxhash/v2` seeded streaming digest.
- `XXH3_64`: `github.com/zeebo/xxh3`.
- `XXH3_64Seeded`: `github.com/zeebo/xxh3`.
- `XXH3_128`: `github.com/zeebo/xxh3`.
- `XXH3_128Seeded`: `github.com/zeebo/xxh3`.
- `XXH3_64_Hasher`: `github.com/zeebo/xxh3` streaming hasher.
- `XXH3_64_HasherSeeded`: `github.com/zeebo/xxh3` seeded streaming hasher.
- `XXH3_128_Hasher`: `github.com/zeebo/xxh3` streaming hasher.
- `XXH3_128_HasherSeeded`: `github.com/zeebo/xxh3` seeded streaming hasher.
- `FarmHash64`: `github.com/dgryski/go-farm`.
- `FarmHash64Seeded`: `github.com/dgryski/go-farm`.
- `FarmHash64TwoSeeds`: `github.com/dgryski/go-farm`.
- `FarmFingerprint64`: `github.com/dgryski/go-farm`.
- `FarmHash128`: `github.com/dgryski/go-farm`.
- `FarmHash128Seeded`: `github.com/dgryski/go-farm`.
- `FarmFingerprint128`: `github.com/dgryski/go-farm`.
- `FarmHash32`: `github.com/dgryski/go-farm`.
- `FarmHash32Seeded`: `github.com/dgryski/go-farm`.
- `FarmFingerprint32`: `github.com/dgryski/go-farm`.
- `MapHash_ProcessLocal`: Go `hash/maphash`.
- `FNV1_64`: Go `hash/fnv`.
- `FNV1a_64`: Go `hash/fnv`.
- `FNV1_32`: Go `hash/fnv`.
- `FNV1a_32`: Go `hash/fnv`.
- `FNV1_128`: Go `hash/fnv`.
- `FNV1a_128`: Go `hash/fnv`.
- `SHA224`: Go `crypto/sha256`.
- `SHA256`: Go `crypto/sha256`.

The suite includes the native output widths exposed by each package for byte
slice content hashing. The native 32-bit entries are included because TreeDB's
current checksum slot is already `uint32`-sized. A fast native 32-bit hash could
be a drop-in format change while gomap does not need backward compatibility for
persisted checksum values.

## Current Apple M3 Read

On the local Apple M3 smoke runs, `FarmHash64` and its seeded 64-bit variants
are the fastest candidates. The 128-bit farm and `XXH3` variants do not beat
the 64-bit farm path, and the native 32-bit farm variants do not beat the
current Go `CRC32_IEEE_Table_TreeDB` path.

Representative 1 MiB expanded-run results:

```text
FarmHash64:           ~27.20 GB/s
FarmHash64Seeded:     ~27.16 GB/s
FarmHash64TwoSeeds:   ~27.22 GB/s
FarmFingerprint64:    ~21.33 GB/s
FarmHash128:          ~19.65 GB/s
FarmHash128Seeded:    ~19.69 GB/s
FarmFingerprint128:   ~19.63 GB/s
XXH3_128:             ~19.13 GB/s
XXH3_64:              ~18.95 GB/s
XXHash64:             ~16.88 GB/s
MapHash_ProcessLocal: ~13.90 GB/s
CRC32_IEEE_Table_TreeDB: ~10.64 GB/s
FarmHash32Seeded:     ~7.71 GB/s
FarmFingerprint32:    ~7.67 GB/s
FarmHash32:           ~7.38 GB/s
SHA256:               ~2.85 GB/s
CRC64_ECMA:           ~1.99 GB/s
CRC64_ISO:            ~1.93 GB/s
```

Practical read from this machine:

- If the checksum field can move to 64 bits, `FarmHash64` is the strongest
  local candidate to validate on target hardware.
- Go's standard-library `CRC64_ECMA` and `CRC64_ISO` provide 64-bit CRC-style
  checksums, but they are much slower than `CRC32_IEEE_Table_TreeDB` and the
  fast 64-bit fingerprint hashes on this machine.
- If the checksum field must stay `uint32`, benchmark truncating fast 64-bit
  hashes, such as `uint32(farm.Hash64(data))`, before choosing a native 32-bit
  hash. The native 32-bit farm variants are slower than CRC32_IEEE in this run.
- `maphash` is not a good persisted checksum default because its seed is
  process-local unless the format explicitly owns and persists seed semantics.

Earlier short-run top-contender pass:

```text
64 KiB:  FarmHash64 ~24.3 GB/s, XXH3 ~15.2 GB/s, CRC32_IEEE ~7.6 GB/s
256 KiB: FarmHash64 ~25.8 GB/s, XXH3 ~16.8 GB/s, CRC32_IEEE ~9.4 GB/s
512 KiB: FarmHash64 ~23.9 GB/s, XXH3 ~16.1 GB/s, CRC32_IEEE ~9.8 GB/s
1 MiB:   FarmHash64 ~20.5 GB/s, XXH3 ~15.6 GB/s, CRC32_IEEE ~9.3 GB/s
```

Treat these numbers as local signal only; rerun on target hardware before using
them to choose an on-disk checksum format.
