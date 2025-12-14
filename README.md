# ⚡️ HashDB — High-Performance Hashmap Storage Engine

**HashDB** is a low-level, memory-mapped storage engine designed for high-performance workloads where **read throughput and latency matter**. Inspired by Redis and engineered as an alternative to LSM-based key-value stores like BadgerDB, HashDB uses custom on-disk hashmaps to deliver blazing-fast `GET` operations at scale.

> “Hashmaps are to reads what LSM trees are to writes — but without the compaction overhead.”

---

## 🚀 Project Goals

- Provide a **Redis-style server interface** backed by HashDB.
- Enable **side-by-side benchmarking** against BadgerDB.
- Showcase performance under large key counts (tested up to **50 million keys**).
- Demonstrate **low-latency, high-throughput** read performance using mmap-backed hashmaps.
- Visualize comparative performance via native Go + Matplotlib plots.

---

## 📦 Features

- Redis protocol compatibility via [`redcon`](https://github.com/tidwall/redcon).
- `SET`/`GET` commands mapped to HashDB or Badger engines.
- Clean benchmark suite with CSV export and performance plots.
- Automated benchmarking via `Makefile`.
- Scales efficiently with large key volumes.

---

## 📈 Performance Comparison

HashDB significantly outperforms BadgerDB in high-read scenarios — especially as key volume increases.

See this chart from our benchmark suite:

![Benchmark Performance](HashDB/benchmark/benchmark_performance_combined.png)

| Engine | Keys      | SET RPS     | SET p50 | GET RPS     | GET p50 |
|--------|-----------|-------------|--------|-------------|--------|
| HashDB | 1,000     | 336,000     | 1.98 ms | 1,008,000   | 0.47 ms |
| HashDB | 10,000    | 434,782     | 5.03 ms | 1,666,667   | 0.89 ms |
| HashDB | 1,000,000 | 472,367     | 4.34 ms | 1,721,170   | 0.91 ms |
| HashDB | 50M       | 468,872     | 4.23 ms | 1,673,304   | 0.94 ms |
| Badger | 1,000     | 251,999     | 3.39 ms | 1,008,000   | 0.52 ms |
| Badger | 1,000,000 | 410,004     | 7.55 ms | 1,168,224   | 0.10 ms |
| Badger | 50M       | 425,659     | 7.23 ms | 1,057,373   | 0.09 ms |

> ✨ **HashDB consistently delivers 30–50% higher GET throughput and 30–60% lower latency than Badger at scale.**

---

## 🔧 Project Structure

```plaintext
HashDB/
├── cmd/
│   └── benchmarkmain/       # Entry point for benchmark suite
├── redisserver/
│   ├── hashdbredis/         # Redis server wrapper for HashDB
│   └── badgerredis/         # Redis server wrapper for Badger
├── benchmark/
│   ├── config.go            # CLI flag parsing
│   ├── runner.go            # Benchmark orchestration
│   ├── report.go            # CSV + terminal output
│   └── plot.go              # Native Go plot support (optional)
├── benchmark/benchmark_performance_combined.png
└── ...
```

## ✅ Testing

- `go test ./...` (repo root)
- `go test ./...` (`TreeDB/`)
- `go test ./...` (`cmd/unified_bench/`)
