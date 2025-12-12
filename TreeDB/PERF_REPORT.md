# TreeDB Performance Report

**Date:** Fri Dec 12 01:22:28 HST 2025
**System:** Darwin 24.6.0

## Performance Snapshot

```
  - Strict Writes: ~437 ops/sec
  - Batch Writes: ~159000 keys/sec
  - Reads: ~603864 ops/sec
  - Full Scans: ~2548 scans/sec
```

## Benchmark Results

| Benchmark | Iterations | Time/Op | Throughput | Memory/Op | Alloc/Op |
|---|---|---|---|---|---|
| BenchmarkStress-8 | 592 | 2.28 ms | 437 | 7885 B | 97 |

| BenchmarkGet-8 | 726831 | 1.65 µs | 603864 | 12635 B | 11 |

| BenchmarkScan-8 | 3062 | 392.40 µs | 2548 | 1292638 B | 20618 |

| BenchmarkBatch-8 | 196 | 6.28 ms | 159 | 840780 B | 4373 |

| BenchmarkLargeVal-8 | 181 | 7.19 ms | 138 | 11827 B | 202 |


## Hotspots (Top 5 Functions)

### BenchmarkStress
```
File: db.test
Type: cpu
Time: 2025-12-12 01:22:28 HST
Duration: 8.97s, Total samples = 610ms ( 6.80%)
Showing nodes accounting for 550ms, 90.16% of 610ms total
Showing top 5 nodes out of 133
      flat  flat%   sum%        cum   cum%
     230ms 37.70% 37.70%      230ms 37.70%  syscall.syscall
     140ms 22.95% 60.66%      140ms 22.95%  runtime.memmove
     130ms 21.31% 81.97%      130ms 21.31%  runtime.fcntl
      30ms  4.92% 86.89%       30ms  4.92%  encoding/binary.littleEndian.PutUint64
      20ms  3.28% 90.16%       20ms  3.28%  runtime.pthread_cond_wait
```

### BenchmarkGet
```
File: db.test
Type: cpu
Time: 2025-12-12 01:22:37 HST
Duration: 9.46s, Total samples = 7780ms (82.21%)
Showing nodes accounting for 5320ms, 68.38% of 7780ms total
Dropped 159 nodes (cum <= 38.90ms)
Showing top 5 nodes out of 172
      flat  flat%   sum%        cum   cum%
    2630ms 33.80% 33.80%     2630ms 33.80%  runtime.usleep
     940ms 12.08% 45.89%      940ms 12.08%  runtime.pthread_cond_wait
     930ms 11.95% 57.84%      930ms 11.95%  runtime.madvise
     520ms  6.68% 64.52%      520ms  6.68%  runtime.pthread_kill
     300ms  3.86% 68.38%      300ms  3.86%  hash/crc32.castagnoliUpdate
```

### BenchmarkScan
```
File: db.test
Type: cpu
Time: 2025-12-12 01:22:46 HST
Duration: 9.27s, Total samples = 3050ms (32.92%)
Showing nodes accounting for 2290ms, 75.08% of 3050ms total
Dropped 56 nodes (cum <= 15.25ms)
Showing top 5 nodes out of 131
      flat  flat%   sum%        cum   cum%
    1120ms 36.72% 36.72%     1120ms 36.72%  runtime.kevent
     390ms 12.79% 49.51%      390ms 12.79%  runtime.pthread_cond_signal
     310ms 10.16% 59.67%      310ms 10.16%  runtime.pthread_kill
     250ms  8.20% 67.87%      250ms  8.20%  runtime.pthread_cond_wait
     220ms  7.21% 75.08%      220ms  7.21%  syscall.syscall
```

### BenchmarkBatch
```
File: db.test
Type: cpu
Time: 2025-12-12 01:22:56 HST
Duration: 8.38s, Total samples = 1040ms (12.41%)
Showing nodes accounting for 880ms, 84.62% of 1040ms total
Showing top 5 nodes out of 177
      flat  flat%   sum%        cum   cum%
     520ms 50.00% 50.00%      520ms 50.00%  runtime.memmove
     170ms 16.35% 66.35%      170ms 16.35%  syscall.syscall
     130ms 12.50% 78.85%      130ms 12.50%  runtime.fcntl
      30ms  2.88% 81.73%       30ms  2.88%  runtime.madvise
      30ms  2.88% 84.62%       30ms  2.88%  runtime.pthread_cond_signal
```

### BenchmarkLargeVal
```
File: db.test
Type: cpu
Time: 2025-12-12 01:23:04 HST
Duration: 8.57s, Total samples = 710ms ( 8.28%)
Showing nodes accounting for 650ms, 91.55% of 710ms total
Showing top 5 nodes out of 135
      flat  flat%   sum%        cum   cum%
     260ms 36.62% 36.62%      260ms 36.62%  runtime.fcntl
     260ms 36.62% 73.24%      260ms 36.62%  syscall.syscall
     110ms 15.49% 88.73%      110ms 15.49%  runtime.memmove
      10ms  1.41% 90.14%       10ms  1.41%  encoding/binary.littleEndian.PutUint32
      10ms  1.41% 91.55%       10ms  1.41%  encoding/binary.littleEndian.PutUint64
```


## Raw Output
```

goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkStress-8   	     592	   2286061 ns/op	    7885 B/op	      97 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.981s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkGet-8   	  726831	      1656 ns/op	   12635 B/op	      11 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	9.469s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkScan-8   	    3062	    392408 ns/op	 1292638 B/op	   20618 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	9.271s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkBatch-8   	     196	   6285803 ns/op	  840780 B/op	    4373 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.386s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkLargeVal-8   	     181	   7195267 ns/op	   11827 B/op	     202 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.576s
```
