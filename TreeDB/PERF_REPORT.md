# TreeDB Performance Report

**Date:** Fri Dec 12 01:04:15 HST 2025
**System:** Darwin 24.6.0

## Benchmark Results

| Benchmark | Iterations | Time/Op | Throughput | Memory/Op | Alloc/Op |
|---|---|---|---|---|---|
| BenchmarkStress-8 | 477 | 2.28 ms | 437 | 7844 B | 87 |
| BenchmarkGet-8 | 560984 | 1.91 µs | 523286 | 12635 B | 11 |
| BenchmarkScan-8 | 2557 | 420.75 µs | 2376 | 1292633 B | 20618 |
| BenchmarkBatch-8 | 234 | 5.00 ms | 199 (batches) | 853650 B | 4571 |
| BenchmarkLargeVal-8 | 172 | 7.22 ms | 138 | 12129 B | 208 |

## Hotspots (Top 5 Functions)

### BenchmarkStress
```
File: db.test
Type: cpu
Time: 2025-12-12 01:04:15 HST
Duration: 8.36s, Total samples = 180ms ( 2.15%)
Showing nodes accounting for 180ms, 100% of 180ms total
Showing top 5 nodes out of 78
      flat  flat%   sum%        cum   cum%
     100ms 55.56% 55.56%      100ms 55.56%  syscall.syscall
      40ms 22.22% 77.78%       40ms 22.22%  runtime.memmove
      30ms 16.67% 94.44%       30ms 16.67%  runtime.fcntl
      10ms  5.56%   100%       10ms  5.56%  syscall.syscall6
         0     0%   100%       10ms  5.56%  github.com/snissn/gomap-gemini/TreeDB/batch.(*Batch).Set
```

### BenchmarkGet
```
File: db.test
Type: cpu
Time: 2025-12-12 01:04:24 HST
Duration: 7.96s, Total samples = 4120ms (51.77%)
Showing nodes accounting for 2940ms, 71.36% of 4120ms total
Dropped 141 nodes (cum <= 20.60ms)
Showing top 5 nodes out of 141
      flat  flat%   sum%        cum   cum%
    1350ms 32.77% 32.77%     1350ms 32.77%  runtime.usleep
     640ms 15.53% 48.30%      640ms 15.53%  runtime.madvise
     440ms 10.68% 58.98%      440ms 10.68%  runtime.pthread_cond_wait
     320ms  7.77% 66.75%      320ms  7.77%  runtime.pthread_kill
     190ms  4.61% 71.36%      190ms  4.61%  hash/crc32.castagnoliUpdate
```

### BenchmarkScan
```
File: db.test
Type: cpu
Time: 2025-12-12 01:04:32 HST
Duration: 7.76s, Total samples = 1510ms (19.45%)
Showing nodes accounting for 1170ms, 77.48% of 1510ms total
Showing top 5 nodes out of 119
      flat  flat%   sum%        cum   cum%
     840ms 55.63% 55.63%      840ms 55.63%  runtime.kevent
     110ms  7.28% 62.91%      110ms  7.28%  runtime.pthread_cond_wait
     100ms  6.62% 69.54%      100ms  6.62%  runtime.pthread_kill
      60ms  3.97% 73.51%       60ms  3.97%  runtime.fcntl
      60ms  3.97% 77.48%       60ms  3.97%  syscall.syscall
```

### BenchmarkBatch
```
File: db.test
Type: cpu
Time: 2025-12-12 01:04:40 HST
Duration: 7.66s, Total samples = 510ms ( 6.66%)
Showing nodes accounting for 440ms, 86.27% of 510ms total
Showing top 5 nodes out of 129
      flat  flat%   sum%        cum   cum%
     270ms 52.94% 52.94%      270ms 52.94%  runtime.memmove
      80ms 15.69% 68.63%       80ms 15.69%  syscall.syscall
      40ms  7.84% 76.47%       40ms  7.84%  runtime.fcntl
      30ms  5.88% 82.35%       30ms  5.88%  runtime.pthread_cond_signal
      20ms  3.92% 86.27%       20ms  3.92%  runtime.typePointers.next
```

### BenchmarkLargeVal
```
File: db.test
Type: cpu
Time: 2025-12-12 01:04:48 HST
Duration: 8.57s, Total samples = 190ms ( 2.22%)
Showing nodes accounting for 190ms, 100% of 190ms total
Showing top 5 nodes out of 62
      flat  flat%   sum%        cum   cum%
      70ms 36.84% 36.84%       70ms 36.84%  runtime.fcntl
      60ms 31.58% 68.42%       60ms 31.58%  runtime.memmove
      40ms 21.05% 89.47%       40ms 21.05%  syscall.syscall
      10ms  5.26% 94.74%       10ms  5.26%  runtime.pthread_cond_wait
      10ms  5.26%   100%       10ms  5.26%  syscall.syscall6
```


## Raw Output
```

goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkStress-8   	     477	   2284145 ns/op	    7844 B/op	      87 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.366s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkGet-8   	  560984	      1911 ns/op	   12635 B/op	      11 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	7.962s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkScan-8   	    2557	    420755 ns/op	 1292633 B/op	   20618 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	7.766s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkBatch-8   	     234	   5006285 ns/op	  853650 B/op	    4571 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	7.662s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkLargeVal-8   	     172	   7225111 ns/op	   12129 B/op	     208 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.571s
```
