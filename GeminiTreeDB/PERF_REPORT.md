# TreeDB Performance Report

**Date:** Fri Dec 12 01:19:32 HST 2025
**System:** Darwin 24.6.0

## Benchmark Results

| Benchmark | Iterations | Time/Op | Throughput | Memory/Op | Alloc/Op |
|---|---|---|---|---|---|
| BenchmarkStress-8 | 553 | 3.48 ms | 286 | 7692 B | 98 |
| BenchmarkGet-8 | 704922 | 1.64 µs | 606796 | 12635 B | 11 |
| BenchmarkScan-8 | 3026 | 394.93 µs | 2532 | 1292637 B | 20618 |
| BenchmarkBatch-8 | 199 | 6.31 ms | 158 (batches) | 841849 B | 4389 |
| BenchmarkLargeVal-8 | 170 | 7.27 ms | 137 | 11857 B | 196 |

## Hotspots (Top 5 Functions)

### BenchmarkStress
```
File: db.test
Type: cpu
Time: 2025-12-12 01:19:32 HST
Duration: 8.97s, Total samples = 640ms ( 7.13%)
Showing nodes accounting for 590ms, 92.19% of 640ms total
Showing top 5 nodes out of 119
      flat  flat%   sum%        cum   cum%
     300ms 46.88% 46.88%      300ms 46.88%  syscall.syscall
     160ms 25.00% 71.88%      160ms 25.00%  runtime.fcntl
      70ms 10.94% 82.81%       70ms 10.94%  runtime.memmove
      30ms  4.69% 87.50%       30ms  4.69%  encoding/binary.littleEndian.PutUint64
      30ms  4.69% 92.19%       30ms  4.69%  runtime.pthread_cond_wait
```

### BenchmarkGet
```
File: db.test
Type: cpu
Time: 2025-12-12 01:19:41 HST
Duration: 8.66s, Total samples = 6990ms (80.76%)
Showing nodes accounting for 4980ms, 71.24% of 6990ms total
Dropped 164 nodes (cum <= 34.95ms)
Showing top 5 nodes out of 148
      flat  flat%   sum%        cum   cum%
    2600ms 37.20% 37.20%     2600ms 37.20%  runtime.usleep
     940ms 13.45% 50.64%      940ms 13.45%  runtime.pthread_cond_wait
     790ms 11.30% 61.95%      790ms 11.30%  runtime.madvise
     340ms  4.86% 66.81%      340ms  4.86%  runtime.pthread_kill
     310ms  4.43% 71.24%      310ms  4.43%  hash/crc32.castagnoliUpdate
```

### BenchmarkScan
```
File: db.test
Type: cpu
Time: 2025-12-12 01:19:50 HST
Duration: 10.07s, Total samples = 2940ms (29.19%)
Showing nodes accounting for 2060ms, 70.07% of 2940ms total
Dropped 57 nodes (cum <= 14.70ms)
Showing top 5 nodes out of 114
      flat  flat%   sum%        cum   cum%
     930ms 31.63% 31.63%      930ms 31.63%  runtime.kevent
     330ms 11.22% 42.86%      330ms 11.22%  runtime.pthread_cond_wait
     290ms  9.86% 52.72%      290ms  9.86%  runtime.pthread_cond_signal
     270ms  9.18% 61.90%      270ms  9.18%  runtime.pthread_kill
     240ms  8.16% 70.07%      240ms  8.16%  syscall.syscall
```

### BenchmarkBatch
```
File: db.test
Type: cpu
Time: 2025-12-12 01:20:00 HST
Duration: 8.77s, Total samples = 1210ms (13.79%)
Showing nodes accounting for 970ms, 80.17% of 1210ms total
Showing top 5 nodes out of 180
      flat  flat%   sum%        cum   cum%
     500ms 41.32% 41.32%      500ms 41.32%  runtime.memmove
     200ms 16.53% 57.85%      200ms 16.53%  syscall.syscall
     150ms 12.40% 70.25%      150ms 12.40%  runtime.fcntl
      80ms  6.61% 76.86%       80ms  6.61%  runtime.pthread_cond_wait
      40ms  3.31% 80.17%       40ms  3.31%  runtime.madvise
```

### BenchmarkLargeVal
```
File: db.test
Type: cpu
Time: 2025-12-12 01:20:09 HST
Duration: 8.37s, Total samples = 690ms ( 8.25%)
Showing nodes accounting for 620ms, 89.86% of 690ms total
Showing top 5 nodes out of 116
      flat  flat%   sum%        cum   cum%
     320ms 46.38% 46.38%      320ms 46.38%  syscall.syscall
     160ms 23.19% 69.57%      160ms 23.19%  runtime.fcntl
      80ms 11.59% 81.16%       80ms 11.59%  runtime.memmove
      30ms  4.35% 85.51%       30ms  4.35%  runtime.pthread_cond_signal
      30ms  4.35% 89.86%       30ms  4.35%  runtime.pthread_cond_wait
```


## Raw Output
```

goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkStress-8   	     553	   3489298 ns/op	    7692 B/op	      98 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.976s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkGet-8   	  704922	      1648 ns/op	   12635 B/op	      11 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.660s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkScan-8   	    3026	    394939 ns/op	 1292637 B/op	   20618 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	10.078s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkBatch-8   	     199	   6318190 ns/op	  841849 B/op	    4389 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.777s
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap-gemini/TreeDB/db
cpu: Apple M3
BenchmarkLargeVal-8   	     170	   7273251 ns/op	   11857 B/op	     196 allocs/op
PASS
ok  	github.com/snissn/gomap-gemini/TreeDB/db	8.375s
```
