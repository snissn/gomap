# Int64 Column Granule Experiment Benchmark Report

This report is intentionally unabridged. It includes the exact ratio log and benchmark output captured for the initial `experiments/colgranule` smoke test.

## Scope

- 8192-row int64 granules.
- Raw int64 encoding.
- Delta+zigzag varint encoding.
- Compression modes: none, snappy, lz4.
- Range scan benchmark decodes matching granules and uses min/max metadata to skip non-overlapping granules.
- No durable files, roots, collection WAL, side refs, or collection APIs are involved.

## Commands

```sh
go test ./experiments/colgranule
go test ./experiments/colgranule -run '^TestCompressionRatios$' -v -count=1
go test ./experiments/colgranule -run '^TestCompressionRatios$' -bench 'Benchmark(Encode|Decode|RangeScan)Int64Granule' -benchmem -count=3 -timeout 20m
```

## Ratio Log

```text
=== RUN   TestCompressionRatios
    granule_bench_test.go:120: fixture=monotonic rows=8192 raw_values_bytes=65536
    granule_bench_test.go:126: fixture=monotonic encoding=raw_int64 requested_compression=none actual_compression=none encoded_raw_bytes=65536 stored_bytes=65536 ratio_vs_values=1.0000 ratio_vs_encoded=1.0000 min=0 max=8191
    granule_bench_test.go:126: fixture=monotonic encoding=raw_int64 requested_compression=snappy actual_compression=snappy encoded_raw_bytes=65536 stored_bytes=32795 ratio_vs_values=0.5004 ratio_vs_encoded=0.5004 min=0 max=8191
    granule_bench_test.go:126: fixture=monotonic encoding=raw_int64 requested_compression=lz4 actual_compression=lz4 encoded_raw_bytes=65536 stored_bytes=32748 ratio_vs_values=0.4997 ratio_vs_encoded=0.4997 min=0 max=8191
    granule_bench_test.go:126: fixture=monotonic encoding=delta_varint requested_compression=none actual_compression=none encoded_raw_bytes=8192 stored_bytes=8192 ratio_vs_values=0.1250 ratio_vs_encoded=1.0000 min=0 max=8191
    granule_bench_test.go:126: fixture=monotonic encoding=delta_varint requested_compression=snappy actual_compression=snappy encoded_raw_bytes=8192 stored_bytes=389 ratio_vs_values=0.0059 ratio_vs_encoded=0.0475 min=0 max=8191
    granule_bench_test.go:126: fixture=monotonic encoding=delta_varint requested_compression=lz4 actual_compression=lz4 encoded_raw_bytes=8192 stored_bytes=55 ratio_vs_values=0.0008 ratio_vs_encoded=0.0067 min=0 max=8191
    granule_bench_test.go:120: fixture=timestamp_jitter rows=8192 raw_values_bytes=65536
    granule_bench_test.go:126: fixture=timestamp_jitter encoding=raw_int64 requested_compression=none actual_compression=none encoded_raw_bytes=65536 stored_bytes=65536 ratio_vs_values=1.0000 ratio_vs_encoded=1.0000 min=1700000000000992 max=1700000008191985
    granule_bench_test.go:126: fixture=timestamp_jitter encoding=raw_int64 requested_compression=snappy actual_compression=snappy encoded_raw_bytes=65536 stored_bytes=41102 ratio_vs_values=0.6272 ratio_vs_encoded=0.6272 min=1700000000000992 max=1700000008191985
    granule_bench_test.go:126: fixture=timestamp_jitter encoding=raw_int64 requested_compression=lz4 actual_compression=lz4 encoded_raw_bytes=65536 stored_bytes=41323 ratio_vs_values=0.6305 ratio_vs_encoded=0.6305 min=1700000000000992 max=1700000008191985
    granule_bench_test.go:126: fixture=timestamp_jitter encoding=delta_varint requested_compression=none actual_compression=none encoded_raw_bytes=16390 stored_bytes=16390 ratio_vs_values=0.2501 ratio_vs_encoded=1.0000 min=1700000000000992 max=1700000008191985
    granule_bench_test.go:126: fixture=timestamp_jitter encoding=delta_varint requested_compression=snappy actual_compression=snappy encoded_raw_bytes=16390 stored_bytes=815 ratio_vs_values=0.0124 ratio_vs_encoded=0.0497 min=1700000000000992 max=1700000008191985
    granule_bench_test.go:126: fixture=timestamp_jitter encoding=delta_varint requested_compression=lz4 actual_compression=lz4 encoded_raw_bytes=16390 stored_bytes=126 ratio_vs_values=0.0019 ratio_vs_encoded=0.0077 min=1700000000000992 max=1700000008191985
    granule_bench_test.go:120: fixture=low_cardinality rows=8192 raw_values_bytes=65536
    granule_bench_test.go:126: fixture=low_cardinality encoding=raw_int64 requested_compression=none actual_compression=none encoded_raw_bytes=65536 stored_bytes=65536 ratio_vs_values=1.0000 ratio_vs_encoded=1.0000 min=0 max=15
    granule_bench_test.go:126: fixture=low_cardinality encoding=raw_int64 requested_compression=snappy actual_compression=snappy encoded_raw_bytes=65536 stored_bytes=3399 ratio_vs_values=0.0519 ratio_vs_encoded=0.0519 min=0 max=15
    granule_bench_test.go:126: fixture=low_cardinality encoding=raw_int64 requested_compression=lz4 actual_compression=lz4 encoded_raw_bytes=65536 stored_bytes=1032 ratio_vs_values=0.0157 ratio_vs_encoded=0.0157 min=0 max=15
    granule_bench_test.go:126: fixture=low_cardinality encoding=delta_varint requested_compression=none actual_compression=none encoded_raw_bytes=8192 stored_bytes=8192 ratio_vs_values=0.1250 ratio_vs_encoded=1.0000 min=0 max=15
    granule_bench_test.go:126: fixture=low_cardinality encoding=delta_varint requested_compression=snappy actual_compression=snappy encoded_raw_bytes=8192 stored_bytes=392 ratio_vs_values=0.0060 ratio_vs_encoded=0.0479 min=0 max=15
    granule_bench_test.go:126: fixture=low_cardinality encoding=delta_varint requested_compression=lz4 actual_compression=lz4 encoded_raw_bytes=8192 stored_bytes=71 ratio_vs_values=0.0011 ratio_vs_encoded=0.0087 min=0 max=15
    granule_bench_test.go:120: fixture=random rows=8192 raw_values_bytes=65536
    granule_bench_test.go:126: fixture=random encoding=raw_int64 requested_compression=none actual_compression=none encoded_raw_bytes=65536 stored_bytes=65536 ratio_vs_values=1.0000 ratio_vs_encoded=1.0000 min=897420951885591 max=9223342629577197525
    granule_bench_test.go:126: fixture=random encoding=raw_int64 requested_compression=snappy actual_compression=snappy encoded_raw_bytes=65536 stored_bytes=65542 ratio_vs_values=1.0001 ratio_vs_encoded=1.0001 min=897420951885591 max=9223342629577197525
    granule_bench_test.go:126: fixture=random encoding=raw_int64 requested_compression=lz4 actual_compression=none encoded_raw_bytes=65536 stored_bytes=65536 ratio_vs_values=1.0000 ratio_vs_encoded=1.0000 min=897420951885591 max=9223342629577197525
    granule_bench_test.go:126: fixture=random encoding=delta_varint requested_compression=none actual_compression=none encoded_raw_bytes=75683 stored_bytes=75683 ratio_vs_values=1.1548 ratio_vs_encoded=1.0000 min=897420951885591 max=9223342629577197525
    granule_bench_test.go:126: fixture=random encoding=delta_varint requested_compression=snappy actual_compression=snappy encoded_raw_bytes=75683 stored_bytes=75692 ratio_vs_values=1.1550 ratio_vs_encoded=1.0001 min=897420951885591 max=9223342629577197525
    granule_bench_test.go:126: fixture=random encoding=delta_varint requested_compression=lz4 actual_compression=none encoded_raw_bytes=75683 stored_bytes=75683 ratio_vs_values=1.1548 ratio_vs_encoded=1.0000 min=897420951885591 max=9223342629577197525
--- PASS: TestCompressionRatios (0.00s)
PASS
ok  	github.com/snissn/gomap/experiments/colgranule	0.538s
```

## Full Benchmark Output

```text
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap/experiments/colgranule
cpu: Apple M3
BenchmarkEncodeInt64Granule/monotonic/raw_int64/none-8         	   72597	     15995 ns/op	4097.26 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/none-8         	   75219	     15969 ns/op	4103.84 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/none-8         	   77358	     20975 ns/op	3124.41 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/snappy-8       	   16094	     66596 ns/op	 984.09 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/snappy-8       	   18901	     63700 ns/op	1028.82 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/snappy-8       	   18882	     63770 ns/op	1027.69 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/lz4-8          	   10000	    101112 ns/op	 648.15 MB/s	  140457 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/lz4-8          	   10000	    101284 ns/op	 647.05 MB/s	  140484 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/raw_int64/lz4-8          	   10000	    103639 ns/op	 632.35 MB/s	  140624 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/none-8      	   42572	     28429 ns/op	2305.23 MB/s	   24576 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/none-8      	   42050	     30246 ns/op	2166.77 MB/s	   24576 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/none-8      	   38811	     30628 ns/op	2139.72 MB/s	   24576 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/snappy-8    	   38172	     31030 ns/op	2112.04 MB/s	   26112 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/snappy-8    	   38101	     29932 ns/op	2189.49 MB/s	   26112 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/snappy-8    	   39470	     28962 ns/op	2262.82 MB/s	   26112 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/lz4-8       	   40717	     29358 ns/op	2232.30 MB/s	   26813 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/lz4-8       	   40803	     29361 ns/op	2232.09 MB/s	   26839 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/monotonic/delta_varint/lz4-8       	   41041	     30963 ns/op	2116.58 MB/s	   26690 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/none-8  	   70704	     15709 ns/op	4171.82 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/none-8  	   75885	     16091 ns/op	4072.85 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/none-8  	   75430	     15841 ns/op	4137.12 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/snappy-8         	   14539	     83325 ns/op	 786.51 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/snappy-8         	   13971	    119064 ns/op	 550.43 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/snappy-8         	   13626	     82592 ns/op	 793.49 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/lz4-8            	    9160	    131128 ns/op	 499.79 MB/s	  140470 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/lz4-8            	    8961	    131352 ns/op	 498.94 MB/s	  140496 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/raw_int64/lz4-8            	    9008	    130860 ns/op	 500.81 MB/s	  140505 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/none-8        	   40603	     29535 ns/op	2218.89 MB/s	   56576 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/none-8        	   40543	     30023 ns/op	2182.87 MB/s	   56576 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/none-8        	   40244	     30115 ns/op	2176.21 MB/s	   56576 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/snappy-8      	   35545	     31229 ns/op	2098.56 MB/s	   58624 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/snappy-8      	   38536	     37719 ns/op	1737.47 MB/s	   58624 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/snappy-8      	   38092	     31310 ns/op	2093.16 MB/s	   58624 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/lz4-8         	   37868	     31653 ns/op	2070.44 MB/s	   58817 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/lz4-8         	   37910	     31616 ns/op	2072.88 MB/s	   58785 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/timestamp_jitter/delta_varint/lz4-8         	   37556	     31582 ns/op	2075.11 MB/s	   58869 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/none-8            	   77034	     15826 ns/op	4140.91 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/none-8            	   75578	     15753 ns/op	4160.21 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/none-8            	   76698	     16145 ns/op	4059.09 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/snappy-8          	   55158	     21757 ns/op	3012.17 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/snappy-8          	   55183	     21928 ns/op	2988.75 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/snappy-8          	   55314	     21785 ns/op	3008.30 MB/s	  147456 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/lz4-8             	   47184	     37939 ns/op	1727.39 MB/s	  142036 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/lz4-8             	   43202	     26325 ns/op	2489.54 MB/s	  141863 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/raw_int64/lz4-8             	   47642	     25262 ns/op	2594.24 MB/s	  141699 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/none-8         	   42672	     28152 ns/op	2327.96 MB/s	   24576 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/none-8         	   42459	     28198 ns/op	2324.13 MB/s	   24576 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/none-8         	   42637	     28095 ns/op	2332.65 MB/s	   24576 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/snappy-8       	   41571	     29374 ns/op	2231.11 MB/s	   26112 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/snappy-8       	   41582	     29249 ns/op	2240.65 MB/s	   26112 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/snappy-8       	   39019	     28918 ns/op	2266.27 MB/s	   26112 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/lz4-8          	   40651	     29516 ns/op	2220.34 MB/s	   26942 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/lz4-8          	   40968	     29433 ns/op	2226.65 MB/s	   26855 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/low_cardinality/delta_varint/lz4-8          	   40917	     29311 ns/op	2235.87 MB/s	   26904 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/none-8                     	   75967	     15913 ns/op	4118.36 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/none-8                     	   76008	     15864 ns/op	4131.16 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/none-8                     	   75051	     15840 ns/op	4137.32 MB/s	  131073 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/snappy-8                   	   69712	     17518 ns/op	3741.05 MB/s	  147457 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/snappy-8                   	   68794	     17743 ns/op	3693.56 MB/s	  147457 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/snappy-8                   	   69297	     17263 ns/op	3796.30 MB/s	  147457 B/op	       2 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/lz4-8                      	   51543	     23617 ns/op	2774.95 MB/s	  210768 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/lz4-8                      	   50721	     23639 ns/op	2772.34 MB/s	  210798 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/random/raw_int64/lz4-8                      	   50970	     29931 ns/op	2189.59 MB/s	  210780 B/op	       3 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/none-8                  	   10000	    103727 ns/op	 631.81 MB/s	  419074 B/op	       8 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/none-8                  	   14766	     76757 ns/op	 853.82 MB/s	  419074 B/op	       8 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/none-8                  	   15704	     76157 ns/op	 860.54 MB/s	  419075 B/op	       8 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/snappy-8                	   15324	     78271 ns/op	 837.30 MB/s	  427266 B/op	       8 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/snappy-8                	   15294	     80641 ns/op	 812.69 MB/s	  427267 B/op	       8 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/snappy-8                	   15067	     81001 ns/op	 809.08 MB/s	  427268 B/op	       8 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/lz4-8                   	   13786	     84774 ns/op	 773.07 MB/s	  509223 B/op	       9 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/lz4-8                   	   14146	     84828 ns/op	 772.57 MB/s	  509454 B/op	       9 allocs/op
BenchmarkEncodeInt64Granule/random/delta_varint/lz4-8                   	   14191	     85281 ns/op	 768.47 MB/s	  509733 B/op	       9 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/none/stored_65536B/raw_65536B-8         	  349468	      3443 ns/op	19036.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/none/stored_65536B/raw_65536B-8         	  349086	      3431 ns/op	19100.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/none/stored_65536B/raw_65536B-8         	  351097	      3433 ns/op	19092.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/snappy/stored_32795B/raw_65536B-8       	   44574	     26842 ns/op	2441.55 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/snappy/stored_32795B/raw_65536B-8       	   44362	     26993 ns/op	2427.93 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/snappy/stored_32795B/raw_65536B-8       	   41536	     26924 ns/op	2434.10 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/lz4/stored_32748B/raw_65536B-8          	   36319	     33082 ns/op	1981.01 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/lz4/stored_32748B/raw_65536B-8          	   36189	     33352 ns/op	1964.98 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/raw_int64/lz4/stored_32748B/raw_65536B-8          	   36231	     41664 ns/op	1572.96 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/none/stored_8192B/raw_8192B-8        	   71295	     14251 ns/op	4598.74 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/none/stored_8192B/raw_8192B-8        	   88671	     13193 ns/op	4967.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/none/stored_8192B/raw_8192B-8        	   90956	     13261 ns/op	4942.00 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/snappy/stored_389B/raw_8192B-8       	   80601	     14892 ns/op	4400.78 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/snappy/stored_389B/raw_8192B-8       	   80143	     14849 ns/op	4413.41 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/snappy/stored_389B/raw_8192B-8       	   80866	     14919 ns/op	4392.92 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/lz4/stored_55B/raw_8192B-8           	   48817	     24961 ns/op	2625.57 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/lz4/stored_55B/raw_8192B-8           	   48906	     24756 ns/op	2647.29 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/monotonic/delta_varint/lz4/stored_55B/raw_8192B-8           	   48859	     24871 ns/op	2635.04 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/none/stored_65536B/raw_65536B-8  	  350872	      3421 ns/op	19158.23 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/none/stored_65536B/raw_65536B-8  	  351098	      3431 ns/op	19099.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/none/stored_65536B/raw_65536B-8  	  321878	      3439 ns/op	19054.07 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/snappy/stored_41102B/raw_65536B-8         	   44074	     27223 ns/op	2407.38 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/snappy/stored_41102B/raw_65536B-8         	   44094	     27462 ns/op	2386.47 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/snappy/stored_41102B/raw_65536B-8         	   44073	     27358 ns/op	2395.53 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/lz4/stored_41323B/raw_65536B-8            	   36883	     32524 ns/op	2015.01 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/lz4/stored_41323B/raw_65536B-8            	   36490	     32582 ns/op	2011.41 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/raw_int64/lz4/stored_41323B/raw_65536B-8            	   36916	     33169 ns/op	1975.83 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/none/stored_16390B/raw_16390B-8        	   68226	     17584 ns/op	3727.00 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/none/stored_16390B/raw_16390B-8        	   68236	     17560 ns/op	3732.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/none/stored_16390B/raw_16390B-8        	   68347	     18190 ns/op	3602.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/snappy/stored_815B/raw_16390B-8        	   43434	     53646 ns/op	1221.64 MB/s	   18432 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/snappy/stored_815B/raw_16390B-8        	   59934	     31562 ns/op	2076.39 MB/s	   18432 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/snappy/stored_815B/raw_16390B-8        	   50557	     26058 ns/op	2514.97 MB/s	   18432 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/lz4/stored_126B/raw_16390B-8           	   52820	     21614 ns/op	3032.06 MB/s	   18432 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/lz4/stored_126B/raw_16390B-8           	   52099	     25063 ns/op	2614.89 MB/s	   18432 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/timestamp_jitter/delta_varint/lz4/stored_126B/raw_16390B-8           	   40382	     24810 ns/op	2641.56 MB/s	   18432 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/none/stored_65536B/raw_65536B-8            	  315702	      4148 ns/op	15799.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/none/stored_65536B/raw_65536B-8            	  254518	      6397 ns/op	10244.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/none/stored_65536B/raw_65536B-8            	  194790	      7279 ns/op	9003.07 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/snappy/stored_3399B/raw_65536B-8           	   73728	     18710 ns/op	3502.64 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/snappy/stored_3399B/raw_65536B-8           	   76952	     15223 ns/op	4305.05 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/snappy/stored_3399B/raw_65536B-8           	   74677	     15828 ns/op	4140.42 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/lz4/stored_1032B/raw_65536B-8              	   95971	     16427 ns/op	3989.57 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/lz4/stored_1032B/raw_65536B-8              	   90712	     15340 ns/op	4272.26 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/raw_int64/lz4/stored_1032B/raw_65536B-8              	   86932	     13984 ns/op	4686.34 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/none/stored_8192B/raw_8192B-8           	   86511	     15863 ns/op	4131.25 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/none/stored_8192B/raw_8192B-8           	   85952	     13951 ns/op	4697.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/none/stored_8192B/raw_8192B-8           	   44751	     30377 ns/op	2157.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/snappy/stored_392B/raw_8192B-8          	   65684	     16448 ns/op	3984.41 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/snappy/stored_392B/raw_8192B-8          	   70939	     22349 ns/op	2932.33 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/snappy/stored_392B/raw_8192B-8          	   62908	     17518 ns/op	3741.00 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/lz4/stored_71B/raw_8192B-8              	   80556	     17015 ns/op	3851.60 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/lz4/stored_71B/raw_8192B-8              	   66972	     17355 ns/op	3776.10 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/low_cardinality/delta_varint/lz4/stored_71B/raw_8192B-8              	   69226	     16385 ns/op	3999.75 MB/s	    8192 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/none/stored_65536B/raw_65536B-8                     	  284780	      4107 ns/op	15957.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/none/stored_65536B/raw_65536B-8                     	  300132	      3880 ns/op	16891.27 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/none/stored_65536B/raw_65536B-8                     	  275475	      4256 ns/op	15398.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/snappy/stored_65542B/raw_65536B-8                   	  118176	      9811 ns/op	6679.81 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/snappy/stored_65542B/raw_65536B-8                   	  153916	      7179 ns/op	9129.47 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/snappy/stored_65542B/raw_65536B-8                   	  166963	      7142 ns/op	9176.16 MB/s	   65536 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/none/stored_65536B/raw_65536B#01-8                  	  282850	      4028 ns/op	16268.29 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/none/stored_65536B/raw_65536B#01-8                  	  332385	      3970 ns/op	16509.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/raw_int64/none/stored_65536B/raw_65536B#01-8                  	  304251	      3834 ns/op	17094.86 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/none/stored_75683B/raw_75683B-8                  	   17726	     68011 ns/op	 963.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/none/stored_75683B/raw_75683B-8                  	   17716	     67607 ns/op	 969.37 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/none/stored_75683B/raw_75683B-8                  	   17658	     71341 ns/op	 918.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/snappy/stored_75692B/raw_75683B-8                	   15853	    116972 ns/op	 560.27 MB/s	   81920 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/snappy/stored_75692B/raw_75683B-8                	   15636	     71572 ns/op	 915.67 MB/s	   81920 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/snappy/stored_75692B/raw_75683B-8                	   16756	     71311 ns/op	 919.01 MB/s	   81920 B/op	       1 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/none/stored_75683B/raw_75683B#01-8               	   17838	     67156 ns/op	 975.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/none/stored_75683B/raw_75683B#01-8               	   17913	     68101 ns/op	 962.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkDecodeInt64Granule/random/delta_varint/none/stored_75683B/raw_75683B#01-8               	   17500	     66373 ns/op	 987.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/none/hit/stored_65536B/raw_65536B-8           	  175669	      6502 ns/op	10080.13 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/none/hit/stored_65536B/raw_65536B-8           	  189044	      6358 ns/op	10306.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/none/hit/stored_65536B/raw_65536B-8           	  185391	      6346 ns/op	10327.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/none/minmax_skip-8                            	534356328	         2.441 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/none/minmax_skip-8                            	439331386	         2.517 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/none/minmax_skip-8                            	464347540	         2.392 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/snappy/hit/stored_32795B/raw_65536B-8         	   40416	     34358 ns/op	1907.46 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/snappy/hit/stored_32795B/raw_65536B-8         	   39890	     30248 ns/op	2166.60 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/snappy/hit/stored_32795B/raw_65536B-8         	   39434	     29914 ns/op	2190.81 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/snappy/minmax_skip-8                          	501190240	         2.355 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/snappy/minmax_skip-8                          	515916469	         2.343 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/snappy/minmax_skip-8                          	506413783	         2.518 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/lz4/hit/stored_32748B/raw_65536B-8            	   32697	     40047 ns/op	1636.47 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/lz4/hit/stored_32748B/raw_65536B-8            	   30313	     54754 ns/op	1196.92 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/lz4/hit/stored_32748B/raw_65536B-8            	   27717	     36499 ns/op	1795.55 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/lz4/minmax_skip-8                             	510432224	         2.372 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/lz4/minmax_skip-8                             	516527997	         2.355 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/raw_int64/lz4/minmax_skip-8                             	503939476	         2.356 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/none/hit/stored_8192B/raw_8192B-8          	   72938	     16456 ns/op	3982.58 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/none/hit/stored_8192B/raw_8192B-8          	   72840	     16622 ns/op	3942.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/none/hit/stored_8192B/raw_8192B-8          	   73923	     16381 ns/op	4000.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/none/minmax_skip-8                         	548774422	         2.195 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/none/minmax_skip-8                         	539723862	         2.160 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/none/minmax_skip-8                         	559481313	         2.168 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/snappy/hit/stored_389B/raw_8192B-8         	   66112	     18206 ns/op	3599.69 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/snappy/hit/stored_389B/raw_8192B-8         	   65859	     18221 ns/op	3596.74 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/snappy/hit/stored_389B/raw_8192B-8         	   66484	     18081 ns/op	3624.60 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/snappy/minmax_skip-8                       	537494034	         2.262 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/snappy/minmax_skip-8                       	531120721	         2.248 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/snappy/minmax_skip-8                       	531811507	         2.262 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/lz4/hit/stored_55B/raw_8192B-8             	   43447	     27576 ns/op	2376.59 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/lz4/hit/stored_55B/raw_8192B-8             	   43354	     27710 ns/op	2365.06 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/lz4/hit/stored_55B/raw_8192B-8             	   43047	     27840 ns/op	2354.05 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/lz4/minmax_skip-8                          	524347950	         2.985 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/lz4/minmax_skip-8                          	508967458	         2.353 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/monotonic/delta_varint/lz4/minmax_skip-8                          	544263145	         2.253 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/none/hit/stored_65536B/raw_65536B-8    	  177393	      6383 ns/op	10267.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/none/hit/stored_65536B/raw_65536B-8    	  170686	      6927 ns/op	9460.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/none/hit/stored_65536B/raw_65536B-8    	  145056	      8104 ns/op	8086.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/none/minmax_skip-8                     	500727531	         2.711 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/none/minmax_skip-8                     	508247641	         2.354 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/none/minmax_skip-8                     	474775959	         2.950 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/snappy/hit/stored_41102B/raw_65536B-8  	   39018	     30507 ns/op	2148.22 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/snappy/hit/stored_41102B/raw_65536B-8  	   39956	     30832 ns/op	2125.57 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/snappy/hit/stored_41102B/raw_65536B-8  	   36908	     30749 ns/op	2131.30 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/snappy/minmax_skip-8                   	509744977	         2.362 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/snappy/minmax_skip-8                   	519275580	         2.596 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/snappy/minmax_skip-8                   	498050254	         2.417 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/lz4/hit/stored_41323B/raw_65536B-8     	   32430	     43522 ns/op	1505.80 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/lz4/hit/stored_41323B/raw_65536B-8     	   33225	     39316 ns/op	1666.90 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/lz4/hit/stored_41323B/raw_65536B-8     	   33794	     36334 ns/op	1803.69 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/lz4/minmax_skip-8                      	518504955	         6.472 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/lz4/minmax_skip-8                      	100000000	        10.90 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/raw_int64/lz4/minmax_skip-8                      	100000000	        14.20 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/none/hit/stored_16390B/raw_16390B-8 	   10000	    151156 ns/op	 433.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/none/hit/stored_16390B/raw_16390B-8 	   10000	    111484 ns/op	 587.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/none/hit/stored_16390B/raw_16390B-8 	   27332	     97149 ns/op	 674.59 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/none/minmax_skip-8                  	100000000	        10.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/none/minmax_skip-8                  	451458894	         2.593 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/none/minmax_skip-8                  	502741160	         2.446 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/snappy/hit/stored_815B/raw_16390B-8 	   53310	     23140 ns/op	2832.17 MB/s	   18432 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/snappy/hit/stored_815B/raw_16390B-8 	   53281	     22719 ns/op	2884.69 MB/s	   18432 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/snappy/hit/stored_815B/raw_16390B-8 	   53209	     22536 ns/op	2908.08 MB/s	   18432 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/snappy/minmax_skip-8                	510678499	         2.339 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/snappy/minmax_skip-8                	520519399	         2.337 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/snappy/minmax_skip-8                	514788550	         2.315 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/lz4/hit/stored_126B/raw_16390B-8    	   52813	     22722 ns/op	2884.31 MB/s	   18432 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/lz4/hit/stored_126B/raw_16390B-8    	   52420	     24154 ns/op	2713.24 MB/s	   18432 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/lz4/hit/stored_126B/raw_16390B-8    	   52845	     32065 ns/op	2043.85 MB/s	   18432 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/lz4/minmax_skip-8                   	428856310	         2.542 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/lz4/minmax_skip-8                   	455155921	         2.354 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/timestamp_jitter/delta_varint/lz4/minmax_skip-8                   	503172255	         2.330 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/none/hit/stored_65536B/raw_65536B-8     	  173437	      6996 ns/op	9367.25 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/none/hit/stored_65536B/raw_65536B-8     	  166066	      7375 ns/op	8885.67 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/none/hit/stored_65536B/raw_65536B-8     	  146428	      8280 ns/op	7915.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/none/minmax_skip-8                      	468830503	         2.418 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/none/minmax_skip-8                      	480536359	         2.522 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/none/minmax_skip-8                      	462790738	         2.362 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/snappy/hit/stored_3399B/raw_65536B-8    	   72211	     16389 ns/op	3998.87 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/snappy/hit/stored_3399B/raw_65536B-8    	   73831	     18239 ns/op	3593.11 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/snappy/hit/stored_3399B/raw_65536B-8    	   73471	     19516 ns/op	3358.12 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/snappy/minmax_skip-8                    	483114800	         2.593 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/snappy/minmax_skip-8                    	384594940	         2.933 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/snappy/minmax_skip-8                    	464754128	         2.534 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/lz4/hit/stored_1032B/raw_65536B-8       	   75357	     15506 ns/op	4226.58 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/lz4/hit/stored_1032B/raw_65536B-8       	   78538	     15834 ns/op	4138.86 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/lz4/hit/stored_1032B/raw_65536B-8       	   77725	     15762 ns/op	4157.96 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/lz4/minmax_skip-8                       	487884043	         2.468 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/lz4/minmax_skip-8                       	489135002	         2.441 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/raw_int64/lz4/minmax_skip-8                       	493157016	         3.610 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/none/hit/stored_8192B/raw_8192B-8    	   60332	     34885 ns/op	1878.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/none/hit/stored_8192B/raw_8192B-8    	   48470	     21035 ns/op	3115.60 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/none/hit/stored_8192B/raw_8192B-8    	   67976	     17852 ns/op	3671.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/none/minmax_skip-8                   	459585369	         2.617 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/none/minmax_skip-8                   	502438309	         2.433 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/none/minmax_skip-8                   	490542828	         2.419 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/snappy/hit/stored_392B/raw_8192B-8   	   67417	     17764 ns/op	3689.33 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/snappy/hit/stored_392B/raw_8192B-8   	   67710	     18205 ns/op	3599.85 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/snappy/hit/stored_392B/raw_8192B-8   	   67840	     17854 ns/op	3670.69 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/snappy/minmax_skip-8                 	496574755	         2.319 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/snappy/minmax_skip-8                 	530316615	         2.469 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/snappy/minmax_skip-8                 	506347183	         2.240 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/lz4/hit/stored_71B/raw_8192B-8       	   68395	     17643 ns/op	3714.47 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/lz4/hit/stored_71B/raw_8192B-8       	   63603	     17827 ns/op	3676.27 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/lz4/hit/stored_71B/raw_8192B-8       	   65816	     18772 ns/op	3491.08 MB/s	    8192 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/lz4/minmax_skip-8                    	531819026	         2.217 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/lz4/minmax_skip-8                    	490530879	         2.560 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/low_cardinality/delta_varint/lz4/minmax_skip-8                    	492234909	         2.331 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/hit/stored_65536B/raw_65536B-8              	   92907	     23226 ns/op	2821.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/hit/stored_65536B/raw_65536B-8              	   93374	     13012 ns/op	5036.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/hit/stored_65536B/raw_65536B-8              	  125173	     10406 ns/op	6297.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/minmax_skip-8                               	520844445	         2.580 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/minmax_skip-8                               	502896631	         2.422 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/minmax_skip-8                               	472687677	         2.569 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/snappy/hit/stored_65542B/raw_65536B-8            	   69534	     15699 ns/op	4174.43 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/snappy/hit/stored_65542B/raw_65536B-8            	   71666	     19307 ns/op	3394.48 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/snappy/hit/stored_65542B/raw_65536B-8            	   69060	     15161 ns/op	4322.62 MB/s	   65536 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/snappy/minmax_skip-8                             	470040009	         2.538 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/snappy/minmax_skip-8                             	472242240	         2.562 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/snappy/minmax_skip-8                             	452727112	         2.762 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/hit/stored_65536B/raw_65536B#01-8           	   56638	     21403 ns/op	3062.05 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/hit/stored_65536B/raw_65536B#01-8           	   48993	     35804 ns/op	1830.43 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/hit/stored_65536B/raw_65536B#01-8           	   56570	     23121 ns/op	2834.49 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/minmax_skip#01-8                            	290583812	         3.691 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/minmax_skip#01-8                            	323895381	         3.463 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/raw_int64/none/minmax_skip#01-8                            	415844542	         3.023 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/hit/stored_75683B/raw_75683B-8           	   13473	     85683 ns/op	 764.87 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/hit/stored_75683B/raw_75683B-8           	   13149	    285966 ns/op	 229.17 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/hit/stored_75683B/raw_75683B-8           	    8994	    122202 ns/op	 536.29 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/minmax_skip-8                            	371727010	         4.293 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/minmax_skip-8                            	225870093	         6.125 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/minmax_skip-8                            	144826713	         9.016 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/snappy/hit/stored_75692B/raw_75683B-8         	    4984	    458637 ns/op	 142.89 MB/s	   81920 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/snappy/hit/stored_75692B/raw_75683B-8         	    5152	    218539 ns/op	 299.88 MB/s	   81920 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/snappy/hit/stored_75692B/raw_75683B-8         	    5731	    218272 ns/op	 300.25 MB/s	   81920 B/op	       1 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/snappy/minmax_skip-8                          	100000000	        11.48 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/snappy/minmax_skip-8                          	295404458	         4.693 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/snappy/minmax_skip-8                          	251943550	         5.736 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/hit/stored_75683B/raw_75683B#01-8        	    5149	    237741 ns/op	 275.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/hit/stored_75683B/raw_75683B#01-8        	    7242	    149291 ns/op	 438.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/hit/stored_75683B/raw_75683B#01-8        	    9853	    115351 ns/op	 568.14 MB/s	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/minmax_skip#01-8                         	321182020	         5.033 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/minmax_skip#01-8                         	100000000	        46.09 ns/op	       0 B/op	       0 allocs/op
BenchmarkRangeScanInt64Granule/random/delta_varint/none/minmax_skip#01-8                         	149192966	         6.760 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/snissn/gomap/experiments/colgranule	459.746s
```


## JSONBench 1M Local Data Report

This section was generated from the local JSONBench Bluesky data directory. The command reads sorted JSONBench files from the directory; the current local install has one 129 MiB compressed file. The compressed input files are not vendored.

```sh
go run ./experiments/colgranule/cmd/jsonbench_colgranule \
  -data $JSONBENCH_DATA \
  -limit 1000000 \
  -rows-per-granule 8192
```

```text
jsonbench_colgranule data=$JSONBENCH_DATA files=1 rows=1000000 columns=17 rows_per_granule=8192 load_duration=5.656413291s
column=cid_bytes rows=1000000 min=0 max=59
column=commit_collection_code rows=1000000 min=1 max=15
column=commit_operation_code rows=1000000 min=1 max=4
column=commit_rev_bytes rows=1000000 min=0 max=13
column=commit_rkey_bytes rows=1000000 min=0 max=15
column=did_bytes rows=1000000 min=14 max=32
column=kind_code rows=1000000 min=1 max=2
column=line_bytes rows=1000000 min=196 max=10980
column=record_created_at_unix_ms rows=1000000 min=0 max=1918531872156
column=record_has_reply rows=1000000 min=0 max=1
column=record_has_subject rows=1000000 min=0 max=1
column=record_langs_count rows=1000000 min=0 max=3
column=record_subject_string_bytes rows=1000000 min=0 max=71
column=record_text_bytes rows=1000000 min=0 max=1079
column=record_type_code rows=1000000 min=1 max=15
column=row_index rows=1000000 min=0 max=999999
column=time_us rows=1000000 min=1732206349000167 max=1732207162789926
cid_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.627583ms	decode=1.6765ms	range_scan=1.0945ms	range_matches=0
cid_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=503178	ratio_vs_values=0.062897	ratio_vs_encoded=0.062897	encode=3.127042ms	decode=3.158833ms	range_scan=2.874584ms	range_matches=0
cid_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=317971	ratio_vs_values=0.039746	ratio_vs_encoded=0.039746	encode=5.117667ms	decode=2.671ms	range_scan=2.041542ms	range_matches=0
cid_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.673125ms	decode=3.780417ms	range_scan=2.283208ms	range_matches=0
cid_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=127657	ratio_vs_values=0.015957	ratio_vs_encoded=0.127657	encode=4.613833ms	decode=2.998417ms	range_scan=2.810791ms	range_matches=0
cid_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=144683	ratio_vs_values=0.018085	ratio_vs_encoded=0.144683	encode=4.221375ms	decode=3.301083ms	range_scan=2.820916ms	range_matches=0
commit_collection_code	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.647167ms	decode=1.627917ms	range_scan=1.082291ms	range_matches=14040
commit_collection_code	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1363781	ratio_vs_values=0.170473	ratio_vs_encoded=0.170473	encode=7.590375ms	decode=4.699917ms	range_scan=3.949209ms	range_matches=14040
commit_collection_code	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1769494	ratio_vs_values=0.221187	ratio_vs_encoded=0.221187	encode=10.76625ms	decode=4.212916ms	range_scan=3.567208ms	range_matches=14040
commit_collection_code	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.422167ms	decode=3.085334ms	range_scan=2.265583ms	range_matches=14040
commit_collection_code	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=537086	ratio_vs_values=0.067136	ratio_vs_encoded=0.537086	encode=5.456166ms	decode=3.771167ms	range_scan=2.887709ms	range_matches=14040
commit_collection_code	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=613390	ratio_vs_values=0.076674	ratio_vs_encoded=0.613390	encode=6.022042ms	decode=3.482459ms	range_scan=3.02025ms	range_matches=14040
commit_operation_code	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.662916ms	decode=1.702125ms	range_scan=1.101291ms	range_matches=5328
commit_operation_code	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=483057	ratio_vs_values=0.060382	ratio_vs_encoded=0.060382	encode=2.922084ms	decode=2.895167ms	range_scan=2.070542ms	range_matches=5328
commit_operation_code	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=301424	ratio_vs_values=0.037678	ratio_vs_encoded=0.037678	encode=3.536667ms	decode=2.59125ms	range_scan=2.007459ms	range_matches=5328
commit_operation_code	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.391709ms	decode=2.670375ms	range_scan=2.234084ms	range_matches=5328
commit_operation_code	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=148297	ratio_vs_values=0.018537	ratio_vs_encoded=0.148297	encode=4.203875ms	decode=3.29475ms	range_scan=2.6905ms	range_matches=5328
commit_operation_code	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=168739	ratio_vs_values=0.021092	ratio_vs_encoded=0.168739	encode=4.402709ms	decode=3.238ms	range_scan=2.801917ms	range_matches=5328
commit_rev_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.625625ms	decode=1.671084ms	range_scan=913.5µs	range_matches=0
commit_rev_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=393838	ratio_vs_values=0.049230	ratio_vs_encoded=0.049230	encode=2.079709ms	decode=2.599417ms	range_scan=1.680292ms	range_matches=0
commit_rev_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=75050	ratio_vs_values=0.009381	ratio_vs_encoded=0.009381	encode=2.559084ms	decode=2.197334ms	range_scan=1.4485ms	range_matches=0
commit_rev_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.458792ms	decode=2.698584ms	range_scan=2.083667ms	range_matches=0
commit_rev_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=58457	ratio_vs_values=0.007307	ratio_vs_encoded=0.058457	encode=3.524958ms	decode=3.058666ms	range_scan=2.225417ms	range_matches=0
commit_rev_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=31257	ratio_vs_values=0.003907	ratio_vs_encoded=0.031257	encode=3.602916ms	decode=3.135792ms	range_scan=2.524458ms	range_matches=0
commit_rkey_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.629833ms	decode=1.73775ms	range_scan=982.375µs	range_matches=0
commit_rkey_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=420284	ratio_vs_values=0.052535	ratio_vs_encoded=0.052535	encode=2.325958ms	decode=2.713625ms	range_scan=1.857125ms	range_matches=0
commit_rkey_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=147447	ratio_vs_values=0.018431	ratio_vs_encoded=0.018431	encode=2.948875ms	decode=2.456167ms	range_scan=1.693958ms	range_matches=0
commit_rkey_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=4.923458ms	decode=2.919458ms	range_scan=2.158709ms	range_matches=0
commit_rkey_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=86947	ratio_vs_values=0.010868	ratio_vs_encoded=0.086947	encode=4.677792ms	decode=3.244625ms	range_scan=2.4085ms	range_matches=0
commit_rkey_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=86179	ratio_vs_values=0.010772	ratio_vs_encoded=0.086179	encode=4.705875ms	decode=3.224333ms	range_scan=2.721958ms	range_matches=0
did_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.639083ms	decode=1.696917ms	range_scan=26.5µs	range_matches=0
did_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=375999	ratio_vs_values=0.047000	ratio_vs_encoded=0.047000	encode=1.848667ms	decode=2.980792ms	range_scan=38.416µs	range_matches=0
did_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=34582	ratio_vs_values=0.004323	ratio_vs_encoded=0.004323	encode=2.326667ms	decode=2.535167ms	range_scan=41.666µs	range_matches=0
did_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.37475ms	decode=2.896459ms	range_scan=43.292µs	range_matches=0
did_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=47502	ratio_vs_values=0.005938	ratio_vs_encoded=0.047502	encode=3.435417ms	decode=3.053041ms	range_scan=48.583µs	range_matches=0
did_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=6749	ratio_vs_values=0.000844	ratio_vs_encoded=0.006749	encode=4.195667ms	decode=3.97725ms	range_scan=55.75µs	range_matches=0
kind_code	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.619416ms	decode=1.653167ms	range_scan=922.541µs	range_matches=994672
kind_code	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=388956	ratio_vs_values=0.048620	ratio_vs_encoded=0.048620	encode=2.045833ms	decode=2.566292ms	range_scan=1.685708ms	range_matches=994672
kind_code	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=66507	ratio_vs_values=0.008313	ratio_vs_encoded=0.008313	encode=2.500125ms	decode=2.164958ms	range_scan=1.479958ms	range_matches=994672
kind_code	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.433ms	decode=2.636958ms	range_scan=2.108625ms	range_matches=994672
kind_code	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=58567	ratio_vs_values=0.007321	ratio_vs_encoded=0.058567	encode=3.602792ms	decode=2.880209ms	range_scan=2.328417ms	range_matches=994672
kind_code	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=31312	ratio_vs_values=0.003914	ratio_vs_encoded=0.031312	encode=5.421583ms	decode=3.477333ms	range_scan=2.640167ms	range_matches=994672
line_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.624834ms	decode=1.721125ms	range_scan=70.583µs	range_matches=0
line_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1567568	ratio_vs_values=0.195946	ratio_vs_encoded=0.195946	encode=8.26025ms	decode=4.899667ms	range_scan=332.25µs	range_matches=0
line_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1918019	ratio_vs_values=0.239752	ratio_vs_encoded=0.239752	encode=11.51525ms	decode=4.521375ms	range_scan=310.125µs	range_matches=0
line_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1495892	stored_bytes=1495892	ratio_vs_values=0.186986	ratio_vs_encoded=1.000000	encode=6.310459ms	decode=5.083292ms	range_scan=361.666µs	range_matches=0
line_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1495892	stored_bytes=952538	ratio_vs_values=0.119067	ratio_vs_encoded=0.636769	encode=9.135667ms	decode=6.249042ms	range_scan=457.208µs	range_matches=0
line_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1495892	stored_bytes=1008386	ratio_vs_values=0.126048	ratio_vs_encoded=0.674103	encode=9.872375ms	decode=7.638959ms	range_scan=495.167µs	range_matches=0
record_created_at_unix_ms	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.826042ms	decode=2.423958ms	range_scan=1.337625ms	range_matches=0
record_created_at_unix_ms	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=4078138	ratio_vs_values=0.509767	ratio_vs_encoded=0.509767	encode=11.564458ms	decode=6.748583ms	range_scan=5.90275ms	range_matches=0
record_created_at_unix_ms	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=3850216	ratio_vs_values=0.481277	ratio_vs_encoded=0.481277	encode=16.818709ms	decode=6.205083ms	range_scan=5.73375ms	range_matches=0
record_created_at_unix_ms	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=3007639	stored_bytes=3007639	ratio_vs_values=0.375955	ratio_vs_encoded=1.000000	encode=6.947042ms	decode=6.205584ms	range_scan=5.806209ms	range_matches=0
record_created_at_unix_ms	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=3007639	stored_bytes=2968163	ratio_vs_values=0.371020	ratio_vs_encoded=0.986875	encode=19.395917ms	decode=9.232208ms	range_scan=6.413125ms	range_matches=0
record_created_at_unix_ms	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=none:8,lz4:115	value_bytes=8000000	encoded_raw_bytes=3007639	stored_bytes=2973046	ratio_vs_values=0.371631	ratio_vs_encoded=0.988498	encode=9.288916ms	decode=6.622875ms	range_scan=6.037625ms	range_matches=0
record_has_reply	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.634792ms	decode=1.740292ms	range_scan=1.115292ms	range_matches=954566
record_has_reply	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=462747	ratio_vs_values=0.057843	ratio_vs_encoded=0.057843	encode=2.637541ms	decode=3.66725ms	range_scan=2.476667ms	range_matches=954566
record_has_reply	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=261670	ratio_vs_values=0.032709	ratio_vs_encoded=0.032709	encode=3.405417ms	decode=5.351042ms	range_scan=4.575708ms	range_matches=954566
record_has_reply	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.397708ms	decode=2.759875ms	range_scan=2.310958ms	range_matches=954566
record_has_reply	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=144988	ratio_vs_values=0.018124	ratio_vs_encoded=0.144988	encode=4.087084ms	decode=3.444041ms	range_scan=6.729833ms	range_matches=954566
record_has_reply	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=167973	ratio_vs_values=0.020997	ratio_vs_encoded=0.167973	encode=4.548834ms	decode=3.320584ms	range_scan=3.01975ms	range_matches=954566
record_has_subject	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.743375ms	decode=1.731833ms	range_scan=1.62ms	range_matches=138240
record_has_subject	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=800289	ratio_vs_values=0.100036	ratio_vs_encoded=0.100036	encode=4.70175ms	decode=4.072916ms	range_scan=4.717417ms	range_matches=138240
record_has_subject	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=892686	ratio_vs_values=0.111586	ratio_vs_encoded=0.111586	encode=6.66325ms	decode=3.849959ms	range_scan=3.689709ms	range_matches=138240
record_has_subject	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.541875ms	decode=2.761083ms	range_scan=2.812708ms	range_matches=138240
record_has_subject	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=261203	ratio_vs_values=0.032650	ratio_vs_encoded=0.261203	encode=4.938083ms	decode=3.770291ms	range_scan=3.427541ms	range_matches=138240
record_has_subject	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=300622	ratio_vs_values=0.037578	ratio_vs_encoded=0.300622	encode=5.149083ms	decode=3.510666ms	range_scan=3.767917ms	range_matches=138240
record_langs_count	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.601166ms	decode=1.67025ms	range_scan=1.259375ms	range_matches=77066
record_langs_count	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=530708	ratio_vs_values=0.066338	ratio_vs_encoded=0.066338	encode=3.213959ms	decode=3.437583ms	range_scan=2.723916ms	range_matches=77066
record_langs_count	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=409312	ratio_vs_values=0.051164	ratio_vs_encoded=0.051164	encode=4.099917ms	decode=5.605083ms	range_scan=4.860875ms	range_matches=77066
record_langs_count	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.435208ms	decode=2.66925ms	range_scan=2.374167ms	range_matches=77066
record_langs_count	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=192851	ratio_vs_values=0.024106	ratio_vs_encoded=0.192851	encode=4.27775ms	decode=3.477291ms	range_scan=3.032209ms	range_matches=77066
record_langs_count	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=223469	ratio_vs_values=0.027934	ratio_vs_encoded=0.223469	encode=4.643417ms	decode=3.337167ms	range_scan=3.02225ms	range_matches=77066
record_subject_string_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.628292ms	decode=1.751833ms	range_scan=747.125µs	range_matches=0
record_subject_string_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=916427	ratio_vs_values=0.114553	ratio_vs_encoded=0.114553	encode=5.413583ms	decode=4.873959ms	range_scan=3.876833ms	range_matches=0
record_subject_string_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1105301	ratio_vs_values=0.138163	ratio_vs_encoded=0.138163	encode=9.235708ms	decode=5.688334ms	range_scan=4.445583ms	range_matches=0
record_subject_string_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1001231	stored_bytes=1001231	ratio_vs_values=0.125154	ratio_vs_encoded=1.000000	encode=3.417958ms	decode=2.739208ms	range_scan=1.921375ms	range_matches=0
record_subject_string_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1001231	stored_bytes=347079	ratio_vs_values=0.043385	ratio_vs_encoded=0.346652	encode=4.9195ms	decode=3.516167ms	range_scan=2.552417ms	range_matches=0
record_subject_string_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1001231	stored_bytes=387822	ratio_vs_values=0.048478	ratio_vs_encoded=0.387345	encode=5.099667ms	decode=3.557542ms	range_scan=2.727375ms	range_matches=0
record_text_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.662ms	decode=1.748958ms	range_scan=587.708µs	range_matches=25
record_text_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=627621	ratio_vs_values=0.078453	ratio_vs_encoded=0.078453	encode=3.604958ms	decode=3.523208ms	range_scan=1.962584ms	range_matches=25
record_text_bytes	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=492980	ratio_vs_values=0.061622	ratio_vs_encoded=0.061622	encode=4.699792ms	decode=6.161ms	range_scan=4.002542ms	range_matches=25
record_text_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1083152	stored_bytes=1083152	ratio_vs_values=0.135394	ratio_vs_encoded=1.000000	encode=3.820542ms	decode=3.017875ms	range_scan=1.734833ms	range_matches=25
record_text_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1083152	stored_bytes=372759	ratio_vs_values=0.046595	ratio_vs_encoded=0.344143	encode=5.426208ms	decode=3.946417ms	range_scan=2.315708ms	range_matches=25
record_text_bytes	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1083152	stored_bytes=401782	ratio_vs_values=0.050223	ratio_vs_encoded=0.370938	encode=6.692959ms	decode=3.657541ms	range_scan=2.348625ms	range_matches=25
record_type_code	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.696292ms	decode=1.783584ms	range_scan=980.709µs	range_matches=13838
record_type_code	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1389487	ratio_vs_values=0.173686	ratio_vs_encoded=0.173686	encode=7.804625ms	decode=4.438542ms	range_scan=3.897291ms	range_matches=13838
record_type_code	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=1811941	ratio_vs_values=0.226493	ratio_vs_encoded=0.226493	encode=10.487084ms	decode=4.198834ms	range_scan=3.540125ms	range_matches=13838
record_type_code	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=1000000	ratio_vs_values=0.125000	ratio_vs_encoded=1.000000	encode=3.417833ms	decode=2.614333ms	range_scan=2.090292ms	range_matches=13838
record_type_code	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=571548	ratio_vs_values=0.071443	ratio_vs_encoded=0.571548	encode=5.54375ms	decode=3.454333ms	range_scan=2.895291ms	range_matches=13838
record_type_code	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000000	stored_bytes=658413	ratio_vs_values=0.082302	ratio_vs_encoded=0.658413	encode=5.773334ms	decode=3.436125ms	range_scan=2.767542ms	range_matches=13838
row_index	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.628375ms	decode=1.736916ms	range_scan=26.208µs	range_matches=15625
row_index	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=4002443	ratio_vs_values=0.500305	ratio_vs_encoded=0.500305	encode=8.03575ms	decode=4.241875ms	range_scan=65.333µs	range_matches=15625
row_index	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=4003725	ratio_vs_values=0.500466	ratio_vs_encoded=0.500466	encode=12.166708ms	decode=5.132917ms	range_scan=79.416µs	range_matches=15625
row_index	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=1000244	stored_bytes=1000244	ratio_vs_values=0.125030	ratio_vs_encoded=1.000000	encode=3.461542ms	decode=2.623166ms	range_scan=40.875µs	range_matches=15625
row_index	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=1000244	stored_bytes=47733	ratio_vs_values=0.005967	ratio_vs_encoded=0.047721	encode=3.439292ms	decode=2.765041ms	range_scan=45.5µs	range_matches=15625
row_index	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=1000244	stored_bytes=6738	ratio_vs_values=0.000842	ratio_vs_encoded=0.006736	encode=3.474083ms	decode=4.221375ms	range_scan=63.292µs	range_matches=15625
time_us	rows=1000000	granules=123	encoding=raw_int64	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=8000000	ratio_vs_values=1.000000	ratio_vs_encoded=1.000000	encode=1.652042ms	decode=1.838417ms	range_scan=32.417µs	range_matches=15022
time_us	rows=1000000	granules=123	encoding=raw_int64	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=5005356	ratio_vs_values=0.625669	ratio_vs_encoded=0.625669	encode=11.271833ms	decode=4.453583ms	range_scan=94.708µs	range_matches=15022
time_us	rows=1000000	granules=123	encoding=raw_int64	requested=lz4	actual=lz4:123	value_bytes=8000000	encoded_raw_bytes=8000000	stored_bytes=5028110	ratio_vs_values=0.628514	ratio_vs_encoded=0.628514	encode=16.076542ms	decode=4.985083ms	range_scan=126.125µs	range_matches=15022
time_us	rows=1000000	granules=123	encoding=delta_varint	requested=none	actual=none:123	value_bytes=8000000	encoded_raw_bytes=2007816	stored_bytes=2007816	ratio_vs_values=0.250977	ratio_vs_encoded=1.000000	encode=3.907125ms	decode=3.92225ms	range_scan=70.833µs	range_matches=15022
time_us	rows=1000000	granules=123	encoding=delta_varint	requested=snappy	actual=snappy:123	value_bytes=8000000	encoded_raw_bytes=2007816	stored_bytes=2008563	ratio_vs_values=0.251070	ratio_vs_encoded=1.000372	encode=3.897959ms	decode=3.385291ms	range_scan=74.667µs	range_matches=15022
time_us	rows=1000000	granules=123	encoding=delta_varint	requested=lz4	actual=none:123	value_bytes=8000000	encoded_raw_bytes=2007816	stored_bytes=2007816	ratio_vs_values=0.250977	ratio_vs_encoded=1.000000	encode=4.054375ms	decode=3.429292ms	range_scan=70.875µs	range_matches=15022
```

## JSONBench 1M Go Benchmark Smoke

This benchmark loads the local 1M-row JSONBench fixture once, then runs one summarization iteration per derived column and codec configuration.

```sh
go test ./experiments/colgranule -run '^$' -bench '^BenchmarkJSONBenchLocalColumns$' -benchmem -benchtime=1x -timeout 5m
```

```text
goos: darwin
goarch: arm64
pkg: github.com/snissn/gomap/experiments/colgranule
cpu: Apple M3
BenchmarkJSONBenchLocalColumns/cid_bytes/raw_int64/none-8         	       1	   5291917 ns/op	1511.74 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/cid_bytes/raw_int64/snappy-8       	       1	   8362875 ns/op	 956.61 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/cid_bytes/raw_int64/lz4-8          	       1	   9139375 ns/op	 875.33 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/cid_bytes/delta_varint/none-8      	       1	   9400792 ns/op	 850.99 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/cid_bytes/delta_varint/snappy-8    	       1	  10761584 ns/op	 743.38 MB/s	37107376 B/op	    2723 allocs/op
BenchmarkJSONBenchLocalColumns/cid_bytes/delta_varint/lz4-8       	       1	  10959292 ns/op	 729.97 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_collection_code/raw_int64/none-8         	       1	   5122584 ns/op	1561.71 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_collection_code/raw_int64/snappy-8       	       1	  17328083 ns/op	 461.68 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_collection_code/raw_int64/lz4-8          	       1	  18893750 ns/op	 423.42 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_collection_code/delta_varint/none-8      	       1	   8878583 ns/op	 901.04 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_collection_code/delta_varint/snappy-8    	       1	  12177334 ns/op	 656.96 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_collection_code/delta_varint/lz4-8       	       1	  12711125 ns/op	 629.37 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_operation_code/raw_int64/none-8          	       1	   5467083 ns/op	1463.30 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_operation_code/raw_int64/snappy-8        	       1	   8718417 ns/op	 917.60 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_operation_code/raw_int64/lz4-8           	       1	   8909209 ns/op	 897.95 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_operation_code/delta_varint/none-8       	       1	   9017417 ns/op	 887.17 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_operation_code/delta_varint/snappy-8     	       1	  10564708 ns/op	 757.24 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_operation_code/delta_varint/lz4-8        	       1	  11042875 ns/op	 724.45 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rev_bytes/raw_int64/none-8               	       1	   5112833 ns/op	1564.69 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rev_bytes/raw_int64/snappy-8             	       1	   7307750 ns/op	1094.73 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rev_bytes/raw_int64/lz4-8                	       1	   6886584 ns/op	1161.68 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rev_bytes/delta_varint/none-8            	       1	   8705875 ns/op	 918.92 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rev_bytes/delta_varint/snappy-8          	       1	   9271209 ns/op	 862.89 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rev_bytes/delta_varint/lz4-8             	       1	  10049208 ns/op	 796.08 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rkey_bytes/raw_int64/none-8              	       1	   5179833 ns/op	1544.45 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rkey_bytes/raw_int64/snappy-8            	       1	   7717833 ns/op	1036.56 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rkey_bytes/raw_int64/lz4-8               	       1	   7524791 ns/op	1063.15 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rkey_bytes/delta_varint/none-8           	       1	   8862250 ns/op	 902.71 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rkey_bytes/delta_varint/snappy-8         	       1	   9528333 ns/op	 839.60 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/commit_rkey_bytes/delta_varint/lz4-8            	       1	  10264792 ns/op	 779.36 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/did_bytes/raw_int64/none-8                      	       1	   3880834 ns/op	2061.41 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/did_bytes/raw_int64/snappy-8                    	       1	   5760042 ns/op	1388.88 MB/s	58051808 B/op	    2601 allocs/op
BenchmarkJSONBenchLocalColumns/did_bytes/raw_int64/lz4-8                       	       1	   5577791 ns/op	1434.26 MB/s	57191528 B/op	    2604 allocs/op
BenchmarkJSONBenchLocalColumns/did_bytes/delta_varint/none-8                   	       1	   6657417 ns/op	1201.67 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/did_bytes/delta_varint/snappy-8                 	       1	   6861250 ns/op	1165.97 MB/s	36123744 B/op	    2601 allocs/op
BenchmarkJSONBenchLocalColumns/did_bytes/delta_varint/lz4-8                    	       1	   8030833 ns/op	 996.16 MB/s	36232872 B/op	    2604 allocs/op
BenchmarkJSONBenchLocalColumns/kind_code/raw_int64/none-8                      	       1	   5346500 ns/op	1496.31 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/kind_code/raw_int64/snappy-8                    	       1	   7255291 ns/op	1102.64 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/kind_code/raw_int64/lz4-8                       	       1	   6921083 ns/op	1155.89 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/kind_code/delta_varint/none-8                   	       1	   8660042 ns/op	 923.78 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/kind_code/delta_varint/snappy-8                 	       1	   9294042 ns/op	 860.77 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/kind_code/delta_varint/lz4-8                    	       1	   9908959 ns/op	 807.35 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/line_bytes/raw_int64/none-8                     	       1	   3933416 ns/op	2033.86 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/line_bytes/raw_int64/snappy-8                   	       1	  14437666 ns/op	 554.11 MB/s	58576096 B/op	    2609 allocs/op
BenchmarkJSONBenchLocalColumns/line_bytes/raw_int64/lz4-8                      	       1	  16678833 ns/op	 479.65 MB/s	57715816 B/op	    2612 allocs/op
BenchmarkJSONBenchLocalColumns/line_bytes/delta_varint/none-8                  	       1	  12386292 ns/op	 645.88 MB/s	35485152 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/line_bytes/delta_varint/snappy-8                	       1	  16654791 ns/op	 480.34 MB/s	37476704 B/op	    2609 allocs/op
BenchmarkJSONBenchLocalColumns/line_bytes/delta_varint/lz4-8                   	       1	  17074291 ns/op	 468.54 MB/s	37339368 B/op	    2612 allocs/op
BenchmarkJSONBenchLocalColumns/record_created_at_unix_ms/raw_int64/none-8      	       1	   5168708 ns/op	1547.78 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_created_at_unix_ms/raw_int64/snappy-8    	       1	  25120917 ns/op	 318.46 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_created_at_unix_ms/raw_int64/lz4-8       	       1	  29276208 ns/op	 273.26 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_created_at_unix_ms/delta_varint/none-8   	       1	  19727667 ns/op	 405.52 MB/s	43251808 B/op	    2721 allocs/op
BenchmarkJSONBenchLocalColumns/record_created_at_unix_ms/delta_varint/snappy-8 	       1	  21138583 ns/op	 378.45 MB/s	50202592 B/op	    2967 allocs/op
BenchmarkJSONBenchLocalColumns/record_created_at_unix_ms/delta_varint/lz4-8    	       1	  22170958 ns/op	 360.83 MB/s	49542760 B/op	    2962 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_reply/raw_int64/none-8               	       1	   4949792 ns/op	1616.23 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_reply/raw_int64/snappy-8             	       1	   9046667 ns/op	 884.30 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_reply/raw_int64/lz4-8                	       1	  13557042 ns/op	 590.10 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_reply/delta_varint/none-8            	       1	   8862334 ns/op	 902.70 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_reply/delta_varint/snappy-8          	       1	  10640792 ns/op	 751.82 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_reply/delta_varint/lz4-8             	       1	  11151209 ns/op	 717.41 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_subject/raw_int64/none-8             	       1	   5413375 ns/op	1477.82 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_subject/raw_int64/snappy-8           	       1	  13231375 ns/op	 604.62 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_subject/raw_int64/lz4-8              	       1	  14434334 ns/op	 554.23 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_subject/delta_varint/none-8          	       1	   9298958 ns/op	 860.31 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_subject/delta_varint/snappy-8        	       1	  11955500 ns/op	 669.15 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_has_subject/delta_varint/lz4-8           	       1	  12289584 ns/op	 650.96 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_langs_count/raw_int64/none-8             	       1	   5144250 ns/op	1555.13 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_langs_count/raw_int64/snappy-8           	       1	  10125333 ns/op	 790.10 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_langs_count/raw_int64/lz4-8              	       1	  14986917 ns/op	 533.80 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_langs_count/delta_varint/none-8          	       1	   9140500 ns/op	 875.23 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_langs_count/delta_varint/snappy-8        	       1	  11244875 ns/op	 711.44 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_langs_count/delta_varint/lz4-8           	       1	  11554000 ns/op	 692.40 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_subject_string_bytes/raw_int64/none-8    	       1	   4537208 ns/op	1763.20 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_subject_string_bytes/raw_int64/snappy-8  	       1	  14486500 ns/op	 552.24 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_subject_string_bytes/raw_int64/lz4-8     	       1	  18876750 ns/op	 423.80 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_subject_string_bytes/delta_varint/none-8 	       1	   8850917 ns/op	 903.86 MB/s	35076000 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_subject_string_bytes/delta_varint/snappy-8         	       1	  11254917 ns/op	 710.80 MB/s	37419680 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_subject_string_bytes/delta_varint/lz4-8            	       1	  11696750 ns/op	 683.95 MB/s	37528808 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_text_bytes/raw_int64/none-8                        	       1	   4356833 ns/op	1836.20 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_text_bytes/raw_int64/snappy-8                      	       1	  10167291 ns/op	 786.84 MB/s	64081120 B/op	    2693 allocs/op
BenchmarkJSONBenchLocalColumns/record_text_bytes/raw_int64/lz4-8                         	       1	  15385709 ns/op	 519.96 MB/s	63220840 B/op	    2696 allocs/op
BenchmarkJSONBenchLocalColumns/record_text_bytes/delta_varint/none-8                     	       1	   9293041 ns/op	 860.86 MB/s	35076064 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_text_bytes/delta_varint/snappy-8                   	       1	  12021917 ns/op	 665.45 MB/s	37294560 B/op	    2693 allocs/op
BenchmarkJSONBenchLocalColumns/record_text_bytes/delta_varint/lz4-8                      	       1	  12451666 ns/op	 642.48 MB/s	37263144 B/op	    2696 allocs/op
BenchmarkJSONBenchLocalColumns/record_type_code/raw_int64/none-8                         	       1	   4845417 ns/op	1651.04 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_type_code/raw_int64/snappy-8                       	       1	  17150500 ns/op	 466.46 MB/s	65920992 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_type_code/raw_int64/lz4-8                          	       1	  18924042 ns/op	 422.74 MB/s	65060712 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/record_type_code/delta_varint/none-8                      	       1	   8773958 ns/op	 911.79 MB/s	34919840 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/record_type_code/delta_varint/snappy-8                    	       1	  12360875 ns/op	 647.20 MB/s	37107360 B/op	    2722 allocs/op
BenchmarkJSONBenchLocalColumns/record_type_code/delta_varint/lz4-8                       	       1	  12788042 ns/op	 625.58 MB/s	37216488 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/row_index/raw_int64/none-8                                	       1	   3988583 ns/op	2005.72 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/row_index/raw_int64/snappy-8                              	       1	  12506834 ns/op	 639.65 MB/s	58051808 B/op	    2601 allocs/op
BenchmarkJSONBenchLocalColumns/row_index/raw_int64/lz4-8                                 	       1	  17798292 ns/op	 449.48 MB/s	57191528 B/op	    2604 allocs/op
BenchmarkJSONBenchLocalColumns/row_index/delta_varint/none-8                             	       1	   6681625 ns/op	1197.31 MB/s	35074784 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/row_index/delta_varint/snappy-8                           	       1	   7073208 ns/op	1131.03 MB/s	36281312 B/op	    2601 allocs/op
BenchmarkJSONBenchLocalColumns/row_index/delta_varint/lz4-8                              	       1	   8146583 ns/op	 982.01 MB/s	36390376 B/op	    2604 allocs/op
BenchmarkJSONBenchLocalColumns/time_us/raw_int64/none-8                                  	       1	   3842917 ns/op	2081.75 MB/s	47920352 B/op	    2476 allocs/op
BenchmarkJSONBenchLocalColumns/time_us/raw_int64/snappy-8                                	       1	  16206791 ns/op	 493.62 MB/s	58117344 B/op	    2602 allocs/op
BenchmarkJSONBenchLocalColumns/time_us/raw_int64/lz4-8                                   	       1	  21899416 ns/op	 365.31 MB/s	57257064 B/op	    2605 allocs/op
BenchmarkJSONBenchLocalColumns/time_us/delta_varint/none-8                               	       1	   7455041 ns/op	1073.10 MB/s	38826336 B/op	    2599 allocs/op
BenchmarkJSONBenchLocalColumns/time_us/delta_varint/snappy-8                             	       1	   7631541 ns/op	1048.28 MB/s	41381600 B/op	    2725 allocs/op
BenchmarkJSONBenchLocalColumns/time_us/delta_varint/lz4-8                                	       1	   7975458 ns/op	1003.08 MB/s	41216744 B/op	    2725 allocs/op
PASS
ok  	github.com/snissn/gomap/experiments/colgranule	6.950s
```
