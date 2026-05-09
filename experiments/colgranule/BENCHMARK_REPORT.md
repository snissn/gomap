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
