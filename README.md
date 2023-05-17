data lego for high performance computing

possible use case is a situation where the key == value - in this case the data structure essentially operates as a string allocator where a string will be stored, and can be represented as an offset into the file

Then a separate instance of the datastructure can be used as a 


most blockchains use LSM trees and tout their write optimization. Hashmaps are much much more read optimized than lsms. from wikipedia `As increasingly more read and write workloads co-exist under an LSM-tree storage structure, read data accesses can experience high latency and low throughput due to frequent invalidations of cached data in buffer caches by LSM-tree compaction operations.`

on hashmaps read optimizations:

`In many situations, hash tables turn out to be on average more efficient than search trees or any other table lookup structure. For this reason, they are widely used in many kinds of computer software, particularly for associative arrays, database indexing, caches, and sets.`
