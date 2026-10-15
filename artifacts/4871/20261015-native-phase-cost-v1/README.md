# One public native prune entry-phase cost observation

Runtime production sources: 7df5c8277ace70d8551417d92e8fe4a84842f472.
Only the diagnostic test changes: test.go is the exact measured 10224B source.
Before/after full4343 source censuses match; other4342 match the checkpoint.
One successful same 1024-row multileaf pointer public fixture, count1.
ACK1,2276 intervening durable writes, prepared1;36of37 child refs retained;
old-cut/current/postACK/checkedClose/reopen all pass.

Total2277 public calls:5173256ns,2401480B,3423alloc.
Entry phases6-11:2227calls,4234804ns,256816B,2341alloc (81.9% of time).
Phase0+1:1099176B in2calls (45.8% of total allocation bytes).
Phase16 publication:704us,1024408B,1001alloc.
Whole calls are bucketed by cursor phase AT ENTRY; this is not exclusive CPU
attribution to inner phases. MemStats is process-wide; fixed scalar arrays.
Single functional sample only. No qualified speedup, Q32, native production,
final correctness, cost, platform, review or CI acceptance. Native bounded
phase16 still refuses and the diagnostic publishes exactly one deletion.

Decision: remove the measured whole-namespace prerequisite in favor of truthful
per-selected validation and streaming continuation, preserving anchor/tombstone,
CRC/value/key/floor/provenance, fresh producer, original authority and lifetime.
Legacy pruning already permits earlier batch ACK before a later scan error.
The existing protected fixture and all final gates remain unchanged.
