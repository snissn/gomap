# WIP ordinary PRIMARY generation routing diagnostic

Exact parent2a46465e8b4aeb8c3fff13a610518d289aa85d53. Three production routing files and two regression/benchmark files only. This is an unmerged non-PR checkpoint, with no5111/native/sprint acceptance claim. Native producer routing provenance is retained in writer-frozen/. Ordinary synchronous BeginCandidate resets the origin flag only after inherited metadata setup, before any actual allocations.

Focused seven-mutator producer test and public durable/reopen test PASS. Six fixed128x AB/BA/AB measurements retain all samples. Median baseline523348ns/386908B/535objects and candidate503719ns/365932B/479objects; generation advances/op1→0 in every sample. The slower first baseline650631ns is retained. Setup/Stats/Close are outside timing. The fixture adds64 keys to33 initial keys to force a real DATA transition; it is not the original33 prune qualification fixture.

No new storage, arena or pool: scalar producer provenance only. Persistent output ownership, SAME live allocator, reservation ledger and publication authority remain maintained. No profiles, broad tests, tuning reruns, original-fixture changes or native closure claim. All3903 source/module inputs and both binary identities are bound before/after. Two ELFs are excluded; build recipes and hashes are included.
