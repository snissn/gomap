# Fresh original allocator membership capability checkpoint

The nearest public entry is Store.CommitAt(..., CommitDurable) under ProfileCommandWALDurable with cow_btree and four shards. The tagged public capability test allocates actual private DATA through the original allocator, performs a real intervening public commit, then asks a fresh original allocator candidate and PreparedPrimary constructor to certify the exact generation/DATA/SYSTEM pair. It observes page 7, generation 3, and 19 actual membership/physical-proof records. A subsequent public commit and checked close complete.

Candidate membership rejects zero work, retired and foreign roots, wrong generation/candidate/root pair, and aborted candidates. Published snapshot export preserves the pinned retirement boundary. Actual page mutation at the PreparedPrimary binder rejects with the physical digest error; the page is restored and original transaction/candidate abort completes.

This API did not exist in the baseline public interface. This is a proportionate test-first capability exception with a contract-derived oracle; the v1 vet/formatting compile failure is setup-only, not a semantic RED. The existing protected public native-prune RED remains unchanged and binding.

Scope: allocator candidate prepare, diagnostic page allocation and arena sealing use existing unlimited diagnostic drivers. The 19 records cover only the observed membership/physical binding. This is not full native prune activation, final Q32 proof, O(1) birth certificate, or a performance result. No new registry or allocator is introduced; the witness grants no retention and original ownership remains required.

The final normal and race invocations each pass four top-level tests across three packages. Literal argv/env and explicit 90-second Go / 120-second outer deadlines are retained. The 4,336-file Go/module census includes retained artifact source copies; it is a complete filesystem source census, not a count of compiled production files. All bytes, HEAD, and status remain unchanged across race. Two foreign diagnostic tests remain untouched.

Next production integration: current-generation/cow=nil PRIMARY-only publication can avoid a second candidate only after a genuine producer has incorporated the private DATA output and exact SYSTEM membership into that current generation. Ordinary CWAL point append alone does not establish this. No repeated helper or benchmark campaign is authorized by this packet.
