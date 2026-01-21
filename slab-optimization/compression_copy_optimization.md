# Compression Copy Optimization Plan

Goal: reduce user‑space copy amplification in the value‑log dict compression write path, without changing
memtable vs direct write semantics unless explicitly approved. Focus: mode4/mode3 dict‑on write throughput.

## Scope and Guardrails
- **Default:** only copy‑reduction refactors (no behavior changes).
- **Out of scope unless approved:** changing when/where the memtable is skipped or forcing direct streaming.
- **In scope with approval:** moving compression earlier in the write pipeline (e.g., compress before
  memtable), provided on‑disk format and durability semantics remain unchanged.

## Write Path Branches (API -> Disk)
1) **Memtable path (default, below streaming cutoff)**
   - kvstore/adapters/treedb.(*batch).Commit
   - caching.(*Batch).writeRegular -> Memtable.SetSteal
   - value‑log append (if enabled and value >= threshold), pointer stored in memtable.

2) **Streaming path (above cutoff)**
   - Same call chain, but skips memtable and writes directly to backend/value‑log.

3) **Value‑log writer (dict on/off)**
   - caching.DB.appendValueLog -> valuelog.Writer.AppendFrameWithStatsInto
   - dict‑on path: header+prefix build, compress, write header/prefix/payload.

4) **Journal / commitlog (mode3)**
   - appendWAL* / commitlog writes (secondary for dict copy work).

## Breadth‑First Profiling Matrix (must complete before depth‑first)
For each branch, capture a steady‑state CPU profile (after warmup) and identify copy hotspots.

A) **Below cutoff (memtable path)**
- **Must** run mode4 + dict‑on with ultra/highly compressible values (valsize=1KiB).
- **Must** profile via vlogprof harness (steady‑state) and tag memmove/memclr call stacks.
- **Must** categorize copy cost into:
  - memtable/skiplist arena copies
  - value‑log writer copies
  - zstd internal buffers

B) **Above cutoff (streaming path)**
- **Must** force streaming via batch size or payload size.
- **Must** profile and tag the same copy categories as in A.

C) **Mode3 (journal on)**
- **Must** repeat A/B with journal enabled, dict‑on.
- **Must** annotate additional journal/commitlog copy costs.

## Optimization Inventory (tagged by semantics)
### Safe (no semantics change)
1) **Direct‑to‑append buffer compression**
   - **Must** write header+prefix into appendBuf.
   - **Must** stream zstd output into appendBuf with size cap + rollback on non‑benefit.
   - **Must** compute CRC in‑place over header/prefix/encoded.
2) **Avoid payload concatenation for dict‑on**
   - **Must** remove raw concat copy when dict‑on in streaming/memtable paths.
3) **Reduce temporary allocations**
   - **Must** reuse per‑writer scratch buffers.
   - **Must** keep encoder scratch/limiter state in Writer.
4) **Reduce writeBytes copies**
   - **Must** avoid encode‑>scratch‑>appendBuf double copies.

### Behavioral (requires approval)
1) **Compress before memtable**
   - **Must** store only pointer in memtable for large values; no raw copy.
   - **Must** preserve durability ordering and read‑your‑write semantics.
2) **Alter streaming cutoff logic**
   - **Must** be treated as a behavior change with explicit approval.

## Depth‑First Execution Plan (after approval)
1) **Must** implement direct‑to‑appendBuf dict‑on path (Writer) with rollback safety.
2) **Must** validate correctness: valuelog tests + read‑back + corruption checks.
3) **Must** re‑profile below cutoff and confirm memmove drop in value‑log stack.
4) **Must** re‑run unified_bench vlog_dict suite and record ops/sec + MB/s deltas.

## Reporting / PR Hygiene
- Each PR: **1–2** focused changes, profile before/after, include bench table.
- **Must** use GH CLI to open PR.
- **Must** log results in PR comment.
- **Must** treat this document as an **append‑only work log**: add dated notes, profiles, and bench
  results as work proceeds (do not delete previous entries).
- **Must** execute Branches 1→2→3→4 **in order**, each as a separate PR **based on the previous PR’s
  branch** (waterfall). No skipping ahead without explicit approval.

---

## Branch 1 (Memtable Path) – Unabridged Plan (mode3 + mode4)

### Goal
Compare copy hotspots for the **memtable path** in both **mode3** (journal on, eager durability) and
**mode4** (journal off, value‑log eager/unsafe). They differ in timing of disk writes and can shift
where copies and stalls occur.

### Step 1: Confirm “below cutoff” (memtable path) in each mode
- **Must** choose workloads that do **not** trigger streaming (batchsize/value size below cutoff).
- **Must** validate via profile stack: `Memtable.SetSteal → SkipList.put` is present.
- **Must** confirm no streaming/direct‑write counters or “fast path” logs are triggered.
- **Must** keep dict‑on enabled with a compressible pattern (ultra + highly).

### Step 2: Profile mode4 (memtable path, dict‑on)
- **Must** use vlogprof harness (steady‑state after warmup).
- **Must** tag copy sites:
  - memtable arena copies (skiplist)
  - value‑log writer copies (payload concat, appendBuf copy, header/prefix construction)
  - zstd internal buffers

### Step 3: Profile mode3 (memtable path, dict‑on)
- **Must** run same workload with journal enabled.
- **Must** compare additional copy/alloc costs from journal/commitlog.

### Step 4: Synthesize the deltas
- **Must** document which copy hotspots are **common** (memtable) vs **mode‑specific**
  (value‑log writer vs journal/commitlog).
- **Must** tag each candidate fix as Safe vs Behavioral.

### Step 5: Prioritize copy reductions in value‑log writer
- **Must** implement direct‑to‑appendBuf compression (no raw concat copy, no encoded→appendBuf copy).
- **Must** re‑profile mode4 + mode3 to confirm the value‑log component shrinks.

### Step 6: Decide whether memtable path is now the dominant copy cost
- If memtable copies still dominate, **must** decide whether to:
  - accept baseline (no behavior change), or
  - propose “compress‑before‑memtable” (behavioral change, explicit approval required).

### Mode4 “compress earlier” option (behavioral; includes duplicate‑key guard)
If we pursue early compression in mode4, we **must** guard against overwrite‑heavy batches
so we don’t waste value‑log writes.

Approach:
- **Must** defer compression within the batch and only compress+write the **final** value per key.
- **Must** store pointers in memtable; avoid copying raw values for large entries.
- **Must** preserve the existing “deferred” advantage for duplicate keys.

Required perf case (pathological overwrite):
- Batch size: 1000
- Keys: 1 (or small set like 10)
- Values: 1KiB, highly compressible, small changing tail each write
- Metrics: wal_value_bytes_measure, ops/sec, observed_ratio
- **Expected:** ~1 value‑log write per key, not per update.

---

## Branch 2 (Streaming Path) – Unabridged Plan (mode3 + mode4)

### Goal
Analyze copy/IO hotspots when **streaming/direct‑write** is triggered (memtable is skipped), for both
mode3 (journal on) and mode4 (journal off).

### Step 1: Force streaming path in each mode
- **Must** choose input sizes that exceed the streaming cutoff.
- **Must** validate streaming path via logs/counters and by absence of `Memtable.SetSteal` in profile.

### Step 2: Profile mode4 streaming (dict‑on)
- **Must** capture steady‑state CPU profile.
- **Must** tag copy sites:
  - value‑log writer (payload concat, appendBuf copy, header/prefix construction)
  - backend batch writes
  - zstd internal buffers

### Step 3: Profile mode3 streaming (dict‑on)
- **Must** run same workload with journal enabled.
- **Must** compare additional copy/IO overhead from commitlog/journal writes.

### Step 4: Synthesize deltas + prioritize fixes
- **Must** identify which copy hot spots are common vs mode‑specific.
- **Must** build a **ranked optimization list** for streaming path with explicit targets:
  1) **Early compression path (behavioral, requires approval)**
     - **Must** attempt a design where compression happens *before* any intermediate copy, so only
       compressed bytes flow into the writer buffer.
     - **Mode4:** if approved, implement M4‑B (pointer‑only memtable) for streaming workloads.
     - **Mode3:** only consider M3‑B if durability ordering is unchanged and correctness tests pass.
     - **Must produce** a before/after copy‑count estimate and throughput delta from profiles.
  2) **Direct‑to‑appendBuf compression (safe)**
     - **Must** eliminate raw payload concatenation and encoded→appendBuf copies in the streaming path.
     - **Must** demonstrate reduced memmove in the value‑log writer stack and improved ops/sec.
  3) **Writev + iov batching for raw frames (safe)**
     - **Must** evaluate writev for dict‑off or fallback paths to reduce syscall count.
     - **Must** compare syscall count and avg write size vs non‑writev baseline.
  4) **Scratch reuse / pooling (safe)**
     - **Must** remove per‑batch allocations in writer and backend batch where possible.
     - **Must** show reduced allocs/op and GC time in profiles.
- For each candidate, **must** record:
  - expected copy reduction (bytes moved)
  - whether it changes semantics
  - validation test required (overwrite‑heavy batch, read‑back correctness, recovery)
  - measured perf delta (ops/sec and MB/s)

### Step 5: Validate against overwrite‑heavy input
- **Must** reuse the pathological overwrite pattern to ensure streaming path doesn’t amplify writes for
  duplicate‑key bursts.

---

## Branch 3 (Value‑Log Writer) – Unabridged Plan (dict on/off)

### Goal
Reduce copy amplification and allocations inside the value‑log writer, independent of memtable vs
streaming path, for both dict‑on and dict‑off modes.

### Success Criteria (must hit)
- **Must** reduce value‑log writer memmove share in steady‑state profiles (dict‑on) by a clear margin
  (target: ≥30% relative drop in writer‑stack memmove, or documented reason if not achievable).
- **Must** improve dict‑on throughput for highly/ultra‑compressible workloads (vlog_dict suite) without
  regressing dict‑off throughput or incompressible dict‑on paths.
- **Must not** trade correctness for speed: read‑back and corruption checks must pass.

### Step 1: Profile dict‑on writer path
- **Must** profile `AppendFrameWithStatsInto` using the vlogprof harness (steady‑state).
- **Must** annotate copy hot spots (payload concat, appendBuf copy, header/prefix construction).
- **Must** capture a memmove‑focused report (pprof focus=memmove) and attribute top call stacks.

### Step 2: Profile dict‑off / fallback writer path
- **Must** profile raw‑frame path (dict off or skip/fallback) to measure writev vs non‑writev behavior.
- **Must** collect syscall counts and avg write size if writev is enabled.
- **Must** record the breakpoint where writev is selected (avg value size threshold), and verify it
  improves syscall count rather than increasing it.

### Step 3: Implement copy‑elimination steps (safe)
- **Must** eliminate raw payload concatenation when dict‑on (direct‑to‑appendBuf).
- **Must** eliminate encoded→appendBuf copies (encode directly into appendBuf with rollback).
- **Must** reuse prefix/header scratch buffers and avoid per‑frame allocations.
- **Must** ensure rollback path is correctness‑safe (no partial writes; size/offset restored).

### Step 4: Validate correctness + measure deltas
- **Must** run valuelog read‑back tests and corruption checks after changes.
- **Must** report before/after ops/sec + memmove delta from pprof.
- **Must** show allocs/op improvements when possible (benchmem).

### Anti‑monkey‑paw guardrails
- **Not acceptable**: “optimizing” by disabling dict compression, reducing dict usage, or shrinking
  benchmark scope. The goal is **faster dict‑on** with same semantics.
- **Not acceptable**: moving work out of the measured window without fixing root copy cost
  (e.g., hiding copies in warmup).
- **Must** keep measurement inputs identical when claiming improvements.

---

## Branch 4 (Journal / Commitlog) – Unabridged Plan (mode3)

### Goal
Ensure journal/commitlog writes do not add unnecessary copy amplification on dict‑on write workloads.

### Success Criteria (must hit)
- **Must** show journal‑on (mode3) throughput improvements or clear parity vs baseline after copy fixes.
- **Must** not regress recovery correctness; crash/replay tests must pass.

### Step 1: Profile mode3 with dict‑on
- **Must** profile mode3 (journal on) with compressible data and compare against mode4.
- **Must** identify any copy/alloc hotspots in commitlog/WAL write path.
- **Must** attribute any copy cost to specific log record formatting or buffering steps.

### Step 2: Optimize journal copy behavior (safe)
- **Must** avoid duplicate payload copies in journal path if present.
- **Must** confirm durability order is unchanged.
- **Must** validate that any buffer reuse does not change record ordering or CRC semantics.

### Step 3: Validate correctness + measure deltas
- **Must** re‑run crash/recovery tests if journal path changes are made.
- **Must** report before/after throughput and allocation deltas.
- **Must** include a journal‑on bench slice (mode3) in the PR report.

### Anti‑monkey‑paw guardrails
- **Not acceptable**: “fixing” mode3 by disabling journaling or reducing durability.
- **Not acceptable**: reducing test scope to hide regressions; mode3 benches must be included.

---

## “Compress Earlier” Options — Mode3 vs Mode4

### Mode3 (journal on)
Constraints: durability order matters; journal + value‑log must remain consistent.

**Option M3‑A (safe, semantics‑preserving)**
- Compress at value‑log writer boundary (current behavior), but stream output directly into append buffer.
- Removes extra copies without changing ordering or durability.

**Option M3‑B (behavior change; likely OK with approval)**
- Pre‑compress before memtable insertion only for large values when value‑log pointers are used.
- Flow:
  1. Accept value
  2. Compress into value‑log buffer (or an owned buffer)
  3. Write compressed payload to value‑log
  4. Store pointer only in memtable (no raw copy)
  5. Journal/commitlog still records the pointer as before
- **Must** ensure:
  - Pointers are durable before memtable pointer becomes visible.
  - No regression in read‑your‑write semantics.

**Option M3‑C (not advised yet)**
- Compress before journal entry or store compressed value in journal (changes durability content).

### Mode4 (journal off)
Constraints: no durability guarantees beyond value‑log + backend flush; mode4 already unsafe.

**Option M4‑A (safe, semantics‑preserving)**
- Same as M3‑A: direct‑to‑appendBuf compression in writer.

**Option M4‑B (behavior change; likely acceptable for performance)**
- Pre‑compress and bypass memtable value copy for large values:
  - Compress → write value‑log → store pointer only in memtable.
- Faster because memtable never stores raw values.
- Requires explicit approval because memtable contents differ.

**Option M4‑C (aggressive)**
- For large batches below cutoff, allow “micro‑streaming” into value‑log with pointer‑only memtable insert.

## Recommended order (if behavior changes approved)
1) Implement M4‑B first (mode4 is unsafe anyway, so this gives fastest path quickly).
2) Validate correctness with read‑back + recovery tests (within mode4’s safety envelope).
3) If stable, consider M3‑B (needs more caution due to journal ordering).

---

## Pathological Overwrite Test / Perf Case (required when M4‑B is considered)

Purpose: guard against write amplification when a batch overwrites the same key(s) repeatedly.

Test description:
- Batch of 1000 writes
- Same key repeated, or a small set of keys (e.g., 10)
- Each value ~1KiB, highly compressible with a small evolving tail

Measurements:
- wal_value_bytes_measure
- ops/sec
- observed_ratio

Expected result:
- Deferred strategy writes ~1 value‑log entry per key, not 1000.

Suggested benchmark hook:
- Name: vlog_dict_overwrite_heavy
- batchsize=1000, keys=1 or 10, valsize=1024
- pattern: repeat + small incrementing tail
