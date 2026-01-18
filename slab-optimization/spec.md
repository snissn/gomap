# Optimization Sprint Next (2026-01)
## Slab / Journal Endgame + Index Alignment  
### (with Separate TreeDB Dictionary Store)

This document is the **unabridged, authoritative sprint specification**.  
It integrates all agreed design decisions, including the use of a **separate TreeDB instance for dictionary storage** (“dictdb”), and replaces any prior draft.

The goal is to **land real storage optimizations**, not scaffolding.

Pre-alpha rules apply:
- Backward compatibility is **not required**.
- **Silent corruption is unacceptable**.

---

## 0) Sprint End State (What Must Exist on `main`)

By the end of this sprint, the following must be true:

1) **Journal durability semantics are enforced**
   - The DB reasons about durability in terms of a **Journal abstraction**, not a single WAL file.
   - Journal durable means:
     - Commit intent durable (CommitLog), and
     - Referenced payload bytes durable (ValueLog).

2) **Dictionary compression with dynamic K is on the write path**
   - Values in the ValueLog are written using **dictionary + dynamic-K grouped compression**.
   - K is chosen dynamically using existing code/reference.
   - This is the primary value encoding, not optional scaffolding.

3) **Dictionaries are stored in a separate TreeDB instance (dictdb)**
   - Compression disabled.
   - Immutable entries.
   - Same durability semantics as TreeDB.
   - No reserved namespace inside the main DB.

4) **RID-based join between CommitLog and ValueLog**
   - CommitLog does not depend on slab pointers.
   - Recovery joins commit intent to payload via RIDs.

5) **Parallel active write lanes exist**
   - Multiple ValueLog + CommitLog file pairs are active in parallel.
   - fsync is overlapped with continued writes.
   - Batch semantics are preserved via Journal durability.

6) **Recovery v1 is correct and hardened**
   - Two-pass recovery:
     1) ValueLog scan to build RID→location map.
     2) CommitLog replay to rebuild state.
   - Embedded values (inline B-tree values) are handled correctly.

7) **Index work proceeds against a stable storage contract**
   - Index optimizations do not assume unstable pointer or durability semantics.

If these outcomes are not present, the sprint is incomplete.

---

## 1) Hard Constraints (Non-Negotiable)

### C1 — No silent corruption
- All new formats/parsers must:
  - cap lengths before allocation,
  - fail closed on invalid data,
  - include fuzz coverage where applicable.

### C2 — No zone packing / 2 MB slab zones
- No physical zone layout constraints introduced.

### C3 — No new mmap on mutable or truncating files
- Do not add mmap usage for CommitLog or ValueLog segments.

### C4 — No ValueIndex / ValueID indirection
- No pointer swizzling or secondary indirection tables.

### C5 — Large-value redesign deferred
- No multi-record chunking or reassembly redesign in this sprint.

---

## 2) Baseline Reality (What Exists Today)

- TreeDB already has:
  - slab/value log files,
  - a WAL-like log,
  - batch semantics,
  - file rotation.
- There is an existing per-value zstd compressor (“envelope v0”) used as scaffolding.
- Some values may be stored inline in B-tree nodes.
- Rotation exists, but **multiple active write lanes in parallel** do not.

This sprint may refactor durability semantics, pointer semantics, and write paths, but must not introduce silent corruption.

---

## 3) Locked Storage Model

### 3.1 Journal Semantics (Before vs After)

#### Before
- “WAL durable” meant:
  - the WAL file was fsynced.
- ValueLog durability was implicit or assumed.
- Durability was reasoned about per-file.

#### After
- **Journal durable** is the DB-level concept.
- Journal durable means:
  - CommitLog durable **and**
  - referenced ValueLog bytes durable.
- ACK is gated on Journal durability.
- The DB no longer reasons about individual files.

Internally, Journal durability may be implemented using async buffers, rotation, or parallel lanes; externally, it is a single durability boundary.

---

### 3.2 Dictionary Storage System (Separate TreeDB Instance)

#### Intent
Store dictionaries using **TreeDB itself**, but as a **separate, dedicated database instance** (“dictdb”), not as keys inside the main DB.

This avoids:
- a second durability regime,
- new file formats,
- reserved namespaces in the main DB,
- coupling to user iteration or compaction.

---

#### DictDB Layout
```

<db_root>/
maindb/
dictdb/

```

- DictDB is a full TreeDB instance.
- Configuration:
  - compression disabled,
  - small cache / simple settings,
  - append-dominant workload.
- DictDB is expected to remain small and slow-moving.

---

#### DictDB Key Scheme (Normative)

All keys exist **only inside dictdb**.

1) **Dictionary bytes by ID**
- Key: `bytes/<u64_be dictID>`
- Value: raw dictionary bytes (uncompressed)
- Immutable once written.

2) **Hash → ID (deduplication)**
- Key: `hash/<32B sha256>`
- Value: `<u64_be dictID>`

3) **Current durable dictionary**
- Key: `current`
- Value: `<u64_be dictID>`

Optional:
- `meta/<u64_be dictID>` → small uncompressed metadata.

---

#### Lagging Dictionary Model (Locked Rule)

- Dictionary training happens off the main write path.
- A dictionary becomes eligible **only after it is durably committed to dictdb**.
- At main-DB transaction/batch start:
  - read (or use cached) `dictdb/current`,
  - freeze that dictID for the lifetime of the batch.
- A batch must not create and use a dictionary in the same batch.

**Hard invariant**  
> A value MUST NOT be written referencing `dictID = D` unless `D` is already durable in dictdb.

---

#### DictDB and Recovery Ordering

- DictDB is **outside** the main DB Journal.
- Recovery order:
  1) Recover/open dictdb (normal TreeDB recovery).
  2) Recover/open maindb (Journal recovery).
- During maindb recovery or reads, dict bytes are fetched from dictdb by dictID.
- Missing dictID during decode is a **hard error** (fail fast).

---

### 3.3 ValueLog Encoding (Dict + Dynamic-K Grouped Frames)

- ValueLog stores payload frames only.
- Primary encoding:
  - dictionary compression,
  - grouped frames with **dynamic K**.
- K selection:
  - dynamic,
  - deterministic given batch contents,
  - based on existing code/reference.
- Grouped frames carry a **RID**.

---

### 3.4 CommitLog Contents

CommitLog is the **commit authority stream**.

It stores:
- keys,
- RIDs,
- batch/transaction boundaries,
- **embedded values when required** (e.g. inline B-tree values).

CommitLog does **not** require slab pointers.

---

### 3.5 Parallel Active Write Lanes

#### Intent
Enable real async throughput by overlapping fsync with continued writes.

#### Model
- Maintain **N active lanes** in parallel:
  - ValueLog lane i,
  - CommitLog lane i.
- Each batch is assigned to one lane.
- While one lane is sealing/fsyncing, others remain writable.

This is a throughput optimization only; semantics do not change.

#### Lane Count Selection
- Keep policy simple.
- Acceptable:
  - fixed small N (e.g. 2–4),
  - or a one-off perf probe to recommend N.
- No complex runtime autotuning in this sprint.

---

## 4) Write Path (Target Behavior)

1) Begin batch.
2) Read/freeze `dictdb/current` dictID.
3) Generate RIDs for logical records or grouped frames.
4) Append grouped, dict-compressed payloads to ValueLog (assigned lane).
5) Append commit record to CommitLog (same lane):
   - keys,
   - RIDs,
   - embedded values if needed,
   - batch boundary.
6) ACK only after **Journal durability** for that batch:
   - CommitLog durable,
   - ValueLog bytes durable.

---

## 5) Recovery v1 (Simple, Correct)

1) **Pass 1 — ValueLog scan**
   - Scan ValueLog segments (all lanes).
   - Build `RID → (lane, segment, offset, len)` map.
   - Stop at first torn/invalid tail per segment.

2) **Pass 2 — CommitLog replay**
   - Replay CommitLog segments.
   - Apply committed batches:
     - resolve RIDs via map,
     - apply embedded values directly.

3) **Finalize**
   - Rebuild index state.
   - Ignore unreferenced ValueLog bytes.

Correctness is prioritized over recovery speed.

---

## 6) PR Plan (Executable Milestones)

### PR0 — Benchmarks and Lane Probe
- Stable benchmark entry points.
- Simple lane-count probe harness.
- No behavior change.

### PR1 — Journal Abstraction
- Replace WAL-centric durability with Journal durability.
- Crash-injection tests for ACK invariants.

### PR2 — DictDB (Separate TreeDB Instance)
- Implement dictdb instance.
- Compression disabled.
- Dict promotion + lagging rule.
- Unit tests for dict lifecycle.

### PR3 — RID-Based Join
- RIDs in ValueLog frames.
- CommitLog references RIDs.
- Embedded values supported.
- Recovery v1 works for basic cases.

### PR4 — Dict + Dynamic-K on Write Path
- Grouped frames written by default.
- Dynamic K selection integrated.
- Stored-bytes reduction demonstrated on compressible data.

### PR5 — Parallel Active Lanes
- N active ValueLog/CommitLog pairs.
- Batch-scoped durability tickets.
- Throughput improvement demonstrated.

### PR6 — Recovery Hardening
- Two-pass recovery default-safe.
- Fuzz coverage for parsing.
- Crash tests for edge cases.

### PR7+ — Index Work (Aligned)
- Columnar leaves / internal nodes behind flags.
- Must respect stable storage contract.

---

## 7) Definition of Done

The sprint is complete only if:

1) DictDB exists as a separate TreeDB instance and is used.
2) Dict + dynamic-K compression is on the write path.
3) Journal durability is enforced universally.
4) Parallel lanes provide measurable throughput gains.
5) Recovery v1 is correct and hardened.
6) Index work is aligned with the new storage contract.

---

## 8) Explicit Non-Goals

- Payload duplication into CommitLog.
- Zone packing.
- Large-value chunking redesign.
- Complex lane autotuning.
- New bespoke on-disk formats for dictionaries.

---

**This document supersedes all prior drafts.**
