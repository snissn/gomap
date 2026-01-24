# B+Tree on Top of Gomap – Design Spec

This document describes how to implement a **B+Tree** as a logical index layer on top of **Gomap** (or any similar key–value store). The goal is to build an ordered index (range scans, prefix scans, etc.) while using Gomap purely as a durable page/key–value store.

The spec is written so that a future implementation can live in a separate package (e.g. `btree/`) and depend only on Gomap’s public API.

---

## 1. Goals and Non‑Goals

### Goals

- Provide an ordered index with:
  - `Get(key)`
  - `Put(key, value)`
  - `Delete(key)`
  - Range iteration: `Iter(startKey, endKey)` / prefix scans.
- Use Gomap as the only persistent storage:
  - All B+Tree nodes are stored as Gomap key/value entries.
  - No separate files or direct disk I/O APIs.
- Keep the B+Tree logic in Go (no mmap tricks in this layer).
- Support multiple independent trees (namespaced by tree ID).

### Non‑Goals (for first iteration)

- Transactions across multiple trees.
- MVCC / snapshot isolation.
- Concurrency beyond a coarse per‑tree mutex (fine‑grained locking can be added later).
- On‑disk backward compatibility guarantees (this spec defines the initial format).

---

## 2. Data Model and Key Mapping

We treat Gomap as a generic KV engine:

- `gomap.HashmapDistributed` (or a single `gomap.Hashmap`) is the backing store.
- The B+Tree layer never inspects slab layout; it only uses:
  - `Add(key, value)`, `Get(key)`, `Delete(key)` (and optionally `AddMany`).

### 2.1 Tree Identification

Each logical B+Tree has a **tree ID**:

- `treeID` is an opaque string (e.g. `"accounts"`, `"orders-by-date"`).
- All nodes and metadata keys are namespaced under this ID.

### 2.2 Node IDs and KV Keys

Each B+Tree node is assigned a 64‑bit **NodeID**:

- `type NodeID uint64`
- Node IDs are unique per tree and monotonically allocated from a counter stored in metadata (see 2.3).

We map a node to a Gomap key as:

- `btree:<treeID>:node:<nodeID>`
  - Example: `btree:accounts:node:42`

Metadata is stored as:

- `btree:<treeID>:meta`

### 2.3 Metadata Layout

The metadata value (at `btree:<treeID>:meta`) contains:

- `RootNodeID   NodeID`
- `Height       uint16` (number of levels; leaves are at height 1)
- `NextNodeID   NodeID` (counter for alloc)
- `Options` (optional, e.g. order/fan‑out, key/value size hints)

Encoding: a compact binary format (little‑endian) or msgpack; exact encoding is implementation detail but fixed once chosen.

---

## 3. Node Layout

We implement a **B+Tree**:

- Internal nodes contain only keys and child pointers.
- Leaf nodes contain keys and **values**.
- Leaves also form a linked list to support efficient range scans.

### 3.1 In‑Memory Node Structure

```go
type NodeID uint64

type NodeType uint8

const (
    NodeInternal NodeType = 1
    NodeLeaf     NodeType = 2
)

type BtreeNode struct {
    ID   NodeID
    Type NodeType

    // Common fields
    Keys [][]byte          // sorted, length = nKeys

    // Internal nodes
    Children []NodeID      // len(Children) = len(Keys)+1 if Type == NodeInternal

    // Leaf nodes
    Values   [][]byte      // len(Values) = len(Keys) if Type == NodeLeaf
    NextLeaf NodeID        // 0 if none
    PrevLeaf NodeID        // 0 if none (optional for backward scans)
}
```

### 3.2 Node Encoding

Nodes are stored as Gomap **values**:

- Key: `btree:<treeID>:node:<nodeID>`
- Value: serialized `BtreeNode` (without `ID`, which is implicit from the key).

Encoding guidelines:

- Fixed‑width header:
  - `Type`, `nKeys`, `nChildren`, `NextLeaf`, `PrevLeaf`.
- Then keys and children/values in order:
  - `[keyLen][keyBytes]...`
  - `[childID]...` or `[valueLen][valueBytes]...`

The exact encoding (varints vs fixed) is flexible; the spec only requires:

- Keys remain sorted within a node.
- Children/values align with `Keys` as described above.

---

## 4. B+Tree Operations

We assume a per‑tree struct:

```go
type Btree struct {
    treeID string
    kv     KVStore // wrapper over gomap (see 5)

    // cached metadata
    root    NodeID
    height  uint16
    nextID  NodeID

    mu sync.RWMutex // coarse per-tree lock (first version)
}
```

Where `KVStore` is an interface:

```go
type KVStore interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
}
```

In practice, this is backed by `HashmapDistributed` or `Hashmap`.

### 4.1 Initialization

- `OpenBtree(treeID string, kv KVStore) (*Btree, error)`
  - `Get(metaKey)`:
    - If not found: create an **empty tree**:
      - Allocate a single empty leaf node (ID = 1).
      - Set `RootNodeID = 1`, `Height = 1`, `NextNodeID = 2`.
      - Store meta.
    - If found: decode metadata, set `root`, `height`, `nextID`.

### 4.2 Search (Get)

`Get(key)`:

1. Load metadata (`root`, `height`) from memory.
2. Walk down from the root:
   - Load node: `node := loadNode(nodeID)`.
   - Binary search `node.Keys` for `key`.
   - If `NodeInternal`:
     - Choose child index `i` (standard B+Tree search).
     - Set `nodeID = node.Children[i]` and continue.
   - If `NodeLeaf`:
     - If exact key found, return corresponding `Values[i]`, else return `nil`.

KV interactions: one `Get` per level of the tree (height is expected to be small).

### 4.3 Insert (Put)

`Put(key, value)`:

1. Acquire `Btree.mu` (write lock).
2. If tree is empty, ensure root is a leaf.
3. Call `insertIntoNode(rootID, key, value, height)` which returns:
   - Either `(noSplit, nil)` and updated nodes written to KV.
   - Or `(splitKey, newRightNodeID)` if the root was split.
4. If the root split:
   - Allocate a new root node:
     - `newRoot := NodeInternal{Keys: [splitKey], Children: [leftID, rightID]}`.
     - Store new root as a node with a fresh `NodeID`.
     - Update metadata: `RootNodeID = newRoot.ID`, `Height++`, `NextNodeID`.
5. Write updated metadata to KV (single `Put`).

`insertIntoNode(nodeID, key, value, level)` details:

- If `level == 1` (leaf level):
  - Load leaf.
  - Insert or replace `(key, value)` in `Keys`/`Values` (maintain sorted order).
  - If node size ≤ `maxKeys`, encode + `Put` and return “noSplit”.
  - If node size > `maxKeys`, **split**:
    - Choose a split point `m` (e.g. middle).
    - Left node: `Keys[:m]`, `Values[:m]`.
    - Right node: `Keys[m:]`, `Values[m:]`, `ID = allocNodeID()`.
    - Fix `NextLeaf`/`PrevLeaf` pointers.
    - `Put` both nodes.
    - Return `(splitKey = right.Keys[0], right.ID)`.

- If `level > 1` (internal node):
  - Load node.
  - Find child index `i` for `key`.
  - Recursively `insertIntoNode(node.Children[i], key, value, level-1)`:
    - If child did not split: just write back any modified child if needed, encode + `Put` this node, return “noSplit”.
    - If child split, we get `(splitKey, rightID)`:
      - Insert `splitKey` into this node’s `Keys` and `rightID` into `Children` after the old child.
      - If size ≤ `maxKeys`, encode + `Put` and return “noSplit”.
      - If size > `maxKeys`, split this internal node similarly to leaf split (but children partitioned accordingly) and return `(splitKey, rightInternalID)` to the caller.

All node updates are persisted by encoding and calling `kv.Put` on the corresponding node keys.

### 4.4 Delete

First version: **no merges / rebalancing** (lazy delete):

- Locate the leaf containing `key` (same as Get).
- Remove the key (and value) if present.
- Encode + `Put` the modified leaf.
- Do not attempt to merge underfull nodes; this simplifies correctness.
- Later versions can add:
  - Borrowing keys from siblings.
  - Merging underfull nodes.
  - Shrinking tree height when the root becomes a leaf.

Given our log-structured slab underneath, a bit of fragmentation at the node level is acceptable initially.

### 4.5 Range Iteration

`Iter(startKey, endKey)`:

1. Find the leaf where `startKey` would be:
   - Descend as in `Get`, selecting the first leaf whose keys are ≥ `startKey`.
2. Within that leaf:
   - Start at the first key ≥ `startKey`.
   - Yield keys/values while `key < endKey` (or matches the predicate).
3. Follow `NextLeaf` pointers:
   - Load `NextLeaf` node.
   - Continue scanning until out of range or `NextLeaf == 0`.

This pattern relies on the leaf linked list and keeps node I/O mostly sequential.

---

## 5. Gomap Integration

We wrap Gomap in a small KV interface. Two typical backends:

### 5.1 Single-Shard Backend

For simpler setups or tests:

```go
type GomapKV struct {
    h *gomap.Hashmap
}

func (g *GomapKV) Get(key []byte) ([]byte, error)   { return g.h.Get(key) }
func (g *GomapKV) Put(key, val []byte) error        { return g.h.Add(key, val) }
func (g *GomapKV) Delete(key []byte) error          { return g.h.Delete(key) }
```

### 5.2 Sharded Backend (Distributed)

Use `HashmapDistributed`:

```go
type GomapDistributedKV struct {
    h *gomap.HashmapDistributed
}

func (g *GomapDistributedKV) Get(key []byte) ([]byte, error)   { return g.h.Get(key) }
func (g *GomapDistributedKV) Put(key, val []byte) error        { return g.h.Add(key, val) }
func (g *GomapDistributedKV) Delete(key []byte) error          { return g.h.Delete(key) }
```

The B+Tree sees a flat logical keyspace; the distributed layer handles sharding.

---

## 6. Concurrency and Atomicity

### 6.1 First Version: Coarse Locking

- Each `Btree` has a `sync.RWMutex`:
  - `Get` / `Iter` take `RLock`.
  - `Put` / `Delete` take `Lock`.
- This provides **thread safety within a single process**.
- Underlying Gomap may have its own sharding/locking; the B+Tree layer is agnostic.

### 6.2 Node and Meta Updates

We rely on Gomap’s semantics:

- Writes to single keys are atomic.
- There is no multi-key transaction, so we accept:
  - Brief windows where a subset of modified nodes are written but meta is not yet updated.
  - Recovery strategy:
    - On startup, either:
      - Trust the last meta + nodes (best-effort), or
      - Optionally run a validation/rebuild routine (out of scope for v1).

To keep things robust:

- Always write nodes **before** writing the updated meta key.
- Meta acts as the “commit point” to a new root / layout.

---

## 7. Testing Plan

### 7.1 Unit Tests

- Basic operations:
  - Insert N ascending keys, then Get all.
  - Insert N random keys, then Get all.
  - Delete some keys, ensure they disappear.
- Structural invariants:
  - Keys in nodes are sorted.
  - Internal nodes have `len(Children) = len(Keys)+1`.
  - Leaf linked list traverses all keys in order.
  - Root height matches expected level.

### 7.2 Persistence Tests

- Insert a dataset, close Gomap/B+Tree, reopen:
  - Ensure all keys are still visible and ordered.
- Force at least one split at each level (multi-level tree), then reopen and verify structure.

### 7.3 Range Iteration Tests

- Insert known ranges, then:
  - Iterate `[start, end)` and confirm keys returned and in correct order.
  - Test empty ranges, prefix scans, and full scans.

### 7.4 Stress / Fuzz

- Random sequences of inserts/deletes with periodic:
  - Full scan to compare against an in-memory map used as oracle.
  - Close/reopen cycles to ensure persistence correctness.

---

## 8. Future Extensions

- Replace coarse `RWMutex` with latch coupling / node-level locks for higher concurrency.
- Add optional transactional semantics using an external WAL layered above Gomap.
- Support secondary indexes by storing (index key → primary key) mappings in the B+Tree and resolving via Gomap.
- Compress node pages (e.g. prefix compression on keys) before storing as Gomap values for better cache locality.
