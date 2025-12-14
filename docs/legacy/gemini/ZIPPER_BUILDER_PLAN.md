# Zipper Page Builder Optimization Plan

> Legacy note: this document was produced during an earlier “Gemini-era” planning workflow and may be outdated.

## 1. Problem: Inefficient Write Path
The current `zipper` implementation (and `node` methods) uses `AddLeafEntry` and `AddInternalChild` to construct new pages during the merge phase.
*   **Algorithm:** `AddLeafEntry` performs a **binary search** to find the insertion point, shifts the directory array to make space, and then writes the entry.
*   **Impact:** When merging `M` keys into a page with `N` existing keys (total `N+M`), the complexity is roughly $O((N+M)^2)$ due to repeated array shifting for each insertion.
*   **Context:** In the "zipper" merge, the keys (from old page and batch) are **already sorted**. We iterate through them in order.

## 2. Solution: Streaming Page Builder
Implement a `NodeBuilder` struct that constructs a page sequentially.
*   **Complexity:** $O(N)$ (Linear scan and append).
*   **Mechanism:**
    *   Append offsets to the directory (growing downwards).
    *   Append data to the heap (growing upwards).
    *   No binary search needed (we append in sorted order).
    *   No shifting needed (directory is built once).

## 3. Implementation Plan

### A. New Component: `node.Builder`
Create `gomap-gemini/TreeDB/node/builder.go`:
```go
type Builder struct {
    data      []byte
    dirCount  uint16
    dirOffset int // Current end of directory (grows up)
    heapOffset int // Current start of heap (grows down, or we track used space)
    pageType  page.PageType
}

func NewBuilder(buf []byte, pType page.PageType) *Builder
func (b *Builder) AddLeaf(key, val []byte, flags byte, ptr page.ValuePtr) error
func (b *Builder) AddInternal(key []byte, childID uint64) error
func (b *Builder) Finish(pageID uint64) *Node
```

### B. Refactor Zipper (`zipper/zipper.go`)
Update `mergeLeaf` and `mergeInternal` to use `NodeBuilder`.
*   **Old Logic:** `target.AddLeafEntry(...)` inside loop.
*   **New Logic:** `builder.AddLeaf(...)` inside loop.
*   **Splitting:** The Builder needs to handle "Page Full".
    *   `AddLeaf` returns `ErrNodeFull`.
    *   Zipper allocates a new page, creates a new Builder, and retries.

### C. Reference Implementation (`gomap`)
If approved, port this to `gomap/TreeDB` as well.

## 4. Expected Benefits
*   **CPU:** Massive reduction in instructions for large pages/batches.
*   **Throughput:** Higher write IOPS.
