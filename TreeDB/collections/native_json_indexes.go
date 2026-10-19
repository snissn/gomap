package collections

import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Domain numbers are derived only from the captured schema, exactly like the
// maintained planner's runtimeIdx. Physical roots never determine identity.
func nativeJSONDomainNumber(meta CollectionMeta, name string) uint64 {
	switch name {
	case "primary":
		return 1
	case "templates":
		return 2
	case "index-state":
		return 3
	case "column/manifest":
		if nativeTypedMeta(meta) {
			return 4
		}
	case "column/row-locator":
		if nativeTypedMeta(meta) {
			return 5
		}
	case "retained/semantic-stream-v1":
		if nativeTypedMeta(meta) && columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) {
			return 6
		}
	}
	for i, index := range meta.Indexes {
		if name == "index/"+index.Name {
			return uint64(i) + 4
		}
	}
	return 0
}

func nativeJSONInsertPlanner(meta CollectionMeta) (insertBatchPlanner, error) {
	options, err := collectionPlannerOptions(meta)
	if err != nil {
		return insertBatchPlanner{}, err
	}
	return insertBatchPlanner{collection: meta.Name, primaryRoot: collectionPrimaryRootName(meta.Name), templateRoot: collectionTemplateRootName(meta.Name), indexStateRoot: collectionIndexStateRootName(meta.Name), indexes: plannerIndexes(meta.Indexes), directBufferedRuns: true, options: options}, nil
}

// Reserve all additional scalar-plan scratch before the maintained planner
// parses or encodes indexed documents. MultiKey/compound schemas are fenced;
// each row has at most one value per index. The input-sized allowance covers
// JSON decoder nodes and coexisting growable encode/secondary/state arenas;
// fixed per-row headers cover planner items, unique probes and state refs.
// Existing whole-plan callers retain this allowance. Ordered native staging
// uses it only for reusable validation scratch; final output omits decoder debt.
func nativeJSONIndexedPlanCharge(meta CollectionMeta, ids, documents [][]byte) (uint64, error) {
	return nativeJSONIndexedCharge(meta, ids, documents, true, true)
}

// The final plan keeps encoded states/keys/entries, but never the decoder's
// temporary object graph. Fixed runtime/plan descriptors are admitted once.
func nativeJSONIndexedCharge(meta CollectionMeta, ids, documents [][]byte, headers, decoder bool) (uint64, error) {
	if len(meta.Indexes) == 0 {
		return 0, nil
	}
	var total uint64
	add := func(n uint64) bool {
		n = memtable.COWAllocationCharge(n)
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	mul := func(a, b uint64) (uint64, bool) {
		if b != 0 && a > ^uint64(0)/b {
			return 0, false
		}
		return a * b, true
	}
	perIndex := uint64(unsafe.Sizeof(indexDefinition{})) + uint64(unsafe.Sizeof(indexRuntime{})) + uint64(unsafe.Sizeof(directBufferedSecondaryRootPlan{})) + uint64(unsafe.Sizeof(directBufferedUniqueValueRootPlan{})) + uint64(unsafe.Sizeof(collectionUniqueProbeRun{})) + uint64(unsafe.Sizeof(CollectionSecondaryRunStats{}))
	n, ok := mul(uint64(len(meta.Indexes)), perIndex)
	if !ok || (headers && !add(n)) {
		return 0, memtable.ErrCOWCapacity
	}
	for i, doc := range documents {
		// Encoding strings can double escaped bytes; each of the value, secondary,
		// unique and index-state arenas may coexist with its growing predecessor.
		n, ok = mul(uint64(len(doc))+uint64(len(ids[i]))+uint64(len(meta.Name))+64, 16*uint64(len(meta.Indexes)))
		if !ok || !add(n) {
			return 0, memtable.ErrCOWCapacity
		}
		if decoder {
			n, ok = mul(uint64(len(doc)), 128)
			if !ok || !add(n) {
				return 0, memtable.ErrCOWCapacity
			}
		}
		headers := uint64(unsafe.Sizeof(uniqueProbeCandidate{})) + uint64(unsafe.Sizeof(directBufferedSecondaryRootEntry{})) + uint64(unsafe.Sizeof(directBufferedRootEntry{})) + 8*uint64(unsafe.Sizeof([]byte{}))
		n, ok = mul(uint64(len(meta.Indexes)), headers)
		if !ok || !add(n) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	if !add(uint64(estimateIndexEncodeArenaBytesForCount(len(ids), len(meta.Indexes))) * 2) {
		return 0, memtable.ErrCOWCapacity
	}
	for _, index := range meta.Indexes {
		if !add(uint64(len(index.Name)+len(index.Field)+len(meta.Name)) * 8) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}

// seekLive visits only one exact domain and borrows caller-admitted scratch.
// It skips newer tombstones without exposing an older shadowed entry.
func nativeJSONSeekLive(read *nativeDomainRead, probe *NativeJSONRead, start, end []byte, cursor []byte) (node.LeafEntry, bool, error) {
	scratch, err := probe.physicalScratch()
	if err != nil {
		return node.LeafEntry{}, false, err
	}
	for {
		entry, found, err := read.seekGE(start, end, scratch, probe.readLeaf)
		if err != nil || !found {
			return entry, found, err
		}
		if entry.Flags&node.FlagTombstone == 0 {
			return entry, true, nil
		}
		if len(entry.Key) >= len(cursor) {
			return node.LeafEntry{}, false, memtable.ErrCOWCapacity
		}
		n := copy(cursor, entry.Key)
		cursor[n] = 0
		start = cursor[:n+1]
	}
}

var errNativeIndexVisitComplete = errors.New("collections: native index visit complete")

func (r *NativeJSONRead) visitIndexValueIDs(name string, value any, visit func([]byte) error) (resultErr error) {
	if r == nil {
		return backenddb.ErrClosed
	}
	r.mu.Lock()
	if r.capture == nil || r.failure != nil || r.closing || r.visits == ^uint32(0) {
		r.mu.Unlock()
		return backenddb.ErrClosed
	}
	r.visits++
	defer func() {
		r.visits--
		closeRequested := r.closing && r.visits == 0
		r.mu.Unlock()
		if closeRequested {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	if r.capture == nil || r.failure != nil {
		return backenddb.ErrClosed
	}
	if err := ValidateIndexName(name); err != nil {
		return err
	}
	index, found := findIndex(r.meta.Indexes, name)
	if !found {
		return fmt.Errorf("%w: hybrid scalar filter index %q is unavailable: %w", ErrHybridSearchIndexUnavailable, name, ErrIndexNotFound)
	}
	if visit == nil {
		return errors.New("collections: nil index ID visitor")
	}
	// Public query buffers are caller-scoped; admit original temporary backing
	// before encoding and release it after all key/cursor aliases end.
	bytesNeeded := uint64(32)
	if text, ok := value.(string); ok {
		if uint64(len(text)) > (^uint64(0)-32)/2 {
			return memtable.ErrCOWCapacity
		}
		bytesNeeded += uint64(len(text)) * 2
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(3*memtable.COWAllocationCharge(bytesNeeded) + memtable.COWAllocationCharge(page.PageSize+1))
	if err != nil {
		return err
	}
	operation := func() error {
		encoded, err := encodeIndexScalar(index.ValueType, value)
		if err != nil {
			return err
		}
		end := prefixEnd(encoded)
		if r.collection == nil {
			return backenddb.ErrNativeSemanticUnsupported
		}
		valueType := index.ValueType
		index = IndexDefinition{}
		cursor := make([]byte, page.PageSize+1)
		start := encoded
		for {
			if r.capture == nil || r.failure != nil || r.closing {
				return backenddb.ErrClosed
			}
			domain, ok := r.collection.domains.Get("index/" + name)
			if !ok {
				return nil
			}
			read, err := newNativeDomainRead(domain, r.capture.Snapshot())
			if err != nil {
				return err
			}
			entry, found, err := nativeJSONSeekLive(&read, r, start, end, cursor)
			if err != nil || !found {
				return err
			}
			if !bytes.HasPrefix(entry.Key, encoded) {
				return nil
			}
			id, err := indexKeyDocumentID(valueType, entry.Key)
			if err != nil {
				return err
			}
			if len(entry.Key) >= len(cursor) {
				return memtable.ErrCOWCapacity
			}
			n := copy(cursor, entry.Key)
			cursor[n] = 0
			start = cursor[:n+1]
			// The callback receives an independent caller-owned ID. End every
			// borrowed domain/page alias before allowing reentrant Get or Close.
			id = bytes.Clone(id)
			entry, read, domain = node.LeafEntry{}, nativeDomainRead{}, nil
			err = func() error {
				r.mu.Unlock()
				defer r.mu.Lock()
				return visit(id)
			}()
			id = nil
			if err != nil {
				return err
			}
		}
	}
	err = operation()
	operation = nil
	lease.Close()
	return err
}

// Every unique absence is a range read in the same captured index namespace.
// Exact point writes from the maintained plan conflict with these ranges under
// the original history validation, before latest-state composition/assignment.
func checkNativeInsertUnique(c *nativeCollectionState, probe *NativeJSONRead, plan *directBufferedInsertPlan, track func(uint64, []byte, []byte) error) error {
	if len(plan.uniqueValueRootPlans) == 0 {
		return nil
	}
	var cursor [page.PageSize + 1]byte
	for _, run := range plan.uniqueValueRootPlans {
		domainNumber := nativeJSONDomainNumber(*c.schema.meta, "index/"+run.indexName)
		if domainNumber == 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		domain, hasDomain := c.domains.Get("index/" + run.indexName)
		for _, prefix := range run.prefixes {
			end := prefixEnd(prefix)
			if track != nil {
				if err := track(domainNumber, prefix, end); err != nil {
					return err
				}
			}
			if !hasDomain {
				continue
			}
			read, err := newNativeDomainRead(domain, probe.capture.Snapshot())
			if err != nil {
				return err
			}
			_, found, err := nativeJSONSeekLive(&read, probe, prefix, end, cursor[:])
			if err != nil {
				return err
			}
			if found {
				return fmt.Errorf("%w %q", ErrUniqueIndexConflict, run.indexName)
			}
		}
	}
	return nil
}
