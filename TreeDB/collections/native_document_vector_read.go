package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The operation borrows its original complete cut. Only fixed page scratch and
// sequential JSON/pointer workspace have independent temporary receipts; Close
// drops every scratch alias before refund and never closes the borrowed cut.
type nativeDocumentGraphRead struct {
	scope          *NativeJSONRead
	domain         nativeDomainRead
	scratch        *nativeDomainReadScratch
	lease          *memtable.COWExternalLease
	temporary      *memtable.COWExternalLease
	temporaryBytes uint64
	resume         [page.PageSize + 1]byte
}

func newNativeDocumentGraphRead(scope *NativeJSONRead, domain *nativeDomainState) (*nativeDocumentGraphRead, error) {
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDocumentGraphRead{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainReadScratch{})))
	lease, err := scope.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	r := &nativeDocumentGraphRead{scope: scope, scratch: &nativeDomainReadScratch{}, lease: lease}
	if domain != nil {
		r.domain, err = newNativeDomainRead(domain, scope.capture.Snapshot())
		if err != nil {
			r.Close()
			return nil, err
		}
	}
	return r, nil
}
func (r *nativeDocumentGraphRead) Close() {
	if r == nil {
		return
	}
	r.scope = nil
	r.domain = nativeDomainRead{}
	r.scratch = nil
	temp, lease := r.temporary, r.lease
	r.temporary, r.lease = nil, nil
	if temp != nil {
		temp.Close()
	}
	if lease != nil {
		lease.Close()
	}
}
func (r *nativeDocumentGraphRead) reserve(n uint64) error {
	n = memtable.COWAllocationCharge(n)
	if n <= r.temporaryBytes {
		return nil
	}
	var err error
	if r.temporary == nil {
		r.temporary, err = r.scope.capture.NativeAllocator().AdmitNativeBytes(n)
	} else {
		err = r.temporary.Resize(n)
	}
	if err == nil {
		r.temporaryBytes = n
	}
	return err
}
func (r *nativeDocumentGraphRead) get(key []byte) ([]byte, error) {
	if r.domain.domain == nil {
		return nil, tree.ErrKeyNotFound
	}
	entry, err := r.domain.getEntry(key, r.scratch, r.scope.readLeaf)
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}
	value := entry.Value
	if entry.Flags&node.FlagPointer != 0 {
		value, err = r.scope.readValue(entry.ValuePtr, r.reserve)
		if err != nil {
			return nil, err
		}
	}
	n, err := nativeTypedCheckedMul(uint64(len(value)), 64)
	if err != nil {
		return nil, err
	}
	if err = nativeTypedCheckedAdd(&n, 2048); err != nil {
		return nil, err
	}
	if err = r.reserve(n); err != nil {
		return nil, err
	}
	return value, nil
}
func (r *nativeDocumentGraphRead) scanNode(id int) (vectorIndexPersistNode, error) {
	var result vectorIndexPersistNode
	raw, err := r.get(vectorIndexNativeNodeKey(id))
	if err != nil {
		return result, err
	}
	n, err := nativeTypedCheckedMul(uint64(len(raw)), 64)
	if err != nil {
		return result, err
	}
	if err = r.reserve(n + 2048); err != nil {
		return result, err
	}
	return decodeNativeDocumentGraphNode(raw, json.Unmarshal)
}
func (r *nativeDocumentGraphRead) walk(visit func([]byte, []byte) error) error {
	if r.domain.domain == nil {
		return nil
	}
	var start []byte
	for {
		entry, ok, err := r.domain.seekGE(start, nil, r.scratch, r.scope.readLeaf)
		if err != nil || !ok {
			return err
		}
		// Keep the successor key independently of fixed page scratch, with one
		// reusable page-sized backing covered by the original fixed scratch lease.
		if len(entry.Key) > len(r.scratch.key) {
			return fmt.Errorf("collections: native graph record key exceeds page")
		}
		copied := copy(r.resume[:], entry.Key)
		key := r.resume[:copied]
		if entry.Flags&node.FlagTombstone == 0 {
			value := entry.Value
			if entry.Flags&node.FlagPointer != 0 {
				value, err = r.scope.readValue(entry.ValuePtr, r.reserve)
				if err != nil {
					return err
				}
			}
			if err = visit(key, value); err != nil {
				return err
			}
		}
		// Graph record keys cannot contain NUL after the complete canonical key;
		// the standard strict successor still handles arbitrary document ID bytes.
		r.resume[copied] = 0
		start = r.resume[:copied+1]
	}
}

func nativeDocumentGraphSchema(def VectorIndexDefinition) ([32]byte, error) {
	raw, err := json.Marshal(def)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

func nativeDocumentGenerationAtRead(scope *NativeJSONRead, base *nativeEngineVersion) (uint64, error) {
	key, err := nativeDocumentGenerationKey(scope.collection.incarnation)
	if err != nil {
		return 0, err
	}
	if base != nil && base.catalog != nil {
		var row memtable.COWRecord
		var found bool
		if base.catalog.pending != nil {
			row, found = base.catalog.pending.Get([]byte(key))
		}
		if !found {
			row, found = base.catalog.captured.get([]byte(key))
		}
		if found {
			entry := nativeDomainRecordEntry(row)
			if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
				return 0, fmt.Errorf("collections: invalid native document generation record")
			}
			return decodeRootID(entry.Value)
		}

	}
	token, ok := scope.capture.Snapshot().StateToken()
	if !ok {
		return 0, backenddb.ErrClosed
	}
	raw, err := scope.capture.Snapshot().GetAtRoot(token.SystemRootPageID, []byte(key))
	if err != nil {
		return 0, err
	}
	return decodeRootID(raw)
}

// Full validation is cold Load/recovery work, not the mutation algorithm. It
// streams canonical records and two neighbor headers at a time. It authenticates
// BOTH directions of the document mapping, contiguity, edges, tombs and totals.
// The callback's returned records never enter g.nodes or a retained census.
func (g *nativeDocumentGraph) validateCold(scanNode func(int) (vectorIndexPersistNode, error), walk func(func([]byte, []byte) error) error) error {
	s := g.meta.NativeDocument
	var actual nativeDocumentGraphSummary
	actual.Incarnation = s.Incarnation
	highest := -1
	for ordinal := uint64(0); ordinal < s.Nodes; ordinal++ {
		id := int(ordinal)
		v, err := scanNode(id)
		if err != nil {
			return err
		}
		if v.DocumentID == "" || v.Level < 0 || v.Level > 32 || len(v.Vector) != g.config.dimensions || len(v.Quantized) != 0 || v.QuantScale != 0 {
			return fmt.Errorf("collections: invalid native graph node %d", id)
		}
		if err = validateFloat32Vector(v.Vector); err != nil {
			return err
		}
		if vectorNormSquared(v.Vector) == 0 {
			return fmt.Errorf("collections: zero native graph node")
		}
		actual.Nodes++
		if err = nativeTypedCheckedAdd(&actual.VectorIDBytes, uint64(len(v.DocumentID))+4*uint64(len(v.Vector))); err != nil {
			return err
		}
		tomb, err := g.get(vectorIndexNativeTombstoneKey(id))
		if v.Deleted {
			var t int
			if err != nil {
				return err
			}
			if err = json.Unmarshal(tomb, &t); err != nil {
				return err
			}
			if t != id {
				return fmt.Errorf("collections: wrong native graph tombstone")
			}
		} else {
			if err == nil {
				return fmt.Errorf("collections: live native graph node has tombstone")
			}
			if !errors.Is(err, tree.ErrKeyNotFound) {
				return err
			}
			raw, err := g.get(vectorIndexNativeDocKey(v.DocumentID))
			if err != nil {
				return err
			}
			var mapped int
			if err = json.Unmarshal(raw, &mapped); err != nil {
				return err
			}
			if mapped != id {
				return fmt.Errorf("collections: missing reverse native graph document mapping")
			}
			actual.Live++
			if v.Level > highest {
				highest = v.Level
			}
		}
		for layer := 0; layer <= v.Level; layer++ {
			raw, err := g.get(vectorIndexNativeEdgeKey(id, layer))
			if err != nil {
				return err
			}
			var edge vectorIndexPersistEdges
			// Adjacency is bounded by the declared M. This small sequential decode
			// uses the original admitted cold workspace, never a retained node map.
			if err = json.Unmarshal(raw, &edge); err != nil {
				return err
			}
			if edge.NodeID != id || edge.Layer != layer || len(edge.Neighbor) != len(edge.Distances) || len(edge.Neighbor) > g.config.maxNeighborsForLayer(layer) {
				return fmt.Errorf("collections: malformed native graph adjacency")
			}
			if err = nativeTypedCheckedAdd(&actual.Edges, uint64(len(edge.Neighbor))); err != nil {
				return err
			}
			for i, to := range edge.Neighbor {
				if to == id || to < 0 || uint64(to) >= s.Nodes {
					return fmt.Errorf("collections: native graph edge outside ordinal range")
				}
				if _, ok := normalizeVectorIndexEdgeDistance(edge.Distances[i]); !ok {
					return fmt.Errorf("collections: invalid native graph edge distance")
				}
				for j := 0; j < i; j++ {
					if edge.Neighbor[j] == to {
						return fmt.Errorf("collections: duplicate native graph edge")
					}
				}
				target, err := scanNode(to)
				if err != nil {
					return err
				}
				if target.Level < layer {
					return fmt.Errorf("collections: native graph neighbor layer mismatch")
				}
			}
		}
	}
	if highest != g.meta.MaxLevel {
		return fmt.Errorf("collections: wrong native graph highest live layer")
	}
	if actual != *s {
		return fmt.Errorf("collections: native graph summary does not match canonical records")
	}
	if s.Live != 0 {
		v, err := scanNode(g.meta.Entry)
		if err != nil {
			return err
		}
		if v.Deleted || v.Level != g.meta.MaxLevel {
			return fmt.Errorf("collections: invalid native graph entry")
		}
	}
	return walk(func(key, raw []byte) error {
		k := string(key)
		switch {
		case k == vectorIndexNativeKeyMeta:
			return nil
		case strings.HasPrefix(k, vectorIndexNativeKeyPrefixNode), strings.HasPrefix(k, vectorIndexNativeKeyPrefixTomb):
			prefix := vectorIndexNativeKeyPrefixNode
			if strings.HasPrefix(k, vectorIndexNativeKeyPrefixTomb) {
				prefix = vectorIndexNativeKeyPrefixTomb
			}
			id, ok := parseVectorIndexNativeOrdinal(strings.TrimPrefix(k, prefix))
			if !ok || uint64(id) >= s.Nodes {
				return fmt.Errorf("collections: noncanonical native graph ordinal")
			}
		case strings.HasPrefix(k, vectorIndexNativeKeyPrefixEdge):
			from, layer, ok := strings.Cut(strings.TrimPrefix(k, vectorIndexNativeKeyPrefixEdge), "/")
			id, valid := parseVectorIndexNativeOrdinal(from)
			if !ok || !valid || uint64(id) >= s.Nodes {
				return fmt.Errorf("collections: noncanonical native graph edge key")
			}
			v, err := scanNode(id)
			if err != nil {
				return err
			}
			found := false
			for i := 0; i <= v.Level; i++ {
				if layer == formatVectorIndexNativeEdgeLayer(i) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("collections: unexpected native graph edge layer")
			}
		case strings.HasPrefix(k, vectorIndexNativeKeyPrefixDoc):
			var id int
			if err := json.Unmarshal(raw, &id); err != nil {
				return err
			}
			if id < 0 || uint64(id) >= s.Nodes {
				return fmt.Errorf("collections: native graph mapping outside ordinal range")
			}
			v, err := scanNode(id)
			if err != nil {
				return err
			}
			if v.Deleted || !bytes.Equal([]byte(v.DocumentID), key[len(vectorIndexNativeKeyPrefixDoc):]) {
				return fmt.Errorf("collections: invalid native graph mapping")
			}
		default:
			return fmt.Errorf("collections: unknown native graph record %q", k)
		}
		return nil
	})
}
