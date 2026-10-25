package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeTextPointReader borrows a single named domain from the catalog's exact
// capture. Its stable storage and physical projection belong to the original
// cumulative operation receipt. Close drops all aliases before that receipt and
// read are released; it neither pins a new cut nor owns the enclosing read.
type nativeTextPointReader struct {
	scope    *nativeTextCatalog
	root     string
	cursor   nativeDomainCursor
	readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)
}

func (n *nativeTextCatalog) pointReader(root string) (*nativeTextPointReader, error) {
	r := n.read
	r.mu.Lock()
	defer r.mu.Unlock()
	domain, err := r.borrowedNamedDomain(n.relative(root))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil // Preserve the catalog's ordinary missing-root handling.
	}
	if err != nil {
		return nil, err
	}
	if err := n.admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextPointReader{}))) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))); err != nil {
		return nil, err
	}
	return &nativeTextPointReader{scope: n, root: root, cursor: nativeDomainCursor{read: domain}, readLeaf: r.readLeaf}, nil
}

func (p *nativeTextPointReader) get(key []byte) ([]byte, bool, error) {
	if p == nil || p.scope == nil {
		return nil, false, backenddb.ErrClosed
	}
	r := p.scope.read
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capture == nil || r.closing || r.failure != nil {
		return nil, false, backenddb.ErrClosed
	}
	entry, err := p.cursor.exactEntry(key, p.readLeaf, p.scope.admit)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, false, nil
	}
	value, err := p.scope.value(entry, p.root, key)
	return value, err == nil, err
}

func (p *nativeTextPointReader) close() error {
	if p == nil || p.scope == nil {
		return nil
	}
	r := p.scope.read
	r.mu.Lock()
	defer r.mu.Unlock()
	err := p.cursor.close()
	p.scope, p.readLeaf, p.root = nil, nil, ""
	return err
}
