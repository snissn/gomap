package rootpublication

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

// IdentityCapacityTable is backing storage, not physical identity authority.
// It reuses the original admitted table, hash and geometric overlap accounting.
type IdentityCapacityTable[V any] struct {
	original registryTable[StableIdentity, V]
}

func NewIdentityCapacityTable[V any](owner *retainedalloc.Owner) IdentityCapacityTable[V] {
	return IdentityCapacityTable[V]{original: registryTable[StableIdentity, V]{owner: owner, hash: registryIdentityHash}}
}
func (table *IdentityCapacityTable[V]) Lookup(identity StableIdentity) (V, bool) {
	return table.original.lookup(identity)
}
func (table *IdentityCapacityTable[V]) Set(identity StableIdentity, value V) error {
	return table.original.set(identity, value)
}
func (table *IdentityCapacityTable[V]) Remove(identity StableIdentity) {
	table.original.remove(identity)
}
func (table *IdentityCapacityTable[V]) Next(after int) (int, StableIdentity, V, bool) {
	for i := after + 1; i < len(table.original.cells); i++ {
		cell := table.original.cells[i]
		if cell.status == 1 {
			return i, cell.key, cell.value, true
		}
	}
	var value V
	return 0, StableIdentity{}, value, false
}
func (table *IdentityCapacityTable[V]) Close() { table.original.close() }
