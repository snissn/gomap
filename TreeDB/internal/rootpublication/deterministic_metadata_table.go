package rootpublication

// StableMetadataTable exposes the SAME stamped AVL used by resource registries
// and builders to their metadata producers. It has no mutex or credit owner.
// Callers own all keys/values and the enclosing control; this table stamps only
// its independently allocated nodes. The comparator must be immutable, and
// synchronization/admission belongs to the existing enclosing operation.
// Numeric/value keys do not retain any external ownership graph.
type StableMetadataTable[K any, V any] struct{ table stableTable[K, V] }

func NewStableMetadataTable[K any, V any](less func(K, K) bool) StableMetadataTable[K, V] {
	return StableMetadataTable[K, V]{table: newStableTable[K, V](less)}
}
func (t *StableMetadataTable[K, V]) Lookup(k K) (V, bool) { return t.table.lookup(k) }
func (t *StableMetadataTable[K, V]) Len() int             { return t.table.count }
func (t *StableMetadataTable[K, V]) Set(k K, v V, account StableMetadataAccount) error {
	p, err := t.table.prepare(k, v, account)
	if err != nil {
		return err
	}
	p.apply()
	return nil
}
func (t *StableMetadataTable[K, V]) Visit(fn func(K, V) bool) bool { return t.table.visit(fn) }
func (t *StableMetadataTable[K, V]) Clear()                        { t.table.clear() }
func (t *StableMetadataTable[K, V]) BackingCensus() BackingCensus  { return t.table.census }
