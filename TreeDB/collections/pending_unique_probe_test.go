package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestPendingUniqueReservationProbeIncludesPublishingQueuedAndMutable(t *testing.T) {
	const indexName = "email"
	activePrefix := collectionTestUniquePrefix(t, "active@example.com")
	queuedPrefix := collectionTestUniquePrefix(t, "queued@example.com")
	mutablePrefix := collectionTestUniquePrefix(t, "mutable@example.com")
	domain := &collectionWriteDomain{
		indexedPublishingUnits: []indexedFlushUnit{{
			uniqueValueRuns: map[string][]memtable.Table{
				indexName: {collectionTestUniqueRunTable(activePrefix)},
			},
		}},
		indexedFlushUnits: []indexedFlushUnit{{
			uniqueValueRuns: map[string][]memtable.Table{
				indexName: {collectionTestUniqueRunTable(queuedPrefix)},
			},
		}},
		uniqueValueRuns: map[string][]memtable.Table{
			indexName: {collectionTestUniqueRunTable(mutablePrefix)},
		},
	}
	domain.mu.Lock()
	domain.uniqueValueIndex = rebuildBufferedUniqueValueIndexes(pendingIndexedUniqueValueRunMapLocked(domain))
	domain.mu.Unlock()

	domain.mu.RLock()
	defer domain.mu.RUnlock()
	for _, tt := range []struct {
		name   string
		prefix []byte
	}{
		{name: "active", prefix: activePrefix},
		{name: "queued", prefix: queuedPrefix},
		{name: "mutable", prefix: mutablePrefix},
	} {
		if !pendingUniqueReservationProbeLocked(domain, indexName, tt.prefix) {
			t.Fatalf("pending unique probe missed %s reservation", tt.name)
		}
	}
	if pendingUniqueReservationProbeLocked(domain, indexName, collectionTestUniquePrefix(t, "durable@example.com")) {
		t.Fatal("pending unique probe reported absent durable-only value")
	}
}

func TestPendingUniqueReservationIndexCachesWriteLockRebuild(t *testing.T) {
	const indexName = "email"
	prefix := collectionTestUniquePrefix(t, "queued@example.com")
	domain := &collectionWriteDomain{
		indexedFlushUnits: []indexedFlushUnit{{
			uniqueValueRuns: map[string][]memtable.Table{
				indexName: {collectionTestUniqueRunTable(prefix)},
			},
		}},
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	index := pendingUniqueReservationIndexLocked(domain, indexName, true)
	if index == nil || !index.contains(prefix) {
		t.Fatal("rebuilt pending unique index missed queued reservation")
	}
	if domain.uniqueValueIndex[indexName] != index {
		t.Fatal("rebuilt pending unique index was not cached on write-lock path")
	}
}

func collectionTestUniqueRunTable(prefix []byte) memtable.Table {
	table := newCollectionRunTable(1)
	setCollectionRunValue(table, append([]byte(nil), prefix...), nil)
	table.Freeze()
	return table
}

func collectionTestUniquePrefix(tb testing.TB, value string) []byte {
	tb.Helper()
	encoded, err := encodeIndexScalar(IndexValueString, value)
	if err != nil {
		tb.Fatalf("encode value %q: %v", value, err)
	}
	_, prefix, err := appendIndexValuePrefixSlice(nil, encoded)
	if err != nil {
		tb.Fatalf("prefix value %q: %v", value, err)
	}
	return prefix
}
