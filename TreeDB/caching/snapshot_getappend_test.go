package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type snapshotPublishedValueLookup struct {
	value []byte

	getEntryCalls       int
	getValueAppendCalls int
	getValueUnsafeCalls int
}

func (l *snapshotPublishedValueLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	l.getEntryCalls++
	if string(key) != "k" {
		return nil, page.ValuePtr{}, 0, false
	}
	return l.value, page.ValuePtr{}, node.FlagInline, true
}

func (l *snapshotPublishedValueLookup) GetValueAppend(key, dst []byte) ([]byte, error) {
	l.getValueAppendCalls++
	if string(key) != "k" {
		return dst, tree.ErrKeyNotFound
	}
	return append(dst, l.value...), nil
}

func (l *snapshotPublishedValueLookup) GetValueUnsafe(key []byte) ([]byte, error) {
	l.getValueUnsafeCalls++
	if string(key) != "k" {
		return nil, tree.ErrKeyNotFound
	}
	return l.value, nil
}

type snapshotPublishedEntryOnlyLookup struct {
	value         []byte
	getEntryCalls int
}

func (l *snapshotPublishedEntryOnlyLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	l.getEntryCalls++
	if string(key) != "k" {
		return nil, page.ValuePtr{}, 0, false
	}
	return l.value, page.ValuePtr{}, node.FlagInline, true
}

func TestSnapshotGetAppendPublishedUsesValueAppendDirectly(t *testing.T) {
	lookup := &snapshotPublishedValueLookup{value: []byte("published")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(got) != "p:published" {
		t.Fatalf("value=%q, want p:published", got)
	}
	if lookup.getValueAppendCalls != 1 {
		t.Fatalf("GetValueAppend calls=%d, want 1", lookup.getValueAppendCalls)
	}
	if lookup.getEntryCalls != 0 {
		t.Fatalf("GetEntry calls=%d, want 0", lookup.getEntryCalls)
	}
}

func TestSnapshotGetAppendPublishedFallsBackToEntryLookup(t *testing.T) {
	lookup := &snapshotPublishedEntryOnlyLookup{value: []byte("published")}
	snap := &Snapshot{
		rootPointShards: []rootDomainSnapshot{{
			published:       lookup,
			publishedRootID: 1,
		}},
	}

	got, err := snap.GetAppend([]byte("k"), []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(got) != "p:published" {
		t.Fatalf("value=%q, want p:published", got)
	}
	if lookup.getEntryCalls != 1 {
		t.Fatalf("GetEntry calls=%d, want 1", lookup.getEntryCalls)
	}
}
