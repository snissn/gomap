package caching

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
)

func makeOuterLeafTestKV(n int, valueBytes int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k%04d", i))
		vals[i] = bytes.Repeat([]byte{byte(i + 1)}, valueBytes)
	}
	return keys, vals
}

func TestBuildOuterLeafValueRecords_V2GroupsAndLookup(t *testing.T) {
	keys, vals := makeOuterLeafTestKV(32, 96)
	db := &DB{
		indexOuterLeafMode:        backenddb.IndexOuterLeafModeV2BlockPtr,
		outerLeafBlockTargetBytes: 1024,
		outerLeafBlockCodec:       0,
		outerLeafBlockRestart:     8,
	}

	records, groups, err := db.buildOuterLeafValueRecords(keys, vals)
	if err != nil {
		t.Fatalf("buildOuterLeafValueRecords: %v", err)
	}
	if len(records) == 0 {
		t.Fatalf("expected records")
	}
	if len(groups) != len(records) {
		t.Fatalf("groups mismatch: %d/%d", len(groups), len(records))
	}
	if len(records) >= len(keys) {
		t.Fatalf("expected grouping to reduce records: records=%d keys=%d", len(records), len(keys))
	}

	seen := make([]bool, len(keys))
	for i := range records {
		group := groups[i]
		if group.start >= group.end {
			t.Fatalf("empty group at record %d", i)
		}
		for srcPos := group.start; srcPos < group.end; srcPos++ {
			if srcPos < 0 || srcPos >= len(keys) {
				t.Fatalf("group index out of range: %d", srcPos)
			}
			val, ok, found, _, decErr := outerleaf.DecodeValueForKey(records[i].Value, keys[srcPos], nil)
			if decErr != nil {
				t.Fatalf("DecodeValueForKey(%d): %v", srcPos, decErr)
			}
			if !ok || !found {
				t.Fatalf("DecodeValueForKey(%d): ok=%v found=%v", srcPos, ok, found)
			}
			if !bytes.Equal(val, vals[srcPos]) {
				t.Fatalf("value mismatch for key %q", string(keys[srcPos]))
			}
			seen[srcPos] = true
		}
	}
	for i := range seen {
		if !seen[i] {
			t.Fatalf("missing source index %d", i)
		}
	}
}

func TestBuildOuterLeafValueRecords_V2NonMonotonicFallsBackSafely(t *testing.T) {
	keys := [][]byte{[]byte("k1"), []byte("k3"), []byte("k2")}
	vals := [][]byte{[]byte("v1"), []byte("v3"), []byte("v2")}
	db := &DB{
		indexOuterLeafMode:        backenddb.IndexOuterLeafModeV2BlockPtr,
		outerLeafBlockTargetBytes: 1024,
		outerLeafBlockCodec:       0,
		outerLeafBlockRestart:     8,
	}

	records, groups, err := db.buildOuterLeafValueRecords(keys, vals)
	if err != nil {
		t.Fatalf("buildOuterLeafValueRecords: %v", err)
	}
	if len(records) == 0 {
		t.Fatalf("expected records")
	}
	if len(groups) != len(records) {
		t.Fatalf("groups mismatch: %d/%d", len(groups), len(records))
	}

	covered := make(map[int]struct{}, len(keys))
	for i := range groups {
		group := groups[i]
		for srcPos := group.start; srcPos < group.end; srcPos++ {
			covered[srcPos] = struct{}{}
			val, ok, found, _, decErr := outerleaf.DecodeValueForKey(records[i].Value, keys[srcPos], nil)
			if decErr != nil {
				t.Fatalf("DecodeValueForKey(%d): %v", srcPos, decErr)
			}
			if !ok || !found || !bytes.Equal(val, vals[srcPos]) {
				t.Fatalf("lookup mismatch for source index %d", srcPos)
			}
		}
	}
	if len(covered) != len(keys) {
		t.Fatalf("covered=%d want=%d", len(covered), len(keys))
	}
}

func TestBuildOuterLeafValueRecords_V1NoGrouping(t *testing.T) {
	keys, vals := makeOuterLeafTestKV(8, 32)
	db := &DB{}
	records, groups, err := db.buildOuterLeafValueRecords(keys, vals)
	if err != nil {
		t.Fatalf("buildOuterLeafValueRecords: %v", err)
	}
	if len(records) != len(keys) {
		t.Fatalf("records=%d want=%d", len(records), len(keys))
	}
	if len(groups) != len(keys) {
		t.Fatalf("groups=%d want=%d", len(groups), len(keys))
	}
}
