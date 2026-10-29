package collections

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Exercise the real physical + logical domain merge, including a physical key
// shadowed by a memory tombstone and memory keys on either side of a retained
// physical candidate. Compare every entry/inspection with the existing seek.
func TestNativeDomainForwardCursorCapturedMerge(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	for _, id := range []string{"b", "d", "f"} {
		if _, err := c.Insert([]byte(id), []byte(`{"content":"old policy","tenant":"one"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err := c.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("b"), Update: func([]byte) ([]byte, bool, error) {
		return []byte(`{"content":"new policy","tenant":"one"}`), true, nil
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete([]byte("d")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "c", "e", "g"} {
		if _, err := c.Insert([]byte(id), []byte(`{"content":"new policy","tenant":"one"}`)); err != nil {
			t.Fatal(err)
		}
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	// A physical handoff while both cuts are retained must not change either.
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		read *NativeJSONRead
		want []string
	}{{"old", old, []string{"b", "d", "f"}}, {"mixed", current, []string{"a", "b", "c", "d", "e", "f", "g"}}} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.read
			lease, err := r.capture.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			var charged uint64
			admit := func(n uint64) error {
				if n > ^uint64(0)-charged {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charged + n); err != nil {
					return err
				}
				charged += n
				return nil
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			domain, err := r.borrowedNamedDomain("primary")
			if err != nil || !domain.hasBase {
				t.Fatalf("physical fixture: %v %v", domain.hasBase, err)
			}
			if tc.name == "mixed" && domain.domain.pending == nil && domain.domain.captured == nil {
				t.Fatal("fixture has no logical source")
			}
			scratch, err := r.physicalScratch()
			if err != nil {
				t.Fatal(err)
			}
			cursor := nativeDomainCursor{read: domain}
			defer cursor.close()
			var keys []string
			var start []byte
			for i := 0; ; i++ {
				plainProbes, streamProbes := 0, 0
				want, found, err := domain.seek(start, nil, scratch, r.readLeaf, false, func() error { plainProbes++; return nil })
				if err != nil {
					t.Fatal(err)
				}
				want.Key, want.Value = bytes.Clone(want.Key), bytes.Clone(want.Value)
				got, ok, err := cursor.read.seekWithCursor(start, nil, scratch, r.readLeaf, false, func() error { streamProbes++; return nil }, &cursor, i != 0, admit)
				if err != nil || ok != found || !reflect.DeepEqual(got, want) || plainProbes != streamProbes {
					t.Fatalf("step%d got=%+v want=%+v found=%v/%v probes=%d/%d err=%v", i, got, want, ok, found, streamProbes, plainProbes, err)
				}
				if !ok {
					break
				}
				keys = append(keys, string(got.Key))
				if tc.name == "mixed" && string(got.Key) == "d" && got.Flags&node.FlagTombstone == 0 {
					t.Fatal("physical value resurrected behind memory tombstone")
				}
				start = append(bytes.Clone(got.Key), 0)
			}
			if !reflect.DeepEqual(keys, tc.want) {
				t.Fatal(keys, tc.want)
			}
			physical := cursor.physical
			for _, key := range []string{"f", "b", "e", "a"} {
				want, found, err := domain.seekGE([]byte(key), nil, scratch, r.readLeaf)
				if err != nil {
					t.Fatal(err)
				}
				want.Key, want.Value = bytes.Clone(want.Key), bytes.Clone(want.Value)
				got, ok, err := cursor.read.seekWithCursor([]byte(key), nil, scratch, r.readLeaf, false, nil, &cursor, false, admit)
				if err != nil || ok != found || !reflect.DeepEqual(got, want) || cursor.physical != physical {
					t.Fatalf("arbitrary seek%q got=%+v want=%+v err=%v", key, got, want, err)
				}
			}
			if err := cursor.close(); err != nil || cursor.physical != nil || cursor.read.domain != nil {
				t.Fatal("cursor close did not detach owners", err)
			}
			bounded := nativeDomainCursor{read: domain, start: []byte("d"), end: []byte("g")}
			defer bounded.close()
			for _, key := range []string{"b", "f", "a", "z", "d"} {
				lower := []byte(key)
				if bytes.Compare(lower, bounded.start) < 0 {
					lower = bounded.start
				}
				want, found, err := domain.seekGE(lower, bounded.end, scratch, r.readLeaf)
				if err != nil {
					t.Fatal(err)
				}
				want.Key, want.Value = bytes.Clone(want.Key), bytes.Clone(want.Value)
				got, ok, err := bounded.read.seekWithCursor([]byte(key), bounded.end, scratch, r.readLeaf, false, nil, &bounded, false, admit)
				if err != nil || ok != found || !reflect.DeepEqual(got, want) {
					t.Fatalf("bounded seek%q got=%+v want=%+v err=%v", key, got, want, err)
				}
			}
			if err := bounded.close(); err != nil {
				t.Fatal(err)
			}
			// Refuse both construction admission and inspection before an owned
			// physical iterator exists; the same enclosing cut remains reusable.
			for _, refusal := range []string{"admission", "inspection"} {
				blocked := nativeDomainCursor{read: domain}
				inspect := func() error { return nil }
				reserve := admit
				if refusal == "admission" {
					reserve = func(uint64) error { return memtable.ErrCOWCapacity }
				} else {
					inspect = func() error { return errCollectionIndexScanWorkCap }
				}
				_, _, err := blocked.read.seekWithCursor(nil, nil, scratch, r.readLeaf, false, inspect, &blocked, false, reserve)
				if err == nil || blocked.physical != nil {
					t.Fatal(refusal, "constructed a refused cursor", err)
				}
				if refusal == "admission" && !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatal(err)
				}
			}
			if charged < memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize()) {
				t.Fatal("physical cursor not charged", charged)
			}
		})
	}
}
