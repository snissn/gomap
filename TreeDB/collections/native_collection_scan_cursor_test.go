package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func openNativeScanCursorFixture(t *testing.T, pointerThreshold int) (*treedb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.ValueLog.PointerThreshold = pointerThreshold
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "scan"}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("scan")
	if err != nil {
		t.Fatal(err)
	}
	return owner, manager, c
}

func TestNativeScanCursorSameCutAndCallbackOwnership(t *testing.T) {
	for _, threshold := range []int{1, 1 << 20} {
		t.Run(fmt.Sprintf("pointer_threshold_%d", threshold), func(t *testing.T) {
			owner, manager, c := openNativeScanCursorFixture(t, threshold)
			expected := make(map[string][]byte)
			for i := 0; i < 64; i++ {
				id := fmt.Sprintf("d%03d", i)
				doc := []byte(fmt.Sprintf(`{"value":%d,"payload":"%s"}`, i, strings.Repeat("x", 128)))
				expected[id] = bytes.Clone(doc)
				if _, err := c.Insert([]byte(id), doc); err != nil {
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
			if !old.domain.hasBase {
				t.Fatal("fixture did not establish a physical source")
			}
			currentExpected := make(map[string][]byte, len(expected))
			for id, doc := range expected {
				currentExpected[id] = doc
			}
			replacement := []byte(`{"value":999,"payload":"updated"}`)
			if _, _, err := c.Update([]byte("d001"), func([]byte) ([]byte, bool, error) { return replacement, true, nil }); err != nil {
				t.Fatal(err)
			}
			currentExpected["d001"] = bytes.Clone(replacement)
			if err := c.Delete([]byte("d003")); err != nil {
				t.Fatal(err)
			}
			delete(currentExpected, "d003")
			currentExpected["d000x"] = []byte(`{"value":1000}`)
			if _, err := c.Insert([]byte("d000x"), currentExpected["d000x"]); err != nil {
				t.Fatal(err)
			}
			current, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			budget := current.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			// Physical handoff after capture must not change either logical cut.
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name string
				read *NativeJSONRead
				want map[string][]byte
			}{{"old", old, expected}, {"current", current, currentExpected}} {
				t.Run(tc.name, func(t *testing.T) {
					var got []DocumentRecord
					changed := false
					truncated, err := tc.read.scanDocuments(nil, 128, true, func(record DocumentRecord) (bool, error) {
						got = append(got, record)
						// Reentrant point and nested scan can replace read scratch,
						// but cannot replace the outer cursor or its selected cut.
						point, err := tc.read.Get([]byte("d010"))
						if err != nil || !bytes.Equal(point, tc.want["d010"]) {
							return false, fmt.Errorf("same-cut Get: %q %w", point, err)
						}
						nested := 0
						_, err = tc.read.scanDocuments([]byte("d020"), 2, false, func(row DocumentRecord) (bool, error) {
							nested++
							if string(row.ID) != "d021" || row.Document != nil {
								return false, errors.New("nested scan lost its bound/projection")
							}
							return false, nil
						})
						if err != nil || nested != 1 {
							return false, fmt.Errorf("nested scan count=%d: %w", nested, err)
						}
						if !changed {
							changed = true
							if _, _, err := c.Update([]byte("d010"), func([]byte) ([]byte, bool, error) { return []byte(`{"value":2000}`), true, nil }); err != nil {
								return false, err
							}
						}
						return true, nil
					})
					if err != nil || truncated {
						t.Fatalf("scan truncated=%v err=%v", truncated, err)
					}
					var wantIDs []string
					for id := range tc.want {
						wantIDs = append(wantIDs, id)
					}
					sort.Strings(wantIDs)
					if len(got) != len(wantIDs) {
						t.Fatalf("rows=%d want=%d", len(got), len(wantIDs))
					}
					for i, record := range got {
						if string(record.ID) != wantIDs[i] || !bytes.Equal(record.Document, tc.want[wantIDs[i]]) {
							t.Fatalf("row %d=%q/%s want=%q/%s", i, record.ID, record.Document, wantIDs[i], tc.want[wantIDs[i]])
						}
					}
					// A second traversal must neither mutate prior outputs nor
					// interpret callback-retained buffers as cursor workspace.
					after := []byte("d060")
					var tail []string
					truncated, err = tc.read.scanDocuments(after, 2, false, func(row DocumentRecord) (bool, error) {
						tail = append(tail, string(row.ID))
						clear(after)
						if row.Document != nil {
							return false, errors.New("ID scan returned a document")
						}
						return true, nil
					})
					if err != nil || !truncated || !reflect.DeepEqual(tail, []string{"d061", "d062"}) {
						t.Fatalf("tail=%q truncated=%v err=%v", tail, truncated, err)
					}
					if err := tc.read.Close(); err != nil {
						t.Fatal(err)
					}
					for i, record := range got {
						if string(record.ID) != wantIDs[i] || !bytes.Equal(record.Document, tc.want[wantIDs[i]]) {
							t.Fatal("output changed after movement/Close")
						}
					}
				})
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("full Close leaked: %+v", stats)
			}
		})
	}
}

func TestNativeScanCursorCallbackCloseErrorAndAdmission(t *testing.T) {
	owner, _, c := openNativeScanCursorFixture(t, 1)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := c.Insert([]byte(id), []byte(`{"payload":"owned"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	sentinel := errors.New("callback stop")
	calls := 0
	_, err = read.scanDocuments(nil, 8, true, func(DocumentRecord) (bool, error) { calls++; return false, sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("callback error=%v calls=%d", err, calls)
	}
	if value, err := read.Get([]byte("b")); err != nil || string(value) != `{"payload":"owned"}` {
		t.Fatalf("read after callback error: %q %v", value, err)
	}
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err := blocker.Resize(available); err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	calls = 0
	_, err = read.scanDocuments(nil, 8, true, func(DocumentRecord) (bool, error) { calls++; return true, nil })
	after := budget.Stats()
	if !errors.Is(err, memtable.ErrCOWCapacity) || calls != 0 || after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
		t.Fatalf("refusal err=%v calls=%d before=%+v after=%+v", err, calls, before, after)
	}
	blocker.Close()
	calls = 0
	var retained DocumentRecord
	_, err = read.scanDocuments(nil, 8, true, func(record DocumentRecord) (bool, error) {
		calls++
		retained = record
		if err := read.Close(); err != nil {
			return false, err
		}
		if _, err := read.Get(record.ID); !errors.Is(err, backenddb.ErrClosed) {
			return false, errors.New("callback Close did not close new point reads")
		}
		return true, nil
	})
	if !errors.Is(err, backenddb.ErrClosed) || calls != 1 || read.capture != nil || string(retained.ID) != "a" || string(retained.Document) != `{"payload":"owned"}` {
		t.Fatalf("deferred Close err=%v calls=%d capture=%v retained=%+v", err, calls, read.capture != nil, retained)
	}
}

func TestNativeScanCursorPendingTypedAndReconstruction(t *testing.T) {
	owner, _, manager, c := openNativeTypedGroup(t, t.TempDir(), true)
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	ids, residuals, columns := nativeTypedGroupBatch("pending")
	if _, _, err := c.InsertTypedBatchWithStats(ids, residuals, columns); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	want := make(map[string][]byte)
	for _, id := range []string{"pending", "seed"} {
		doc, err := read.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		want[id] = doc
	}
	var got []DocumentRecord
	truncated, err := read.scanDocuments(nil, 4, true, func(row DocumentRecord) (bool, error) { got = append(got, row); return true, nil })
	if err != nil || truncated || len(got) != 2 {
		t.Fatalf("typed scan rows=%d truncated=%v err=%v", len(got), truncated, err)
	}
	if string(got[0].ID) != "pending" || string(got[1].ID) != "seed" {
		t.Fatalf("typed order: %+v", got)
	}
	for _, row := range got {
		if !bytes.Equal(row.Document, want[string(row.ID)]) {
			t.Fatalf("typed result=%s want=%s", row.Document, want[string(row.ID)])
		}
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, row := range got {
		if !bytes.Equal(row.Document, want[string(row.ID)]) {
			t.Fatal("typed output borrowed a closed read")
		}
	}
}
