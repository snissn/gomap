package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// Keep the original operation receipt until the test has detached every cursor
// and decoded block. The fixture itself uses public inserts/rewrite/checkpoint.
func nativeTextPrivateRecordCatalog(t *testing.T, c *Collection) (*NativeJSONRead, *collectionCatalog) {
	t.Helper()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		_ = read.Close()
		t.Fatal(err)
	}
	var charged uint64
	catalog, err := newNativeTextCatalog(read, func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	})
	if err != nil {
		lease.Close()
		_ = read.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
		lease.Close()
	})
	return read, catalog
}

// Two scanners retain raw while a foreign cursor moves and point reads replace
// the read's shared scratch. Old cuts, physical inline records and memory rows
// must all obey the same lifetime through a Close request.
func TestNativeTextPrivateRecordCustody(t *testing.T) {
	for _, mode := range []string{"memory", "physical", "old_cut"} {
		t.Run(mode, func(t *testing.T) {
			owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			for i := 0; i < 12; i++ {
				id := fmt.Sprintf("%03d", i) + strings.Repeat("x", 61+i)
				doc := []byte(fmt.Sprintf(`{"content":"alpha beta policy policy","tenant":"one","payload":%q}`, strings.Repeat("p", 12000)))
				if _, err := c.Insert([]byte(id), doc); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.RewriteTextIndex("content", TextIndexRewriteOptions{TargetPostingsPerBlock: 4}); err != nil {
				t.Fatal(err)
			}
			if mode != "memory" {
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			read, catalog := nativeTextPrivateRecordCatalog(t, c)
			snap := read.capture.Snapshot()
			ctx, err := newTextV2SearchContext(snap, catalog, read.meta.TextIndexes[0], []string{"alpha", "beta"})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "old_cut" {
				if err := c.Delete([]byte("000" + strings.Repeat("x", 61))); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Insert([]byte("new"), []byte(`{"content":"gamma","tenant":"two"}`)); err != nil {
					t.Fatal(err)
				}
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			cache := textV2SearchBlockCache{}
			norm, err := cache.normBlockAt(snap, catalog, ctx, 1, nil)
			if err != nil || len(norm.Entries) != 12 {
				t.Fatalf("norm entries=%d err=%v", len(norm.Entries), err)
			}
			normLengths := append([]uint32(nil), norm.FieldLengths...)
			docmap, err := cache.docMapBlockAt(snap, catalog, ctx, 1)
			if err != nil || len(docmap.Entries) != 12 {
				t.Fatalf("docmap entries=%d err=%v", len(docmap.Entries), err)
			}
			var savedIDs []string
			for _, entry := range docmap.Entries {
				savedIDs = append(savedIDs, string(entry.DocumentID))
			}
			if err := catalog.nativeText.admitSingleTermServing(read.meta.TextIndexes[0], TextSearchOptions{TopK: 10}); err != nil {
				t.Fatal(err)
			}
			aPrefix, bPrefix := encodeTextV2PostingBlockTermPrefix("alpha"), encodeTextV2PostingBlockTermPrefix("beta")
			a, err := catalog.nativeText.iterator(ctx.postingBlocksRootName, aPrefix, prefixEnd(aPrefix), false, 0, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b, err := catalog.nativeText.iterator(ctx.postingBlocksRootName, bPrefix, prefixEnd(bPrefix), false, 0, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if !a.Valid() || !b.Valid() {
				t.Fatalf("posting iterators invalid: %v / %v", a.Error(), b.Error())
			}
			for _, candidate := range []*nativeTextIterator{a.(*nativeTextIterator), b.(*nativeTextIterator)} {
				if !candidate.heldVisit {
					t.Fatal("selected serving iterator did not retain its original read visit")
				}
				read.mu.Lock()
				scratch, err := read.physicalScratch()
				if err != nil {
					read.mu.Unlock()
					t.Fatal(err)
				}
				entry, err := candidate.cursor.read.getEntry(candidate.UnsafeKey(), scratch, read.readLeaf)
				read.mu.Unlock()
				if err != nil || entry.Flags&node.FlagPointer != 0 {
					t.Fatalf("posting fixture must exercise borrowed inline values: flags=%d err=%v", entry.Flags, err)
				}
				if mode != "memory" && candidate.cursor.physical == nil {
					t.Fatal("physical posting fixture never reached its owned projection")
				}
			}
			aRaw, bRaw := a.UnsafeValue(), b.UnsafeValue()
			aWant, bWant := bytes.Clone(aRaw), bytes.Clone(bRaw)
			aScan, err := newTextV2PostingBlockEntryScanner(aRaw, nil)
			if err != nil {
				t.Fatal(err)
			}
			bScan, err := newTextV2PostingBlockEntryScanner(bRaw, nil)
			if err != nil {
				t.Fatal(err)
			}
			var aEntry, bEntry textV2PostingBlockEntry
			if !aScan.Next(&aEntry) || !bScan.Next(&bEntry) || aEntry.Ordinal != bEntry.Ordinal {
				t.Fatal("initial posting decode disagreed")
			}
			// Point lookups replace scratch; an arbitrary backwards/aliased Seek
			// on one projection must leave the other scanner's raw untouched.
			for _, entry := range docmap.Entries {
				if _, ok, err := catalog.nativeText.get(ctx.docIDRootName, encodeTextV2DocIDKey(entry.DocumentID), nil); err != nil || !ok {
					t.Fatalf("foreign point read found=%v err=%v", ok, err)
				}
				fresh := textV2SearchBlockCache{}
				if _, err := fresh.normBlockAt(snap, catalog, ctx, 1, nil); err != nil {
					t.Fatal(err)
				}
			}
			b.Next()
			b.Seek(bPrefix)
			b.Seek(b.UnsafeKey())
			if !bytes.Equal(aRaw, aWant) || !aScan.Next(&aEntry) {
				t.Fatal("foreign movement or point decode invalidated first scanner")
			}
			// Rebuild b's scanner only after its own movement, as required by
			// UnsafeIterator. Both current values now survive a Close request.
			bRaw = b.UnsafeValue()
			bWant = bytes.Clone(bRaw)
			bScan, err = newTextV2PostingBlockEntryScanner(bRaw, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(aRaw, aWant) || !bytes.Equal(bRaw, bWant) || !bScan.Next(&bEntry) {
				t.Fatal("Close invalidated an active scanner")
			}
			aScan, aRaw = nil, nil
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(bRaw, bWant) || !bScan.Next(&bEntry) {
				t.Fatal("first iterator Close invalidated second scanner")
			}
			if !reflect.DeepEqual(norm.FieldLengths, normLengths) {
				t.Fatal("retained norm arrays changed")
			}
			for i, entry := range docmap.Entries {
				if string(entry.DocumentID) != savedIDs[i] {
					t.Fatal("owned docmap IDs changed after scratch reuse/Close")
				}
			}
			b.Next()
			if b.Valid() || !errors.Is(b.Error(), backenddb.ErrClosed) {
				t.Fatalf("movement after Close: valid=%v err=%v", b.Valid(), b.Error())
			}
			// No scanner/raw alias is used after this iterator's Close.
			aScan, bScan, aRaw, bRaw = nil, nil, nil, nil
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if read.capture != nil || read.visits != 0 {
				t.Fatal("last iterator failed to finish original Close")
			}
		})
	}
}

func TestNativeTextPrivateRecordAdmissionAndFailure(t *testing.T) {
	owner, manager, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	if _, err := c.Insert([]byte("a"), []byte(fmt.Sprintf(`{"content":"policy alpha","payload":%q}`, strings.Repeat("p", 12000)))); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var budget nativeHNSWAdmissionBudget
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		if budget != nil {
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("full Close retained receipts: %+v", stats)
			}
		}
	})
	normRoot := collectionTextV2NormBlocksRootName(c.name, "content")
	denied := errors.New("record admission denied")
	for _, fields := range []uint64{0, 1} {
		for _, rootKey := range []struct {
			name string
			root string
			key  []byte
		}{{"inline_norm", normRoot, encodeTextV2BlockKey(1)}, {"pointer_primary", c.name + "/primary", []byte("a")}} {
			// Each comparison arm is a complete operation. Unrelated synthetic
			// arms must not accumulate their conservative promises on one lease.
			lastAdmission := 0
			for _, mode := range []string{"success", "refuse_first", "refuse_last"} {
				t.Run(fmt.Sprintf("fields_%d/%s/%s", fields, rootKey.name, mode), func(t *testing.T) {
					denyAt := 0
					switch mode {
					case "refuse_first":
						denyAt = 1
					case "refuse_last":
						denyAt = lastAdmission
						if denyAt < 1 {
							t.Fatal("successful path made no admission")
						}
					}
					var traces [2][]uint64
					var values [2][]byte
					for path := 0; path < 2; path++ {
						if !t.Run(fmt.Sprintf("path_%d", path), func(t *testing.T) {
							read, catalog := nativeTextPrivateRecordCatalog(t, c)
							budget = read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
							scope := catalog.nativeText
							read.mu.Lock()
							defer read.mu.Unlock()
							domain, err := read.borrowedNamedDomain(scope.relative(rootKey.root))
							if err != nil {
								t.Fatal(err)
							}
							scratch, err := read.physicalScratch()
							if err != nil {
								t.Fatal(err)
							}
							entry, err := domain.getEntry(rootKey.key, scratch, read.readLeaf)
							if err != nil {
								t.Fatal(err)
							}
							if rootKey.name == "pointer_primary" && entry.Flags&node.FlagPointer == 0 {
								t.Fatal("pointer fallback fixture remained inline")
							}
							original := scope.admit
							local := nativeTextCatalog{read: read, singleTermFields: fields, admit: func(n uint64) error {
								traces[path] = append(traces[path], n)
								if denyAt != 0 && len(traces[path]) == denyAt {
									return denied
								}
								return original(n)
							}}
							var value []byte
							if path == 0 {
								value, err = local.value(entry, rootKey.root, rootKey.key)
							} else {
								value, err = local.iteratorValue(entry, rootKey.root, rootKey.key)
							}
							if denyAt == 0 && err != nil {
								t.Fatalf("path=%d trace=%v: %v", path, traces[path], err)
							}
							if denyAt != 0 && (!errors.Is(err, denied) || value != nil || len(traces[path]) != denyAt) {
								t.Fatalf("path=%d refusal=%d value=%x trace=%v err=%v", path, denyAt, value, traces[path], err)
							}
							// Test-owned comparison data survives the complete original
							// operation; no raw/product alias escapes its cleanup.
							values[path] = bytes.Clone(value)
						}) {
							return
						}
					}
					if !reflect.DeepEqual(traces[0], traces[1]) || !bytes.Equal(values[0], values[1]) {
						t.Fatalf("owned/borrowed admission or value differs traces=%v", traces)
					}
					if denyAt == 0 {
						lastAdmission = len(traces[0])
					}
				})
			}
		}
	}
	read, catalog := nativeTextPrivateRecordCatalog(t, c)
	budget = read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	scope := catalog.nativeText
	// Refuse after wrapper construction, while its held visit must roll back.
	// Shared physical scratch belongs to the original read, rather than the
	// refused iterator. Construct it before measuring iterator rollback.
	read.mu.Lock()
	_, scratchErr := read.physicalScratch()
	read.mu.Unlock()
	if scratchErr != nil {
		t.Fatal(scratchErr)
	}
	beforeVisits, beforeBudget := read.visits, budget.Stats()
	calls := 0
	var successfulCharge uint64
	blocked := nativeTextCatalog{read: read, singleTermFields: 1, admit: func(n uint64) error {
		calls++
		if calls != 1 {
			return denied
		}
		if err := scope.admit(n); err != nil {
			return err
		}
		successfulCharge += n
		return nil
	}}
	if it, err := blocked.iterator(normRoot, encodeTextV2BlockKey(1), nil, false, 0, nil, nil); it != nil || !errors.Is(err, denied) {
		t.Fatalf("construction refusal iterator=%v err=%v", it, err)
	}
	if after := budget.Stats(); read.visits != beforeVisits || after.ExternalLeases != beforeBudget.ExternalLeases || after.ReservedBytes != beforeBudget.ReservedBytes+successfulCharge {
		t.Fatalf("refused construction retained unexpected debt visits=%d/%d successfulCharge=%d before=%+v after=%+v", read.visits, beforeVisits, successfulCharge, beforeBudget, after)
	}
	if _, found, err := scope.normBlockSelected(normRoot, encodeTextV2BlockKey(1), nil); err != nil || !found {
		t.Fatalf("read unusable after refused construction: found=%v err=%v", found, err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if it, err := scope.iterator(normRoot, nil, nil, false, 0, nil, nil); it != nil || !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed read iterator=%v err=%v", it, err)
	}
}

// Even an empty selected output must validate the entire consumed norm record.
// Lookup counters distinguish misses, codec errors and pre-decode refusals.
func TestNativeTextPrivateNormCompleteValidation(t *testing.T) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	if _, err := c.Insert([]byte("a"), []byte(`{"content":"policy alpha"}`)); err != nil {
		t.Fatal(err)
	}
	root := collectionTextV2NormBlocksRootName(c.name, "content")
	raw := encodeTextV2NormBlockValue(textV2NormBlockValue{BlockStart: 1, BlockSize: textV2DefaultNormBlockSize, FieldCount: 1, Entries: []textV2NormBlockEntry{{Ordinal: 1, Generation: 1, FieldLengths: []uint32{2}}}})
	textV2ForwardPublishRecord(t, c, root, encodeTextV2BlockKey(1), append(raw, 0))
	read, catalog := nativeTextPrivateRecordCatalog(t, c)
	ctx := &textV2SearchContext{normBlocksRootName: root, fieldNames: []string{"content"}}
	stats := &TextSearchStats{}
	cache := textV2SearchBlockCache{allowed: &textV2SearchOrdinalAllowSet{}}
	if _, found, err := cache.normBlockAtOptional(read.capture.Snapshot(), catalog, ctx, 1, stats); found || !errors.Is(err, ErrTextIndexStorageCorrupt) || stats.TextNormLookups != 1 {
		t.Fatalf("excluded corrupt norm found=%v err=%v lookups=%d", found, err, stats.TextNormLookups)
	}
	if _, found, err := cache.normBlockAtOptional(read.capture.Snapshot(), catalog, ctx, uint64(textV2DefaultNormBlockSize)+1, stats); found || err != nil || stats.TextNormLookups != 2 {
		t.Fatalf("missing norm found=%v err=%v lookups=%d", found, err, stats.TextNormLookups)
	}
	original := catalog.nativeText.admit
	catalog.nativeText.admit = func(uint64) error { return memtable.ErrCOWCapacity }
	defer func() { catalog.nativeText.admit = original }()
	if _, found, err := cache.normBlockAtOptional(read.capture.Snapshot(), catalog, ctx, 1, stats); found || !errors.Is(err, memtable.ErrCOWCapacity) || stats.TextNormLookups != 2 {
		t.Fatalf("refused norm found=%v err=%v lookups=%d", found, err, stats.TextNormLookups)
	}
}
