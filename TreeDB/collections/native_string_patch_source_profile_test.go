package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// This discriminator captures actual production-shaped roots inside the real
// publisher's serialized pre-WAL preflight. It returns before assigning LSNs.
// Counts and constructor debits are evidence for a source profile, not a byte
// certificate or permission to open the closed finite publisher.
func TestNativeStringPatchCapturedSourceProfile(t *testing.T) {
	for _, population := range []int{4096, 16384} {
		t.Run(fmt.Sprintf("rows_%d", population), func(t *testing.T) {
			dir := t.TempDir()
			db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := cleanup(); err != nil {
					t.Error(err)
				}
			}()
			manager := NewCollectionManager(db)
			meta := r1MutationMeta5059(true)
			if _, err := manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			// Exact scalar/string values and ID widths from the accepted R1 fixture.
			for start := 0; start < population; start += 32 {
				rows := make([]map[string]any, 32)
				for j := range rows {
					i := start + j
					rows[j] = map[string]any{"id": fmt.Sprintf("doc-%012d", i), "email": fmt.Sprintf("user%012d@example.test", i),
						"city": fmt.Sprintf("city-%02d", i%8), "name": fmt.Sprintf("User %06d", i), "bio": strings.Repeat("x", 96),
						"age": int64(18 + i%67), "score": float64(i%1000) / 10, "active": i%2 == 0, "revision": int64(0)}
					if i%2 == 0 {
						rows[j]["optional"] = nil
					}
				}
				ids, retained, columns := r1MutationBatch5059(t, rows...)
				if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
					t.Fatal(err)
				}
			}
			if err := manager.FlushAll(); err != nil {
				t.Fatal(err)
			}
			schema := col.Meta().Options.ColumnStore.SchemaHash
			for _, tc := range []struct {
				name   string
				rows   int
				fields []string
				spread bool
			}{
				{"bio_1", 1, []string{"bio"}, false}, {"bio_32", 32, []string{"bio"}, false},
				{"email_1", 1, []string{"email"}, false}, {"email_32", 32, []string{"email"}, false},
				{"city_1", 1, []string{"city"}, false}, {"city_32", 32, []string{"city"}, false},
				{"combined_4x32", 128, []string{"bio", "email", "city"}, false},
				{"combined_4x32_spread", 128, []string{"bio", "email", "city"}, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					patches := make([]TypedStringPatch, tc.rows)
					for j := range patches {
						id := j
						if tc.spread {
							id = j * (population / tc.rows)
						}
						patches[j].ID = []byte(fmt.Sprintf("doc-%012d", id))
						for _, field := range tc.fields {
							value := strings.Repeat("y", 96)
							if field == "email" {
								value = fmt.Sprintf("changed%012d@example.test", id)
							}
							if field == "city" {
								value = fmt.Sprintf("town-%02d", id%8)
							}
							patches[j].Edits = append(patches[j].Edits, TypedStringEdit{Column: field, Value: value})
						}
					}
					plan, payload, result, err := col.buildTypedStringPatchPlan(patches, schema, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer plan.close()
					if result.ModifiedCount != tc.rows {
						t.Fatalf("modified=%d want=%d", result.ModifiedCount, tc.rows)
					}
					ordered, closeOrdered, err := buildRootDeltaBatchPublishInputsFromTables(plan.meta.Name, plan.rootNames, plan.deltaTables, plan.baseRootIDs, plan.policies)
					if err != nil {
						t.Fatal(err)
					}
					defer closeOrdered()
					raw, err := commitlog.EncodeCollectionTypedStringsPayload(payload)
					if err != nil {
						t.Fatal(err)
					}
					intent, err := db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionTypedStringsPatchV1, raw)
					if err != nil {
						t.Fatal(err)
					}
					roots := append([]string(nil), plan.rootNames...)
					rootIDs := cloneColumnPublishBaseRootIDs(plan.baseRootIDs)
					for _, name := range []string{collectionColumnManifestRootName(plan.meta.Name), collectionColumnRowLocatorRootName(plan.meta.Name)} {
						roots = append(roots, name)
						rootIDs[name] = plan.catalog.rootID(name)
					}
					input := columnWritePublishInput{meta: plan.meta, catalog: plan.catalog, operation: ColumnPublishOperationUpdate,
						sparseOnly: true, declaredRowsReady: true, documents: plan.nativeStringDocuments, rows: len(plan.nativeStringDocuments),
						rootNames: plan.rootNames, baseRootIDs: rootIDs, baseCommitSeq: plan.baseCommitSeq, baseSystemRoot: plan.baseSystemRoot}
					input.declaredRows = make([]columnDeclaredRow, len(input.documents))
					for i := range input.documents {
						input.declaredRows[i] = columnDeclaredRow{ID: input.documents[i].ID, Values: input.documents[i].declaredValues, Stored: input.documents[i].storedColumns}
					}
					before := db.State()
					beforeLSN := db.CommandWALNextLSN()
					stop := errors.New("source profile captured before WAL")
					type rootCapture struct {
						Name   string
						Ledger backenddb.PreparedOwnedPointLedger
						Debits uint64
					}
					captures := make([]rootCapture, 0, len(ordered))
					var base backenddb.PreparedRootPublicationBaseProfile
					var contexts []backenddb.PreparedRootPointProfile
					var system backenddb.PreparedRootPointProfile
					var refusal error
					var refusalRoot, refusalPhase string
					var refusalShape nativeSourceClosureShape
					var captureDiagnostic backenddb.PreparedOwnedPointCaptureDiagnostic
					preflight := func() error {
						base = db.PreparedRootPublicationBaseProfile()
						if err := checkPreparedInsertPublicationBase(base); err != nil {
							return err
						}
						for i := range ordered {
							ops := ordered[i].Delta.SortedEntries()
							if len(ops) == 0 {
								continue
							}
							var debit uint64
							owner, err := db.CapturePreparedOwnedPointRootWithDiagnostic(plan.snap, ordered[i].BaseRoot, ordered[i].StoragePolicy, ops,
								backenddb.PreparedOwnedPointLimits{MaxClosurePages: 8192, MaxClosureEntries: 8192, MaxOutputPages: preparedInsertPublisherMaxOutputPages, MaxDepth: 32, MaxKeyBytes: preparedInsertMaxIDBytes},
								func(n uint64) error { debit += n; return nil }, &captureDiagnostic)
							if err != nil {
								refusal = err
								refusalRoot = plan.rootNames[i]
								refusalPhase = "capture"
								refusalShape = nativeSourceClosureShapeAt(plan.snap, ordered[i].BaseRoot, ops)
								return stop
							}
							if _, err := owner.Prepare(ops); err != nil {
								owner.Close()
								refusal = err
								refusalRoot = plan.rootNames[i]
								refusalPhase = "prepare"
								refusalShape = nativeSourceClosureShapeAt(plan.snap, ordered[i].BaseRoot, ops)
								return stop
							}
							captures = append(captures, rootCapture{Name: plan.rootNames[i], Ledger: owner.Ledger(), Debits: debit})
							owner.Close()
						}
						var err error
						contexts, system, err = col.profilePreparedNativeStringLateRoots(input, roots, rootIDs)
						if err != nil {
							return err
						}
						return stop
					}
					_, _, err = db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, preflight, intent,
						func(backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaBatchPublishInput, error) {
							panic("source probe reached post-WAL context builder")
						},
						func(backenddb.CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
							panic("source probe reached post-WAL system builder")
						})
					if !errors.Is(err, stop) {
						t.Fatal(err)
					}
					if intent.AssignedLSN() != 0 || db.CommandWALNextLSN() != beforeLSN || db.State().CommitSeq != before.CommitSeq || db.State().SystemRootPageID != before.SystemRootPageID {
						t.Fatal("source preflight probe changed authoritative state")
					}
					packet := struct {
						Base              backenddb.PreparedRootPublicationBaseProfile
						Captures          []rootCapture
						Contexts          []backenddb.PreparedRootPointProfile
						System            backenddb.PreparedRootPointProfile
						Refusal           string
						FiniteAdmission   bool
						RefusalRoot       string
						RefusalPhase      string
						RefusalShape      nativeSourceClosureShape
						CaptureDiagnostic backenddb.PreparedOwnedPointCaptureDiagnostic
					}{Base: base, Captures: captures, Contexts: contexts, System: system, RefusalRoot: refusalRoot, RefusalPhase: refusalPhase, RefusalShape: refusalShape, CaptureDiagnostic: captureDiagnostic}
					if refusal != nil {
						packet.Refusal = refusal.Error()
					}
					encoded, _ := json.Marshal(packet)
					t.Log("NATIVE_SOURCE_PROFILE " + string(encoded))
					if refusal != nil && !tc.spread {
						t.Fatalf("target source capture refused: %v", refusal)
					}
					if refusal == nil {
						var output uint64
						for _, capture := range captures {
							output += capture.Ledger.Profile.OutputPages
						}
						for _, context := range contexts {
							output += context.OutputPages
						}
						output += system.OutputPages
						if output > preparedInsertPublisherMaxOutputPages {
							t.Fatalf("source output=%d ceiling=%d", output, preparedInsertPublisherMaxOutputPages)
						}
					}
				})
			}
		})
	}
}

type nativeSourceClosureShape struct {
	Pages, PagerInternal, PagerLeaf, ExternalLeaves, UntouchedUnaryInternal int
	DictionaryFrames, SnappyFrames, LZ4Frames, OtherCompressedFrames        int
	InspectionFailure                                                       string
}

func nativeSourceClosureShapeAt(snap *backenddb.Snapshot, root uint64, ops []batch.Entry) nativeSourceClosureShape {
	var result nativeSourceClosureShape
	var walk func(page.ChildRef, []batch.Entry, int, bool) error
	walk = func(ref page.ChildRef, local []batch.Entry, depth int, touched bool) error {
		result.Pages++
		if depth > 32 || result.Pages > 8192 {
			return errors.New("diagnostic closure capacity")
		}
		if ref.Kind == page.ChildRefLeafLog {
			result.ExternalLeaves++
			f, _, err := snap.PinnedValueLogFile(ref.Log.ValuePtr().FileID)
			if err != nil {
				return err
			}
			shape, err := valuelog.InspectCOWLeafRecord(f, ref.Log.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: valuelog.MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize})
			if err != nil {
				return err
			}
			if shape.DictID != 0 {
				result.DictionaryFrames++
			}
			if shape.Compressed {
				switch shape.Codec {
				case valuelog.BlockCodecSnappy:
					result.SnappyFrames++
				case valuelog.BlockCodecLZ4:
					result.LZ4Frames++
				default:
					result.OtherCompressedFrames++
				}
			}
			return nil
		}
		data, err := snap.Pager().Get(ref.Page)
		if err != nil {
			return err
		}
		n := node.NewNodeView(data)
		if n.Type() == page.PageTypeLeaf {
			result.PagerLeaf++
			return nil
		}
		if n.Type() != page.PageTypeInternal {
			return errors.New("diagnostic unknown node")
		}
		result.PagerInternal++
		count := n.Count()
		if !touched {
			if count == 1 {
				result.UntouchedUnaryInternal++
			}
			return nil
		}
		next := 0
		for i := uint16(0); i < count; i++ {
			_, child, err := n.GetInternalEntryRefView(i)
			if err != nil {
				return err
			}
			start := next
			if i+1 == count {
				next = len(local)
			} else {
				end, _, err := n.GetInternalEntryRefView(i + 1)
				if err != nil {
					return err
				}
				for next < len(local) && bytes.Compare(local[next].Key, end) < 0 {
					next++
				}
			}
			if err := walk(child, local[start:next], depth+1, start != next); err != nil {
				return err
			}
		}
		return nil
	}
	if root != 0 {
		if err := walk(page.PageChildRef(root), ops, 1, true); err != nil {
			result.InspectionFailure = err.Error()
		}
	}
	return result
}
