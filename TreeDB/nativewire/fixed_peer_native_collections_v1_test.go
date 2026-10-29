package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The explicit native role must survive actual TCP submission, provider snapshot
// installation and a second writable lifetime, without closing its borrowed DB.
func TestFixedPeerTCPNativeCollectionsSnapshotLifecycleV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("Raft snapshot install requires durable rename and removal namespaces")
	}
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			c := fixedPeerTestConfigsV1(t)[0]
			c.Nodes, c.Catalog.Peers, c.Groups = c.Nodes[:1], c.Catalog.Peers[:1], c.Groups[:1]
			c.Groups[0].NativeCollections = true
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var r *FixedPeerTCPRuntimeV1
			t.Cleanup(func() {
				if r != nil {
					if err := r.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			open := func() {
				var err error
				r, err = fixedPeerOpenTestRuntimeV1(t, c)
				if err != nil {
					t.Fatal(err)
				}
				fixedPeerWaitV1(t, ctx, func() bool {
					s, err := r.Status(ctx)
					return err == nil && s.CatalogRaft.State == "Leader" && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
				})
				if r.reopened {
					fixedPeerWaitV1(t, ctx, func() bool { ready, err := r.ReadinessV1(ctx); return err == nil && ready.Ready })
				}
			}
			open()
			client, err := NewFixedPeerTCPClientV1(c)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			fixedPeerPublishNativeUsersCatalogV1(t, ctx, client, c.NodeID)
			var last raftcluster.SubmitResultV1
			submit := func(id iwire.CommandID, key string, sections []iwire.Section) {
				t.Helper()
				last = fixedPeerSubmitNativeCollectionV1(t, ctx, client, c.NodeID, id, key, sections)
			}
			meta := collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: format}, Indexes: []collections.IndexDefinition{{Name: "by_city", Field: "city", ValueType: collections.IndexValueString, Unique: true}}}
			submit(iwire.CommandCreateCollection, "native-create", []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: mustEncodeCollectionMeta(meta)}})
			document := func(city string) []byte {
				if format == collections.DocumentFormatJSON {
					return []byte(`{"city":"` + city + `"}`)
				}
				out, err := bson.Marshal(bson.D{{Key: "city", Value: city}})
				if err != nil {
					t.Fatal(err)
				}
				return out
			}
			old, updated, final := document("hnl"), document("sea"), document("sfo")
			mutation := func(id iwire.CommandID, key string, value []byte) {
				sections := []iwire.Section{collectionNameRef("users"), {ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))}}
				if value != nil {
					sections = append(sections, documentFormatSection(format), iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, value)})
				}
				if id == iwire.CommandReplaceBatch {
					sections = append(sections, iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{1}})
				}
				submit(id, key, sections)
			}
			mutation(iwire.CommandInsertBatch, "native-insert", old)
			current := func() (*collections.Collection, *backenddb.DB) {
				t.Helper()
				col, db, err := r.data["group-a"].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: c.NodeID, GroupID: "group-a", MinAppliedIndex: last.Evidence.Index}, "users")
				if err != nil {
					t.Fatal(err)
				}
				if !db.HasNativeSemanticBridge() {
					t.Fatal("current FSM lost native public owner")
				}
				return col, db
			}
			col, db := current()
			capture, err := db.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			budget, ok := capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
			if !ok {
				t.Fatal("original COW allocator unavailable")
			}
			if capture.LogicalSequence() == 0 {
				t.Fatal("TCP apply bypassed native logical publication")
			}
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			pins := db.ValueLogIdentityPinRegistry()
			held, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			assertDocument := func(got, want []byte) {
				t.Helper()
				if format == collections.DocumentFormatBSON {
					if !bytes.Equal(got, want) {
						t.Fatalf("document=%x want=%x", got, want)
					}
					return
				}
				var a, b any
				if json.Unmarshal(got, &a) != nil || json.Unmarshal(want, &b) != nil || !reflect.DeepEqual(a, b) {
					t.Fatalf("document=%s want=%s", got, want)
				}
			}
			mutation(iwire.CommandReplaceBatch, "native-replace", updated)
			got, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatal(err)
			}
			assertDocument(got, updated)
			prior, err := held.FetchDocumentsByID([][]byte{[]byte("u1")}, collections.DocumentFetchOptions{})
			if err != nil || len(prior.Results) != 1 || !prior.Results[0].Found {
				t.Fatalf("held=%+v %v", prior, err)
			}
			assertDocument(prior.Results[0].Document, old)
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			snapshot, err := r.data["group-a"].provider.Snapshot(ctx)
			if err != nil || snapshot.SizeBytes == 0 || snapshot.LastIncludedIndex < last.Evidence.Index {
				t.Fatalf("snapshot=%+v %v", snapshot, err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if s := budget.Stats(); s.TotalBytes != 0 || s.Views != 0 || s.Sources != 0 || s.Generations != 0 || s.ExternalLeases != 0 {
				t.Fatalf("closed original COW=%+v", s)
			}
			if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("closed original pins=%+v", s)
			}
			open()
			if r.data["group-a"].fsm.HasCurrentDBV1(r.data["group-a"].db) {
				t.Fatal("provider did not install snapshot onto replacement owner")
			}
			col, _ = current()
			got, err = col.Get([]byte("u1"))
			if err != nil {
				t.Fatal(err)
			}
			assertDocument(got, updated)
			mutation(iwire.CommandDeleteBatch, "native-delete-restored", nil)
			mutation(iwire.CommandInsertBatch, "native-reinsert-restored", final)
			col, db = current()
			got, err = col.Get([]byte("u1"))
			if err != nil {
				t.Fatal(err)
			}
			assertDocument(got, final)
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			open()
			col, _ = current()
			got, err = col.Get([]byte("u1"))
			if err != nil {
				t.Fatal(err)
			}
			assertDocument(got, final)
			ids, err := col.FindByIndexValue("by_city", "sfo")
			if err != nil || len(ids) != 1 || string(ids[0]) != "u1" {
				t.Fatalf("restored index=%q %v", ids, err)
			}
			_, db = current()
			capture, err = db.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			budget, ok = capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
			if !ok {
				t.Fatal("restored original allocator stats unavailable")
			}
			pins = db.ValueLogIdentityPinRegistry()
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if s := budget.Stats(); s.TotalBytes != 0 || s.Views != 0 || s.Sources != 0 || s.Generations != 0 || s.ExternalLeases != 0 {
				t.Fatalf("final original COW=%+v", s)
			}
			if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("final original pins=%+v", s)
			}
		})
	}
}

func TestFixedPeerTCPNativeCollectionRoleIdentityV1(t *testing.T) {
	c := fixedPeerTestConfigsV1(t)[0]
	_, before, err := validateFixedPeerConfigV1(c)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(c.Groups[0])
	if err != nil || bytes.Contains(encoded, []byte("NativeCollections")) {
		t.Fatalf("default role changed serialization: %s %v", encoded, err)
	}
	c.Groups[0].NativeCollections = true
	_, after, err := validateFixedPeerConfigV1(c)
	if err != nil || before == after {
		t.Fatalf("native role not bound to config: %s %s %v", before, after, err)
	}
	c.Catalog.NativeCollections = true
	if _, _, err := validateFixedPeerConfigV1(c); err == nil {
		t.Fatal("native catalog accepted")
	}
}

// The status guard is the current backend CommitSeq, not a stable schema epoch.
// Another publication can therefore invalidate a sampled guard. Only a definite
// ordinary-collection precommit rejection is safe to re-encode:
// a committed or ambiguous command must retain its original key/digest identity.
func fixedPeerSubmitNativeCollectionV1(t testing.TB, ctx context.Context, client *FixedPeerTCPClientV1, node raftcluster.NodeID, id iwire.CommandID, key string, sections []iwire.Section, interpose ...func(uint64)) raftcluster.SubmitResultV1 {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		status, err := client.Status(ctx, node)
		if err != nil || len(status.Groups) != 1 {
			t.Fatalf("native collection status: %+v %v", status, err)
		}
		raw := fixedPeerNativeCollectionEntryV1(t, id, key, status.Groups[0].CatalogVersion, sections)
		request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
		route, err := client.Route(ctx, node, request)
		if err != nil {
			t.Fatal(err)
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		// The deterministic guard-race control interposes only in this test helper.
		for _, beforeSubmit := range interpose {
			beforeSubmit(status.Groups[0].CatalogVersion)
		}
		result, err := client.Submit(ctx, node, raw, metadata)
		if errors.Is(err, raftcluster.ErrCatalogVersionMismatch) && !errors.Is(err, raftcluster.ErrCommitAmbiguous) && reflect.DeepEqual(result, raftcluster.SubmitResultV1{}) {
			t.Logf("submit %s: definite precommit catalog mismatch, refreshing guard (attempt %d/3): %v", key, attempt+1, err)
			continue
		}
		if err != nil || !result.CommittedRecoverable || !result.CommittedApplied {
			t.Fatalf("submit %s: %+v %v", key, result, err)
		}
		return result
	}
	t.Fatalf("submit %s: catalog guard remained stale after 3 attempts", key)
	return raftcluster.SubmitResultV1{}
}

func fixedPeerNativeCollectionEntryV1(t testing.TB, id iwire.CommandID, key string, version uint64, sections []iwire.Section) []byte {
	t.Helper()
	request := append([]iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: id, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(key)},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
	}, sections...)
	request = append(request, ackSection(AckRaftCommitted))
	validated, err := iwire.MustV1Registry().ValidateRequestSections(request)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func fixedPeerPublishNativeUsersCatalogV1(t testing.TB, ctx context.Context, client *FixedPeerTCPClientV1, node raftcluster.NodeID) {
	t.Helper()
	catalog := raftplacement.CatalogV1{
		Groups:     []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{node}}},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "users"}, GroupID: "group-a"}},
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, node, command); err != nil {
		t.Fatal(err)
	}
}

// Force the status-to-submit race without relying on background timer timing.
// Interpose actual TCP create/insert commands after sampling the guard. The
// publisher behind the historical intermittent failure remains unproven.
func TestFixedPeerTCPNativeCollectionCatalogGuardInterpositionV1(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			diagnostic := installGuardPublicationDiagnostic(t)
			c := fixedPeerTestConfigsV1(t)[0]
			c.Nodes, c.Catalog.Peers, c.Groups = c.Nodes[:1], c.Catalog.Peers[:1], c.Groups[:1]
			c.Groups[0].NativeCollections = true
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			r, err := fixedPeerOpenTestRuntimeV1(t, c)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			})
			fixedPeerWaitV1(t, ctx, func() bool {
				s, err := r.Status(ctx)
				return err == nil && s.CatalogRaft.State == "Leader" && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
			})
			client, err := NewFixedPeerTCPClientV1(c)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			fixedPeerPublishNativeUsersCatalogV1(t, ctx, client, c.NodeID)
			meta := collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: format}, Indexes: []collections.IndexDefinition{{Name: "by_city", Field: "city", ValueType: collections.IndexValueString, Unique: true}}}
			document := func(city string) []byte {
				if format == collections.DocumentFormatJSON {
					return []byte(`{"city":"` + city + `"}`)
				}
				doc, err := bson.Marshal(bson.D{{Key: "city", Value: city}})
				if err != nil {
					t.Fatal(err)
				}
				return doc
			}
			old, updated := document("hnl"), document("sea")
			sections := func(doc []byte) []iwire.Section {
				return []iwire.Section{collectionNameRef("users"), documentFormatSection(format),
					{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
					{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, doc)}}
			}
			replacement := append(sections(updated), iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{1}})
			const key = "guard-replace"
			fsm := r.data["group-a"].fsm
			var created raftcluster.SubmitResultV1
			var col *collections.Collection
			var db *backenddb.DB
			var before FixedPeerTCPGroupStatusV1
			var seq, nextLSN, staleVersion uint64
			attempts := 0
			logicalSequence := func() uint64 {
				t.Helper()
				capture, err := db.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				seq := capture.LogicalSequence()
				if err := capture.Close(); err != nil {
					t.Fatal(err)
				}
				return seq
			}
			// On the first attempt, publish real commands between the helper's
			// status/encoding/route and submit. On the second, prove that the stale
			// attempt left no effects before allowing its fresh-guard submission.
			applied := fixedPeerSubmitNativeCollectionV1(t, ctx, client, c.NodeID, iwire.CommandReplaceBatch, key, replacement, func(version uint64) {
				attempts++
				diagnostic.mark("attempt=%d sampled_guard=%d stale_guard=%d", attempts, version, staleVersion)
				if attempts == 1 {
					staleVersion = version
					created = fixedPeerSubmitNativeCollectionV1(t, ctx, client, c.NodeID, iwire.CommandCreateCollection, "guard-create", []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: mustEncodeCollectionMeta(meta)}})
					inserted := fixedPeerSubmitNativeCollectionV1(t, ctx, client, c.NodeID, iwire.CommandInsertBatch, "guard-insert", sections(old))
					col, db, err = fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: c.NodeID, GroupID: "group-a", MinAppliedIndex: inserted.Evidence.Index}, "users")
					if err != nil {
						t.Fatal(err)
					}
					if err := db.SyncNativeSemanticPrefix(); err != nil {
						t.Fatal(err)
					}
					seq, nextLSN = logicalSequence(), db.CommandWALNextLSN()
					status, err := client.Status(ctx, c.NodeID)
					if err != nil || status.Groups[0].CatalogVersion <= staleVersion {
						t.Fatalf("interposed commands did not advance sampled guard: sampled=%d current=%+v err=%v", staleVersion, status, err)
					}
					before = status.Groups[0]
					diagnostic.mark("baseline root=%q guard=%d logical_seq=%d next_lsn=%d last_index=%d commit_index=%d applied_index=%d", db.Dir(), before.CatalogVersion, seq, nextLSN, before.LastIndex, before.CommitIndex, before.RaftAppliedIndex)
					return
				}
				if attempts != 2 || version <= staleVersion {
					t.Fatalf("expected one stale attempt then fresh guard: attempts=%d stale=%d current=%d", attempts, staleVersion, version)
				}
				after, err := client.Status(ctx, c.NodeID)
				if err != nil {
					t.Fatal(err)
				}
				b, a := before, after.Groups[0]
				diagnostic.mark("precheck attempt=%d guard=%d last_index=%d commit_index=%d applied_index=%d", attempts, a.CatalogVersion, a.LastIndex, a.CommitIndex, a.RaftAppliedIndex)
				if b.LastIndex != a.LastIndex || b.CommitIndex != a.CommitIndex || b.RaftAppliedIndex != a.RaftAppliedIndex || !reflect.DeepEqual(b.Applied, a.Applied) || logicalSequence() != seq || db.CommandWALNextLSN() != nextLSN {
					t.Fatalf("stale guard changed raft/logical state: before=%+v after=%+v", b, a)
				}
				if _, _, found, err := fsm.AppliedIdempotencyGuardV1(ctx, []byte(key)); err != nil || found {
					t.Fatalf("rejected command allocated idempotency identity: found=%v err=%v", found, err)
				}
				if got, err := col.Get([]byte("u1")); err != nil || !bytes.Equal(got, old) {
					t.Fatalf("rejected replacement changed row: %x %v", got, err)
				}
				if ids, err := col.FindByIndexValue("by_city", "hnl"); err != nil || len(ids) != 1 || string(ids[0]) != "u1" {
					t.Fatalf("rejected replacement changed old index: %q %v", ids, err)
				}
				if ids, err := col.FindByIndexValue("by_city", "sea"); err != nil || len(ids) != 0 {
					t.Fatalf("rejected replacement changed new index: %q %v", ids, err)
				}
				diagnostic.mark("checks passed attempt=%d sampled_guard=%d", attempts, version)
				t.Logf("interposed create/insert advanced catalog guard %d -> %d; helper's stale replacement rejected without raft, WAL, logical, idempotency, row, or index effects", staleVersion, before.CatalogVersion)
			})
			if attempts != 2 || applied.ApplyResult.DeterministicErrorCode != "" || applied.ApplyResult.AffectedCount != 1 || logicalSequence() != seq+1 || db.CommandWALNextLSN() != nextLSN+1 {
				t.Fatalf("fresh replacement result: attempts=%d result=%+v", attempts, applied.ApplyResult)
			}
			if got, err := col.Get([]byte("u1")); err != nil || !bytes.Equal(got, updated) {
				t.Fatalf("fresh replacement row: %x %v", got, err)
			}
			guard, digest, found, err := fsm.AppliedIdempotencyGuardV1(ctx, []byte(key))
			if err != nil || !found || guard <= staleVersion || digest != applied.DecodedEntry.Digest || !bytes.Equal(applied.CommittedEntry.Bytes, fixedPeerNativeCollectionEntryV1(t, iwire.CommandReplaceBatch, key, guard, replacement)) {
				t.Fatalf("fresh replacement changed operation/key: guard=%d digest=%x found=%v err=%v", guard, digest, found, err)
			}
			// Preserve the special exact-create replay contract: never re-encode a
			// command that has already committed, even when its guard is now stale.
			replayed, err := client.Submit(ctx, c.NodeID, created.CommittedEntry.Bytes, created.CommittedEntry.RequestMetadata)
			if err != nil || !replayed.CommittedRecoverable || !replayed.CommittedApplied || replayed.ApplyResult.AffectedCount != 0 || replayed.ApplyResult.MatchedCount != 0 || replayed.ApplyResult.CommandDigest != created.ApplyResult.CommandDigest || replayed.ApplyResult.ResultDigest != created.ApplyResult.ResultDigest || logicalSequence() != seq+1 {
				t.Fatalf("exact create replay changed identity or logical state: %+v %v", replayed, err)
			}
		})
	}
}
