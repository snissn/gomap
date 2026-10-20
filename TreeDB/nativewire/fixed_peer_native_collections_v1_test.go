package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
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
			catalog := raftplacement.CatalogV1{
				Groups:     []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{c.NodeID}}},
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
			if _, err := client.PublishCatalog(ctx, c.NodeID, command); err != nil {
				t.Fatal(err)
			}
			var last raftcluster.SubmitResultV1
			submit := func(id iwire.CommandID, key string, sections []iwire.Section) {
				t.Helper()
				status, err := client.Status(ctx, c.NodeID)
				if err != nil {
					t.Fatal(err)
				}
				sections = append([]iwire.Section{
					{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: id, Version: 1})},
					{ID: iwire.SectionIdempotencyKey, Bytes: []byte(key)},
					{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, status.Groups[0].CatalogVersion)},
				}, sections...)
				sections = append(sections, ackSection(AckRaftCommitted))
				validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := iwire.AppendDeterministicEntry(nil, validated)
				if err != nil {
					t.Fatal(err)
				}
				request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "users", Shape: ClusterRouteShapeCollection}
				route, err := client.Route(ctx, c.NodeID, request)
				if err != nil {
					t.Fatal(err)
				}
				metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
				ApplyClusterRouteMetadata(&metadata, request, route)
				last, err = client.Submit(ctx, c.NodeID, raw, metadata)
				if err != nil || !last.CommittedRecoverable || !last.CommittedApplied {
					t.Fatalf("submit %s: %+v %v", key, last, err)
				}
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
			if err := db.SyncNativeSemanticPrefix(); err != nil {
				t.Fatal(err)
			}
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
