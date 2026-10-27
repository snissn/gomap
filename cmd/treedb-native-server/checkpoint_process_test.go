package main

import (
	"bytes"
	"os"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

// A native Synced boundary must persist the original owner's accepted prefix,
// including visible writes and the no-new-work case, before process death.
func TestNativeServerCommandVisiblePrefixSyncedBoundarySurvivesHardExit(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, boundary := range []string{"checkpoint", "mutation_ack"} {
			t.Run(string(profile)+"/"+boundary, func(t *testing.T) {
				dir := t.TempDir()
				cmd, client := startNativeCommand(t, dir, "-profile", string(profile))
				ctx := t.Context()
				if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "wire", Indexes: []collections.IndexDefinition{{Name: "tag", Field: "tag", ValueType: collections.IndexValueString}}}); err != nil {
					t.Fatal(err)
				}
				ids := [][]byte{[]byte("visible")}
				docs := [][]byte{[]byte(`{"tag":"blue"}`)}
				if _, err := client.InsertBatch(ctx, "wire", collections.DocumentFormatJSON, ids, docs, nativewire.AckVisible); err != nil {
					t.Fatal(err)
				}
				switch boundary {
				case "checkpoint":
					if err := client.CheckpointWithAck(ctx, nativewire.AckSynced); err != nil {
						t.Fatal(err)
					}
					// No further publication precedes the second durable completion.
					if err := client.CheckpointWithAck(ctx, nativewire.AckSynced); err != nil {
						t.Fatal(err)
					}
				case "mutation_ack":
					ids = append(ids, []byte("synced"))
					docs = append(docs, []byte(`{"tag":"green"}`))
					if _, err := client.InsertBatch(ctx, "wire", collections.DocumentFormatJSON, ids[1:], docs[1:], nativewire.AckSynced); err != nil {
						t.Fatal(err)
					}
				}
				// Neither graceful shutdown nor owner Close may make the prefix durable.
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := cmd.Wait(); err == nil {
					t.Fatal("hard-exit child returned success")
				}
				_ = client.Close()
				reopened, client := startNativeCommand(t, dir, "-profile", string(profile))
				got, present, err := client.GetMany(ctx, "wire", ids)
				if err != nil || len(got) != len(docs) || len(present) != len(ids) {
					t.Fatalf("recovered prefix docs=%q present=%v err=%v", got, present, err)
				}
				for i := range ids {
					if !present[i] || !bytes.Equal(got[i], docs[i]) {
						t.Fatalf("Synced boundary lost accepted prefix id=%q doc=%q present=%v", ids[i], got[i], present[i])
					}
				}
				for i, tag := range []string{"blue", "green"} {
					if i >= len(ids) {
						break
					}
					got, truncated, err := client.IndexLookup(ctx, "wire", "tag", tag, nativewire.CursorLimits{})
					if err != nil || truncated || len(got) != 1 || !bytes.Equal(got[0], ids[i]) {
						t.Fatalf("recovered index tag=%s ids=%q truncated=%v err=%v", tag, got, truncated, err)
					}
				}
				_ = client.Close()
				if err := reopened.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
				if err := reopened.Wait(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
