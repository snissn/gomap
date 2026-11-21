package mongogateway

import (
	"bytes"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func requireStandaloneReceiptTestOwner(tb testing.TB, standalone *StandaloneServer) {
	tb.Helper()
	if standalone.Backend == nil || standalone.Collections == nil || standalone.ownerStats == nil || standalone.cleanup == nil ||
		!standalone.Backend.CommandWALEnabled() || !standalone.Collections.UsesNativeCommandWALApply() {
		tb.Fatal("standalone must retain its original public CommandWAL/native owner and cleanup")
	}
	stats := standalone.Stats()
	if mode := stats["treedb.cache.memtable_mode"]; mode != "cow_btree" {
		tb.Fatalf("original cache-owner mode=%q want cow_btree", mode)
	}
	// These are original public completion-group and cache-owner counters, not
	// backend aliases or the public wrapper's Checkpoint API call count.
	for _, key := range []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.group_commit.forced_total", "treedb.command_wal.sync.count_total", "treedb.command_wal.file_sync.calls_total"} {
		writeConcernBenchmarkStat(tb, stats, key)
	}
}

func standaloneReceiptTestDocument(id string, generation int) bson.D {
	doc := bson.D{{Key: "_id", Value: id}}
	for field := 0; field < 10; field++ {
		payload := make([]byte, 100)
		for i := range payload {
			payload[i] = byte(generation*31 + field*7 + i)
		}
		doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", field), Value: bson.Binary{Subtype: 0, Data: payload}})
	}
	return doc
}

func requireStandaloneReceiptTestDocument(tb testing.TB, raw bson.Raw, id string, generation int) {
	tb.Helper()
	if got, ok := raw.Lookup("_id").StringValueOK(); !ok || got != id {
		tb.Fatalf("document _id=%q string=%t want %q", got, ok, id)
	}
	fields, err := raw.Elements()
	if err != nil || len(fields) != 11 {
		tb.Fatalf("document fields=%d err=%v want _id plus ten binary fields", len(fields), err)
	}
	for _, field := range standaloneReceiptTestDocument(id, generation)[1:] {
		want := field.Value.(bson.Binary)
		subtype, payload, ok := raw.Lookup(field.Key).BinaryOK()
		if !ok || subtype != want.Subtype || !bytes.Equal(payload, want.Data) {
			tb.Fatalf("document %s field %s: binary=%t subtype=%d bytes=%v want subtype=%d bytes=%v", id, field.Key, ok, subtype, payload, want.Subtype, want.Data)
		}
	}
}
