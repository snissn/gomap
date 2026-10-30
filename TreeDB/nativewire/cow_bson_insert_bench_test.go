package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BenchmarkCOWNativewireBSONInsert measures successful serial, batch-size-one
// public Client requests over loopback TCP to the actual shared COW owner. Both
// cases use the maintained 10x100-byte BSON-binary YCSB encoder and unique key
// index, with requested AckVisible. It is a focused API cost comparison, not a
// concurrent YCSB load or a comparison with the formerly rejected insert path.
//
// Payload construction, two identical response-control inserts, collection setup,
// all readbacks, checkpoint and checked shutdown are outside the timer. Timed
// allocations include client/server request, storage and response work; they
// exclude BSON encoding. The returned-ID case also checks the returned key.
// NoResult omits ACK metadata, so a checked successful response is counted without
// inventing a decoded actual ACK for that response. The untimed returned-ID
// control independently requires actual_ack_policy=1 and inserted_count=1.
//
// Always use a fixed operation count (maximum 4096) to bound database growth:
//
//	TREEDB_NATIVEWIRE_YCSB_PROFILE=command_wal_relaxed \
//	  env -u GOMAXPROCS go test ./TreeDB/nativewire -run '^$' \
//	  -bench '^BenchmarkCOWNativewireBSONInsert$' -benchtime=1024x -benchmem -count=1
//
// The maintained profile selector also accepts command_wal_durable/no_wal_fast.
// Checkpoint/clean-close costs are excluded, not waived durability work; this
// benchmark provides no crash/recovery certification. Run timing only after the
// coordinator releases the measurement slot, retaining every planned sample.
func BenchmarkCOWNativewireBSONInsert(b *testing.B) {
	profile := ycsbBenchTreeDBProfile(b)
	for _, omit := range []bool{false, true} {
		name := "returned_ids"
		if omit {
			name = "omitted_ids_and_meta"
		}
		b.Run(string(profile)+"/"+name, func(b *testing.B) {
			benchmarkCOWNativewireBSONInsert(b, profile, omit)
		})
	}
}

func benchmarkCOWNativewireBSONInsert(b *testing.B, profile treedb.Profile, omit bool) {
	b.StopTimer()
	if b.N < 1 || b.N > 4096 {
		b.Fatalf("bounded fixture requires -benchtime=Nx with 1 <= N <= 4096; got %d", b.N)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	b.Cleanup(cancel)
	opts := treedb.OptionsFor(profile, b.TempDir()) // Honors the supplied TMPDIR.
	opts.MemtableMode = "cow_btree"
	owner, err := treedb.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		b.StopTimer()
		if err := owner.Close(); err != nil {
			b.Errorf("owner Close: %v", err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		b.Fatal(err)
	}
	mgr := collections.NewCollectionManager(backend)
	b.Cleanup(func() {
		b.StopTimer()
		if err := mgr.Close(); err != nil {
			b.Errorf("manager Close: %v", err)
		}
	})
	if !mgr.UsesNativeCommandWALApply() {
		b.Fatal("public factory did not select shared COW native owner")
	}
	server := NewServer(ServerOptions{Collections: mgr, Backend: backend})
	b.Cleanup(func() {
		b.StopTimer()
		if err := server.Close(); err != nil {
			b.Errorf("server Close: %v", err)
		}
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	serveCtx, stopServing := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx, ln) }()
	b.Cleanup(func() {
		b.StopTimer()
		stopServing()
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			b.Errorf("listener Close: %v", err)
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				b.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			b.Error("Serve did not stop within five seconds")
		}
	})
	client, err := DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		b.StopTimer()
		if err := client.Close(); err != nil {
			b.Errorf("client Close: %v", err)
		}
	})
	_, err = client.CreateCollection(ctx, collections.CollectionMeta{
		Name:    ycsbBenchCollection,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
		Indexes: []collections.IndexDefinition{{Name: ycsbBenchKeyIndex, Field: ycsbBenchKeyField, ValueType: collections.IndexValueString, Unique: true}},
	})
	if err != nil {
		b.Fatal(err)
	}
	handle, err := client.OpenCollection(ctx, ycsbBenchCollection)
	if err != nil {
		b.Fatal(err)
	}
	// Both cases have identical warmup rows and independently checked response
	// controls; no-result metadata absence is never interpreted as ACK zero.
	for _, controlOmit := range []bool{false, true} {
		cowBSONBenchResponseControl(b, ctx, client, handle, controlOmit)
	}
	b.Logf("owner=cow_btree/NativeSemanticBackend transport=tcp clients=1 batch=1 requested_ack=%d; untimed controls: actual_ack=1 inserted_count=1 returned_ids=1; omitted response: IDs/meta absent", AckVisible)
	ids, docs := make([][]byte, b.N), make([][]byte, b.N)
	var payloadBytes int64
	for i := range ids {
		key := ycsbBenchKey(i)
		ids[i] = []byte(key)
		docs[i], err = encodeYCSBBSONBinaryDocument(key)
		if err != nil {
			b.Fatal(err)
		}
		if err := bson.Raw(docs[i]).Validate(); err != nil {
			b.Fatal(err)
		}
		payloadBytes += int64(len(docs[i]))
	}
	var requestIDs, requestDocs [1][]byte
	successful := 0
	b.ReportAllocs()
	b.SetBytes(payloadBytes / int64(b.N))
	b.ResetTimer()
	b.StartTimer()
	for i := range ids {
		requestIDs[0], requestDocs[0] = ids[i], docs[i]
		if omit {
			err = client.InsertBatchHandleNoResult(ctx, handle, collections.DocumentFormatBSON, requestIDs[:], requestDocs[:], AckVisible)
		} else {
			var returned [][]byte
			returned, err = client.InsertBatchHandle(ctx, handle, collections.DocumentFormatBSON, requestIDs[:], requestDocs[:], AckVisible)
			if err == nil && (len(returned) != 1 || !bytes.Equal(returned[0], ids[i])) {
				err = fmt.Errorf("returned IDs do not match operation %d", i)
			}
		}
		if err != nil {
			b.StopTimer()
			b.Fatalf("operation %d failed after %d successful responses: %v", i, successful, err)
		}
		successful++
	}
	b.StopTimer()
	b.ReportMetric(float64(successful), "successful_responses")
	b.ReportMetric(float64(successful)/b.Elapsed().Seconds(), "successful_responses/s")
	// Check every successful insert through the same public client, outside the
	// timer. Bounded chunks avoid a large single response and check exact BSON.
	for start := 0; start < len(ids); start += 64 {
		end := min(start+64, len(ids))
		actual, present, err := client.GetManyHandle(ctx, handle, ids[start:end])
		if err != nil || len(actual) != end-start || len(present) != end-start {
			b.Fatalf("readback [%d:%d]: docs=%d present=%d err=%v", start, end, len(actual), len(present), err)
		}
		for i := range actual {
			if !present[i] || !bytes.Equal(actual[i], docs[start+i]) {
				b.Fatalf("exact BSON readback mismatch at operation %d", start+i)
			}
		}
	}
	if err := client.Checkpoint(ctx); err != nil {
		b.Fatalf("untimed Checkpoint: %v", err)
	}
}

func cowBSONBenchResponseControl(b *testing.B, ctx context.Context, client *Client, handle CollectionHandle, omit bool) {
	b.Helper()
	key := fmt.Sprintf("response-control-omit-%t", omit)
	doc, err := encodeYCSBBSONBinaryDocument(key)
	if err != nil {
		b.Fatal(err)
	}
	guard, err := client.replicatedMutationGuard(ctx, "insert_batch")
	if err != nil {
		b.Fatal(err)
	}
	var flags uint64
	if omit {
		flags = iwire.CommandFlagOmitResultIDs | iwire.CommandFlagOmitResponseMeta
	}
	body, err := appendInsertBatchRequestBodyRefFlags(nil, "", handle, true, collections.DocumentFormatBSON, [][]byte{[]byte(key)}, [][]byte{doc}, AckVisible, flags, guard)
	if err != nil {
		b.Fatal(err)
	}
	_, response, err := client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if err != nil {
		b.Fatal(err)
	}
	sections, err := iwire.DecodeSections(response, client.limits)
	if err != nil {
		b.Fatal(err)
	}
	if omit {
		for _, section := range sections {
			if section.ID == iwire.SectionDocumentIDs || section.ID == iwire.SectionResponseMeta {
				b.Fatalf("omitted-ID response included section %d", section.ID)
			}
		}
		return
	}
	rawIDs, found, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil || !found {
		b.Fatalf("returned-ID control: IDs found=%t err=%v", found, err)
	}
	returned, err := decodeByteVectorBorrowed(rawIDs, client.limits)
	if err != nil || len(returned) != 1 || !bytes.Equal(returned[0], []byte(key)) {
		b.Fatalf("returned-ID control: IDs=%q err=%v", returned, err)
	}
	actualACK, err := responseAckPolicy(sections)
	if err != nil || actualACK != AckVisible {
		b.Fatalf("returned-ID control: actual ACK=%d err=%v", actualACK, err)
	}
	inserted, err := responseCount(sections, "inserted_count")
	if err != nil || inserted != 1 {
		b.Fatalf("returned-ID control: inserted_count=%d err=%v", inserted, err)
	}
}
