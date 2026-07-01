package commitlog

import "testing"

func BenchmarkCommandJournalAllocateAndAppendRawKVBatch(b *testing.B) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("bench-key"), Value: []byte("bench-value")},
	})
	if err != nil {
		b.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	j, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{})
	if err != nil {
		b.Fatalf("OpenCommandJournal: %v", err)
	}
	b.Cleanup(func() { _ = j.Close() })

	env := CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       payload,
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := j.AppendCommand(env); err != nil {
			b.Fatalf("AppendCommand: %v", err)
		}
	}
}

func BenchmarkCommandJournalAppendRawKVPointTrustedAndFlush(b *testing.B) {
	key := []byte("bench-key")
	value := []byte("bench-value")
	for _, tc := range []struct {
		name     string
		revision uint64
	}{
		{name: "no_revision"},
		{name: "revision", revision: 123},
	} {
		b.Run(tc.name, func(b *testing.B) {
			j, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{})
			if err != nil {
				b.Fatalf("OpenCommandJournal: %v", err)
			}
			b.Cleanup(func() { _ = j.Close() })
			b.ReportAllocs()
			b.SetBytes(int64(len(key) + len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tc.revision == 0 {
					if _, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, key, value, false); err != nil {
						b.Fatalf("AppendRawKVPointCommandTrustedAndFlush: %v", err)
					}
				} else {
					if _, err := j.AppendRawKVPointCommandTrustedWithRevisionAndFlush(0, RawKVOpSet, key, value, tc.revision, false); err != nil {
						b.Fatalf("AppendRawKVPointCommandTrustedWithRevisionAndFlush: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkCommandJournalAppendCollectionInsertPayload(b *testing.B) {
	payload, err := EncodeCollectionInsertBatchByIDPayload("usertable", []CollectionDocument{
		{
			ID:       []byte("user000000000000"),
			Document: []byte(`{"_id":"user000000000000","field0":"value0","field1":"value1","field2":"value2","field3":"value3","field4":"value4","field5":"value5","field6":"value6","field7":"value7","field8":"value8","field9":"value9"}`),
		},
	})
	if err != nil {
		b.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	env := CommandEnvelope{
		Kind:          CommandKindCollectionInsertBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1,
		Payload:       payload,
	}
	b.Run("generic", func(b *testing.B) {
		j, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{})
		if err != nil {
			b.Fatalf("OpenCommandJournal: %v", err)
		}
		b.Cleanup(func() { _ = j.Close() })
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := j.AppendCommand(env); err != nil {
				b.Fatalf("AppendCommand: %v", err)
			}
		}
	})
	b.Run("trusted", func(b *testing.B) {
		j, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{})
		if err != nil {
			b.Fatalf("OpenCommandJournal: %v", err)
		}
		b.Cleanup(func() { _ = j.Close() })
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := j.AppendCommandPayloadTrusted(CommandKindCollectionInsertBatchByID, CommandScopeCollection, PayloadFormatCollectionInsertBatchByIDV1, 0, payload); err != nil {
				b.Fatalf("AppendCommandPayloadTrusted: %v", err)
			}
		}
	})
}
