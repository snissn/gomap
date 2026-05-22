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
