package db

import (
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"testing"
)

func TestCapturedDurableMutationWaitsForPhysicalPrefixAndRelaxedTailPromotion(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	appendWithMode := func(key string, mode RawKVCommandWALAppendMode) uint64 {
		t.Helper()
		lsn, err := database.AppendRawKVCommandWALOrderedEntriesWithMode([]batchpkg.Entry{{Type: batchpkg.OpPut, Key: []byte(key), Value: []byte("value")}}, mode)
		if err != nil {
			t.Fatal(err)
		}
		return lsn
	}
	lsn := appendWithMode("captured", RawKVCommandWALAppendCapturedDurable)
	if database.commandWALDurableMarkerLSN.Load() != lsn || database.commandWALDurableLSN.Load() >= lsn {
		t.Fatal("emitted durable class was confused with completed physical durability")
	}
	var storage CommandWALCapturedPrefix
	target, result, err := database.CaptureCommandWALPrefixInto(&storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.LSN != 0 || database.CommandWALNextLSN() != lsn+1 {
		t.Fatal("self-covered durable mutation appended a redundant barrier")
	}
	if _, _, err := database.CaptureCommandWALPrefixInto(&storage, nil); err == nil {
		t.Fatal("active target reuse was accepted")
	}
	if database.commandWALDurableLSN.Load() >= lsn {
		t.Fatal("capture alone advanced physical durability")
	}
	if _, err := target.Sync(); err != nil {
		t.Fatal(err)
	}
	if database.commandWALDurableLSN.Load() != lsn {
		t.Fatal("completed capture did not advance exact durable frontier")
	}
	relaxedLSN := appendWithMode("relaxed", RawKVCommandWALAppendRelaxed)
	if relaxedLSN != lsn+1 {
		t.Fatal("mutation LSN gap")
	}
	storage = CommandWALCapturedPrefix{}
	target, result, err = database.CaptureCommandWALPrefixInto(&storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.LSN != relaxedLSN+1 {
		t.Fatalf("relaxed tail promotion barrier=%d want=%d", result.LSN, relaxedLSN+1)
	}
	if database.commandWALDurableLSN.Load() != lsn {
		t.Fatal("uncompleted promoted capture advanced durable frontier")
	}
	if _, err := target.Sync(); err != nil {
		t.Fatal(err)
	}
	if database.commandWALDurableLSN.Load() != result.LSN {
		t.Fatal("promoted capture did not complete exact frontier")
	}
}
