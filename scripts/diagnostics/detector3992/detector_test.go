//go:build treedb_test

package mvcc

import (
 "encoding/json"
 "flag"
 "fmt"
 "testing"
 "time"
)

// Diagnostic only: never installed in a product branch. The maintained benchmark
// is called unchanged; these counters deliberately remain inside measured work.
var detectorMode = flag.String("detector-mode", "raw", "raw, off or positive")
type detectorRecord struct { OperationNS int64 `json:"operation_ns"`; RequestedNS int64 `json:"requested_ns"`; RealizedNS int64 `json:"realized_ns"` }
var detectorRecords [129]detectorRecord
var detectorCount int
var detectorInvalid string

func detectorStart(ts uint64, ms []Mutation, mode CommitMode) time.Time {
 if ts != 100 || mode != CommitDurable || len(ms) != 1 || string(ms[0].Key) != "k031" || string(ms[0].Value) != "after" { return time.Time{} }
 return time.Now()
}
func detectorRequested(ns int64) (int64, bool) {
 // Cap protects arithmetic and declares a >10s operation engineering-invalid.
 if ns <= 0 || ns > int64(10*time.Second) { return 0, false }
 return (ns*8 + 99)/100, true
}
func detectorFinish(start time.Time) {
 if start.IsZero() { return }
 if detectorCount >= len(detectorRecords) { detectorInvalid = "extra target invocation"; return }
 ns := time.Since(start).Nanoseconds()
 requested, ok := detectorRequested(ns)
 if !ok { detectorInvalid = "operation duration outside frozen bound"; return }
 r := detectorRecord{OperationNS:ns}
 if *detectorMode == "positive" {
  // Only reached after successful public durable CommitGroupAt return. Busy
  // deadline avoids sleep quantum rounding; actual elapsed includes preemption.
  r.RequestedNS = requested
  waitStart := time.Now()
  deadline := waitStart.Add(time.Duration(requested))
  for time.Now().Before(deadline) {}
  r.RealizedNS = time.Since(waitStart).Nanoseconds()
 } else if *detectorMode != "off" { detectorInvalid = "hook activated in raw mode" }
 detectorRecords[detectorCount] = r
 detectorCount++
}

func TestDetectorRawSample(t *testing.T) {
 if *detectorMode != "raw" && *detectorMode != "off" && *detectorMode != "positive" { t.Fatal("unknown mode") }
 if testing.Short() { t.Fatal("no short diagnostic") }
 detectorRecords = [129]detectorRecord{}
 detectorCount, detectorInvalid = 0, ""
 r := testing.Benchmark(BenchmarkCommitAtPrimaryOnlyAfterDataGeneration)
 if r.N != 128 || r.T <= 0 || r.Extra["generation_advances/op"] != 0 { t.Fatalf("invalid benchmark result: %+v", r) }
 want := 129 // fixed128x launches calibration N=1, then measured N=128.
 if *detectorMode == "raw" { want = 0 }
 if detectorCount != want || detectorInvalid != "" { t.Fatalf("activation %d != %d: %s", detectorCount, want, detectorInvalid) }
 // Same rounded metrics and actual maintained evaluator retained beside totals.
 fmt.Printf("BenchmarkCommitAtPrimaryOnlyAfterDataGeneration %d %d ns/op %d B/op %d allocs/op %g generation_advances/op\n", r.N, r.NsPerOp(), r.AllocedBytesPerOp(), r.AllocsPerOp(), r.Extra["generation_advances/op"])
 payload := struct {
  N int `json:"n"`; TNS int64 `json:"t_ns"`; MemBytes uint64 `json:"mem_bytes"`; MemAllocs uint64 `json:"mem_allocs"`; Mode string `json:"mode"`; Count int `json:"activation_count"`; Calibration *detectorRecord `json:"calibration"`; Timed []detectorRecord `json:"timed"`
 }{N:r.N,TNS:r.T.Nanoseconds(),MemBytes:r.MemBytes,MemAllocs:r.MemAllocs,Mode:*detectorMode,Count:detectorCount}
 if want != 0 { payload.Calibration = &detectorRecords[0]; payload.Timed = detectorRecords[1:] }
 raw, err := json.Marshal(payload); if err != nil { t.Fatal(err) }
 fmt.Printf("DETECTOR_RAW_SAMPLE %s\n", raw)
}

func TestDetectorArithmetic(t *testing.T) {
 for _, tc := range []struct{ns,want int64; ok bool}{{0,0,false},{-1,0,false},{1,1,true},{100,8,true},{101,9,true},{int64(10*time.Second),800000000,true},{int64(10*time.Second)+1,0,false}} {
  got, ok := detectorRequested(tc.ns); if got != tc.want || ok != tc.ok { t.Fatalf("%+v -> %d %v",tc,got,ok) }
 }
}
func TestDetectorActivationPredicate(t *testing.T) {
 if !detectorStart(99, []Mutation{{Key:[]byte("k031"),Value:[]byte("after")}}, CommitDurable).IsZero() { t.Fatal("wrong timestamp") }
 if !detectorStart(100, []Mutation{{Key:[]byte("k032"),Value:[]byte("after")}}, CommitDurable).IsZero() { t.Fatal("wrong key") }
 if !detectorStart(100, nil, CommitDurable).IsZero() { t.Fatal("setup activated") }
 if detectorStart(100, []Mutation{{Key:[]byte("k031"),Value:[]byte("after")}}, CommitDurable).IsZero() { t.Fatal("target inactive") }
}
