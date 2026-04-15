package main

import (
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParseUint64CSV(t *testing.T) {
	got, err := parseUint64CSV("3, 1, 3, 2, 0")
	if err != nil {
		t.Fatalf("parseUint64CSV: %v", err)
	}
	want := []uint64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%d, want %d (%v)", i, got[i], want[i], got)
		}
	}
}

func TestFormatUint32List(t *testing.T) {
	if got, want := formatUint32List(nil), "-"; got != want {
		t.Fatalf("formatUint32List(nil)=%q, want %q", got, want)
	}
	if got, want := formatUint32List([]uint32{7, 11, 42}), "7,11,42"; got != want {
		t.Fatalf("formatUint32List=%q, want %q", got, want)
	}
}

func TestUsageTextMentionsLeafGenerationGC(t *testing.T) {
	if got := usageText; !strings.Contains(got, "leafgen-gc") {
		t.Fatalf("usageText missing leafgen-gc command: %q", got)
	}
}

func TestChooseLeafGenerationPackIDs(t *testing.T) {
	ids, err := chooseLeafGenerationPackIDs([]uint64{7, 11}, false, nil)
	if err != nil {
		t.Fatalf("explicit choose err=%v", err)
	}
	if got, want := len(ids), 2; got != want || ids[0] != 7 || ids[1] != 11 {
		t.Fatalf("explicit ids=%v, want [7 11]", ids)
	}

	plan := &treedb.LeafGenerationPlan{Admission: "eligible", CandidateGenerationIDs: []uint64{3, 5}}
	ids, err = chooseLeafGenerationPackIDs(nil, true, plan)
	if err != nil {
		t.Fatalf("from-plan choose err=%v", err)
	}
	if got, want := len(ids), 2; got != want || ids[0] != 3 || ids[1] != 5 {
		t.Fatalf("from-plan ids=%v, want [3 5]", ids)
	}

	if _, err := chooseLeafGenerationPackIDs([]uint64{1}, true, plan); err == nil {
		t.Fatalf("expected conflicting explicit/from-plan selection to fail")
	}
	if _, err := chooseLeafGenerationPackIDs(nil, true, nil); err == nil {
		t.Fatalf("expected nil plan to fail")
	}
	if _, err := chooseLeafGenerationPackIDs(nil, true, &treedb.LeafGenerationPlan{Admission: "reclaim_per_copy_too_low", CandidateGenerationIDs: []uint64{3}}); err == nil {
		t.Fatalf("expected non-eligible plan admission to fail")
	}
	if _, err := chooseLeafGenerationPackIDs(nil, true, &treedb.LeafGenerationPlan{Admission: "eligible"}); err == nil {
		t.Fatalf("expected empty eligible plan to fail")
	}
	if _, err := chooseLeafGenerationPackIDs(nil, false, nil); err == nil {
		t.Fatalf("expected empty explicit selection to fail")
	}
}
