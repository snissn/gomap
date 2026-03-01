package main

import "testing"

func TestParseTreeDBOuterLeafMode_V1Only(t *testing.T) {
	t.Run("v1 accepted", func(t *testing.T) {
		got, err := parseTreeDBOuterLeafMode("v1")
		if err != nil {
			t.Fatalf("parse v1: %v", err)
		}
		if got != "v1" {
			t.Fatalf("mode=%q want v1", got)
		}
	})

	t.Run("legacy modes rejected", func(t *testing.T) {
		cases := []string{"v1_leaflog", "v1_leaflog_route", "v1_leaflog_legacy", "v2_blockptr", "v2_fenceptr"}
		for _, c := range cases {
			if _, err := parseTreeDBOuterLeafMode(c); err == nil {
				t.Fatalf("expected parse failure for %q", c)
			}
		}
	})
}

func TestParseTreeDBWALFenceMode_RIDJoinOnly(t *testing.T) {
	if _, err := parseTreeDBWALFenceMode("rid_join"); err != nil {
		t.Fatalf("rid_join should be accepted: %v", err)
	}
	if _, err := parseTreeDBWALFenceMode("simple_inline"); err == nil {
		t.Fatalf("simple_inline should be rejected")
	}
}
