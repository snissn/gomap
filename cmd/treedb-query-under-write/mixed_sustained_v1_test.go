package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

// JSON supplies the new optional field without making the predecessor fail to
// compile. The assertions require the field to affect admission and planning.
func TestMixedSustainedAdmissionV1(t *testing.T) {
	base := mixedOptions{Profile: mixedProfileChangingTop10, Window: windowOptions{Admission: recallOptions{Timeout: 10 * time.Minute, RPCTimeout: 3 * time.Second}, Concurrency: 1, Warmup: 64, MaxAttempts: 65536, OutputBytes: 128 << 20, Duration: 300 * time.Second}, Interval: 5 * time.Second}
	for _, tc := range []struct {
		name      string
		originals int
		duration  time.Duration
		rpc       time.Duration
		want      bool
	}{
		{"planned-58", 58, 300 * time.Second, 3 * time.Second, true},
		{"maximum-63", 63, 300 * time.Second, time.Second, true},
		{"default-six", 6, time.Minute, 3 * time.Second, true},
		{"too-few", 5, time.Minute, 3 * time.Second, false},
		{"too-many", 64, 300 * time.Second, 3 * time.Second, false},
		{"negative", -1, time.Minute, 3 * time.Second, false},
		{"duration-cap", 58, 301 * time.Second, 3 * time.Second, false},
		{"no-final-headroom", 58, 291 * time.Second, 3 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := base
			o.Window.Duration, o.Window.Admission.RPCTimeout = tc.duration, tc.rpc
			if tc.name == "maximum-63" {
				o.Interval = 4 * time.Second
			}
			if err := json.Unmarshal([]byte(fmt.Sprintf(`{"Originals":%d}`, tc.originals)), &o); err != nil {
				t.Fatal(err)
			}
			if err := mixedValidate(o); (err == nil) != tc.want {
				t.Fatalf("originals%d duration%v: %v", tc.originals, tc.duration, err)
			}
		})
	}
	// Extending mixed duration must not extend the ordinary read-window mode.
	if err := windowValidate(base.Window); err == nil {
		t.Fatal("ordinary read-window accepted 300 seconds")
	}
}

func mixedSustainedTestPlanV1(t testing.TB, originals int) (recallInput, mixedReport) {
	t.Helper()
	in, old := mixedTestPlanV1(t)
	// The legacy fixture repeats one query sixteen times. Use a distinct
	// second direction and independently rebuild its exported corpus truth.
	in.queries[1] = append([]float32(nil), in.vectors[in.corpusIDs[1]]...)
	scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(in.queries[1])
	if err != nil {
		t.Fatal(err)
	}
	truth, err := recallTop10(context.Background(), scorer, in.corpusIDs, in.vectors)
	if err != nil {
		t.Fatal(err)
	}
	in.exported[1] = nil
	for _, n := range truth {
		in.exported[1] = append(in.exported[1], n.ID)
	}
	old.Admission.Queries = nil
	if err := recallPlan(context.Background(), &in, &old.Admission); err != nil {
		t.Fatal(err)
	}
	r := mixedReport{Profile: mixedProfileChangingTop10, windowReport: old.windowReport, PaceInterval: 5 * time.Second}
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{"Originals":%d}`, originals)), &r); err != nil {
		t.Fatal(err)
	}
	if _, err := mixedPlan(context.Background(), &in, &r); err != nil {
		t.Fatal(err)
	}
	return in, r
}

func TestMixedSustainedMaximumPlanV1(t *testing.T) {
	in, r := mixedSustainedTestPlanV1(t, 63)
	if len(r.Writes) != 63 || len(r.Prefixes) != 64 {
		t.Fatalf("want63 originals/64 native prefixes, got%d/%d", len(r.Writes), len(r.Prefixes))
	}
	if r.Writes[0].Replace == nil || r.Writes[1].Replace == nil || r.Writes[2].Delete == nil || r.Writes[3].Replace == nil || r.Writes[4].Delete == nil || r.Writes[5].Delete == nil {
		t.Fatal("original six-operation sequence changed")
	}
	seen := map[string]bool{}
	for i, w := range r.Writes {
		key := ""
		if w.Replace != nil {
			key = string(w.Replace.IdempotencyKey)
		} else if w.Delete != nil {
			key = string(w.Delete.IdempotencyKey)
		}
		if w.Ordinal != i || key == "" || seen[key] || w.IntendedOffsetNS != int64(time.Duration(i)*r.PaceInterval) {
			t.Fatalf("original%d identity/schedule", i)
		}
		seen[key] = true
		if i >= 6 {
			if w.Replace == nil || w.Delete != nil || string(w.Replace.ID) != string(r.Writes[0].Replace.ID) {
				t.Fatalf("original%d must replace surviving A", i)
			}
			prior := r.Writes[1].Replace.Vector
			if i > 6 {
				prior = r.Writes[i-1].Replace.Vector
			}
			if slices.Equal(prior, w.Replace.Vector) {
				t.Fatalf("original%d unchanged vector", i)
			}
		}
	}
	for i, p := range r.Prefixes {
		if p.Prefix != i || len(p.Truth) != 16 || p.PopulationSHA256 == "" || p.Top10SHA256 == "" {
			t.Fatalf("prefix%d incomplete", i)
		}
		state, err := mixedPopulation(&in, r.Writes[:i])
		if err != nil {
			t.Fatal(err)
		}
		_, want, err := mixedTruthForProfile(context.Background(), state, r.Admission, i, r.Profile)
		if err != nil || want.PopulationSHA256 != p.PopulationSHA256 || want.Top10SHA256 != p.Top10SHA256 {
			t.Fatalf("prefix%d differs from native full-population truth: %v", i, err)
		}
	}
}

func TestMixedSustainedPrefixBit63V1(t *testing.T) {
	in, r := mixedTestPlanV1(t)
	// Synthetic identical native truths isolate the mask boundary; this is not
	// an executed 63-write campaign or a population qualification.
	p := r.Prefixes[0]
	r.Prefixes = make([]mixedPrefix, 64)
	for i := range r.Prefixes {
		r.Prefixes[i] = p
		r.Prefixes[i].Prefix = i
	}
	q := r.Admission.Queries[0]
	response := windowTestResponse(&r.Admission)
	for _, tc := range []struct {
		lower, upper int
		mask         uint64
	}{{63, 63, uint64(1) << 63}, {0, 63, ^uint64(0)}} {
		proof, err := mixedValidatePrefixProof(&q, response, &in, &r, tc.lower, tc.upper)
		if err != nil || uint64(proof.CompatibleMask) != tc.mask || proof.RecallAt10 != 1 {
			t.Fatalf("range%d..%d mask%x want%x: %v", tc.lower, tc.upper, proof.CompatibleMask, tc.mask, err)
		}
	}
	for _, bounds := range [][2]int{{-1, 63}, {0, 64}, {64, 64}, {63, 62}} {
		if _, err := mixedValidatePrefixProof(&q, response, &in, &r, bounds[0], bounds[1]); err == nil {
			t.Fatalf("invalid range accepted%v", bounds)
		}
	}
	r.Prefixes[63].Prefix = 62
	if _, err := mixedValidatePrefixProof(&q, response, &in, &r, 63, 63); err == nil {
		t.Fatal("duplicate prefix ordinal accepted")
	}
	r.Prefixes[63].Prefix = 63
	r.Prefixes[63].Truth = nil
	if _, err := mixedValidatePrefixProof(&q, response, &in, &r, 63, 63); err == nil {
		t.Fatal("missing prefix oracle accepted")
	}
	r.Prefixes[63] = p
	r.Prefixes[63].Prefix = 63
	r.Prefixes = append(r.Prefixes, p)
	if _, err := mixedValidatePrefixProof(&q, response, &in, &r, 0, 63); err == nil {
		t.Fatal("65-prefix overflow accepted")
	}
}

func TestMixedSustainedCLIAndBoundsV1(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		reason string
	}{
		{[]string{"-mode", "mixed-window", "-mixed-originals", "0"}, "mixed-originals"},
		{[]string{"-mode", "mixed-window", "-mixed-originals", "64"}, "mixed-originals"},
		{[]string{"-mode", "read-window", "-mixed-originals", "6"}, "mixed flags require"},
		{[]string{"-mode", "mixed-window", "-mixed-originals", "58", "-read-window", "291s", "-rpc-timeout", "3s", "-timeout", "10m", "-read-concurrency", "1"}, "positive headroom"},
	} {
		var output bytes.Buffer
		if err := runArgs(context.Background(), tc.args, &output); err == nil || !strings.Contains(err.Error(), tc.reason) {
			t.Fatalf("CLI admission%v: %v", tc.args, err)
		}
	}
	raw, err := json.Marshal(mixedReadPrefix{Ordinal: 65535, Lower: 63, Upper: 63, Matched: 63, CompatibleMask: ^uint64(0), RecallAt10: .9})
	if err != nil || len(raw)+1 > mixedReadPrefixMaxBytes {
		t.Fatalf("maximum prefix encoding exceeds reserved bound: %d %v", len(raw), err)
	}
	if !bytes.Contains(raw, []byte("18446744073709551615")) {
		t.Fatal("mask lost uint64 precision")
	}
	in, old := mixedTestPlanV1(t)
	r := mixedReport{Originals: 7, Profile: mixedProfileChangingTop10, windowReport: old.windowReport, PaceInterval: time.Second}
	// Legacy queries are identical. Extended alternating vector changes must refuse.
	if _, err := mixedPlan(context.Background(), &in, &r); err == nil {
		t.Fatal("extended identical vector directions accepted")
	}
}

// Local setup and canonical validation only; neither RPC nor population audit is timed.
func BenchmarkMixedSustained58V1(b *testing.B) {
	b.Run("plan", func(b *testing.B) {
		in, prepared := mixedSustainedTestPlanV1(b, 58)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r := mixedReport{Originals: 58, Profile: mixedProfileChangingTop10, windowReport: windowReport{Admission: prepared.Admission}, PaceInterval: 5 * time.Second}
			if _, err := mixedPlan(context.Background(), &in, &r); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("validate-all-compatible", func(b *testing.B) {
		in, r := mixedSustainedTestPlanV1(b, 58)
		q := r.Admission.Queries[0]
		response := windowTestResponse(&r.Admission)
		var ids []string
		for id := range in.vectors {
			changed := false
			for _, v := range r.Prefixes[0].Changed[0] {
				changed = changed || id == v.ID
			}
			if !changed {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		var err error
		response.Neighbors, err = recallTop10(context.Background(), q.scorer, ids, in.vectors)
		if err != nil {
			b.Fatal(err)
		}
		proof, err := mixedValidatePrefixProof(&q, response, &in, &r, 0, 58)
		if err != nil || proof.CompatibleMask != (uint64(1)<<59)-1 {
			b.Fatalf("fixture not all-compatible: %+v %v", proof, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := mixedValidatePrefixProof(&q, response, &in, &r, 0, 58); err != nil {
				b.Fatal(err)
			}
		}
	})
}
