//go:build linux

package caching

import (
	"bytes"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// Keep the prior implementation as a behavioral oracle, including its Unicode
// whitespace, ignored label/unit, ParseUint failures and uint64 multiplication.
func parseProcStatusKBLineReference(line []byte) (uint64, bool) {
	fields := bytes.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	v, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0, false
	}
	return v * 1024, true
}

func TestParseProcStatusKBLine(t *testing.T) {
	for _, tc := range []struct {
		line string
		want uint64
		ok   bool
	}{
		{"VmRSS:\t12345 kB", 12345 * 1024, true},
		{" VmHWM: 0 ", 0, true},
		{"anything\v00042\fignored trailing fields", 42 * 1024, true},
		{"VmRSS:\u00a0123\u202fkB", 123 * 1024, true},
		{"VmRSS: 18446744073709551615 kB", math.MaxUint64 - 1023, true},
		{"VmRSS: 18014398509481984 kB", 0, true}, // Preserve multiplication wraparound.
		{"VmRSS: 12 kB\xff", 12 * 1024, true},    // Ignored suffix is not validated.
		{"", 0, false},
		{" \t\n", 0, false},
		{"VmRSS:", 0, false},
		{"123", 0, false},
		{"VmRSS: +1 kB", 0, false},
		{"VmRSS: -1 kB", 0, false},
		{"VmRSS: 1_000 kB", 0, false},
		{"VmRSS: 0x10 kB", 0, false},
		{"VmRSS: 18446744073709551616 kB", 0, false},
		{"VmRSS: 12kB", 0, false},
		{"VmRSS: \xff12 kB", 0, false},
		{"VmRSS:\x8512 kB", 0, false}, // Raw invalid UTF-8 is not Unicode space.
	} {
		t.Run(strconv.QuoteToASCII(tc.line), func(t *testing.T) {
			got, ok := parseProcStatusKBLine([]byte(tc.line))
			if got != tc.want || ok != tc.ok {
				t.Fatalf("parse = (%d, %t), want (%d, %t)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestParseProcStatusKBLineMatchesFields(t *testing.T) {
	check := func(line []byte) {
		t.Helper()
		got, ok := parseProcStatusKBLine(line)
		want, wantOK := parseProcStatusKBLineReference(line)
		if got != want || ok != wantOK {
			t.Fatalf("parse %q = (%d, %t), prior Fields parser = (%d, %t)", line, got, ok, want, wantOK)
		}
	}
	check(nil)
	// Cover every Unicode White_Space rune, including multibyte separators and
	// ASCII controls, with valid, invalid and overflowing second fields.
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if !unicode.IsSpace(r) {
			continue
		}
		sep := string(r)
		for _, value := range []string{"0", "12345", "+1", "-1", "12\xff", "18446744073709551615", "18446744073709551616"} {
			check([]byte(sep + "VmRSS:" + sep + value + sep + "kB" + sep))
		}
	}
	for _, n := range []int{32, 1024, 65536} {
		check([]byte("VmRSS: " + strings.Repeat("0", n) + "1 kB"))
		check([]byte("VmRSS: " + strings.Repeat("9", n) + " kB"))
		check([]byte("VmRSS: 123 " + strings.Repeat("\xff\u2003", n)))
	}
	// Fixed seed gives repeatable arbitrary malformed bytes and structured
	// second tokens without a timed fuzz campaign.
	rng := rand.New(rand.NewSource(1))
	separators := []string{" ", "\t", "\n", "\v\f\r", "\u0085", "\u00a0", "\u2003", "\u202f", "\u3000", "\xff", ""}
	for i := 0; i < 4096; i++ {
		line := make([]byte, rng.Intn(128))
		_, _ = rng.Read(line)
		check(line)
		value := strconv.FormatUint(rng.Uint64(), 10)
		if i%2 == 0 {
			value += string(line)
		}
		check([]byte("VmRSS:" + separators[rng.Intn(len(separators))] + value + separators[rng.Intn(len(separators))] + "kB"))
	}
}

func TestParseProcStatusKBLineAllocations(t *testing.T) {
	for _, line := range []string{"VmRSS:\t12345 kB", "VmHWM:\u00a012345\u2003kB"} {
		input := []byte(line)
		var got uint64
		var ok bool
		prior := testing.AllocsPerRun(1000, func() { got, ok = parseProcStatusKBLineReference(input) })
		if !ok || got != 12345*1024 {
			t.Fatal("prior parser result changed")
		}
		current := testing.AllocsPerRun(1000, func() { got, ok = parseProcStatusKBLine(input) })
		if !ok || got != 12345*1024 {
			t.Fatal("parser result changed")
		}
		t.Logf("input %q: prior=%g current=%g allocations/call (non-timed control)", line, prior, current)
		if current != 0 {
			t.Errorf("parser allocates %g objects/call, want zero", current)
		}
	}
}
