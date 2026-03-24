package main

import "testing"

func TestParseValueLogPostsyncStrategy(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: "auto"},
		{in: "auto", want: "auto"},
		{in: "offline", want: "offline"},
		{in: "explicit", want: "explicit"},
		{in: "hybrid", want: "hybrid"},
		{in: "HYBRID", want: "hybrid"},
		{in: "bogus", wantErr: true},
	}
	for _, tc := range tests {
		got, err := parseValueLogPostsyncStrategy(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseValueLogPostsyncStrategy(%q): want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseValueLogPostsyncStrategy(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("parseValueLogPostsyncStrategy(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}
