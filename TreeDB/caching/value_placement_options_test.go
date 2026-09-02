package caching

import (
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestOpenRejectsInvalidValueLogDomainInlineThresholds(t *testing.T) {
	cases := []struct {
		name    string
		domains []backenddb.ValueLogDomainThreshold
		want    string
	}{
		{
			name: "empty prefix",
			domains: []backenddb.ValueLogDomainThreshold{
				{Prefix: nil, InlineThreshold: 16},
			},
			want: "empty prefix",
		},
		{
			name: "negative threshold",
			domains: []backenddb.ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: -1},
			},
			want: "negative inline threshold",
		},
		{
			name: "duplicate prefix",
			domains: []backenddb.ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 16},
				{Prefix: []byte("hot/"), InlineThreshold: 8},
			},
			want: "duplicate value-log domain threshold prefix",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{Dir: dir})
			if err != nil {
				t.Fatalf("backend open: %v", err)
			}
			defer backend.Close()

			_, err = Open(dir, backend, Options{
				FlushThreshold:                 1 << 20,
				ValueLogDomainInlineThresholds: tc.domains,
			})
			if err == nil {
				t.Fatalf("Open error = nil, want %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Open error = %q, want substring %q", err, tc.want)
			}
		})
	}
}
