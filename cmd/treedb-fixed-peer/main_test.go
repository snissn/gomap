package main

import (
	"strings"
	"testing"
)

func TestReadFixedPeerConfig(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		valid       bool
	}{
		{"valid", `{"NodeID":"node-a"}`, true},
		{"trailing", `{"NodeID":"node-a"} {}`, false},
		{"unknown", `{"NotAConfigField":true}`, false},
		{"over-1-MiB", `{"NodeID":"node-a"}` + strings.Repeat(" ", 1<<20), true},
		{"over-8-MiB", `{"NodeID":"node-a"}` + strings.Repeat(" ", maxFixedPeerConfigBytes), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config, err := readConfig(strings.NewReader(tt.input))
			if (err == nil) != tt.valid {
				t.Fatalf("config=%+v err=%v", config, err)
			}
		})
	}
}
