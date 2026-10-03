package main

import (
	"bytes"
	"context"
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

func TestFixedPeerFixtureCLIRejectsBeforeConfigOrNetwork(t *testing.T) {
	for _, args := range [][]string{
		{"-mode", "initialize"},
		{"-mode", "qualify", "-request-id", "bad/id"},
		{"-mode", "initialize", "-request-id", "good", "-operation-timeout", "0s"},
		{"-mode", "qualify", "-request-id", "good", "-operation-timeout", "11m"},
		{"-mode", "initialize", "-request-id", "good", "-trusted-network-test"},
	} {
		var output bytes.Buffer
		if err := runArgs(context.Background(), args, &output); err == nil || output.Len() != 0 {
			t.Fatalf("args=%v err=%v output=%s", args, err, output.String())
		}
	}
}
