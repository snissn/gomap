//go:build !windows

package caching

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestStableCapturePartialTransferDetachesOwnedSlots(t *testing.T) {
	writer, err := valuelog.NewWriter(filepath.Join(t.TempDir(), "leaf.log"), 9)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	makeToken := func() *rootpublication.StableResourceToken {
		token, err := writer.StableResourceToken(valuelog.StableResourceRegistration{
			Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf", Generation: 9,
			DiagnosticPath: "leaf_vlog/leaf.log", Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	transferred := makeToken()
	rejected := makeToken()
	rejected.Release() // second builder.Add fails after the first transfer.
	capture := newStableOuterLeafCapture(nil, nil)
	if err := capture.addToken(transferred); err != nil {
		t.Fatal(err)
	}
	if err := capture.addToken(rejected); err != nil {
		t.Fatal(err)
	}
	backing := capture.tokens[:cap(capture.tokens)]
	if set, err := capture.freeze([]page.ValuePtr{{FileID: 9}}); !errors.Is(err, rootpublication.ErrResourceOwnership) || set != nil {
		t.Fatalf("partial transfer failure set=%v err=%v", set, err)
	}
	for _, token := range backing {
		if token != nil {
			t.Fatal("partial transfer retained token alias")
		}
	}
	if err := transferred.WithPinnedFile(func(_ *os.File) error { return nil }); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("builder abandon did not release first transferred token: %v", err)
	}
	capture.abandon() // idempotent cleanup after failed partial transfer.
}
