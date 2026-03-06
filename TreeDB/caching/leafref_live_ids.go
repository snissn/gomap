package caching

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pageGetter interface {
	Get(pageID uint64) ([]byte, error)
}

func collectLeafRefValueLogLiveIDs(p pageGetter, rootID uint64, live map[uint32]struct{}) error {
	return leafrefscan.Walk(context.Background(), rootID, p.Get, func(pageID uint64, n node.Node) error {
		if !n.VerifyChecksum() {
			return fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		return nil
	}, func(ptr page.ValuePtr) error {
		live[ptr.FileID] = struct{}{}
		return nil
	})
}
