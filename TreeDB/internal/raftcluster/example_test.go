package raftcluster_test

import (
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func ExampleValidate() {
	resolved, err := raftcluster.Validate(raftcluster.Config{
		Dir:     "/var/lib/treedb",
		NodeID:  "node-a",
		GroupID: "default",
		Peers: []raftcluster.Peer{
			{ID: "node-a", Address: "10.0.0.1:9201"},
			{ID: "node-b", Address: "10.0.0.2:9201"},
			{ID: "node-c", Address: "10.0.0.3:9201"},
		},
	})
	fmt.Println(err == nil)
	fmt.Println(filepath.ToSlash(resolved.Layout.LogDir))
	fmt.Println(filepath.ToSlash(resolved.Layout.StableDir))
	fmt.Println(filepath.ToSlash(resolved.Layout.SnapshotDir))
	// Output:
	// true
	// /var/lib/treedb/raftcluster/nodes/node-a/groups/default/log
	// /var/lib/treedb/raftcluster/nodes/node-a/groups/default/stable
	// /var/lib/treedb/raftcluster/nodes/node-a/groups/default/snapshots
}
