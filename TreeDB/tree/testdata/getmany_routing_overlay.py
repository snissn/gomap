#!/usr/bin/env python3
"""Count existing node method calls in an untimed, disposable Go overlay."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1]).resolve()
output = pathlib.Path(sys.argv[2]).resolve()
output.mkdir(exist_ok=False)
source = root / "TreeDB/tree/tree.go"
text = source.read_text().replace(
    "n.SearchInternalChildRef(", "algorithmWorkChildSearch(&n, "
).replace("return n.GetInternalEntryRefView(", "return algorithmWorkEntryRef(n, ")
(output / "tree.go").write_text(text)
(output / "routing.go").write_text('''package tree
import (
 "github.com/snissn/gomap/TreeDB/node"
 "github.com/snissn/gomap/TreeDB/page"
)
var algorithmWorkChildSearches, algorithmWorkEntryRefs uint64
func algorithmWorkChildSearch(n *node.Node, key []byte) (page.ChildRef, bool, error) {
 algorithmWorkChildSearches++
 return n.SearchInternalChildRef(key)
}
func algorithmWorkEntryRef(n *node.Node, index uint16) ([]byte, page.ChildRef, error) {
 algorithmWorkEntryRefs++
 return n.GetInternalEntryRefView(index)
}
''')
(output / "overlay.json").write_text(json.dumps({"Replace": {
    str(source): str(output / "tree.go"),
    str(root / "TreeDB/tree/getmany_routing_overlay.go"): str(output / "routing.go"),
}}, indent=2) + "\n")
