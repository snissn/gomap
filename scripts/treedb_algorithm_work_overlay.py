#!/usr/bin/env python3
"""Create a disposable Go overlay; never alter the measured production source."""
import hashlib
import json
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1]).resolve()
output = pathlib.Path(sys.argv[2]).resolve()
output.mkdir(parents=True, exist_ok=False)
source = root / "TreeDB/tree/tree.go"
original = source.read_text()
start = original.index("func (t *Tree) loadNodeViewWithLoadKindInto(")
end = original.index("\n// GetEntry returns", start)
function = original[start:end]
assert function.count("\treturn nil\n}") == 1
function = function.replace("\treturn nil\n}", "\tif dst.Type() == page.PageTypeInternal { algorithmWorkVisit(t.rootPageID, pageID) }\n\treturn nil\n}")
instrumented = output / "tree.go"
instrumented.write_text(original[:start] + function + original[end:])
counter = output / "counter.go"
counter.write_text('''package tree
import "sync"
var algorithmWorkCounter struct {
 sync.Mutex
 enabled bool
 visits uint64
 nodes map[[2]uint64]struct{}
}
func AlgorithmWorkBegin() {
 algorithmWorkCounter.Lock(); defer algorithmWorkCounter.Unlock()
 algorithmWorkCounter.enabled = true
 algorithmWorkCounter.visits = 0
 algorithmWorkCounter.nodes = make(map[[2]uint64]struct{})
}
func algorithmWorkVisit(root, page uint64) {
 algorithmWorkCounter.Lock(); defer algorithmWorkCounter.Unlock()
 if !algorithmWorkCounter.enabled { return }
 algorithmWorkCounter.visits++
 algorithmWorkCounter.nodes[[2]uint64{root,page}] = struct{}{}
}
func AlgorithmWorkEnd() (uint64, uint64) {
 algorithmWorkCounter.Lock(); defer algorithmWorkCounter.Unlock()
 algorithmWorkCounter.enabled = false
 return algorithmWorkCounter.visits, uint64(len(algorithmWorkCounter.nodes))
}
''')
diagnostic = output / "diagnostic_test.go"
diagnostic.write_text('''package treedb
import (
 "encoding/binary"
 "fmt"
 "os"
 "testing"
 "github.com/snissn/gomap/TreeDB/tree"
)
func TestAlgorithmWorkInternalVisits(t *testing.T) {
 keys:=250000; if os.Getenv("TREEDB_ALGORITHM_PILOT")=="1" { keys=8192 }
 for _,pointer:=range []bool{false,true} {
 d:=algorithmFixture(t,algorithmOptions(t.TempDir(),pointer,false),keys)
 defer func(){ if err:=d.Close();err!=nil {t.Error(err)} }()
 algorithmVerify(t,d,keys,0)
 for _,shape:=range []string{"sorted","clustered","uniform"} {
  for _,view:=range []bool{false,true} {
   var total, union uint64
   for _,batch:=range algorithmBatches(keys,shape) {
    consume:=func(_ int,key,value []byte,found bool)error {
     physical:=int(binary.BigEndian.Uint64(key[24:]));if physical%2==1 {if found || value!=nil {return fmt.Errorf("missing found")};return nil}
     if !found || !algorithmValid(value,physical/2,0) {return fmt.Errorf("invalid value")};return nil
    }
    tree.AlgorithmWorkBegin()
    var err error
    if view {err=d.GetManyView(batch,consume)} else { var out [][]byte; out,err=d.GetMany(batch);if err==nil {if len(out)!=len(batch) {t.Fatal("output count")};for i,value:=range out {if err=consume(i,batch[i],value,value!=nil);err!=nil {break}}} }
    visits,unique:=tree.AlgorithmWorkEnd()
    if err!=nil || visits==0 || unique==0 || unique>visits {t.Fatal("invalid diagnostic",err,visits,unique)}
    total+=visits;union+=unique
   }
   t.Logf("pointer=%v shape=%s view=%v batches=128 keys_per_batch=64 internal_visits=%d potential_union_visits=%d repeated_visits=%d",pointer,shape,view,total,union,total-union)
  }
 }
 }
}
''')
replacements = {
    str(source): str(instrumented),
    str(root / "TreeDB/tree/algorithm_work_overlay.go"): str(counter),
    str(root / "TreeDB/algorithm_work_overlay_test.go"): str(diagnostic),
}
(output / "overlay.json").write_text(json.dumps({"Replace": replacements}, indent=2) + "\n")
manifest = {
    "schema": "algorithm-work-overlay-v1",
    "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
    "source_git_blob": subprocess.check_output(["git", "hash-object", str(source)], cwd=root, text=True).strip(),
    "runtime_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "runtime_tree": subprocess.check_output(["git", "rev-parse", "HEAD:TreeDB"], cwd=root, text=True).strip(),
    "files": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (instrumented, counter, diagnostic)},
    "interpretation": "actual validated internal-node loads; per-batch unique (root,page) pairs are potential union visits, not implemented traversal or timing evidence",
}
(output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(output / "overlay.json")
