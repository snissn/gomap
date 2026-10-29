//go:build treedb_test
package mvcc
import("errors";"testing")
func TestDetectorNegativeShapeSuccessor(t *testing.T) {
 target:=[]Mutation{{Key:[]byte("k031"),Value:[]byte("after")}}
 for _,mode:=range []CommitMode{CommitRelaxed,CommitMode(99)} {
  if !detectorStart(100,target,mode).IsZero(){t.Fatal("non-durable/invalid mode activated")}
 }
 if !detectorStart(100,[]Mutation{{Key:[]byte("k031"),Value:[]byte("before")}},CommitDurable).IsZero(){t.Fatal("wrong value activated")}
 if !detectorStart(100,append(target,Mutation{Key:[]byte("k032"),Value:[]byte("after")}),CommitDurable).IsZero(){t.Fatal("multi-mutation activated")}
}
func TestDetectorErrorPreservedSuccessor(t *testing.T) {
 saved:=*detectorMode;*detectorMode="positive";defer func(){*detectorMode=saved}()
 detectorCount=0;detectorInvalid=""
 injected:=errors.New("exact underlying durable error")
 probe:=&logicalWriterProbe{enabled:true,err:injected}
 store,_:=logicalProbeStore(probe,0)
 err:=store.CommitAt(100,[]Mutation{{Key:[]byte("k031"),Value:[]byte("after")}},CommitDurable)
 if !errors.Is(err,injected)||!errors.Is(err,ErrStorage)||!probe.sync||probe.calls!=1{t.Fatalf("lost error/durable path: %v %+v",err,probe)}
 if detectorCount!=0||detectorInvalid!=""{t.Fatalf("failed commit waited/recorded: %d %s",detectorCount,detectorInvalid)}
}
