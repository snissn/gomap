if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import source_path, isolate_paths
"""Inert local constructor for an offline test-only oracle overlay.
Does not invoke Go/Git/subprocess/network. Root supplies frozen final pins and
runs the proposed command separately. Native output is pending root acceptance.
"""
import argparse, hashlib, importlib.util, json, os, pathlib, re, shlex
COLLECTOR=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py')
COLLECTOR_SHA='901c07745bd245868a1c530794045a5cd3ef0a8174963b54216fb4a3009c57b9'
GO_SOURCE=r'''package main

import (
 "context"
 "encoding/json"
 "os"
 "path/filepath"
 "sort"
 "testing"
 "time"
 public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// Test-only virtual file. No client/network constructor or serving call.
func TestTrial24FrozenCanonicalPrefixOraclePreparation(t *testing.T) {
 var cfg struct { SourceHead, SourceTree, InputInventorySHA256, InitialPopulationSHA256, Config, Bootstrap, Dataset, Provenance, Probe, Output string }
 raw,err:=os.ReadFile(os.Getenv("GOMAP_TRIAL24_ORACLE_INPUT"));if err!=nil {t.Fatal(err)}
 if err=recallDecode(context.Background(),raw,&cfg);err!=nil {t.Fatal(err)}
 if !recallHex(cfg.SourceHead,20)||!recallHex(cfg.SourceTree,20)||!recallHex(cfg.InputInventorySHA256,32)||!recallHex(cfg.InitialPopulationSHA256,32)||!filepath.IsAbs(cfg.Output) {t.Fatal("incomplete frozen identities")}
 ctx,cancel:=context.WithTimeout(context.Background(),120*time.Second);defer cancel()
 admission:=recallReport{Version:1,Kind:"fixed_cluster_mixed_window_admission_v1",Phase:"post-only",RunID:"rf4trial24mixedchangingc1mixedc1v1",Timeout:120*time.Second,RPCTimeout:3*time.Second}
 var in recallInput
 if err=recallPrepare(ctx,recallOptions{Config:cfg.Config,Bootstrap:cfg.Bootstrap,Dataset:cfg.Dataset,Provenance:cfg.Provenance,Probe:cfg.Probe,Phase:admission.Phase,RunID:admission.RunID,Timeout:admission.Timeout,RPCTimeout:admission.RPCTimeout},&in,&admission);err!=nil {t.Fatal(err)}
 if admission.PopulationRows!=10005||admission.PopulationSHA256!=cfg.InitialPopulationSHA256||len(admission.Queries)!=16||len(in.corpusIDs)!=10000 {t.Fatal("actual admitted population mismatch")}
 exportedBefore:=hashJSON(in.exported)
 r:=mixedReport{Originals:48,Profile:mixedProfileChangingTop10,windowReport:windowReport{Admission:admission},PaceInterval:6*time.Second}
 final,err:=mixedPlan(ctx,&in,&r);if err!=nil {t.Fatal(err)}
 if len(r.Writes)!=48||len(r.Prefixes)!=49||len(final.vectors)!=10002||hashJSON(in.exported)!=exportedBefore {t.Fatal("prefix shape/exported admission changed")}
 // Recheck complete population and native canonical top10 at each causal state.
 for prefix,p:=range r.Prefixes {
  state,e:=mixedPopulation(&in,r.Writes[:prefix]);if e!=nil {t.Fatal(e)}
  ids:=make([]string,0,len(state.vectors));for id:=range state.vectors {ids=append(ids,id)};sort.Strings(ids)
  if p.Prefix!=prefix||p.PopulationRows!=len(ids) {t.Fatal("prefix population shape")}
  for qi,q:=range admission.Queries {
   truth,e:=recallTop10(ctx,q.scorer,ids,state.vectors)
   if e!=nil||!pacedSameTruth(truth,p.Truth[qi]) {t.Fatalf("full native prefix%d query%d: %v",prefix,qi,e)}
  }
 }
 again:=mixedReport{Originals:48,Profile:r.Profile,windowReport:windowReport{Admission:admission},PaceInterval:r.PaceInterval}
 if _,err=mixedPlan(ctx,&in,&again);err!=nil||hashJSON(again.Writes)!=hashJSON(r.Writes)||hashJSON(again.Prefixes)!=hashJSON(r.Prefixes) {t.Fatalf("non-deterministic originals/oracles: %v",err)}
 type original struct { Ordinal int; Kind string; Replace *public.ReplaceRequestV1 `json:",omitempty"`; Delete *public.DeleteRequestV1 `json:",omitempty"` }
 originals:=make([]original,0,48)
 for i,w:=range r.Writes {
  if w.Ordinal!=i||(w.Replace==nil)==(w.Delete==nil)||w.Replace!=nil&&w.Kind!="replace"||w.Delete!=nil&&w.Kind!="delete" {t.Fatal("invalid original pointer/kind")}
  originals=append(originals,original{Ordinal:w.Ordinal,Kind:w.Kind,Replace:w.Replace,Delete:w.Delete})
 }
 packet:=struct {
  State string `json:"state"`
  RunID, Profile string
  SourceHead string `json:"source_head"`
  SourceTree string `json:"source_tree"`
  InputInventorySHA256, InitialPopulationSHA256 string
  Prefixes []mixedPrefix
  OriginalRequests []original
 }{"NATIVE_CANONICAL_PREFIX_ORACLES_GENERATED_PENDING_ROOT_VALIDATION",admission.RunID,r.Profile,cfg.SourceHead,cfg.SourceTree,cfg.InputInventorySHA256,admission.PopulationSHA256,r.Prefixes,originals}
 data,err:=json.Marshal(packet);if err!=nil {t.Fatal(err)}
 if len(data)>1<<20 {t.Fatal("bounded oracle packet exceeded")}
 f,err:=os.OpenFile(cfg.Output,os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600);if err!=nil {t.Fatal(err)}
 n,writeErr:=f.Write(append(data,'\n'));closeErr:=f.Close()
 if writeErr!=nil||closeErr!=nil||n!=len(data)+1 {t.Fatalf("retained oracle write: %v %v",writeErr,closeErr)}
 if err=ctx.Err();err!=nil {t.Fatal(err)}
}
'''

def read(p,cap=64<<20):
 p=pathlib.Path(p);assert p.is_file() and not p.is_symlink() and p.stat().st_size<=cap
 raw=p.read_bytes();assert len(raw)<=cap;return raw

def sha(raw):return hashlib.sha256(raw).hexdigest()

def prepare(pin_path,out):
 assert sha(read(COLLECTOR))==COLLECTOR_SHA
 spec=importlib.util.spec_from_file_location('trial24_collector_v2',COLLECTOR)
 c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
 a=c.strict_json(read(pin_path,1<<20))
 assert set(a)=={'source_root','source_head','source_tree','source_inventory','source_inventory_sha256','input_root','input_inventory_sha256','initial_oracle','initial_oracle_sha256','RootAcceptedFinalSourceAndInputs'}
 assert a['RootAcceptedFinalSourceAndInputs'] is True
 assert all(c.digest_valid(a[k],40) for k in ('source_head','source_tree'))
 assert all(c.digest_valid(a[k]) for k in ('source_inventory_sha256','input_inventory_sha256','initial_oracle_sha256'))
 root=pathlib.Path(a['source_root']);inputs=pathlib.Path(a['input_root']);out=pathlib.Path(out)
 assert all(p.is_absolute() for p in (root,inputs,out)) and root.is_dir() and inputs.is_dir()
 assert not out.exists() and not out.is_symlink()
 isolate_paths([out],[root.resolve(),inputs,pathlib.Path(__file__).resolve().parent,pathlib.Path(__file__).resolve(),pathlib.Path(COLLECTOR).resolve(),pathlib.Path(__file__).with_name('source_paths.py').resolve(),pin_path,a['source_inventory'],a['initial_oracle']])
 inventory_raw=read(a['source_inventory']);assert sha(inventory_raw)==a['source_inventory_sha256']
 source=c.strict_json(inventory_raw);assert set(source)=={'head','tree','rows','overlays'}
 assert source['head']==a['source_head'] and source['tree']==a['source_tree'] and source['overlays']=={}
 expected={}
 for row in source['rows']:
  name=c.input_name(row['path']);assert name not in expected and row['mode'] in ('100644','100755')
  raw=read(root/name);blob=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
  assert blob==row['git_blob'] and bool((root/name).stat().st_mode & 0o111)==(row['mode']=='100755'),name
  expected[name]=blob
 package='cmd/treedb-query-under-write'
 actual={package+'/'+p.name for p in (root/package).glob('*.go')}
 assert actual=={name for name in expected if pathlib.PurePosixPath(name).parent.as_posix()==package and name.endswith('.go')}
 inv_raw=read(inputs/'input-inventory.json');assert sha(inv_raw)==a['input_inventory_sha256']
 inv=c.validate_input_inventory(c.strict_json(inv_raw));files={};total=0
 for name,digest in inv.items():
  raw=read(inputs/name);total+=len(raw);assert total<=256<<20 and sha(raw)==digest,name;files[name]=raw
 baseline=c.strict_json(files['baseline.json']);bootstrap=c.strict_json(files['bootstrap.json'])
 vectors=c.build_initial_population(files,bootstrap,baseline)
 oracle_raw=read(a['initial_oracle']);assert sha(oracle_raw)==a['initial_oracle_sha256']
 initial=c.strict_json(oracle_raw);assert initial==c.population_identity(vectors,128) and initial['Rows']==10005
 for name in ('config.json','bootstrap.json','provenance.json','probe.jsonl','dataset/manifest.json','dataset/documents.f32','dataset/queries.f32','dataset/exact_truth.jsonl'):assert name in files,name
 virtual=str(root/package/'trial24_prefix_oracle_prepare_test.go');assert not pathlib.Path(virtual).exists()
 out.mkdir()
 go_path=out/'prefix_oracle_prepare_test.go';go_path.write_text(GO_SOURCE)
 overlay=out/'overlay.json';overlay.write_text(json.dumps({'Replace':{virtual:str(go_path)}})+'\n')
 cfg={'SourceHead':a['source_head'],'SourceTree':a['source_tree'],'InputInventorySHA256':a['input_inventory_sha256'],'InitialPopulationSHA256':initial['SHA256'],'Config':str(inputs/'config.json'),'Bootstrap':str(inputs/'bootstrap.json'),'Dataset':str(inputs/'dataset'),'Provenance':str(inputs/'provenance.json'),'Probe':str(inputs/'probe.jsonl'),'Output':str(out/'native-prefix-oracles-pending.json')}
 config=out/'oracle-input.json';config.write_text(json.dumps(cfg)+'\n')
 command=['env','GOWORK=off','GOMAXPROCS=2','GOTOOLCHAIN=local','GOPROXY=off','GOSUMDB=off','GOMAP_TRIAL24_ORACLE_INPUT='+str(config),'go','test','-p=1','-count=1','-timeout=180s','-overlay='+str(overlay),'-run=^TestTrial24FrozenCanonicalPrefixOraclePreparation$','./'+package]
 proposal={'state':'PREPARED_INERT_OFFLINE_OVERLAY_NOT_EXECUTED','source_head':a['source_head'],'source_tree':a['source_tree'],'source_inventory_sha256':a['source_inventory_sha256'],'input_inventory_sha256':a['input_inventory_sha256'],'collector_sha256':COLLECTOR_SHA,'helper_sha256':sha(read(__file__)),'payload_sha256':{p.name:sha(read(p)) for p in (go_path,overlay,config)},'working_directory':str(root),'proposed_argv':command,'proposed_shell_command':shlex.join(command),'runtime_started':False,'root_action':'Freeze/review overlay and pins, own sole Go slot, verify exact source/input before and after; retain raw normal execution; independently validate/promote native pending packet. No campaign SSH by this helper.'}
 with (out/'proposal.json').open('x') as f:json.dump(proposal,f,indent=2);f.write('\n')
 return proposal

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--pins',required=True);p.add_argument('--out',required=True);a=p.parse_args()
 print(json.dumps(prepare(a.pins,a.out),indent=2))
if __name__=='__main__':main()
