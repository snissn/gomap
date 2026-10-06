import os,pathlib,subprocess,json,time
p=pathlib.Path(__file__).resolve().parent
repo=pathlib.Path("/private/tmp/gomap-cow-execution-o2nauuzm/c2")
env=dict(os.environ,GOWORK="off")
receipts=[]
for name,cmd in [("actual-cow-open",["go","run",str(p/"probe.go")]),("existing-template-policy",["go","test","-json","-count=3","-timeout=3m","-run=^TestSideStoreChunkSize_TemplateModeRequestIsIgnored$","./TreeDB"])]:
 start=time.time()
 with (p/(name+".stdout")).open("w") as out,(p/(name+".stderr")).open("w") as err:r=subprocess.run(cmd,cwd=repo,env=env,stdout=out,stderr=err)
 receipts.append({"name":name,"command":cmd,"cwd":str(repo),"GOWORK":"off","exit_code":r.returncode,"elapsed_seconds":time.time()-start})
 (p/"execution-receipts.json").write_text(json.dumps(receipts,indent=2)+"\n")
 print(json.dumps(receipts[-1]),flush=True)
 if r.returncode:raise SystemExit(r.returncode)
