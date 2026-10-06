import json,os,pathlib,subprocess,time
a=pathlib.Path(__file__).resolve().parent
repo=pathlib.Path("/private/tmp/gomap-cow-execution-o2nauuzm/c2")
env=dict(os.environ,GOWORK="off")
cases=[("normal",["go","test","-json","-count=3","-timeout=5m","-run=^TestCOWClose(ErrorNotificationCanReadClosedDatabase|HookCanWaitForAsynchronousNotificationRead)$","./TreeDB"]),("race",["go","test","-race","-json","-count=3","-timeout=5m","-run=^TestCOWClose(ErrorNotificationCanReadClosedDatabase|HookCanWaitForAsynchronousNotificationRead)$","./TreeDB"]),("safe",["go","test","-tags=treedb_safe","-json","-count=3","-timeout=5m","-run=^TestCOWClose(ErrorNotificationCanReadClosedDatabase|HookCanWaitForAsynchronousNotificationRead)$","./TreeDB"])]
receipts=[]
for name,cmd in cases:
 start=time.time()
 with (a/(name+".json")).open("w") as out,(a/(name+".stderr")).open("w") as err:
  result=subprocess.run(cmd,cwd=repo,env=env,stdout=out,stderr=err)
 receipt={"stage":name,"command":cmd,"cwd":str(repo),"GOWORK":"off","exit_code":result.returncode,"elapsed_seconds":time.time()-start}
 receipts.append(receipt)
 (a/"execution-receipts.json").write_text(json.dumps(receipts,indent=2)+"\n")
 print(json.dumps(receipt),flush=True)
 if result.returncode: raise SystemExit(result.returncode)
