"""Actual Linux ps/wait4 positive admission; Python custody only, no Go/product timing."""
import json,os,sys,time
from pathlib import Path
sys.dont_write_bytecode=True
settings=json.loads(Path(sys.argv[1]).read_text())
out=Path(sys.argv[2]);out.mkdir(exist_ok=False)
source=Path(settings["variants"]["baseline"]["source"])
sys.path.insert(0,str(source/"scripts/cow_c3_read"))
from collect import host_snapshot,run_child
from protocol import process_environment,validate_census_file,validate_run_processes,linux_comm,sha,write,now
env=process_environment(settings["controls"]);storage=Path(settings["controls"]["TMPDIR"])
names=sorted({linux_comm(v["build"]+"/cowbench-normal.test") for v in settings["variants"].values()})
name="actual-linux-python-census"
try:
 before=host_snapshot(out,name+"-before",storage,source,env)
 validate_census_file(out/(name+"-before-processes.txt"),before["processes_sha256"],benchmark_names=names)
 argv=["/usr/bin/python3","-B","-c","import time; time.sleep(0.35)"]
 with (out/"child.stdout").open("wb") as stdout,(out/"child.stderr").open("wb") as stderr:
  child,monitor,(status,usage,timed_out)=run_child(argv,source,env,stdout,stderr,5,out,name,names)
 after=host_snapshot(out,name+"-after",storage,source,env)
 receipt={"child_pid":child.pid,"child_pgid":monitor.owner["pgid"],"collector_pid":monitor.owner["ppid"],
          "child_comm":monitor.owner["comm"],"command":argv,"waited_pid":monitor.value["waited_pid"],
          "wait_status":status,"exit_code":child.returncode,"monitor_sha256":sha(out/(name+"-monitor.json")),
          "before":before,"after":after,"timed_out":timed_out}
 write(out/"receipt.json",receipt)
 assert child.returncode==0 and not timed_out and len(monitor.value["samples"])>=2
 validate_run_processes(out,name,receipt,names)
 try:os.waitpid(child.pid,os.WNOHANG)
 except ChildProcessError:pass
 else:raise AssertionError("child was not reaped")
 write(out/"result.json",{"status":"PASS_ACTUAL_LINUX_CENSUS_AND_OFFLINE_JOIN","at":now(),"samples":len(monitor.value["samples"]),
       "all_children_joined":True,"Go_executions":0,"product_timings":0,"native_qualification":False,
       "tooling":{"collect.py":sha(source/"scripts/cow_c3_read/collect.py"),"protocol.py":sha(source/"scripts/cow_c3_read/protocol.py")},
       "claim":"Actual quiet observations for a single owned Python child only; no future host exclusivity proof"})
 print(json.dumps({"status":"PASS_ACTUAL_LINUX_CENSUS_AND_OFFLINE_JOIN","samples":len(monitor.value["samples"]),"Go_executions":0}))
except BaseException as error:
 write(out/"failure.json",{"at":now(),"type":type(error).__name__,"error":str(error),"Go_executions":0})
 raise
