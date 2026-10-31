#!/usr/bin/env python3
"""Separate, provisional MongoDB reference lifecycle. Never a TreeDB owner claim.

The maintained shell keeps its execution guard until this path passes review and
an explicitly authorized tiny live support control. This module has no implicit
image pull, external-service mode, or timed product run on import/validation.
"""
import argparse
import decimal
import ipaddress
import json
import os
import pathlib
import re
import select
import signal
import subprocess
import sys
import time

import ycsb_compare_validate as c
import ycsb_compare_live as live
import ycsb_correctness_validate as exact
from ycsb_evidence_json import loads

KIND = "mongodb-reference-checkpoint-close-reopen"
PROFILE = "mongodb_w1_default"  # Frozen fixture ELF's compatibility label only.
def ack_profile(journal_requested):
    c.require(journal_requested is None or journal_requested is True,"only journaled or journal-unspecified ACK class supported")
    return "mongodb_w1_journaled" if journal_requested is True else "mongodb_w1_journal_unspecified"
def fixture_manifest(m):
    return dict(m,profile=m.get("fixture_profile",m["profile"]))

ROLES = ("server", "reopened", "fixture-write", "fixture-reopened")
ID = re.compile(r"[0-9a-f]{64}$")
OID = re.compile(r"[0-9a-f]{24}$")
COMMIT = re.compile(r"[0-9a-f]{40}$")
SUPPORTED_SOURCE = {"8.2.7":"9fbea53313e85fbbf864a963ff17b0bd6dafa0be"}
# The public mirror commit above declares this GitOrigin-RevId. MongoDB's
# buildInfo reports that origin ID, not the mirror's different commit hash.
# Exact upstream commit metadata is retained in testdata/ycsb_mongodb_reference.
SUPPORTED_GIT_VERSION = {"8.2.7":"8f6785264a643d2eeb5e4046f7876a23e40e1a09"}
OBSERVATIONS = ("hello","buildInfo","serverStatus","getCmdLineOpts","getParameter")


def observation_request(name):
    value={name:{"$numberInt":"1"}}
    if name=="getParameter":value.update(journalCommitInterval={"$numberInt":"1"},syncdelay={"$numberInt":"1"})
    return value


def journal_parameters(parameters):
    c.require(isinstance(parameters,dict),"missing actual journal parameters")
    c.require(1<=ext_integer(parameters["journalCommitInterval"],"journalCommitInterval")<=500,"unsupported journal commit interval")
    delay=parameters["syncdelay"]
    if isinstance(delay,dict) and set(delay)=={"$numberDouble"}:
        raw=delay["$numberDouble"]
        c.require(type(raw) is str and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?",raw),"invalid syncdelay BSON double")
        number=decimal.Decimal(raw)
        c.require(number.is_finite() and 0<=number<=decimal.Decimal("1.7976931348623157e308"),"invalid syncdelay")
    else:c.require(ext_integer(delay,"syncdelay")>=0,"invalid syncdelay")
    return dict(journalCommitInterval=parameters["journalCommitInterval"],syncdelay=delay)


def integer(value, label, minimum=0):
    c.require(type(value) is int and value >= minimum, "invalid integer " + label)
    return value


def address(value):
    c.require(isinstance(value,str) and value.count(":")==1,"reference address must be IPv4 loopback:port")
    host,port=value.split(":")
    c.require(ipaddress.ip_address(host).is_loopback and host=="127.0.0.1" and port.isascii() and port.isdecimal() and 0<int(port)<65536,"reference address must be 127.0.0.1:port")
    return host,int(port)


def build(value):
    c.require(c.schema(value) and value["kind"]=="mongodb_image_build","separate MongoDB image build contract required")
    c.require(isinstance(value["image_id"],str) and re.fullmatch(r"sha256:[0-9a-f]{64}",value["image_id"]),"immutable MongoDB image ID required")
    c.require(isinstance(value["repo_digest"],str) and re.fullmatch(r"mongo@sha256:[0-9a-f]{64}",value["repo_digest"]),"immutable MongoDB repository digest required")
    c.require(value["os"]=="linux" and value["architecture"]=="amd64","unsupported reference platform")
    c.require(isinstance(value["version"],str) and re.fullmatch(r"8\.[0-9]+\.[0-9]+",value["version"]),"exact observed MongoDB 8 version required")
    c.require(isinstance(value["source_commit"],str) and COMMIT.fullmatch(value["source_commit"]),"reviewed MongoDB public source commit required")
    c.require(SUPPORTED_SOURCE.get(value["version"])==value["source_commit"],"unreviewed MongoDB source checkpoint/close contract")
    c.require(type(value["git_version"]) is str and COMMIT.fullmatch(value["git_version"]) and SUPPORTED_GIT_VERSION.get(value["version"])==value["git_version"],"unreviewed MongoDB reported GitOrigin version")
    for key in ("exe_sha256","docker_cli_sha256"):
        c.require(isinstance(value[key],str) and c.SHA.fullmatch(value[key]),"missing reference ELF/tool hash")
    c.require(isinstance(value["runtime"],dict) and set(value["runtime"])=={"engine","engine_commit","containerd","runc","driver","cgroup_version","default_runtime"},"explicit Docker runtime binding required")
    c.require(all(type(v) is str and v for v in value["runtime"].values()),"invalid Docker runtime binding")
    c.require(isinstance(value["image_labels"],dict) and all(type(k) is str and type(v) is str and not k.startswith("gomap.ycsb.") for k,v in value["image_labels"].items()),"invalid immutable image labels")


def manifest(value):
    if "common_load" in value:
        import ycsb_public_sentinel as sentinel
        sentinel.policy(value)
        c.require(value["common_load"]["address"]==value["address"],"reference common-load address mismatch")
    c.require("require_client_runtime_observation" not in value or value["require_client_runtime_observation"] is True,"reference runtime requirement must be explicit true when present")
    c.require(c.schema(value) and value["kind"]==KIND and value["targets"]==["mongodb"] and type(value["lifecycle_contract"]) is int and value["lifecycle_contract"]==1,"separate singleton reference manifest required")
    c.require(isinstance(value["campaign_id"],str) and re.fullmatch(r"[0-9a-f]{32}",value["campaign_id"]),"invalid reference campaign")
    if "acknowledgement_class" in value:
        c.require(value["profile"]==ack_profile(value["write_concern"]["journal_requested"]) and value["acknowledgement_class"]==value["profile"] and value["fixture_profile"]==PROFILE,"reference ACK profile/concern/fixture mismatch")
    else:
        c.require(value["profile"]==PROFILE and value["write_concern"]["journal_requested"] is None,"legacy reference profile only proves journal-unspecified ACK")
    c.require(value["formats"]=={"mongodb":"bson"},"MongoDB BSON settings required")
    c.require(set(value["write_concern"])=={"w","journal_requested"} and type(value["write_concern"]["w"]) is int and value["write_concern"]["w"]==1 and (value["write_concern"]["journal_requested"] is None or value["write_concern"]["journal_requested"] is True),"reference explicit w:1 journal contract required")
    for key in ("recordcount","operationcount","threadcount","run_repeats","uid","gid"):
        integer(value[key],key,0 if key in ("uid","gid") else 1)
    address(value["address"])
    c.require(isinstance(value["reference_source_sha256"],str) and c.SHA.fullmatch(value["reference_source_sha256"]),"reference launcher source must be bound")
    c.require(isinstance(value["docker_binary"],str) and pathlib.Path(value["docker_binary"]).is_absolute() and isinstance(value["client_binary"],str) and pathlib.Path(value["client_binary"]).is_absolute(),"absolute actual Docker/client executable required")
    build(value["server_builds"]["mongodb"])
    c.client_build(value["client"])
    for key in ("fixture","control"):
        bound=value[key]
        c.require(c.schema(bound) and isinstance(bound["binary"],str) and pathlib.Path(bound["binary"]).is_absolute(),"absolute bound reference helper required")
        for field in ("exe_sha256","source_sha256"):
            c.require(isinstance(bound[field],str) and c.SHA.fullmatch(bound[field]),"unbound reference helper")
    for key in ("phase_seconds","helper_seconds","cleanup_seconds","phase_outer_seconds","helper_outer_seconds","readiness_seconds","outer_seconds"):
        c.positive_timeout(value["deadlines"][key],key)
    if "journal_parameters" in value:
        c.require(type(value["journal_parameters"]) is dict and set(value["journal_parameters"])=={"journalCommitInterval","syncdelay"},"invalid prepared journal parameter keys")
        journal_parameters(value["journal_parameters"])


def ext_integer(value,label):
    # Canonical Extended JSON retains BSON numeric type. Never Python == on a
    # bool/float copy, and never turn a public process identity into a float.
    c.require(isinstance(value,dict) and set(value) in ({"$numberInt"},{"$numberLong"}),"invalid BSON integer "+label)
    raw=next(iter(value.values()))
    c.require(isinstance(raw,str) and re.fullmatch(r"-?(0|[1-9][0-9]*)",raw),"invalid BSON integer "+label)
    number=int(raw)
    # Match the pinned public driver's canonical BSON decoder: $numberInt is
    # signed int32; accepting its overflow as int64 would lose the type claim.
    bits=32 if "$numberInt" in value else 64
    c.require(-(1<<(bits-1))<=number<(1<<(bits-1)),"out-of-range BSON integer "+label)
    return number


def ok(reply):
    c.require(isinstance(reply,dict),"missing MongoDB command reply")
    c.require(c.same(reply.get("ok"),{"$numberDouble":"1.0"}) or c.same(reply.get("ok"),{"$numberInt":"1"}),"MongoDB command did not return ok:1")


def checked_command(command,request,addr,bounds):
    c.require(c.same(command["request"],request),"public command request mismatch")
    interval=c.result(command["result"],"MongoDB public command",bounds=bounds)
    # mongo-driver/v2 v2.6 topology/connection.go constructs addr[-N]. This is
    # the monitored driver's actual connection identity, not a server ACK.
    c.require(isinstance(command["connection_id"],str) and re.fullmatch(re.escape(addr)+r"\[-[1-9][0-9]*\]",command["connection_id"]),"command was not sent to reference address")
    ok(command["reply"])
    return command["reply"],interval


def public_observation(commands,prefix,m,container,bounds):
    replies={}
    last=bounds[0]
    for name in OBSERVATIONS:
        reply,interval=checked_command(commands[prefix+name],observation_request(name),m["address"],bounds)
        c.require(last<=interval[0],"public observation commands overlap/out of order")
        last=interval[1]
        replies[name]=reply
    hello,info,status,opts=[replies[x] for x in ("hello","buildInfo","serverStatus","getCmdLineOpts")]
    c.require(hello["isWritablePrimary"] is True and "setName" not in hello and "msg" not in hello,"reference must be standalone writable mongod")
    process_id=hello["topologyVersion"]["processId"]
    c.require(isinstance(process_id,dict) and set(process_id)=={"$oid"} and isinstance(process_id["$oid"],str) and OID.fullmatch(process_id["$oid"]),"missing server-generated reference process identity")
    b=m["server_builds"]["mongodb"]
    c.require(info["version"]==b["version"] and info["gitVersion"]==b["git_version"] and status["version"]==b["version"] and status["process"]=="mongod","actual MongoDB version/build/process mismatch")
    c.require(ext_integer(status["pid"],"server PID")==container["namespace_pid"],"public/container PID mismatch")
    # server_status_command.cpp uses prettyHostName(localPort), which omits
    # the port at DefaultDBPort=27017. This runner fixes that internal port;
    # require the exact source-defined hostname, with no permissive suffix.
    c.require(status["host"]==container["hostname"],"public/container hostname mismatch")
    engine=status["storageEngine"]
    c.require(engine["name"]=="wiredTiger" and engine["persistent"] is True,"persistent WiredTiger required")
    parsed=opts["parsed"]
    c.require(parsed["storage"]["dbPath"]=="/data/db" and parsed["storage"]["engine"]=="wiredTiger" and "replication" not in parsed and "sharding" not in parsed,"reference storage/topology configuration mismatch")
    c.require((parsed["net"].get("bindIpAll") is True or parsed["net"].get("bindIp") in ("0.0.0.0","*")) and ext_integer(parsed["net"]["port"],"port")==27017,"reference internal endpoint mismatch")
    # Persist exact actual parsed settings. No inference that w:1 is j:true,
    # that a default journal policy is TreeDB durable, or that WAL replay ran.
    c.require("journal" not in parsed["storage"],"reference journal override is outside default settings contract")
    return dict(process_id=process_id["$oid"],settings=parsed,journal_parameters=journal_parameters(replies["getParameter"]),last_utc_ns=last)


def container_inspect(value,m,role,data,terminal=False):
    b=m["server_builds"]["mongodb"]
    c.require(isinstance(value,dict) and ID.fullmatch(value.get("Id","")),"full container identity missing")
    config=value["Config"];host=value["HostConfig"]
    expected_labels=dict(b["image_labels"],**{"gomap.ycsb.campaign":m["campaign_id"],"gomap.ycsb.role":role,"gomap.ycsb.target":"mongodb"})
    c.require(c.same(config["Labels"],expected_labels),"container ownership labels mismatch")
    c.require(value["Image"]==b["image_id"] and config["Image"]==b["image_id"],"container immutable image mismatch")
    c.require(config["Hostname"]=="ycsb-"+m["campaign_id"][:12]+"-"+role and config["User"]==f'{m["uid"]}:{m["gid"]}',"container user/hostname mismatch")
    c.require(config["Entrypoint"]==["/usr/bin/mongod"] and config["Cmd"]==["--dbpath","/data/db","--storageEngine","wiredTiger","--bind_ip_all","--port","27017"],"actual mongod command mismatch")
    c.require(all(type(x) is str and not x.startswith("GOMAXPROCS=") for x in config["Env"]),"container GOMAXPROCS was set")
    # The nullable Init *bool uses JSON omitempty: Docker 29 omits the
    # unspecified value. Omitted/null delegates to daemon configuration;
    # neither proves init is disabled. Actual PID1/ELF/namespace observation
    # below remains mandatory and rejects an unexpected init wrapper.
    init=host.get("Init")
    c.require(host["Runtime"]==b["runtime"]["default_runtime"] and host["RestartPolicy"]["Name"]=="no" and host["AutoRemove"] is False and host["Privileged"] is False and (init is None or init is False),"container runtime/custody mismatch")
    c.require(host["NetworkMode"]=="bridge" and c.same(host["PortBindings"],{"27017/tcp":[{"HostIp":"127.0.0.1","HostPort":str(address(m["address"])[1])}]}),"reference port mapping mismatch")
    if m.get("bounded_container") is True:
        c.require(host["Memory"]==12884901888 and host["MemorySwap"]==12884901888,"container memory bounds differ")
        c.require(host["Ulimits"]==[{"Name":"cpu","Soft":449,"Hard":450}],"container CPU-time bounds differ")
        c.require(host["LogConfig"]=={"Type":"local","Config":{"max-size":"16m","max-file":"2"}},"container log bounds differ")
        c.require(all(not host.get(k) for k in ("NanoCpus","CpuQuota","CpuPeriod","CpusetCpus","CpusetMems")),"container CPU policy must inherit host")
    mounts=value["Mounts"]
    c.require(len(mounts)==2 and {x["Destination"] for x in mounts}=={"/data/db","/data/configdb"},"unexpected/anonymous reference volume")
    for item in mounts:
        suffix="" if item["Destination"]=="/data/db" else ".configdb"
        c.require(item["Type"]=="bind" and item["Source"]==data+suffix and item["RW"] is True,"reference bind-volume identity mismatch")
    state=value["State"]
    integer(state["Pid"],"Docker state PID")
    integer(value["RestartCount"],"restart count")
    c.require(value["RestartCount"]==0,"reference container restarted")
    if terminal:
        c.require(state["Pid"]==0 and state["Running"] is False and state["OOMKilled"] is False and state["Dead"] is False and state["Error"]=="","reference terminal state failed")
        c.exit_success(state["ExitCode"],"reference mongod did not exit zero")
    return value["Id"]


def identity(value,m,role):
    c.require(c.schema(value) and value["kind"]=="mongodb_container_process_observation","missing actual reference identity")
    c.process(value["process"])
    c.require(value["process"]["exe_sha256"]==m["server_builds"]["mongodb"]["exe_sha256"],"reference actual ELF mismatch")
    c.require(type(value["container_id"]) is str and ID.fullmatch(value["container_id"]),"missing full observed container ID")
    for key in ("data_device","data_inode","mounted_data_device","mounted_data_inode","namespace_pid"):
        integer(value[key],key,1 if key.endswith("inode") or key=="namespace_pid" else 0)
    c.require(value["namespace_pid"]==1,"reference mongod must be actual PID1 without wrapper")
    for key in ("pid_namespace","mount_namespace","network_namespace"):
        c.require(isinstance(value[key],str) and re.fullmatch(r"[a-z]+:\[[0-9]+\]",value[key]),"missing process namespace")
    c.require(value["data_device"]==value["mounted_data_device"] and value["data_inode"]==value["mounted_data_inode"],"actual container mounted DB differs from host DB")
    c.require(isinstance(value["data_dir"],str) and pathlib.Path(value["data_dir"]).is_absolute(),"absolute reference DB required")
    c.require(value["hostname"]=="ycsb-"+m["campaign_id"][:12]+"-"+role,"reference observed hostname mismatch")
    c.timestamp(value["observed_utc"])


def control(directory,stem,m,owner,mode,evidence_directory=None):
    launch=c.read(directory/(stem+".launch.json"))
    receipt=c.read(directory/(stem+".close.json"))
    packet=c.read(directory/(stem+".json"))
    command=c.read(directory/(stem+".command.json"))
    c.require(c.schema(command) and c.same(command["argv"],control_argv(evidence_directory or directory,stem,m,mode)),"reference control argv/address mismatch")
    c.require(c.schema(launch) and c.schema(receipt) and c.schema(packet),"reference control schema mismatch")
    c.process(launch["process"])
    c.exit_success(launch["exit_code"],"reference control child failed")
    c.require(launch["process"]["exe_sha256"]==m["control"]["exe_sha256"] and launch["campaign_id"]==m["campaign_id"] and launch["timed_out"] is False,"reference control child/source mismatch")
    c.require(launch["receipt_sha256"]==c.digest(directory/(stem+".close.json")) and receipt["observed_sha256"]==c.digest(directory/(stem+".json")),"reference control raw evidence hash mismatch")
    c.require(receipt["kind"]=="mongodb_reference_control_client_close" and receipt["result_error"]=="","reference control/cleanup failed")
    integer(receipt["pid"],"control PID",1)
    c.require(receipt["pid"]==launch["process"]["pid"],"reference control PID mismatch")
    c.require(packet["kind"]=="mongodb_reference_public_commands" and packet["mode"]==receipt["mode"]==mode and packet["fixture_id"]==receipt["fixture_id"]==m["campaign_id"]+":"+stem and packet["address"]==m["address"],"reference control identity mismatch")
    bounds=c.timestamp(launch["start_utc"]),c.timestamp(launch["end_utc"])
    call_bounds=c.timestamp(receipt["started_utc"]),c.timestamp(receipt["ended_utc"])
    c.require(bounds[0]<=call_bounds[0]<=call_bounds[1]<=bounds[1],"reference control outside child lifetime")
    closed=c.result(receipt["disconnect"],"reference actual client Disconnect",bounds=call_bounds)
    commands=packet["commands"]
    expected={p+n for p in ("before.","after.") for n in OBSERVATIONS}
    if mode=="checkpoint":expected.add("checkpoint")
    c.require(set(commands)==expected,"missing/unexpected public reference commands")
    before=public_observation(commands,"before.",m,owner,(call_bounds[0],closed[0]))
    after=public_observation(commands,"after.",m,owner,(before["last_utc_ns"],closed[0]))
    c.require(c.same(before["settings"],after["settings"]) and c.same(before["journal_parameters"],after["journal_parameters"]) and before["process_id"]==after["process_id"],"reference public process/settings changed")
    role=next(role for role in ROLES if owner["hostname"]=="ycsb-"+m["campaign_id"][:12]+"-"+role)
    ready=c.read(directory/(role+"-ready.json"))["commands"]
    c.require(c.same(before["settings"],ready["before.getCmdLineOpts"]["reply"]["parsed"]) and c.same(before["journal_parameters"],journal_parameters(ready["before.getParameter"]["reply"])),"reference settings changed since readiness")
    if "journal_parameters" in m:c.require(c.same(before["journal_parameters"],journal_parameters(m["journal_parameters"])),"reference resolved journal settings differ from support")
    checkpoint=None
    if mode=="checkpoint":
        _,checkpoint=checked_command(commands["checkpoint"],{"fsync":{"$numberInt":"1"},"lock":False},m["address"],(before["last_utc_ns"],closed[0]))
        first_after=c.timestamp(commands["after.hello"]["result"]["start_utc"])
        c.require(checkpoint[1]<=first_after,"checkpoint follows after-observation")
    for key,deadline in (("timeout_seconds","helper_seconds"),("cleanup_timeout_seconds","cleanup_seconds"),("outer_timeout_seconds","helper_outer_seconds")):
        c.require(c.same_timeout(launch[key],m["deadlines"][deadline]),"reference control deadline mismatch")
    return before,checkpoint,bounds


def failure_free(out):
    for directory in (out,out/"mongodb"):
        c.require(not any(p.name=="failure.json" or p.name.endswith(".failure.json") for p in directory.iterdir()),"retained failed reference attempt")
    c.require(not any(os.path.lexists(out/"mongodb"/(role+".cleanup.json")) for role in ROLES),"forced reference cleanup cannot qualify")


def emergency_cli(binary,args,m):
    """Nonqualifying custody only: no artifact open/write is needed.

    Pin the direct Docker CLI before waiting, collect at most 1 MiB in memory,
    and retain exact child ownership through bounded TERM/KILL/reap even if pin
    acquisition or pipe reading fails. The caller has deferred cancellation
    throughout service cleanup; the CLI never authorizes a reusable host PID.
    """
    return bounded_cli(binary,args,m["server_builds"]["mongodb"]["docker_cli_sha256"],m["deadlines"]["cleanup_seconds"])


def bounded_cli(binary,args,expected_sha256,timeout):
    """Artifact-independent direct-child capture for custody or metadata only."""
    c.require(c.exe_digest(binary)==expected_sha256,"bounded CLI executable changed")
    timeout=c.positive_timeout(timeout,"bounded CLI timeout")
    env=os.environ.copy();env.pop("GOMAXPROCS",None)
    child=None;descriptor=None;output=bytearray();original=None
    try:
        with c.defer_startup_cancellation() as pending:
            child=subprocess.Popen([binary,*args],env=env,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,start_new_session=True)
            descriptor=os.pidfd_open(child.pid)
        c.require(not pending,"bounded CLI interrupted during startup")
        deadline=time.monotonic()+timeout
        while True:
            remaining=deadline-time.monotonic()
            c.require(remaining>0,"bounded CLI output deadline exceeded")
            c.require(select.select([child.stdout],[],[],remaining)[0],"bounded CLI output deadline exceeded")
            chunk=os.read(child.stdout.fileno(),65536)
            if not chunk:break
            output.extend(chunk)
            c.require(len(output)<=1024*1024,"bounded CLI output exceeds custody bound")
        code=child.wait(timeout=max(0,deadline-time.monotonic()))
        c.exit_success(code,"bounded CLI failed")
        return output.decode("utf-8")
    except BaseException as error:
        original=error
        raise
    finally:
        previous=signal.pthread_sigmask(signal.SIG_BLOCK,c.CANCELLATION_SIGNALS)
        try:
            try:
                if child is not None and child.returncode is None:
                    if descriptor is None:c.terminate_unpinned_child(child,timeout)
                    else:
                        c.terminate_pinned_child(descriptor,timeout)
                        child.wait(timeout=timeout)
            except BaseException as cleanup_error:
                raise RuntimeError("emergency child cleanup failed: "+str(cleanup_error)+("; original: "+type(original).__name__+": "+str(original) if original is not None else "")) from cleanup_error
            finally:
                if child is not None and child.stdout is not None:child.stdout.close()
                if descriptor is not None:os.close(descriptor)
        finally:signal.pthread_sigmask(signal.SIG_SETMASK,previous)


def docker_action(directory,stem,m,args):
    c.require(isinstance(stem,str) and re.fullmatch(r"docker-[0-9]{4}",stem),"missing exact Docker action")
    command=c.read(directory/(stem+".command.json"))
    value=c.read(directory/(stem+".launch.json"))
    c.require(c.schema(command) and c.same(command["argv"],[m["docker_binary"],*args]),"Docker action arguments mismatch")
    c.require(c.schema(value),"Docker launch schema mismatch");c.process(value["process"])
    c.exit_success(value["exit_code"],"Docker CLI failed")
    c.require(value["process"]["exe_sha256"]==m["server_builds"]["mongodb"]["docker_cli_sha256"] and value["timed_out"] is False and value["campaign_id"]==m["campaign_id"],"Docker CLI source/campaign failure")
    c.require(value["output_sha256"]==c.digest(directory/(stem+".out")),"Docker CLI stdout changed")
    for key,deadline in (("timeout_seconds","helper_seconds"),("cleanup_timeout_seconds","cleanup_seconds"),("outer_timeout_seconds","helper_outer_seconds")):
        c.require(c.same_timeout(value[key],m["deadlines"][deadline]),"Docker action deadline mismatch")
    bounds=c.timestamp(value["start_utc"]),c.timestamp(value["end_utc"])
    c.require(bounds[0]<=bounds[1],"Docker action reversed lifetime")
    return (directory/(stem+".out")).read_text(),bounds


def log_timestamp(value):
    """Mongo log UTC only: Z or explicit +00:00, exact integer nanoseconds.

    The pinned JSONFormatter uses DateStringBuffer::iso8601. Its local form
    includes an offset (the retained image emits +00:00); the UTC form uses Z.
    Other offsets, unknown -00:00 and missing zones are outside this contract.
    Receipt and Docker timestamps retain their separate strict Z-only parser.
    """
    c.require(isinstance(value,str),"invalid Mongo log UTC timestamp")
    if value.endswith("+00:00"):value=value[:-6]+"Z"
    return c.timestamp(value)


def lifetime(directory,role,m,evidence_directory=None):
    observed=c.read(directory/(role+".identity.json"));identity(observed,m,role)
    initial=c.read(directory/(role+".inspect.json"));terminal=c.read(directory/(role+".terminal.json"))
    cid=container_inspect(initial,m,role,observed["data_dir"])
    start=c.read(directory/(role+".start.json"))
    c.require(c.schema(start) and start["container_id"]==cid,"missing reference creation/start custody")
    created_output,created=docker_action(directory,start["actions"]["create"],m,create_argv(m,role,observed["data_dir"]))
    started_output,started=docker_action(directory,start["actions"]["start"],m,["start",cid])
    initial_output,inspected_initial=docker_action(directory,start["actions"]["inspect"],m,["inspect",cid])
    c.require(created_output.strip()==cid and started_output.strip()==cid and c.same(loads(initial_output),[initial]),"reference creation/start/initial inspect raw output mismatch")
    c.require(created[1]<=started[0]<=started[1]<=inspected_initial[0]<=inspected_initial[1]<=c.timestamp(observed["observed_utc"]),"reference creation/start observation ordering mismatch")
    c.require(cid==observed["container_id"]==container_inspect(terminal,m,role,observed["data_dir"],True),"reference terminal/container identity mismatch")
    c.require(initial["State"]["Running"] is True and type(initial["State"]["Pid"]) is int and initial["State"]["Pid"]==observed["process"]["pid"],"reference running process mismatch")
    opened,_,ready_bounds=control(directory,role+"-ready",m,observed,"observe",evidence_directory)
    before,_,before_bounds=control(directory,role+"-before-close",m,observed,"observe",evidence_directory)
    c.require(opened["process_id"]==before["process_id"] and c.same(opened["settings"],before["settings"]) and c.same(opened["journal_parameters"],before["journal_parameters"]),"reference owner changed during lifetime")
    stopped=c.read(directory/(role+".close.json"))
    c.require(c.schema(stopped) and stopped["kind"]=="mongodb_observed_graceful_process_exit" and stopped["container_id"]==cid and c.same(stopped["process"],observed["process"]),"missing reference observed process close")
    c.process(stopped["process"])
    c.require(stopped["signal"]=="SIGTERM" and stopped["forced_kill"] is False and stopped["close_return_observed"] is False,"unobserved Close return/forced shutdown claim")
    c.exit_success(stopped["exit_code"],"reference process exit failure")
    stop_bounds=c.result(stopped["observed_exit"],"reference observed graceful exit")
    c.require(ready_bounds[1]<=before_bounds[0]<=before_bounds[1]<=stop_bounds[0],"reference readiness/close lifetime order mismatch")
    c.require(stopped["terminal_sha256"]==c.digest(directory/(role+".terminal.json")) and stopped["logs_sha256"]==c.digest(directory/(role+".log")),"reference terminal/log binding mismatch")
    _,sent=docker_action(directory,stopped["actions"]["sigterm"],m,["kill","--signal=SIGTERM",cid])
    wait_output,waited=docker_action(directory,stopped["actions"]["wait"],m,["wait",cid])
    terminal_output,inspected=docker_action(directory,stopped["actions"]["terminal"],m,["inspect",cid])
    logs_output,logged=docker_action(directory,stopped["actions"]["logs"],m,["logs",cid])
    c.require(stop_bounds[0]<=sent[0]<=sent[1]<=waited[0]<=waited[1]<=inspected[0]<=inspected[1]<=stop_bounds[1]<=logged[0],"reference shutdown action chronology mismatch")
    c.require(wait_output.strip()=="0" and c.same(loads(terminal_output),[terminal]) and logs_output==(directory/(role+".log")).read_text(),"reference terminal/log copies differ from actual Docker outputs")
    c.require(terminal["State"]["StartedAt"]==initial["State"]["StartedAt"] and c.timestamp(initial["State"]["StartedAt"])<=c.timestamp(observed["observed_utc"])<=ready_bounds[0],"reference initial container lifetime mismatch")
    finished=c.timestamp(terminal["State"]["FinishedAt"])
    c.require(started[0]<=c.timestamp(initial["State"]["StartedAt"])<=started[1],"reference process start outside Docker start action")
    c.require(sent[0]<=finished<=waited[1],"reference exit outside actual shutdown wait")
    entries=[loads(line) for line in (directory/(role+".log")).read_text().splitlines() if line.strip()]
    storage=[(i,x) for i,x in enumerate(entries) if type(x.get("id")) is int and x["id"]==4795901]
    done=[(i,x) for i,x in enumerate(entries) if type(x.get("id")) is int and x["id"]==8423404]
    c.require(len(storage)==len(done)==1 and storage[0][0]<done[0][0],"actual source-bound WiredTiger close/mongod completion missing")
    c.require(sent[0]<=log_timestamp(storage[0][1]["t"]["$date"])<=log_timestamp(done[0][1]["t"]["$date"])<=finished,"source close/completion logs outside actual shutdown lifetime")
    removed=c.read(directory/(role+".removed.json"))
    c.require(c.schema(removed) and removed["container_id"]==cid and removed["removed"] is True,"owned reference container removal incomplete")
    output,removal=docker_action(directory,removed["action"],m,["rm",cid])
    c.require(output.strip()==cid and logged[1]<=removal[0],"reference removal acknowledgement/order mismatch")
    return observed,opened["process_id"],(ready_bounds[1],before_bounds[0]),stop_bounds


def fixture_helper(directory,stem,m,mode,owner,public_id,lifetime_bounds,evidence_directory=None):
    # Reuse existing strict helper/receipt/binary/artifact/deadline validation.
    receipt,bounds=live.helper(directory,stem,fixture_manifest(m),"mongodb",mode,m["campaign_id"]+":mongodb:"+("random" if mode=="snapshot" else "deterministic"))
    command=c.read(directory/(stem+".command.json"))
    stage=stem.removeprefix("snapshot_") if mode=="snapshot" else None
    c.require(c.schema(command) and c.same(command["argv"],fixture_argv(evidence_directory or directory,stem,m,mode,stage)),"reference fixture argv/address mismatch")
    live.require_interval(bounds,lifetime_bounds,"reference "+stem)
    route,_,route_bounds=control(directory,stem+"-owner",m,owner,"observe",evidence_directory)
    c.require(route["process_id"]==public_id and route_bounds[1]<=bounds[0],"reference fixture route/owner mismatch")
    live.require_interval(route_bounds,lifetime_bounds,"reference fixture route")
    c.require(receipt["checkpoint_attempted"] is False and receipt["checkpoint_succeeded"] is False and receipt["checkpoint"] is None and receipt.get("owner_checkpoint") is None,"reference fixture close cannot stand in for explicit fsync")
    return receipt,bounds


def observe_container_cpu_policy(pid,identity,group,container_id):
    # Host view of an independently owned Docker PID. Never bypass the generic
    # same-namespace native observer guard or resolve a container mount root.
    proc=pathlib.Path("/proc",str(pid));root=pathlib.Path("/sys/fs/cgroup")
    c.require(c.same(c.observe(pid),identity),"container changed before CPU policy")
    init_lines=pathlib.Path("/proc/1/cgroup").read_text().splitlines()
    init_paths=[x[3:] for x in init_lines if x.startswith("0::/")]
    c.require(len(init_paths)==1 and ".." not in pathlib.Path(init_paths[0]).parts,"invalid visible PID1 cgroup path")
    init_group=root/init_paths[0].lstrip("/");init_stat=init_group.stat();root_stat=root.stat()
    c.require("1" in (init_group/"cgroup.procs").read_text().split(),"visible cgroup mount does not contain PID1")
    mounts=[line.split(" - ",1)[0].split() for line in pathlib.Path("/proc/self/mountinfo").read_text().splitlines() if " - cgroup2 " in line]
    c.require(len(mounts)==1 and mounts[0][3:5]==["/","/sys/fs/cgroup"],"full host cgroup mount required")
    lines=(proc/"cgroup").read_text().splitlines();unified=[x[3:] for x in lines if x.startswith("0::/")]
    c.require(len(unified)==1 and ".." not in pathlib.Path(unified[0]).parts,"invalid host-visible container cgroup")
    c.require(root/unified[0].lstrip("/")==group and container_id in unified[0],"container CPU policy group identity mismatch")
    before=group.stat();members=(group/"cgroup.procs").read_text().split()
    c.require(members==[str(pid)],"container CPU policy has extra processes")
    namespaces={name:os.readlink(proc/"ns"/name) for name in ("cgroup","mnt")}
    affinity=sorted(os.sched_getaffinity(pid));rows=c.runtime_observer.ancestor_policy(root,group)
    c.require(lines==(proc/"cgroup").read_text().splitlines() and affinity==sorted(os.sched_getaffinity(pid)),"container CPU policy changed")
    c.require(namespaces=={name:os.readlink(proc/"ns"/name) for name in namespaces},"container namespaces changed")
    after=group.stat();c.require((before.st_dev,before.st_ino)==(after.st_dev,after.st_ino) and members==(group/"cgroup.procs").read_text().split(),"container cgroup identity changed")
    c.require(c.same(c.observe(pid),identity),"container changed during CPU policy")
    c.require(init_lines==pathlib.Path("/proc/1/cgroup").read_text().splitlines() and "1" in (init_group/"cgroup.procs").read_text().split(),"visible PID1 membership changed")
    c.require((root.stat().st_dev,root.stat().st_ino)==(root_stat.st_dev,root_stat.st_ino) and (init_group.stat().st_dev,init_group.stat().st_ino)==(init_stat.st_dev,init_stat.st_ino),"visible cgroup root identity changed")
    policy=dict(process=identity,leader_affinity=affinity,cgroup=dict(membership=unified[0],ancestors=rows,host_mount_root="/",visible_mount_pid1_evidence=dict(path=init_paths[0],root_device=root_stat.st_dev,root_inode=root_stat.st_ino,pid1_group_device=init_stat.st_dev,pid1_group_inode=init_stat.st_ino,scope="visible full mount and PID1 membership; does not independently prove namespace equivalence"),container_namespaces=namespaces),scope="host full-cgroup view of exact owned container; leader affinity and exported ancestor CPU constraints; sampled only")
    c.validate_cpu_policy(policy,identity);return policy


def bounded_observations(directory,role,m,owner,lifetime_bounds):
    import struct
    import ycsb_reference_wire as wire
    value=c.read(directory/(role+".resource-observation.json"))
    default=c.read(directory/(role+".default-concern.json"))
    for item in (value,default):
        c.require(c.schema(item) and item["campaign_id"]==m["campaign_id"] and item["role"]==role and c.same(item["process"],owner["process"]),"bounded reference observation identity mismatch")
    c.require(re.search(r"^Max cpu time\s+449\s+450\s+seconds[ \t]*$",value["limits"],re.M),"retained actual CPU limit mismatch")
    c.require(value["members"]==[owner["process"]["pid"]] and owner["container_id"] in value["cgroup"],"retained actual container membership mismatch")
    c.require(value["memory_max"]=="12884901888" and value["memory_swap_max"]=="0","retained actual memory bounds mismatch")
    c.require(type(value["cgroup_device"]) is int and value["cgroup_device"]>0 and type(value["cgroup_inode"]) is int and value["cgroup_inode"]>0,"retained cgroup directory identity invalid")
    c.validate_cpu_policy(value["cpu_policy"],owner["process"])
    live.require_interval((c.timestamp(default["start_utc"]),c.timestamp(default["end_utc"])),lifetime_bounds,"actual default concern query")
    frames=directory/(role+".default-concern-frames")
    raw=(frames/"01-request.bin").read_bytes();body=b"\0"*5+wire.document({"getDefaultRWConcern":1,"$db":"admin"})
    c.require(raw==struct.pack("<iiii",len(body)+16,1,0,2013)+body,"default concern actual request mismatch")
    raw=(frames/"01-response.bin").read_bytes();length,rid,to,op=struct.unpack_from("<iiii",raw)
    c.require(length==len(raw) and to==1 and op==2013 and raw[16:21]==b"\0"*5,"default concern raw response framing mismatch")
    reply,end=wire.decode(raw,21)
    c.require(end==len(raw) and c.same(reply,default["reply"]),"default concern raw reply mismatch")
    c.require(reply.get("ok")==1 or (reply.get("ok")==0 and reply.get("code")==51300 and reply.get("codeName")=="Location51300" and reply.get("errmsg")=="'getDefaultRWConcern' is not supported on standalone nodes."),"unexpected default concern response")


def validate(out,require_completion=True,evidence_root=None):
    # Relocated support retains its original absolute output argv verbatim.
    # The preparation receipt hash-binds this original root; only expected
    # output paths use it. All evidence reads use out, all routes use m.address.
    evidence_root=pathlib.Path(evidence_root) if evidence_root is not None else out
    c.require(evidence_root.is_absolute(),"absolute original evidence root required")
    evidence_directory=evidence_root/"mongodb"
    m=c.read(out/"manifest.json");manifest(m)
    if "preparation_sha256" in m:
        from ycsb_mongodb_reference_prepare import verify_prepared
        verify_prepared(out,m)
    directory=out/"mongodb";failure_free(out)
    if require_completion:
        c.require(not os.path.lexists(out/"reference-incomplete.json"),"reference attempt is incomplete or failed")
        completed=c.read(out/"reference-complete.json")
        c.require(c.schema(completed) and completed["kind"]=="mongodb_reference_completed_attempt" and completed["campaign_id"]==m["campaign_id"] and completed["manifest_sha256"]==c.digest(out/"manifest.json"),"reference completion receipt mismatch")
        c.timestamp(completed["completed_utc"])
    c.require(c.digest(out/"reference-source.py")==m["reference_source_sha256"],"reference launcher source copy differs")
    c.require(c.same(c.read(out/"reference-build.json"),m["server_builds"]["mongodb"]),"reference build copy differs")
    c.require(c.same(c.read(out/"client-build.json"),m["client"]),"reference client build copy differs")
    image=c.read(out/"reference-image.json");b=m["server_builds"]["mongodb"]
    c.require(image["Id"]==b["image_id"] and b["repo_digest"] in image["RepoDigests"] and image["Architecture"]==b["architecture"] and image["Os"]==b["os"],"retained actual image mismatch")
    c.require(c.same(image["Config"].get("Labels") or {},b["image_labels"]),"retained immutable image labels mismatch")
    c.require(all(isinstance(x,str) and not x.startswith("GOMAXPROCS=") for x in image["Config"]["Env"]),"retained image GOMAXPROCS set")
    runtime=c.read(out/"reference-runtime.json")
    c.require(c.same(runtime_record(runtime["version"],runtime["info"]),b["runtime"]),"retained actual Docker runtime mismatch")
    inventory=c.read(out/"reference-inventory.json")
    c.require(c.schema(inventory),"missing raw reference inventory binding")
    for name,args,expected in (("image",["image","inspect",b["image_id"]],[image]),("version",["version","--format","{{json .Server}}"],runtime["version"]),("info",["info","--format","{{json .}}"],runtime["info"])):
        output,_=docker_action(directory,inventory["actions"][name],m,args)
        c.require(c.same(loads(output),expected),"reference inventory differs from actual Docker output")
    c.require(c.digest(out/"client.patch")==m["client"]["source_patch_sha256"] and (out/"client-buildinfo.txt").read_text().strip(),"reference client source/buildinfo missing")
    for key,name in (("fixture","fixture-source.go"),("control","reference-control-source.go")):
        c.require(c.digest(out/name)==m[key]["source_sha256"],"reference helper source mismatch")
    owners={role:lifetime(directory,role,m,evidence_directory) for role in ROLES}
    if m.get("bounded_container") is True:
        for role in ROLES:bounded_observations(directory,role,m,owners[role][0],owners[role][2])
    settings=[c.read(directory/(role+"-ready.json"))["commands"] for role in ROLES]
    c.require(all(c.same(journal_parameters(x["before.getParameter"]["reply"]),journal_parameters(settings[0]["before.getParameter"]["reply"])) and c.same(x["before.getCmdLineOpts"]["reply"]["parsed"],settings[0]["before.getCmdLineOpts"]["reply"]["parsed"]) for x in settings),"reference settings differ across independent lifetimes")
    if require_completion:
        c.require(all(v[3][1]<=c.timestamp(completed["completed_utc"]) for v in owners.values()),"reference completion precedes checked shutdown")
    c.require(len({v[0]["container_id"] for v in owners.values()})==4 and len({(v[0]["process"]["pid"],v[0]["process"]["start_ticks"]) for v in owners.values()})==4 and len({v[1] for v in owners.values()})==4,"reference requires four independent process/container lifetimes")
    for first,second in (("server","reopened"),("fixture-write","fixture-reopened")):
        a,b=owners[first],owners[second]
        c.require(all(c.same(a[0][k],b[0][k]) for k in ("data_dir","data_device","data_inode")),"reference reopen uses different volume")
        actual_start=c.timestamp(c.read(directory/(second+".inspect.json"))["State"]["StartedAt"])
        c.require(a[3][1]<=actual_start,"reference reopen starts before checked shutdown")
    deterministic_start=c.timestamp(c.read(directory/"fixture-write.inspect.json")["State"]["StartedAt"])
    c.require(owners["server"][0]["data_dir"]!=owners["fixture-write"][0]["data_dir"] and owners["reopened"][3][1]<=deterministic_start,"reference deterministic DB is not fresh/separate")
    previous=None
    phase_ids=set()
    for stem in ["load"]+["run_"+str(i) for i in range(1,m["run_repeats"]+1)]:
        phase="load" if stem=="load" else "run"
        c.check_counts(c.parse_log(directory/(stem+".out")),m["recordcount"] if phase=="load" else m["operationcount"],phase,directory/(stem+".out"))
        phase_id=c.phase_evidence(directory,stem,"mongodb",m)
        c.require(phase_id not in phase_ids,"reference phase identity reused");phase_ids.add(phase_id)
        launch=c.read(directory/(stem+".launch.json"))
        command=c.read(directory/(stem+".command.json"))
        c.require(c.schema(command) and c.same(command["argv"],phase_argv(m,stem)),"reference YCSB argv/address/workload mismatch")
        bounds=c.timestamp(launch["start_utc"]),c.timestamp(launch["end_utc"])
        live.require_interval(bounds,owners["server"][2],"reference measured phase")
        route,_,route_bounds=control(directory,stem+"-owner",m,owners["server"][0],"observe",evidence_directory)
        c.require(route["process_id"]==owners["server"][1] and route_bounds[1]<=bounds[0],"reference measured client route/owner mismatch")
        live.require_interval(route_bounds,owners["server"][2],"reference measured route")
        c.require(previous is None or previous<=bounds[0],"reference measured phases overlap")
        previous=bounds[1]
    snapshots={};receipts={}
    for stage in ("loaded","preclose","reopened"):
        role="reopened" if stage=="reopened" else "server"
        receipt,bounds=fixture_helper(directory,"snapshot_"+stage,m,"snapshot",owners[role][0],owners[role][1],owners[role][2],evidence_directory)
        receipts[stage]=(receipt,bounds)
        snapshots[stage]=live.canonical_snapshot(c.read(directory/("snapshot_"+stage+".json")),fixture_manifest(m),"mongodb",stage)
    c.require(c.timestamp(c.read(directory/"load.launch.json")["end_utc"])<=receipts["loaded"][1][0]<=receipts["loaded"][1][1]<=c.timestamp(c.read(directory/"run_1.launch.json")["start_utc"]),"reference loaded snapshot placement mismatch")
    c.require(previous<=receipts["preclose"][1][0],"reference snapshot precedes settled clients")
    c.require(c.same(snapshots["preclose"][0],snapshots["reopened"][0]),"reference canonical random state differs after reopen")
    for stem,role in (("fixture_write","fixture-write"),("fixture_verify","fixture-reopened")):
        fixture_helper(directory,stem,m,"write" if stem=="fixture_write" else "verify",owners[role][0],owners[role][1],owners[role][2],evidence_directory)
    expected=c.read(directory/"fixture.expected.json")
    c.require(expected["source_id"]==m["server_builds"]["mongodb"]["source_commit"] and expected["fixture_id"]==m["campaign_id"]+":mongodb:deterministic" and expected["profile"]==PROFILE and expected["target"]=="mongodb","reference expected fixture identity mismatch")
    exact.validate(expected,[c.read(directory/name) for name in ("fixture.updated.json.load.json","fixture.updated.json","fixture.reopened.json")])
    if "common_load" in m:
        import ycsb_public_sentinel as sentinel
        sentinel.validate(directory,m,owners["server"][0]["process"],m["address"])
    for role,stem,snapshot,end in (("server","random-checkpoint","snapshot_preclose.json",receipts["preclose"][1][1]),("fixture-write","fixture-checkpoint","fixture.updated.json",c.timestamp(c.read(directory/"fixture_write.launch.json")["end_utc"]))):
        owner,public_id,lifetime_bounds,_=owners[role]
        observed,checkpoint,bounds=control(directory,stem,m,owner,"checkpoint",evidence_directory)
        live.require_interval(bounds,lifetime_bounds,"reference explicit fsync")
        c.require(end<=bounds[0] and observed["process_id"]==public_id,"reference checkpoint before settled clients/wrong owner")
        bound=c.read(directory/(stem+".binding.json"))
        c.require(c.schema(bound) and bound["container_id"]==owner["container_id"] and bound["snapshot_sha256"]==c.digest(directory/snapshot) and bound["control_sha256"]==c.digest(directory/(stem+".json")),"reference checkpoint snapshot/owner mismatch")
    # Every Docker helper is independently checked, including removal and wait;
    # service exit is Docker's terminal status, not this CLI process's exit.
    permitted=[["image","inspect",m["server_builds"]["mongodb"]["image_id"]],["version","--format","{{json .Server}}"],["info","--format","{{json .}}"]]
    for role,(owner,*_) in owners.items():
        cid=owner["container_id"]
        permitted += [[verb,cid] for verb in ("start","inspect","wait","logs","rm")]
        permitted += [["kill","--signal=SIGTERM",cid],create_argv(m,role,owner["data_dir"])]
    for path in sorted(directory.glob("docker-*.launch.json")):
        stem=path.name.removesuffix(".launch.json")
        argv=c.read(directory/(stem+".command.json"))["argv"]
        c.require(isinstance(argv,list) and argv and all(isinstance(x,str) for x in argv),"Docker argv malformed")
        c.require(any(c.same(argv[1:],args) for args in permitted),"unexpected Docker action outside reference custody")
        docker_action(directory,stem,m,argv[1:])
    return dict(schema_version=1,status="validated-reference-lifecycle-evidence",kind=KIND,version=m["server_builds"]["mongodb"]["version"],write_concern=m["write_concern"],random_scope="observed-state-preservation-only",deterministic_exact_values=True,close_return_observed=False,cow_applicable=False,runtime_elf_attested=False,wal_replay_proven=False,power_loss_certified=False,performance_matrix_complete=False)


def create_argv(m,role,data):
    labels={"gomap.ycsb.campaign":m["campaign_id"],"gomap.ycsb.role":role,"gomap.ycsb.target":"mongodb"}
    args=["create","--pull=never","--name","ycsb-"+m["campaign_id"]+"-"+role,"--hostname","ycsb-"+m["campaign_id"][:12]+"-"+role,"--user",f'{m["uid"]}:{m["gid"]}',"--restart=no","--runtime",m["server_builds"]["mongodb"]["runtime"]["default_runtime"],"--network","bridge","--entrypoint","/usr/bin/mongod","-p",m["address"]+":27017","--mount",f"type=bind,src={data},dst=/data/db","--mount",f"type=bind,src={data}.configdb,dst=/data/configdb"]
    for key,value in labels.items():args += ["--label",key+"="+value]
    if m.get("bounded_container") is True:
        args += ["--memory","12884901888","--memory-swap","12884901888","--ulimit","cpu=449:450","--log-driver","local","--log-opt","max-size=16m","--log-opt","max-file=2"]
    return args+[m["server_builds"]["mongodb"]["image_id"],"--dbpath","/data/db","--storageEngine","wiredTiger","--bind_ip_all","--port","27017"]


def control_argv(directory,stem,m,mode):
    return [m["control"]["binary"],"-mode",mode,"-addr",m["address"],"-observed",str(directory/(stem+".json")),"-receipt",str(directory/(stem+".close.json")),"-fixture-id",m["campaign_id"]+":"+stem]


def fixture_argv(directory,stem,m,mode,stage=None):
    observed="snapshot_"+stage+".json" if mode=="snapshot" else "fixture.updated.json" if mode=="write" else "fixture.reopened.json"
    args=[m["fixture"]["binary"],"-mode",mode,"-target","mongodb","-addr",m["address"],"-format","bson","-profile",PROFILE,"-source-id",m["server_builds"]["mongodb"]["source_commit"],"-fixture-id",m["campaign_id"]+":mongodb:"+("random" if mode=="snapshot" else "deterministic"),"-expected",str(directory/"fixture.expected.json"),"-observed",str(directory/observed),"-receipt",str(directory/(stem+".close.json"))]
    if stage:args += ["-stage",stage]
    return args


def phase_argv(m,stem):
    journal=None if "common_load" in m and stem=="load" else m["write_concern"]["journal_requested"]
    args=[m["client_binary"],"load" if stem=="load" else "run","mongodb","-p","mongodb.url=mongodb://"+m["address"]+"/ycsb?w=1"+("" if journal is None else "&journal="+str(journal).lower())]
    settings=dict(recordcount=m["recordcount"],operationcount=m["operationcount"],threadcount=m["threadcount"],fieldcount=10,fieldlength=100,readallfields="true",writeallfields="false",insertorder="hashed",keyprefix="user",zeropadding=1,insertstart=0,readproportion="0.95",updateproportion="0.05",scanproportion=0,insertproportion=0,readmodifywriteproportion=0)
    for key,value in settings.items():args += ["-p",f"{key}={value}"]
    return args


class Docker:
    """All CLI children use the existing finite pidfd/direct-child custody."""
    def __init__(self,out,binary):
        self.out=out;self.directory=out/"mongodb";self.m=c.read(out/"manifest.json")
        self.binary=binary;self.ordinal=0
        self.owned={}
        self.cleanup_records={};self.emergency_actions=[]
        self.deadline=time.monotonic()+self.m["deadlines"]["outer_seconds"]

    def command(self,*args):
        self.ordinal+=1
        stem=f"docker-{self.ordinal:04d}"
        command=[self.binary,*args]
        c.write_new(self.directory/(stem+".command.json"),dict(schema_version=1,argv=command))
        d=self.m["deadlines"]
        c.require(time.monotonic()+d["helper_outer_seconds"]<=self.deadline,"reference run budget exhausted before Docker action")
        with c.outer_deadline(d["helper_outer_seconds"]):
            code=c.launch(argparse.Namespace(directory=str(self.directory),stem=stem,command=command,timeout=d["helper_seconds"],cleanup_timeout=d["cleanup_seconds"],role="docker"))
        c.require(code==0,"owned Docker CLI failed: "+stem)
        self.last_stem=stem
        return (self.directory/(stem+".out")).read_text().strip()

    def inspect(self,cid):
        values=loads(self.command("inspect",cid));c.require(isinstance(values,list) and len(values)==1,"ambiguous container inspect")
        return values[0]

    def checked(self,cid,role,data):
        value=self.inspect(cid)
        c.require(container_inspect(value,self.m,role,str(data))==cid,"container identity changed")
        return value

    def observe(self,cid,role,data):
        value=self.checked(cid,role,data)
        c.require(value["State"]["Running"] is True,"owned reference is not running")
        pid=integer(value["State"]["Pid"],"actual container PID",1)
        observed=c.observe(pid)
        root=pathlib.Path(f"/proc/{pid}")
        nspid=next(line.split()[1:] for line in (root/"status").read_text().splitlines() if line.startswith("NSpid:"))
        stat=data.stat();mounted=(root/"root/data/db").stat()
        result=dict(schema_version=1,kind="mongodb_container_process_observation",container_id=cid,process=observed,namespace_pid=int(nspid[-1]),hostname=value["Config"]["Hostname"],data_dir=str(data),data_device=stat.st_dev,data_inode=stat.st_ino,mounted_data_device=mounted.st_dev,mounted_data_inode=mounted.st_ino,observed_utc=c.now(),**{key+"_namespace":os.readlink(root/"ns"/short) for key,short in (("pid","pid"),("mount","mnt"),("network","net"))})
        identity(result,self.m,role)
        c.require(c.same(c.observe(pid),observed),"reference process changed during observation")
        c.write_new(self.directory/(role+".identity.json"),result)
        c.write_new(self.directory/(role+".inspect.json"),value)
        return result

    def start(self,role,data):
        name="ycsb-"+self.m["campaign_id"]+"-"+role
        labels={"gomap.ycsb.campaign":self.m["campaign_id"],"gomap.ycsb.role":role,"gomap.ycsb.target":"mongodb"}
        # Intent is retained BEFORE create. Cleanup can recover by this unique
        # name only after checking complete immutable ID/config/labels/mounts.
        self.owned[role]=dict(name=name,data=data,cid=None)
        c.write_new(self.directory/(role+".create-intent.json"),dict(schema_version=1,name=name,labels=labels,data_dir=str(data)))
        cid=self.command(*create_argv(self.m,role,str(data)));c.require(ID.fullmatch(cid),"invalid created container ID")
        actions=dict(create=self.last_stem)
        self.owned[role]["cid"]=cid
        self.checked(cid,role,data)
        self.command("start",cid)
        actions["start"]=self.last_stem
        owner=self.observe(cid,role,data)
        actions["inspect"]=self.last_stem
        c.write_new(self.directory/(role+".start.json"),dict(schema_version=1,container_id=cid,actions=actions))
        self.owned[role]["owner"]=owner
        deadline=time.monotonic()+self.m["deadlines"]["readiness_seconds"]
        # TCP alone is not ready. Public commands are bounded; partial/failure
        # receipts remain failures rather than retrying toward a favorable run.
        import socket
        host,port=address(self.m["address"])
        while True:
            self.checked(cid,role,data)
            c.require(c.same(c.observe(owner["process"]["pid"]),owner["process"]),"reference changed before readiness")
            try:
                with socket.create_connection((host,port),timeout=min(1,max(.001,deadline-time.monotonic()))):break
            except OSError:
                c.require(time.monotonic()<deadline,"reference readiness timed out")
                time.sleep(.05)
        self.control(role+"-ready",owner,"observe")
        if self.m.get("bounded_container") is True:
            pid=owner["process"]["pid"];proc=pathlib.Path("/proc",str(pid))
            limits=(proc/"limits").read_text()
            c.write_new(self.directory/(role+".actual-proc-limits.json"),dict(schema_version=1,campaign_id=self.m["campaign_id"],role=role,process=owner["process"],raw=limits,observed_utc=c.now()))
            c.require(re.search(r"^Max cpu time\s+449\s+450\s+seconds[ \t]*$",limits,re.M),"actual mongod CPU limits differ")
            groups=(proc/"cgroup").read_text().splitlines();unified=[x.split(":",2)[2] for x in groups if x.startswith("0::")]
            c.require(len(unified)==1 and owner["container_id"] in unified[0],"actual mongod cgroup identity mismatch")
            group=pathlib.Path("/sys/fs/cgroup")/unified[0].lstrip("/")
            c.require((group/"memory.max").read_text().strip()=="12884901888","actual mongod memory bound differs")
            c.require((group/"memory.swap.max").read_text().strip()=="0","actual mongod swap bound differs")
            members=[int(x) for x in (group/"cgroup.procs").read_text().split()]
            c.require(members==[pid],"bounded container must have one actual mongod process")
            group_stat=group.stat()
            policy=observe_container_cpu_policy(pid,owner["process"],group,owner["container_id"])
            c.write_new(self.directory/(role+".resource-observation.json"),dict(schema_version=1,campaign_id=self.m["campaign_id"],role=role,process=owner["process"],limits=limits,cgroup=str(group),cgroup_device=group_stat.st_dev,cgroup_inode=group_stat.st_ino,memory_max=(group/"memory.max").read_text().strip(),memory_swap_max=(group/"memory.swap.max").read_text().strip(),members=members,cpu_policy=policy,scope="sampled single mongod identity; four sequential process CPU limits <=1800s; container memory includes cache; separate client limits"))
            import ycsb_reference_wire as wire
            frames=self.directory/(role+".default-concern-frames");frames.mkdir()
            c.require(c.same(c.observe(pid),owner["process"]),"reference changed before default concern query")
            query_started=c.now()
            client=wire.Client(self.m["address"],frames)
            try:reply=client.call({"getDefaultRWConcern":1,"$db":"admin"})
            finally:client.close()
            query_ended=c.now()
            c.require(c.same(c.observe(pid),owner["process"]),"reference changed during default concern query")
            c.require(reply.get("ok")==1 or (reply.get("ok")==0 and reply.get("code")==51300 and reply.get("codeName")=="Location51300" and reply.get("errmsg")=="'getDefaultRWConcern' is not supported on standalone nodes."),"actual reference default concern query failed unexpectedly")
            c.write_new(self.directory/(role+".default-concern.json"),dict(schema_version=1,campaign_id=self.m["campaign_id"],role=role,process=owner["process"],start_utc=query_started,end_utc=query_ended,reply=reply,scope="actual server default; does not reinterpret omitted client j as false"))
        return owner

    def control(self,stem,owner,mode):
        role=next(role for role,item in self.owned.items() if item["cid"]==owner["container_id"])
        self.checked(owner["container_id"],role,pathlib.Path(owner["data_dir"]))
        c.require(c.same(c.observe(owner["process"]["pid"]),owner["process"]),"reference changed before public control")
        command=control_argv(self.directory,stem,self.m,mode)
        self.helper(stem,command)
        return control(self.directory,stem,self.m,owner,mode)

    def helper(self,stem,command,phase=False):
        d=self.m["deadlines"];kind="phase" if phase else "helper"
        c.require(time.monotonic()+d[kind+"_outer_seconds"]<=self.deadline,"reference run budget exhausted before helper action")
        c.write_new(self.directory/(stem+".command.json"),dict(schema_version=1,argv=command))
        with c.outer_deadline(d[kind+"_outer_seconds"]):
            role="client" if phase else ("fixture" if command[0]==self.m["fixture"]["binary"] else "control")
            code=c.launch(argparse.Namespace(directory=str(self.directory),stem=stem,command=command,timeout=d[kind+"_seconds"],cleanup_timeout=d["cleanup_seconds"],role=role))
        c.require(code==0,"reference child failed: "+stem)

    def fixture(self,stem,mode,stage=None):
        c.require(len(self.owned)==1,"reference helper requires exactly one owned active service")
        self.control(stem+"-owner",next(iter(self.owned.values()))["owner"],"observe")
        self.helper(stem,fixture_argv(self.directory,stem,self.m,mode,stage))

    def checkpoint(self,stem,owner,snapshot):
        self.control(stem,owner,"checkpoint")
        c.write_new(self.directory/(stem+".binding.json"),dict(schema_version=1,container_id=owner["container_id"],snapshot_sha256=c.digest(self.directory/snapshot),control_sha256=c.digest(self.directory/(stem+".json"))))

    def stop(self,role,owner):
        cid=owner["container_id"];data=pathlib.Path(owner["data_dir"])
        self.control(role+"-before-close",owner,"observe")
        self.checked(cid,role,data)
        c.require(c.same(c.observe(owner["process"]["pid"]),owner["process"]),"reference changed before SIGTERM")
        started=c.now()
        self.command("kill","--signal=SIGTERM",cid)
        actions=dict(sigterm=self.last_stem)
        waited=self.command("wait",cid)
        actions["wait"]=self.last_stem
        c.require(waited=="0","mongod actual exit was not zero")
        terminal=self.checked(cid,role,data);container_inspect(terminal,self.m,role,str(data),True)
        actions["terminal"]=self.last_stem
        ended=c.now()
        c.write_new(self.directory/(role+".terminal.json"),terminal)
        logs=self.command("logs",cid)
        actions["logs"]=self.last_stem
        # MongoDB structured stdout is authoritative; docker logs stderr is
        # retained by the CLI launcher too. Do not manufacture completion logs.
        (self.directory/(role+".log")).write_bytes((self.directory/(self.last_stem+".out")).read_bytes())
        c.write_new(self.directory/(role+".close.json"),dict(schema_version=1,kind="mongodb_observed_graceful_process_exit",container_id=cid,process=owner["process"],signal="SIGTERM",forced_kill=False,close_return_observed=False,exit_code=0,observed_exit=live.interval(started,ended),actions=actions,terminal_sha256=c.digest(self.directory/(role+".terminal.json")),logs_sha256=c.digest(self.directory/(role+".log"))))
        self.checked(cid,role,data)
        c.require(self.command("rm",cid)==cid,"owned container removal acknowledgement mismatch")
        c.write_new(self.directory/(role+".removed.json"),dict(schema_version=1,container_id=cid,removed=True,action=self.last_stem))
        del self.owned[role]

    def cleanup(self):
        # Permanent failure is retained before forced cleanup; this path cannot
        # generate qualifying close evidence. Container IDs never get reused.
        failures=[]
        for role,item in list(self.owned.items()):
            try:
                values=loads(self.emergency_command("inspect",item["cid"] or item["name"]))
                c.require(isinstance(values,list) and len(values)==1,"ambiguous emergency container inspect")
                value=values[0]
                cid=container_inspect(value,self.m,role,str(item["data"]))
                c.require((item["cid"] is not None and cid==item["cid"]) or (item["cid"] is None and value["Name"]=="/"+item["name"]),"emergency container identity differs from owned intent")
                c.require(type(value["State"]["Running"]) is bool,"invalid emergency running state")
                if value["State"]["Running"] is True:
                    self.emergency_command("kill","--signal=SIGKILL",cid)
                    waited=self.emergency_command("wait",cid).strip()
                    c.require(re.fullmatch(r"[0-9]+",waited),"invalid emergency container wait status")
                else:waited=None
                values=loads(self.emergency_command("inspect",cid))
                c.require(isinstance(values,list) and len(values)==1,"ambiguous emergency terminal inspect")
                terminal=values[0]
                c.require(container_inspect(terminal,self.m,role,str(item["data"]))==cid,"emergency terminal identity mismatch")
                state=terminal["State"]
                c.require(state["Running"] is False and type(state["Pid"]) is int and state["Pid"]==0 and type(state["ExitCode"]) is int,"emergency container not terminal")
                c.require(waited is None or str(state["ExitCode"])==waited,"emergency wait/terminal exit mismatch")
                c.require(self.emergency_command("rm",cid).strip()==cid,"cleanup removal mismatch")
                self.cleanup_records[role]=dict(schema_version=1,container_id=cid,removed=True,qualification_invalid=True,exit_code=state["ExitCode"])
                del self.owned[role]
            except BaseException as error:failures.append(role+": "+str(error))
        c.require(not failures,"unresolved owned reference custody: "+"; ".join(failures))

    def emergency_command(self,*args):
        # No ordinary command()/launch() artifact path is used here. Records
        # live in memory until every exact-owned service has been dealt with.
        record=dict(argv=[self.binary,*args],started_utc=c.now(),success=False)
        self.emergency_actions.append(record)
        try:
            output=emergency_cli(self.binary,list(args),self.m)
            record.update(success=True,stdout=output)
            return output
        except BaseException as error:
            record.update(error_type=type(error).__name__,error=str(error))
            raise
        finally:record["ended_utc"]=c.now()


def runtime_record(version,info):
    components={x["Name"]:x for x in version["Components"]}
    return dict(engine=version["Version"],engine_commit=version["GitCommit"],containerd=components["containerd"]["Version"],runc=components["runc"]["Version"],driver=info["Driver"],cgroup_version=info["CgroupVersion"],default_runtime=info["DefaultRuntime"])


def failed_attempt(out,driver,error):
    """Custody takes precedence over best-effort failure publication."""
    previous=signal.pthread_sigmask(signal.SIG_BLOCK,c.CANCELLATION_SIGNALS)
    issues=[]
    original=type(error).__name__+": "+(str(error) or type(error).__name__)
    def publish(path,value):
        try:c.write_new(path,value)
        except BaseException as failure:issues.append(str(path)+": "+type(failure).__name__+": "+str(failure))
    try:
        signal.setitimer(signal.ITIMER_REAL,0)
        if not os.path.lexists(out/"failure.json"):
            publish(out/"failure.json",dict(schema_version=1,kind="mongodb_reference_run_failure",error=original,end_utc=c.now()))
        # Never put cleanup behind an artifact operation or artifact success.
        try:driver.cleanup()
        except BaseException as failure:issues.append("cleanup: "+type(failure).__name__+": "+str(failure))
        for role,value in driver.cleanup_records.items():publish(driver.directory/(role+".cleanup.json"),value)
        publish(out/"cleanup.failure.json",dict(schema_version=1,kind="mongodb_reference_failed_custody",error=original,secondary_errors=list(issues),actions=driver.emergency_actions,unresolved_roles=list(driver.owned),end_utc=c.now()))
    finally:
        # If cancellation is pending, retain the original and every secondary
        # error on stderr before unblocking can deliver it.
        message="reference attempt failed: "+original
        if issues:message+="; secondary failures: "+"; ".join(issues)
        print(message,file=sys.stderr)
        signal.pthread_sigmask(signal.SIG_SETMASK,previous)
    raise RuntimeError(message) from error


def run(out,docker_binary):
    c.require(out.is_absolute(),"absolute reference output directory required before Docker actions")
    m=c.read(out/"manifest.json");manifest(m);failure_free(out)
    if "preparation_sha256" in m:
        from ycsb_mongodb_reference_prepare import verify_prepared
        verify_prepared(out,m,check_current_sources=True)
    if m.get("require_client_runtime_observation") is True:c.runtime_client_preflight(m["client"])
    directory=out/"mongodb"
    c.require(not list(directory.iterdir()),"fresh empty reference directory required")
    c.require(c.digest(__file__)==m["reference_source_sha256"] and c.digest(out/"reference-source.py")==m["reference_source_sha256"],"actual reference launcher source is not bound")
    c.require(c.exe_digest(docker_binary)==m["server_builds"]["mongodb"]["docker_cli_sha256"],"Docker executable mismatch")
    c.require(docker_binary==m["docker_binary"] and c.exe_digest(m["client_binary"])==m["client"]["exe_sha256"],"reference Docker/client executable binding mismatch")
    for key in ("fixture","control"):
        c.require(c.exe_digest(m[key]["binary"])==m[key]["exe_sha256"],"reference helper binary mismatch")
    for path in (m["fixture"]["binary"],m["control"]["binary"],docker_binary):c.require(pathlib.Path(path).is_absolute(),"absolute supplied tools required")
    c.require(not any(os.path.lexists(out/name) for name in ("reference-incomplete.json","reference-complete.json")),"reference attempt already started")
    # Published before any Docker action; remains after every failed attempt,
    # including failures that prevent all later receipt writes.
    c.write_new(out/"reference-incomplete.json",dict(schema_version=1,kind="mongodb_reference_incomplete_attempt",campaign_id=m["campaign_id"],started_utc=c.now()))
    driver=Docker(out,docker_binary)
    try:
        image=loads(driver.command("image","inspect",m["server_builds"]["mongodb"]["image_id"]))[0]
        inventory=dict(image=driver.last_stem)
        b=m["server_builds"]["mongodb"]
        c.require(image["Id"]==b["image_id"] and b["repo_digest"] in image["RepoDigests"] and image["Architecture"]==b["architecture"] and image["Os"]==b["os"],"actual local image differs from pinned image")
        c.require(c.same(image["Config"].get("Labels") or {},b["image_labels"]),"actual immutable image labels changed")
        c.require(all(not x.startswith("GOMAXPROCS=") for x in image["Config"]["Env"]),"image sets GOMAXPROCS")
        version=loads(driver.command("version","--format","{{json .Server}}"))
        inventory["version"]=driver.last_stem
        info=loads(driver.command("info","--format","{{json .}}"))
        inventory["info"]=driver.last_stem
        c.require(c.same(runtime_record(version,info),b["runtime"]),"Docker runtime changed")
        c.write_new(out/"reference-image.json",image)
        c.write_new(out/"reference-runtime.json",dict(version=version,info=info))
        c.write_new(out/"reference-inventory.json",dict(schema_version=1,actions=inventory))
        for name in ("db","db.configdb","fixture-db","fixture-db.configdb"):(directory/name).mkdir()
        owner=driver.start("server",directory/"db")
        for i in range(m["run_repeats"]+1):
            stem="load" if i==0 else "run_"+str(i)
            driver.checked(owner["container_id"],"server",directory/"db")
            c.require(c.same(c.observe(owner["process"]["pid"]),owner["process"]),"reference changed before measured client")
            driver.control(stem+"-owner",owner,"observe")
            if "common_load" in m:
                import ycsb_public_sentinel as sentinel
                sentinel.phase_command(directory,stem,m,phase_argv(m,stem))
            driver.helper(stem,phase_argv(m,stem),True)
            if i==0:
                driver.fixture("snapshot_loaded","snapshot","loaded")
                if "common_load" in m:sentinel.run(directory,m,owner["process"],m["address"])
        driver.fixture("snapshot_preclose","snapshot","preclose")
        driver.checkpoint("random-checkpoint",owner,"snapshot_preclose.json")
        driver.stop("server",owner)
        owner=driver.start("reopened",directory/"db")
        driver.fixture("snapshot_reopened","snapshot","reopened")
        driver.stop("reopened",owner)
        owner=driver.start("fixture-write",directory/"fixture-db")
        driver.fixture("fixture_write","write")
        driver.checkpoint("fixture-checkpoint",owner,"fixture.updated.json")
        driver.stop("fixture-write",owner)
        owner=driver.start("fixture-reopened",directory/"fixture-db")
        driver.fixture("fixture_verify","verify")
        driver.stop("fixture-reopened",owner)
        report=validate(out,require_completion=False)
        c.write_new(out/"reference-complete.json",dict(schema_version=1,kind="mongodb_reference_completed_attempt",campaign_id=m["campaign_id"],manifest_sha256=c.digest(out/"manifest.json"),completed_utc=c.now()))
        (out/"reference-incomplete.json").unlink()
        return report
    except BaseException as error:
        failed_attempt(out,driver,error)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=("validate","run"))
    parser.add_argument("out",type=pathlib.Path)
    parser.add_argument("--docker",default="/usr/bin/docker")
    args=parser.parse_args()
    args.out=args.out.resolve()
    try:
        if args.mode=="validate":report=validate(args.out)
        else:
            m=c.read(args.out/"manifest.json");manifest(m)
            def interrupted(signum,frame):raise InterruptedError("reference interrupted by signal "+str(signum))
            signal.signal(signal.SIGTERM,interrupted)
            report=run(args.out,args.docker)
        print(json.dumps(report,allow_nan=False))
        return 0
    except (Exception,KeyboardInterrupt) as error:
        if args.mode=="run" and args.out.is_dir() and not os.path.lexists(args.out/"failure.json"):
            previous=signal.pthread_sigmask(signal.SIG_BLOCK,c.CANCELLATION_SIGNALS)
            try:
                try:c.write_new(args.out/"failure.json",dict(schema_version=1,kind="mongodb_reference_terminal_failure",error=str(error) or type(error).__name__,error_type=type(error).__name__,end_utc=c.now()))
                except BaseException as publication:print("reference terminal receipt publication failed: "+type(publication).__name__+": "+str(publication),file=sys.stderr)
            finally:signal.pthread_sigmask(signal.SIG_SETMASK,previous)
        print("invalid MongoDB reference: "+(str(error) or type(error).__name__),file=sys.stderr)
        return 3


if __name__=="__main__":raise SystemExit(main())
