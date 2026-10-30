#!/usr/bin/env python3
"""Offline preparation only; never invokes Docker or contacts a service.

Consume a complete, separately retained tiny support packet. Revalidation and
hashes bind its evidence; they do not authenticate hand-created input or grant
permission to activate the guarded shell. No image tag/environment is used to
manufacture actual buildInfo, journal settings, or runtime ELF attestation.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import signal
import sys
import uuid

import ycsb_compare_validate as c
import ycsb_mongodb_reference as reference

SOURCES = ("ycsb_mongodb_reference_prepare.py", "ycsb_mongodb_reference.py",
           "ycsb_mongodb_reference_client.go", "ycsb_correctness_fixture.go",
           "ycsb_compare_validate.py", "ycsb_compare_live.py",
           "ycsb_correctness_validate.py", "ycsb_evidence_json.py",
           "ycsb_compare_mongodb_treedb.sh", "ycsb_runtime_observer.py", "ycsb_reference_wire.py")
ROOT = pathlib.Path(__file__).resolve().parent
KIND = "mongodb_reference_offline_preparation"

def source_names(m):
    return SOURCES+(("ycsb_public_sentinel.py",) if "common_load" in m else ())
INPUT_COPIES = {"reference-support.json", "reference-source.py", "fixture-source.go",
                "reference-control-source.go", "client.patch", "client-build.json",
                "reference-build.json", "client-buildinfo.txt", "fixture-buildinfo.txt",
                "control-buildinfo.txt"}
REQUEST_FIELDS = {"schema_version", "kind", "support_dir", "mongodb_addr",
                  "client_source_dir", "client_build_manifest", "client_binary",
                  "fixture_binary", "control_binary", "docker_binary", "go_binary",
                  "recordcount", "operationcount", "threadcount", "run_repeats", "deadlines"}


def absolute(value,label):
    c.require(type(value) is str and pathlib.Path(value).is_absolute(),"absolute "+label+" required")
    return pathlib.Path(value).resolve(strict=True)


def command(argv):
    # Git and Go buildinfo inspection only. No Docker/helper/product executable
    # is invoked by preparation, and inherited GOMAXPROCS is removed.
    binary=shutil.which(argv[0])
    c.require(binary is not None,"missing metadata inspection executable")
    return reference.bounded_cli(binary,argv[1:],c.exe_digest(binary),15).encode("utf-8")


def evidence_files(directory):
    """Regular evidence files only; never copy service DBs or follow symlinks."""
    files={}
    for parent in (directory,directory/"mongodb"):
        c.require(parent.is_dir() and not parent.is_symlink(),"missing/linked support evidence directory")
        for path in parent.iterdir():
            c.require(not path.is_symlink(),"linked support evidence is unsupported")
            if path.is_file():files[str(path.relative_to(directory))]=path
    return files


def support_record(directory,evidence_root=None):
    evidence_root=pathlib.Path(evidence_root) if evidence_root is not None else directory
    c.require(evidence_root.is_absolute(),"absolute original support evidence root required")
    m=c.read(directory/"manifest.json")
    c.require("preparation_sha256" not in m,"support must be an independent bootstrap packet, not nested preparation")
    reference.manifest(m)
    c.require(m["recordcount"]<=1024 and m["operationcount"]<=1024 and m["run_repeats"]==1,"tiny separately labeled support control required")
    files=evidence_files(directory)
    c.require(len(files)<=2048 and all(path.stat().st_size<=16*1024*1024 for path in files.values()) and sum(path.stat().st_size for path in files.values())<=128*1024*1024,"support evidence exceeds bounded preparation input")
    reference.validate(directory,evidence_root=evidence_root)
    observations={}
    for role in reference.ROLES:
        owner=c.read(directory/"mongodb"/(role+".identity.json"))
        commands=c.read(directory/"mongodb"/(role+"-ready.json"))["commands"]
        observations[role]=dict(container_id=owner["container_id"],process=owner["process"],
            buildInfo=commands["before.buildInfo"]["reply"],
            parsed_settings=commands["before.getCmdLineOpts"]["reply"]["parsed"],
            journal_parameters=reference.journal_parameters(commands["before.getParameter"]["reply"]),
            public_process_id=commands["before.hello"]["reply"]["topologyVersion"]["processId"]["$oid"])
    return dict(schema_version=1,kind="mongodb_reference_retained_support",
        manifest_sha256=c.digest(directory/"manifest.json"),campaign_id=m["campaign_id"],evidence_root=str(evidence_root),
        address=m["address"],image_build=m["server_builds"]["mongodb"],
        write_concern=m["write_concern"],runtime_metadata=m["server_builds"]["mongodb"]["runtime"],
        runtime_elf_attested=False,observations=observations)


def verify_prepared(out,m,check_current_sources=False,_allow_incomplete=False):
    c.require(_allow_incomplete or not os.path.lexists(out/"preparation-incomplete.json"),"incomplete reference preparation")
    c.require(type(m["preparation_sha256"]) is str and c.SHA.fullmatch(m["preparation_sha256"]),"invalid preparation digest")
    c.require(c.digest(out/"reference-preparation.json")==m["preparation_sha256"],"preparation receipt changed")
    value=c.read(out/"reference-preparation.json")
    c.require(c.schema(value) and value["kind"]==KIND and value["activation_authorized"] is False and value["runtime_elf_attested"] is False,"invalid provisional preparation contract")
    c.timestamp(value["prepared_utc"])
    c.require(c.same(value["manifest_fields"],{k:v for k,v in m.items() if k!="preparation_sha256"}),"prepared manifest fields changed")
    files=value["artifacts_sha256"]
    c.require(type(files) is dict and files,"missing preparation artifact bindings")
    for name,digest in files.items():
        path=pathlib.PurePosixPath(name)
        c.require(type(name) is str and not path.is_absolute() and ".." not in path.parts and type(digest) is str and c.SHA.fullmatch(digest),"unsafe preparation artifact path/hash")
        c.require(all(not (out/pathlib.Path(*path.parts[:i])).is_symlink() for i in range(1,len(path.parts)+1)),"linked preparation artifact")
        c.require(c.digest(out/name)==digest,"preparation artifact changed: "+name)
    support=out/"reference-support"
    c.require(type(value["support_evidence_root"]) is str and pathlib.Path(value["support_evidence_root"]).is_absolute(),"invalid original support evidence root")
    actual=support_record(support,evidence_root=value["support_evidence_root"])
    c.require(c.same(c.read(out/"reference-support.json"),actual),"retained support copies changed")
    b=m["server_builds"]["mongodb"]
    c.require(c.same(actual["image_build"],b) and actual["address"]==m["address"] and c.same(actual["write_concern"],m.get("support_write_concern",m["write_concern"])),"prepared reference identity/endpoint differs from support")
    c.require(c.same(actual["observations"]["server"]["journal_parameters"],m["journal_parameters"]),"prepared journal settings differ from actual support")
    supported=c.read(support/"manifest.json")
    for key in ("client","fixture","control"):
        c.require(c.same(m[key],supported[key]),"prepared client/helper differs from supported build")
    expected={"reference-support/"+name for name in evidence_files(support)}
    c.require(set(files)==expected|INPUT_COPIES|{"preparation-sources/"+name for name in source_names(m)},"missing or extra preparation artifact binding")
    c.require(c.same(c.read(out/"client-build.json"),m["client"]) and c.same(c.read(out/"reference-build.json"),b),"prepared build copies differ")
    for name,digest in (("client.patch",m["client"]["source_patch_sha256"]),("reference-source.py",m["reference_source_sha256"]),("fixture-source.go",m["fixture"]["source_sha256"]),("reference-control-source.go",m["control"]["source_sha256"])):
        c.require(files[name]==digest,"prepared source/overlay differs from manifest")
    c.require(all((out/(key+"-buildinfo.txt")).read_text().strip() for key in ("client","fixture","control")),"missing prepared executable buildinfo")
    for name in source_names(m):
        copied="preparation-sources/"+name
        c.require(copied in files,"missing preparation source binding")
        if check_current_sources:c.require(c.digest(ROOT/name)==files[copied],"actual preparation/consumer source changed")
    return value


def prepare(config,out):
    c.require(c.schema(config) and config["kind"]=="mongodb_reference_preparation_request","explicit reference preparation request required")
    c.require(REQUEST_FIELDS <= set(config) <= REQUEST_FIELDS|{"journal_requested","bounded_container","common_load"},"missing or unknown reference preparation request fields")
    c.require(out.is_absolute() and not os.path.lexists(out),"fresh absolute preparation output required")
    c.require(type(config["mongodb_addr"]) is str,"explicit MONGODB_ADDR required")
    reference.address(config["mongodb_addr"])
    support=absolute(config["support_dir"],"retained support directory")
    c.require(out!=support and support not in out.parents and out not in support.parents,"preparation and support paths must be separate")
    # Require full original evidence before creating output; a summary/build
    # label cannot substitute for a support packet.
    actual=support_record(support)
    sm=c.read(support/"manifest.json")
    c.require(actual["address"]==config["mongodb_addr"],"MONGODB_ADDR differs from observed support endpoint")
    repo=absolute(config["client_source_dir"],"producer source")
    client_binary=absolute(config["client_binary"],"producer executable")
    go=absolute(config["go_binary"],"Go buildinfo tool")
    client=c.read(absolute(config["client_build_manifest"],"producer build manifest"));c.client_build(client)
    c.runtime_client_preflight(client)
    git=lambda *args:command(["git","-C",str(repo),*args])
    git("merge-base","--is-ancestor",c.PRODUCER,"HEAD")
    c.require(not git("ls-files","--others","--exclude-standard").strip(),"untracked producer input")
    patch=git("diff","--binary","HEAD")
    c.require(git("rev-parse","HEAD").decode().strip()==client["source_commit"] and hashlib.sha256(patch).hexdigest()==client["source_patch_sha256"] and c.exe_digest(client_binary)==client["exe_sha256"],"producer source/overlay/ELF mismatch")
    c.require(c.same(client,sm["client"]),"producer differs from supported client")
    buildinfos={"client":command([str(go),"version","-m",str(client_binary)])}
    for key in ("fixture","control"):
        binary=absolute(config[key+"_binary"],key+" executable")
        source="ycsb_correctness_fixture.go" if key=="fixture" else "ycsb_mongodb_reference_client.go"
        c.require(str(binary)==sm[key]["binary"] and c.exe_digest(binary)==sm[key]["exe_sha256"] and c.digest(ROOT/source)==sm[key]["source_sha256"],"helper source/ELF differs from actual supported build")
        buildinfos[key]=command([str(go),"version","-m",str(binary)])
    docker=absolute(config["docker_binary"],"Docker executable")
    c.require(str(docker)==sm["docker_binary"] and c.exe_digest(docker)==actual["image_build"]["docker_cli_sha256"],"Docker executable differs from supported CLI")
    c.require(all(info.strip() for info in buildinfos.values()),"missing actual Go executable buildinfo")
    m=dict(schema_version=1,kind=reference.KIND,lifecycle_contract=1,campaign_id=uuid.uuid4().hex,
        require_client_runtime_observation=True,targets=["mongodb"],profile=reference.ack_profile(config.get("journal_requested")),acknowledgement_class=reference.ack_profile(config.get("journal_requested")),fixture_profile=reference.PROFILE,formats={"mongodb":"bson"},
        write_concern=dict(w=1,journal_requested=config.get("journal_requested")),address=config["mongodb_addr"],
        docker_binary=str(docker),client_binary=str(client_binary),reference_source_sha256=c.digest(ROOT/"ycsb_mongodb_reference.py"),
        server_builds={"mongodb":actual["image_build"]},client=client,fixture=sm["fixture"],control=sm["control"],
        journal_parameters=actual["observations"]["server"]["journal_parameters"],deadlines=config["deadlines"],
        uid=os.getuid(),gid=os.getgid())
    if config.get("journal_requested") is not None:
        c.require(config["journal_requested"] is True,"only true or omitted journal class supported")
        m["support_write_concern"]=actual["write_concern"]
    if config.get("bounded_container") is True:m["bounded_container"]=True
    c.require("bounded_container" not in config or config["bounded_container"] is True,"container bound opt-in must be true")
    for key in ("recordcount","operationcount","threadcount","run_repeats"):m[key]=config[key]
    if "common_load" in config:m["common_load"]=config["common_load"]
    reference.manifest(m)
    c.require(m["uid"]==sm["uid"] and m["gid"]==sm["gid"],"task user differs from supported custody")
    out.mkdir();(out/"mongodb").mkdir()
    c.write_new(out/"preparation-incomplete.json",dict(schema_version=1,kind="mongodb_reference_preparation_incomplete"))
    def copy_bytes(name,data):
        path=out/name;path.parent.mkdir(parents=True,exist_ok=True)
        with path.open("xb") as stream:stream.write(data)
    for name,path in evidence_files(support).items():copy_bytes("reference-support/"+name,path.read_bytes())
    # Check copied evidence rather than trusting a path that can change between
    # validation and publication. Service DB directories are never copied.
    actual=support_record(out/"reference-support",evidence_root=support)
    c.write_new(out/"reference-support.json",actual)
    for name in source_names(m):copy_bytes("preparation-sources/"+name,(ROOT/name).read_bytes())
    copy_bytes("reference-source.py",(ROOT/"ycsb_mongodb_reference.py").read_bytes())
    copy_bytes("fixture-source.go",(ROOT/"ycsb_correctness_fixture.go").read_bytes())
    copy_bytes("reference-control-source.go",(ROOT/"ycsb_mongodb_reference_client.go").read_bytes())
    copy_bytes("client.patch",patch)
    for key,data in buildinfos.items():copy_bytes(key+"-buildinfo.txt",data)
    c.write_new(out/"client-build.json",client);c.write_new(out/"reference-build.json",m["server_builds"]["mongodb"])
    artifacts={str(path.relative_to(out)):c.digest(path) for path in out.rglob("*") if path.is_file() and path.name!="preparation-incomplete.json"}
    record=dict(schema_version=1,kind=KIND,prepared_utc=c.now(),activation_authorized=False,runtime_elf_attested=False,support_evidence_root=str(support),manifest_fields=m,artifacts_sha256=artifacts)
    c.write_new(out/"reference-preparation.json",record)
    m=dict(m,preparation_sha256=c.digest(out/"reference-preparation.json"))
    c.write_new(out/"manifest.json",m)
    verify_prepared(out,m,check_current_sources=True,_allow_incomplete=True)
    (out/"preparation-incomplete.json").unlink()
    return dict(status="prepared-reference-inputs-not-activated",campaign_id=m["campaign_id"],support_campaign_id=actual["campaign_id"],activation_authorized=False,runtime_elf_attested=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config",type=pathlib.Path)
    parser.add_argument("out",type=pathlib.Path)
    args=parser.parse_args()
    previous=signal.getsignal(signal.SIGTERM)
    def interrupted(signum,frame):
        raise InterruptedError("reference preparation interrupted by signal "+str(signum))
    signal.signal(signal.SIGTERM,interrupted)
    try:
        print(json.dumps(prepare(c.read(args.config),args.out.resolve()),allow_nan=False))
        return 0
    except (Exception,KeyboardInterrupt) as error:
        print("invalid reference preparation: "+(str(error) or type(error).__name__),file=sys.stderr)
        return 3
    finally:
        signal.signal(signal.SIGTERM,previous)


if __name__=="__main__":raise SystemExit(main())
