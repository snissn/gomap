#!/usr/bin/env python3
"""Fixed-peer EC2 substrate: offline plan, exact changeset apply, scoped cleanup.

No dataset generator, ANN benchmark, consensus implementation, or secret delivery
is embedded here. Existing production configs/binaries and benchmark artifacts
remain authoritative. See TreeDB/docs/operations/fixed-peer-ec2.md.
"""
import argparse
import base64
import collections
import datetime as dt
import hashlib
import ipaddress
import json
import pathlib
import re
import subprocess
import sys


class Refused(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise Refused(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def load(path, limit=4 << 20):
    with open(path, "rb") as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, "input exceeds limit")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(Refused("non-finite JSON")))


def save_new(path, value):
    # Do not overwrite a prior deployment receipt or evidence.
    with open(path, "x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
        stream.flush()
        import os
        os.fsync(stream.fileno())


def stamp(value):
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(result.utcoffset() == dt.timedelta(0), "timestamps must be UTC")
    return result


def inspect_artifacts(spec, binary):
    require(binary, "plan requires the exact production --binary")
    actual = hashlib.sha256()
    with open(binary, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""): actual.update(block)
    require(actual.hexdigest() == spec["binary_sha256"], "local executable digest mismatch")
    for node in spec["nodes"]:
        path = node.pop("config_path")
        config = load(path, 1 << 20)
        result = subprocess.run([str(pathlib.Path(binary).resolve()), "-mode", "inspect", "-config", path,
                                 "-expected-binary-sha256", spec["binary_sha256"]], capture_output=True, text=True, timeout=30)
        require(result.returncode == 0 and len(result.stdout) <= 1 << 20, "production config inspection failed: " + result.stderr[:1024])
        inspected = json.loads(result.stdout)
        build, identity = inspected["Binary"], inspected["Config"]
        require(build["VCSAvailable"] and not build["Modified"] and build["Revision"] == spec["source_revision"], "clean exact source identity required")
        require(build["OS"] == "linux" and build["Architecture"] == spec["binary_architecture"], "Linux binary architecture mismatch")
        require(identity["Authenticated"] and identity["NodeID"] == node["node_id"] and identity["LocalSHA256"] == node["config_sha256"] and identity["SharedSHA256"] == spec["shared_config_sha256"], "config identity mismatch")
        require(identity["ResourceLimits"]["InflightBytes"] == node["inflight_bytes"], "admission reservation differs from inventory")
        require(config["DataRoot"] == node["data_root"] and config["RaftRoot"] == node["raft_root"], "persistent mount/config mismatch")
        endpoint = config["ListenAddress"].split(":")
        require(len(endpoint) == 2 and endpoint[0] == node["private_ip"] and int(endpoint[1]) in spec["peer_ports"], "control endpoint differs from inventory")
        for address in config["RaftListen"].values():
            endpoint = address.split(":")
            require(len(endpoint) == 2 and endpoint[0] == node["private_ip"] and int(endpoint[1]) in spec["peer_ports"], "Raft endpoint differs from inventory")
        actual_groups = {g["ID"]: sorted(p["ID"] for p in g["Peers"]) for g in [config["Catalog"]] + config["Groups"]}
        require(actual_groups == {g["group_id"]: sorted(g["voters"]) for g in spec["groups"]}, "voter inventory differs from production config")
        node["inspected_config_identity"] = identity
    return spec


def make_plan(spec, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    require(spec.get("schema_version") == 1, "unsupported inventory schema")
    require(spec["binary_architecture"] in ("amd64", "arm64"), "unsupported Linux binary architecture")
    require(re.fullmatch(r"\d{12}", spec["account"]), "exact account required")
    require(re.fullmatch(r"[a-z]{2}(-[a-z]+)+-\d", spec["region"]), "exact region required")
    require(re.fullmatch(r"[a-z][a-z0-9-]{7,39}", spec["run_id"]), "invalid deployment run ID")
    for key in ("binary_sha256", "shared_config_sha256"):
        require(re.fullmatch(r"[a-f0-9]{64}", spec[key]), "missing artifact digest: " + key)
    require(re.fullmatch(r"[a-f0-9]{40}", spec["source_revision"]), "exact source revision required")
    expires = stamp(spec["expires_at"])
    require(dt.timedelta(minutes=5) < expires - now <= dt.timedelta(hours=24), "expiry must be 5m..24h from plan")
    require(0 < spec["duration_hours"] <= 24, "invalid duration")
    require((expires - now).total_seconds() <= spec["duration_hours"] * 3600, "expiry exceeds priced duration")
    require(spec["max_cost_usd"] > 0 and spec["network_reserve_usd"] >= 0 and spec["retained_storage_reserve_usd"] > 0, "cost and retained-volume reserve required")
    require(now - dt.timedelta(days=7) <= stamp(spec["price_checked_at"]) <= now, "refresh price attestation")
    prefix = f'arn:aws:iam::{spec["account"]}:role/'
    for role in ("expiry_role_arn", "cloudformation_role_arn"):
        require(spec[role].startswith(prefix) and len(spec[role]) > len(prefix), "account-scoped execution role required")
    require(re.fullmatch(r"fl-[a-f0-9]+", spec["flow_log_id"]), "existing all-traffic VPC flow log required")
    require(spec["peer_ports"] and len(spec["peer_ports"]) <= 256 and all(type(p) is int and 0 < p <= 65535 for p in spec["peer_ports"]), "explicit peer ports required")
    nodes = spec["nodes"]
    # Inline CloudFormation templates have a 51,200-byte API ceiling. Large
    # deployments use multiple independently owned packets, not larger RPCs.
    require(1 <= len(nodes) <= 64, "packet must contain 1..64 instances")
    by_id, ips = {}, set()
    total = spec["network_reserve_usd"] + spec["retained_storage_reserve_usd"]
    for node in nodes:
        name = node["node_id"]
        require(re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", name) and name not in by_id, "duplicate/invalid node")
        ip = ipaddress.ip_address(node["private_ip"])
        require(ip.version == 4 and any(ip in ipaddress.ip_network(c) for c in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")) and str(ip) not in ips, "unique RFC1918 IPv4 required")
        ips.add(str(ip)); by_id[name] = node
        require(node["az"].startswith(spec["region"]) and node["az_id"], "AZ identity required")
        for key, pattern in (("subnet_id", r"subnet-[a-f0-9]+"), ("image_id", r"ami-[a-f0-9]+")):
            require(re.fullmatch(pattern, node[key]), "invalid " + key)
        require(node["security_group_ids"] and all(re.fullmatch(r"sg-[a-f0-9]+", x) for x in node["security_group_ids"]), "private security groups required")
        require(node["vcpus"] >= node["gomaxprocs"] > 0 and node["memory_bytes"] > node["go_memory_limit_bytes"] >= node["inflight_bytes"] > 0, "CPU/memory capacity exceeded")
        require(node["data_gib"] * (1 << 30) >= 2 * node["required_data_bytes"] > 0 and node["raft_gib"] * (1 << 30) >= 2 * node["required_raft_bytes"] > 0, "persistent roots require 2x declared live capacity")
        require(node["temp_bytes"] >= node["required_temp_bytes"] >= 0, "temporary capacity exceeded")
        require(node["data_root"] != node["raft_root"] and all(pathlib.PurePosixPath(node[k]).is_absolute() for k in ("data_root", "raft_root", "temp_root")), "absolute distinct roots required")
        paths = [pathlib.PurePosixPath(node[k]) for k in ("data_root", "raft_root", "temp_root")]
        require(all(".." not in p.parts for p in paths), "unresolved parent path refused")
        require(all(a != b and a not in b.parents for i, a in enumerate(paths) for j, b in enumerate(paths) if i != j), "persistent and disposable paths overlap")
        require(re.fullmatch(r"[a-f0-9]{64}", node["config_sha256"]), "config digest required")
        require(re.fullmatch(r"\d{12}", node["image_owner_account"]), "AMI owner required")
        require(node["all_in_hourly_usd"] > 0, "instance+storage price required")
        total += node["all_in_hourly_usd"] * spec["duration_hours"]
        require(len(node["user_data"].encode()) <= 16 << 10, "user data exceeds EC2 limit")
    require(total <= spec["max_cost_usd"], "declared price estimate exceeds approved budget")
    require(spec["groups"] and len(spec["groups"]) <= 129, "catalog and data voter inventory required")
    names = set()
    for group in spec["groups"]:
        require(group["group_id"] not in names, "duplicate group"); names.add(group["group_id"])
        voters = group["voters"]
        require(len(voters) == len(set(voters)) and 3 <= len(voters) <= 31 and len(voters) % 2 == 1, "odd 3..31 voters required")
        require(all(v in by_id for v in voters), "voter outside packet inventory")
        domains = collections.Counter(by_id[v]["az_id"] for v in voters)
        quorum = len(voters) // 2 + 1
        require(all(len(voters) - count >= quorum for count in domains.values()), "voter placement loses quorum after one AZ loss")
    body = {"schema_version": 1, "inventory": spec, "estimated_cost_usd": round(total, 6)}
    body["plan_sha256"] = digest(body)
    body["template"] = template(body)
    require(len(canonical(body["template"]).encode()) <= 51200, "template exceeds inline limit; split independent groups into packets")
    return body


def tags(plan):
    s = plan["inventory"]
    return {"TreeDBRun": s["run_id"], "TreeDBPlan": plan["plan_sha256"], "TreeDBExpires": s["expires_at"]}


def template(plan):
    s = plan["inventory"]
    common = [{"Key": k, "Value": v} for k, v in tags(plan).items()]
    resources = {"Expiry": {"Type": "AWS::Scheduler::Schedule", "Properties": {
        "Name": "treedb-" + s["run_id"], "FlexibleTimeWindow": {"Mode": "OFF"},
        "ScheduleExpression": "at(" + stamp(s["expires_at"]).strftime("%Y-%m-%dT%H:%M:%S") + ")",
        "ScheduleExpressionTimezone": "UTC", "State": "ENABLED",
        "Target": {"Arn": "arn:aws:scheduler:::aws-sdk:cloudformation:deleteStack", "RoleArn": s["expiry_role_arn"],
                   "Input": {"Fn::Sub": '{"StackName":"${AWS::StackId}"}'},
                   "RetryPolicy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 10}}}}}
    for index, node in enumerate(s["nodes"]):
        node_tags = common + [{"Key": "TreeDBNode", "Value": node["node_id"]}]
        mounts = []
        for label, device, size in (("Data", "/dev/sdf", node["data_gib"]), ("Raft", "/dev/sdg", node["raft_gib"])):
            key = label + str(index)
            resources[key] = {"Type": "AWS::EC2::Volume", "DependsOn": "Expiry", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain",
                              "Properties": {"AvailabilityZone": node["az"], "Encrypted": True, "Size": size, "VolumeType": "gp3", "Tags": node_tags + [{"Key": "TreeDBRoot", "Value": label.lower()}]}}
            mounts.append({"Device": device, "VolumeId": {"Ref": key}})
        resources["Node" + str(index)] = {"Type": "AWS::EC2::Instance", "DependsOn": "Expiry", "Properties": {
            "ImageId": node["image_id"], "InstanceType": node["instance_type"], "IamInstanceProfile": node["instance_profile"],
            "AvailabilityZone": node["az"], "MetadataOptions": {"HttpTokens": "required", "HttpPutResponseHopLimit": 1},
            "NetworkInterfaces": [{"DeviceIndex": "0", "AssociatePublicIpAddress": False, "SubnetId": node["subnet_id"], "PrivateIpAddress": node["private_ip"], "GroupSet": node["security_group_ids"]}],
            "UserData": base64.b64encode(node["user_data"].encode()).decode(), "Volumes": mounts, "Tags": node_tags}}
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "TreeDB fixed-peer substrate; persistent volumes retained on cleanup", "Resources": resources}


def verify_plan(plan):
    body = {k: plan[k] for k in ("schema_version", "inventory", "estimated_cost_usd")}
    require(digest(body) == plan["plan_sha256"] and template(plan) == plan["template"], "plan/template digest mismatch")


class AWS:
    def __init__(self, region):
        self.region = region

    def call(self, service, operation, value):
        proc = subprocess.run(["aws", "--region", self.region, "--output", "json", "--no-cli-pager", service, operation,
                               "--cli-input-json", canonical(value)], capture_output=True, text=True, timeout=60)
        if proc.returncode:
            raise RuntimeError(proc.stderr.strip())
        return json.loads(proc.stdout) if proc.stdout.strip() else {}


def account_check(aws, plan):
    verify_plan(plan)
    require(aws.call("sts", "get-caller-identity", {})["Account"] == plan["inventory"]["account"], "AWS account mismatch")


def stage(aws, plan):
    account_check(aws, plan)
    s = plan["inventory"]
    require(stamp(s["expires_at"]) > dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5), "plan expired")
    # Validate the live subnet/AZ, instance capacity, image and all-traffic flow
    # log before CloudFormation is allowed to create any compute resources.
    for n in s["nodes"]:
        subnet = aws.call("ec2", "describe-subnets", {"SubnetIds": [n["subnet_id"]]})["Subnets"]
        require(len(subnet) == 1 and subnet[0]["AvailabilityZone"] == n["az"] and subnet[0]["AvailabilityZoneId"] == n["az_id"], "live subnet/AZ differs from plan")
        require(ipaddress.ip_address(n["private_ip"]) in ipaddress.ip_network(subnet[0]["CidrBlock"]), "IP outside subnet")
        kinds = aws.call("ec2", "describe-instance-types", {"InstanceTypes": [n["instance_type"]]})["InstanceTypes"]
        require(len(kinds) == 1 and kinds[0]["VCpuInfo"]["DefaultVCpus"] == n["vcpus"] and kinds[0]["MemoryInfo"]["SizeInMiB"] * (1 << 20) == n["memory_bytes"], "live instance capacity differs from plan")
        images = aws.call("ec2", "describe-images", {"ImageIds": [n["image_id"]]})["Images"]
        require(len(images) == 1 and images[0]["State"] == "available" and images[0]["OwnerId"] == n["image_owner_account"], "AMI identity unavailable/mismatched")
        require(images[0]["Architecture"] == {"amd64": "x86_64", "arm64": "arm64"}[s["binary_architecture"]] and images[0]["Architecture"] in kinds[0]["ProcessorInfo"]["SupportedArchitectures"], "AMI/binary architecture not supported")
        groups = aws.call("ec2", "describe-security-groups", {"GroupIds": n["security_group_ids"]})["SecurityGroups"]
        require(len(groups) == len(n["security_group_ids"]) and all(g["VpcId"] == subnet[0]["VpcId"] for g in groups), "security group VPC mismatch")
        for g in groups:
            for rule in g["IpPermissions"]:
                require(rule.get("IpProtocol") == "tcp" and rule.get("FromPort") == rule.get("ToPort") and rule.get("FromPort") in s["peer_ports"], "only declared TCP peer ports may enter")
                require(not rule.get("Ipv6Ranges") and not rule.get("PrefixListIds"), "unbounded ingress source refused")
                known_ips = {n["private_ip"] for n in s["nodes"]}
                known_groups = {g for n in s["nodes"] for g in n["security_group_ids"]}
                require(all(x["CidrIp"] in {ip + "/32" for ip in known_ips} for x in rule.get("IpRanges", [])), "ingress CIDR must identify an inventory host")
                require(all(x["GroupId"] in known_groups and x.get("UserId", s["account"]) == s["account"] for x in rule.get("UserIdGroupPairs", [])), "ingress group outside deployment inventory")
        flows = aws.call("ec2", "describe-flow-logs", {"FlowLogIds": [s["flow_log_id"]]})["FlowLogs"]
        require(len(flows) == 1 and flows[0]["ResourceId"] == subnet[0]["VpcId"] and flows[0]["TrafficType"] == "ALL" and flows[0]["FlowLogStatus"] == "ACTIVE", "active all-traffic VPC flow log required")
        required = {"${bytes}", "${az-id}", "${flow-direction}", "${interface-id}", "${srcaddr}", "${dstaddr}", "${pkt-srcaddr}", "${pkt-dstaddr}", "${log-status}"}
        require(required <= set(flows[0].get("LogFormat", "").split()) and flows[0].get("DeliverLogsStatus") == "SUCCESS", "flow logs must deliver wire-byte/AZ/direction evidence")
    name = "treedb-" + s["run_id"]
    result = aws.call("cloudformation", "create-change-set", {"StackName": name, "ChangeSetName": "plan-" + plan["plan_sha256"][:32], "ChangeSetType": "CREATE", "ClientToken": plan["plan_sha256"], "RoleARN": s["cloudformation_role_arn"], "TemplateBody": canonical(plan["template"]), "Tags": [{"Key": k, "Value": v} for k, v in tags(plan).items()]})
    return {"plan_sha256": plan["plan_sha256"], "stack_id": result["StackId"], "change_set_id": result["Id"]}


def owned_stack(aws, plan, receipt):
    account_check(aws, plan)
    s = plan["inventory"]
    require(receipt["plan_sha256"] == plan["plan_sha256"], "receipt belongs to another plan")
    prefix = f'arn:aws:cloudformation:{s["region"]}:{s["account"]}:stack/treedb-{s["run_id"]}/'
    require(receipt["stack_id"].startswith(prefix), "receipt account/region/stack mismatch")
    stack = aws.call("cloudformation", "describe-stacks", {"StackName": receipt["stack_id"]})["Stacks"]
    require(len(stack) == 1 and stack[0]["StackId"] == receipt["stack_id"], "stack identity mismatch")
    require(stack[0].get("RoleARN") == s["cloudformation_role_arn"], "stack execution role mismatch")
    observed = {x["Key"]: x["Value"] for x in stack[0].get("Tags", [])}
    require(all(observed.get(k) == v for k, v in tags(plan).items()), "wrong resource tags; refusing mutation")
    return stack[0]


def apply(aws, plan, receipt):
    stack = owned_stack(aws, plan, receipt)
    require(stamp(plan["inventory"]["expires_at"]) > dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5), "plan expired")
    changes = aws.call("cloudformation", "describe-change-set", {"ChangeSetName": receipt["change_set_id"], "StackName": receipt["stack_id"]})
    require(changes["StackId"] == stack["StackId"] and changes["Status"] == "CREATE_COMPLETE" and changes["ExecutionStatus"] == "AVAILABLE", "changeset not available; inspect partial/previous execution")
    actual = aws.call("cloudformation", "get-template", {"StackName": receipt["stack_id"], "ChangeSetName": receipt["change_set_id"], "TemplateStage": "Original"})["TemplateBody"]
    if isinstance(actual, str): actual = json.loads(actual)
    require(actual == plan["template"], "staged template differs from reviewed plan")
    return aws.call("cloudformation", "execute-change-set", {"ChangeSetName": receipt["change_set_id"], "StackName": receipt["stack_id"], "ClientRequestToken": plan["plan_sha256"]})


def inventory(aws, plan, receipt):
    stack = owned_stack(aws, plan, receipt)
    resources = []; token = None
    while True:
        request = {"StackName": receipt["stack_id"]}
        if token: request["NextToken"] = token
        page = aws.call("cloudformation", "list-stack-resources", request)
        resources.extend(page["StackResourceSummaries"])
        require(len(resources) <= 512, "unexpected stack resource cardinality")
        token = page.get("NextToken")
        if not token: break
    ids = [r["PhysicalResourceId"] for r in resources if r["ResourceType"] == "AWS::EC2::Instance" and r.get("PhysicalResourceId")]
    instances = []
    if ids:
        result = aws.call("ec2", "describe-instances", {"InstanceIds": ids})
        instances = [i for reservation in result["Reservations"] for i in reservation["Instances"]]
    return {"plan_sha256": plan["plan_sha256"], "stack": stack, "resources": resources, "instances": instances}


def cleanup(aws, plan, receipt):
    stack = owned_stack(aws, plan, receipt)
    if stack["StackStatus"] in ("DELETE_COMPLETE", "DELETE_IN_PROGRESS"):
        return {"already_deleting_or_deleted": True}
    if stack["StackStatus"] != "REVIEW_IN_PROGRESS":
        current = aws.call("cloudformation", "get-template", {"StackName": receipt["stack_id"], "TemplateStage": "Original"})["TemplateBody"]
        if isinstance(current, str): current = json.loads(current)
        require(current == plan["template"], "stack changed after planning; refusing cleanup")
    # Works for partial/failed creates too. CloudFormation owns the resource
    # graph. Persistent volume Retain policies are never overridden here.
    return aws.call("cloudformation", "delete-stack", {"StackName": receipt["stack_id"], "ClientRequestToken": "cleanup-" + plan["plan_sha256"]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["plan", "stage", "apply", "inventory", "cleanup"])
    parser.add_argument("--input", required=True)
    parser.add_argument("--receipt")
    parser.add_argument("--binary", help="exact built production executable, required for offline plan")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    data = load(args.input)
    if args.action == "plan":
        save_new(args.out, make_plan(inspect_artifacts(data, args.binary)))
        return
    verify_plan(data)
    # Reserve a new receipt file before any cloud request. An existing output
    # cannot cause successful provisioning followed by lost local ownership.
    with open(args.out, "x", encoding="utf-8") as output:
        try:
            aws = AWS(data["inventory"]["region"])
            if args.action == "stage": result = stage(aws, data)
            else:
                require(args.receipt, "exact deployment receipt required")
                result = {"apply": apply, "inventory": inventory, "cleanup": cleanup}[args.action](aws, data, load(args.receipt))
        except Exception as error:
            output.write(json.dumps({"action": args.action, "plan_sha256": data["plan_sha256"], "request_failed_or_ambiguous": True, "error": str(error)}) + "\n")
            output.flush()
            raise
        output.write(json.dumps(result, indent=2, allow_nan=False) + "\n")
        output.flush()
        import os
        os.fsync(output.fileno())



if __name__ == "__main__":
    try: main()
    except (Refused, KeyError, TypeError, OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
