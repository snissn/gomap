#!/usr/bin/env python3
"""Stream VPC flow records into explicit host/AZ byte observations.

Use the exact custom LogFormat from describe-flow-logs as --fields. This is
flow-record accounting, not a bill or proof of complete capture. SKIPDATA,
unknown addresses and non-OK records are retained as incompleteness evidence.
"""
import argparse
import hashlib
import json

from treedb_peer_ec2 import Refused, load, require, save_new, verify_plan


def summarize(plan, lines, fields):
    verify_plan(plan)
    columns = [x.removeprefix('${').removesuffix('}') for x in fields.split()]
    required = {'bytes','az-id','flow-direction','interface-id','srcaddr','dstaddr','pkt-srcaddr','pkt-dstaddr','log-status'}
    require(required <= set(columns) and len(columns) == len(set(columns)) <= 64, 'exact custom flow-log format required')
    nodes = {n['private_ip']: n for n in plan['inventory']['nodes']}
    counters = {n['node_id']: dict(ingress_bytes=0, egress_bytes=0, same_az_egress_bytes=0, cross_az_egress_bytes=0, unknown_destination_egress_bytes=0) for n in nodes.values()}
    result = dict(plan_sha256=plan['plan_sha256'], nodes=counters, records=0, no_data_records=0, incomplete_records=0, outside_inventory_records=0, complete=False,
                  note='Flow-record observation; delivery completeness requires external interval/ENI/log-delivery coverage, and is never inferred here.')
    sha=hashlib.sha256()
    for raw in lines:
        require(len(raw) <= 64 << 10, 'oversized flow record')
        sha.update(raw.encode())
        parts=raw.split()
        if not parts: continue
        require(len(parts)==len(columns), 'flow record does not match declared format')
        record=dict(zip(columns,parts)); result['records']+=1
        if record['log-status']=='NODATA': result['no_data_records']+=1;continue
        if record['log-status']!='OK': result['incomplete_records']+=1;continue
        require(record['bytes'].isdigit() and len(record['bytes'])<=20, 'invalid byte count')
        count=int(record['bytes']);require(count<1<<64,'byte count overflow')
        src=record['pkt-srcaddr'] if record['pkt-srcaddr']!='-' else record['srcaddr']
        dst=record['pkt-dstaddr'] if record['pkt-dstaddr']!='-' else record['dstaddr']
        if record['flow-direction']=='egress':
            node=nodes.get(src)
            if node is None:result['outside_inventory_records']+=1;continue
            if record['az-id']!=node['az_id']:result['incomplete_records']+=1;continue
            value=counters[node['node_id']];value['egress_bytes']+=count
            remote=nodes.get(dst)
            key='unknown_destination_egress_bytes' if remote is None else ('same_az_egress_bytes' if remote['az_id']==node['az_id'] else 'cross_az_egress_bytes')
            value[key]+=count
        elif record['flow-direction']=='ingress':
            node=nodes.get(dst)
            if node is None:result['outside_inventory_records']+=1;continue
            if record['az-id']!=node['az_id']:result['incomplete_records']+=1;continue
            counters[node['node_id']]['ingress_bytes']+=count
        else:result['incomplete_records']+=1
    result['input_sha256']=sha.hexdigest()
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',required=True);parser.add_argument('--input',required=True);parser.add_argument('--fields',required=True);parser.add_argument('--out',required=True)
    args=parser.parse_args()
    with open(args.input,encoding='utf-8') as stream:
        # readline's bound is enforced before an untrusted full-line allocation.
        def rows():
            while True:
                row=stream.readline((64<<10)+1)
                if not row:return
                yield row
        result=summarize(load(args.plan),rows(),args.fields)
    save_new(args.out,result)


if __name__=='__main__':main()
