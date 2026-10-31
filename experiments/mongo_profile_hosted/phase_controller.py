#!/usr/bin/env python3
"""One checked exchange; failures are immutable veto evidence, never retried."""
import argparse
import json
from pathlib import Path
import sys
import run as r

def checked(socket_path, target, receipt):
    value = dict(schema_version=1, target=target, start_utc=r.now(), success=False)
    try:
        with __import__('contextlib').closing(r.UnixHTTP(socket_path)) as conn:
            conn.request('POST', target, body=b'')
            response = conn.getresponse()
            raw = response.read(1024 * 1024 + 1)
            value.update(status=response.status, content_length=response.getheader('Content-Length'),
                         raw_body=raw.decode('utf-8',errors='replace'))
            r.require(response.status == 200 and len(raw) <= 1024 * 1024, 'profile controller status/body cap failed')
            r.require(int(value['content_length']) == len(raw), 'profile controller full response length failed')
            value['body'] = json.loads(raw)
        value['success'] = True
        return value['body']
    except BaseException as error:
        value['error'] = str(error)
        raise
    finally:
        value['end_utc'] = r.now()
        r.write(Path(receipt), value)

def accepted_outcome(value):
    r.require(value['success'] is True and 'error' not in value, 'controller exchange veto')
    return value['body']

def outcomes(directory, profiles):
    c = __import__('ycsb_compare_validate')
    for action in ('start', 'stop'):
        outcome = c.read(Path(directory) / ('controller.' + action + '.json'))
        accepted_outcome(outcome)
        r.require(c.same(outcome['body'], c.read(Path(profiles) / ('phase.' + action + '.json'))), 'controller/physical receipt mismatch')

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('socket'); parser.add_argument('target'); parser.add_argument('receipt')
    args=parser.parse_args()
    checked(args.socket,args.target,args.receipt)
