#!/usr/bin/env python3
"""Authenticate a loopback TCP listener to a pinned, launched server process."""
import argparse
import ipaddress
import json
import os
import pathlib
import sys
import time

import ycsb_compare_validate as c


def listener(pid, address, expected, pending=False):
    host, sep, port_text = address.rpartition(':')
    c.require(sep and host and port_text.isdigit(), 'invalid listener address')
    if host == 'localhost':
        host = '127.0.0.1'
    ip = ipaddress.IPv4Address(host)
    port = int(port_text)
    c.require(ip.is_loopback and 0 < port < 65536, 'listener must be explicit IPv4 loopback')
    identity = {key: expected[key] for key in ('pid', 'start_ticks', 'exe_sha256')}
    c.require(pid == identity['pid'] and c.same(c.observe_core(pid), identity), 'listener process changed before inspection')
    local = ip.packed[::-1].hex().upper() + ':' + format(port, '04X')
    rows = pathlib.Path('/proc', str(pid), 'net/tcp').read_text().splitlines()[1:]
    matches = [fields[9] for fields in (row.split() for row in rows)
               if len(fields) > 9 and fields[1] == local and fields[3] == '0A']
    if not matches and pending:
        c.require(c.same(c.observe_core(pid), identity), 'listener process changed while awaiting bind')
        return None
    c.require(len(matches) == 1, 'missing or ambiguous loopback listener')
    fds = set()
    for path in pathlib.Path('/proc', str(pid), 'fd').iterdir():
        try:
            fds.add(os.readlink(path))
        except FileNotFoundError:
            continue
    c.require('socket:[' + matches[0] + ']' in fds, 'listener is not owned by exact server')
    c.require(c.same(c.observe_core(pid), identity), 'listener process changed during inspection')
    return matches[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('launch', type=pathlib.Path)
    parser.add_argument('address')
    parser.add_argument('receipt', type=pathlib.Path)
    parser.add_argument('--timeout', type=float, default=60)
    parser.add_argument('--verify', action='store_true')
    args = parser.parse_args()
    try:
        c.require(0 < args.timeout <= 3600, 'invalid readiness timeout')
        identity = c.read(args.launch)['process']
        if args.verify:
            previous = c.read(args.receipt)
            c.require(c.same(previous['process'], identity) and previous['address'] == args.address, 'listener receipt binding changed')
            c.require(listener(identity['pid'], args.address, identity) == previous['inode'], 'listener inode changed')
            return 0
        deadline = time.monotonic() + args.timeout
        while True:
            inode = listener(identity['pid'], args.address, identity, pending=True)
            if inode is not None:
                c.write_new(args.receipt, {'schema_version': 1, 'address': args.address, 'inode': inode, 'process': identity})
                return 0
            c.require(time.monotonic() < deadline, 'timed out waiting for owned listener')
            time.sleep(min(.1, max(0, deadline-time.monotonic())))
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print('invalid server listener: ' + str(exc), file=sys.stderr)
        return 3


if __name__ == '__main__':
    sys.exit(main())
