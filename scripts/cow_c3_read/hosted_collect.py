"""Explicit cooperative pre-child adapter; canonical collect.py stays byte-identical.

The admission deadline never signals an active benchmark. Existing canonical
leaf timeouts/cancellation remain distinct, with their original monitor proof.
"""
import argparse
import json
from pathlib import Path
import sys
import time

from hosted_contract import accepted_window, admission, root_acceptance
from hosted_runner import json_read, state_read
from hosted_custody import durable, monitored_release
from protocol import digest, need, now, sha, write
import collect


def guarded_child(original, state, records, ledger, clock=time.monotonic):
    def call(*args, **kwargs):
        event=dict(at=now(),monotonic=clock(),argv_sha256=digest(args[0]),
                   deadline_monotonic=state['admission_deadline_monotonic'],decision='REFUSED')
        try:
            admission(state,clock=clock)
            event['decision']='ADMITTED_BEFORE_CHILD'
        finally:
            records.append(event)
            write(ledger,dict(adapter_sha256=sha(Path(__file__)),events=records,
                              claim='Pre-child observations only; never a termination timer'))
        return original(*args,**kwargs)
    return call


def main():
    p=argparse.ArgumentParser();p.add_argument('--state',type=Path,required=True)
    p.add_argument('--state-sha256',required=True);p.add_argument('--config',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True);args=p.parse_args()
    state=state_read(args);n=Path(state['namespace'])
    root=root_acceptance(json_read(n/'root-acceptance.json')['api_comment'],state,json_read(n/'construction-result.json'))
    need(args.config==n/'evidence/construction/config.json' and sha(args.config)==root['config_sha256'], 'adapter config authority mismatch')
    need(args.out==n/'evidence/matched' and not args.out.exists(),'new full matched namespace required')
    state=accepted_window(state,root)
    records=[];ledger=n/'evidence/matched-driver/deadline-hooks.json'
    collect.run_child=guarded_child(collect.run_child,state,records,ledger)
    sys.argv=[str(Path(collect.__file__)),'--config',str(args.config),'--out',str(args.out)]
    error=None
    try:
        collect.main()
    except BaseException as exc:
        error=dict(type=type(exc).__name__,error=str(exc));raise
    finally:
        durable(n/'evidence/matched-driver/adapter-closure.json',dict(at=now(),
            adapter_sha256=sha(Path(__file__)),canonical_collect_sha256=sha(Path(collect.__file__)),
            config_sha256=sha(args.config),events=len(records),error=error,
            descendant_release_proven=monitored_release(args.out),
            expiry_signals_sent=0,scope='Explicit pre-child admission adapter; unchanged canonical collection'))


if __name__=='__main__':main()
