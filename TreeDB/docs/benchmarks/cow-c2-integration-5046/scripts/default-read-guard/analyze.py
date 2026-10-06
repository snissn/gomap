"""Fail-closed complete guard analysis; every sample and paired spread retained."""
import argparse
import json
import statistics
from contract import COMMITS, FIXTURES, ORDERS, METRICS, CASES, OPERATIONS, PATTERN, sha, write, identity, validate_run


def main():
    p=argparse.ArgumentParser();p.add_argument('packet');args=p.parse_args()
    from pathlib import Path
    packet=Path(args.packet)
    ids={r:identity(packet/(r+'-source-identity.json'),r) for r in COMMITS}
    policy=json.loads((packet/'policy.json').read_text())
    if policy['commits']!=COMMITS or policy['fixture_hashes']!=FIXTURES or policy['orders']!=[list(o) for o in ORDERS] or policy['operations']!=OPERATIONS or policy['GOMAXPROCS']!=12 or policy['TREEDB_BENCH_SHARDS']!=2:
        raise ValueError('unexpected guard policy')
    binaries={}
    for role in COMMITS:
        binaries[role]=json.loads((packet/role/'binary.json').read_text())
        binary=packet/role/'default-read.test'
        if binary.exists() and (sha(binary)!=binaries[role]['sha256'] or binary.stat().st_size!=binaries[role]['bytes']):
            raise ValueError('private binary binding differs')
    for name in ('hydration.json','build-source-drift.json'):
        data=json.loads((packet/name).read_text());d=data['drift'] if name=='hydration.json' else data
        if set(d)!=set(COMMITS) or any(d.values()):raise ValueError('source drift/missing role')
    completion=json.loads((packet/'completion.json').read_text())
    if completion['runs']!=6 or set(completion['source_drift'])!=set(COMMITS) or any(completion['source_drift'].values()):raise ValueError('incomplete/drifted completion')
    receipts=json.loads((packet/'receipts.json').read_text())
    labels=[f'r{i}-{r}' for i,o in enumerate(ORDERS,1) for r in o]
    if len(receipts)!=6 or [r['label'] for r in receipts]!=labels:raise ValueError('wrong interleaving, extra or missing runs')
    rows=[]
    for receipt in receipts:
        role=receipt['role'];label=receipt['label']
        if role not in COMMITS or label!=f'r{receipt["repeat"]}-{role}':raise ValueError('label/role/repeat mismatch')
        expected_flags=['-test.run=^$','-test.bench='+PATTERN,'-test.benchmem','-test.benchtime=100000x','-test.count=1','-test.timeout=600s']
        if receipt['command'][1:]!=expected_flags or Path(receipt['command'][0]).name!='default-read.test':
            raise ValueError('changed command/instrumentation')
        if receipt['binary_sha256']!=binaries[role]['sha256'] or receipt['goenv_sha256']!=sha(packet/role/'goenv.json'):
            raise ValueError('binary/environment receipt binding differs')
        if role not in COMMITS or receipt['commit']!=COMMITS[role] or receipt['source_tree_sha256']!=ids[role]['source_tree_sha256']:
            raise ValueError('receipt source differs')
        if receipt['exit_code'] or receipt['validation_error'] or receipt['case_validation']!={'rows':3,'exact_case_set':True}:
            raise ValueError('failed/unvalidated run')
        for field in ('source_drift_before','source_drift_after'):
            if set(receipt[field])!=set(COMMITS) or any(receipt[field].values()):raise ValueError('source drift/missing role')
        stdout,stderr=packet/(label+'.log'),packet/(label+'.stderr.log')
        if sha(stdout)!=receipt['log_sha256'] or sha(stderr)!=receipt['stderr_sha256']:raise ValueError('raw stream hash differs')
        for row in validate_run(stdout,stderr):
            rows.append(dict(row,role=role,repeat=receipt['repeat'],run=label))
    if len(rows)!=18:raise ValueError('wrong complete row count')
    def spread(values):return {'samples':values,'min':min(values),'median':statistics.median(values),'max':max(values)}
    summary=[];ratios=[]
    for case in CASES:
        for role in COMMITS:
            selected=sorted((r for r in rows if r['case']==case and r['role']==role),key=lambda r:r['repeat'])
            if [r['repeat'] for r in selected]!=[1,2,3]:raise ValueError('repeat mismatch')
            summary.append({'case':case,'role':role,'metrics':{m:spread([r['metrics'][m] for r in selected]) for m in METRICS}})
        metrics={}
        for m in METRICS:
            pairs=[]
            for repeat in (1,2,3):
                baseline=next(r['metrics'][m] for r in rows if r['case']==case and r['role']=='main' and r['repeat']==repeat)
                candidate=next(r['metrics'][m] for r in rows if r['case']==case and r['role']=='candidate' and r['repeat']==repeat)
                pairs.append({'repeat':repeat,'main':baseline,'candidate':candidate,'delta':candidate-baseline,
                              'ratio':candidate/baseline if baseline else None,
                              'ratio_qualification':None if baseline else 'zero baseline; ratio undefined'})
            finite=[p['ratio'] for p in pairs if p['ratio'] is not None]
            metrics[m]={'paired':pairs,'ratio_spread':spread(finite) if finite else None}
        ratios.append({'case':case,'metrics':metrics})
    write(packet/'rows.json',rows);write(packet/'matched-summary.json',summary);write(packet/'candidate-over-main.json',ratios)
    write(packet/'analysis-validation.json',{'raw_runs':6,'rows':18,'matched_cases':6,'paired_cases':3,
        'raw_hashes_verified':True,'stderr_hashes_verified':True,'exact_interleaving':True,
        'commits':COMMITS,'source_tree_sha256':{r:ids[r]['source_tree_sha256'] for r in COMMITS},
        'limitations':'three descriptive paired repeats; fixture final Close errors ignored; no persistence/drain/default-writer/all-profile/sustained claims'})
    print(json.dumps({'rows':18,'matched_cases':6,'paired_cases':3}))


if __name__=='__main__':main()
