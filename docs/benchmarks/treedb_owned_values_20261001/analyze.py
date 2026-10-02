#!/usr/bin/env python3
"""Parse raw fresh-process Go benchmark rows; retain every sample and metric."""
from pathlib import Path
import json,re,statistics,sys
root=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).parent/'raw'
samples=[]
for p in sorted(root.glob('*.txt')):
    match=re.fullmatch(r'(internal|public-route|public-guard)-(base|candidate)-(\d+)\.txt',p.name)
    if not match: continue
    block,revision,rep=match.groups(); current=None
    for line in p.read_text().splitlines():
        name=re.match(r'^(Benchmark\S+)',line)
        if name: current=name.group(1); line=line[len(current):]
        row=re.match(r'^\s*(\d+)\s+([0-9.eE+-]+)\s+ns/op\s+(.*)',line)
        if not row: continue
        if not current: raise ValueError(f'missing benchmark name: {p}')
        metrics={'ns/op':float(row.group(2))}
        tokens=row.group(3).split()
        if len(tokens)%2: raise ValueError(f'odd metric tokens: {p}: {line}')
        for i in range(0,len(tokens),2): metrics[tokens[i+1]]=float(tokens[i])
        samples.append(dict(block=block,revision=revision,repetition=int(rep),benchmark=current,iterations=int(row.group(1)),metrics=metrics))
assert len(samples)==100, f'expected 5 pairs x10 rows, got {len(samples)}'
for row in samples:
    m=row['metrics']; name=row['benchmark']
    if row['block']=='public-route':
        expected={'pointer_hits/op':1,'inline_hits/op':0,'crc_checks/op':1,'cache_hits/op':1,'fallbacks/op':int(row['revision']=='base'),'mmap_hits/op':int(row['revision']=='candidate')}
        assert all(m[k]==v for k,v in expected.items()),row
        assert m['retained_raw_B']==9389893 and m['raw_budget_B']==67108864,row
    if '/mmap_cache-' in name:
        assert m['retained_raw_B']==262144 and m['cache_hits/op']==1,row
summary={}
for block,name in sorted({(r['block'],r['benchmark']) for r in samples}):
    stats={}
    for revision in ['base','candidate']:
        rows=[r for r in samples if (r['block'],r['benchmark'],r['revision'])==(block,name,revision)]
        assert len(rows)==5,(block,name,revision,len(rows))
        stats[revision]={k:dict(median=statistics.median(r['metrics'][k] for r in rows),min=min(r['metrics'][k] for r in rows),max=max(r['metrics'][k] for r in rows)) for k in rows[0]['metrics']}
    stats['median_time_change_pct']=100*(stats['candidate']['ns/op']['median']/stats['base']['ns/op']['median']-1)
    summary[f'{block}/{name}']=stats
rss={}
for p in sorted(root.glob('*.rss.txt')):
    m=re.search(r'Maximum resident set size \(kbytes\): (\d+)',p.read_text())
    if m: rss[p.name]=int(m.group(1))
assert len(rss)==30,len(rss)
result=dict(samples=samples,summary=summary,setup_inclusive_peak_rss_KiB=rss)
(root.parent/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
for name,stats in summary.items():
    a,c=stats['base'],stats['candidate']
    print(f"{name}: {a['ns/op']['median']:g} -> {c['ns/op']['median']:g} ns/op ({stats['median_time_change_pct']:+.1f}%); {a['B/op']['median']:g} -> {c['B/op']['median']:g} B/op; {a['allocs/op']['median']:g} -> {c['allocs/op']['median']:g} allocs/op")
