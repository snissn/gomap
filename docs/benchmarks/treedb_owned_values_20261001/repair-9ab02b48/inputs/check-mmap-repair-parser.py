"""Synthetic parser validation only: all numbers are fabricated, never measurements."""
from pathlib import Path
import ast
import runpy
import subprocess
import sys
root=Path('tmp/mmap-parser-synthetic');root.mkdir(exist_ok=True)
p=Path('tmp/analyze-mmap-repair.py');ast.parse(p.read_text());scope=runpy.run_path(str(p))
rows=scope['ROWS'];denied=scope['DENIED']
for rev,blocks in rows.items():
 for block,names in blocks.items():
  for rep in range(1,6):
   lines=['SYNTHETIC PARSER VALIDATION ONLY: FABRICATED DATA']
   for name in sorted(names):
    m={'B/op':0,'allocs/op':0}
    if block=='public':m.update({'pointer_hits/op':1,'inline_hits/op':0,'crc_checks/op':1,'cache_hits/op':1,'fallbacks/op':int(rev=='base'),'mmap_hits/op':int(rev=='candidate'),'retained_raw_B':9389893,'raw_budget_B':67108864})
    if name in denied:m.update({'fallbacks/op':1,'crc_checks/op':1,'cache_hits/op':1,'retained_raw_B':8192,'denial_events':1,'B/op':0 if name.endswith('/reused_true') else 512,'allocs/op':0 if name.endswith('/reused_true') else 1})
    if name.endswith('ConcurrentFirstAdmission'):m.update({'mmap_hits/op':2,'crc_checks/op':2,'fallbacks/op':0})
    if name.endswith('/mmap_decode'):m.update({'fallbacks/op':0,'crc_checks/op':1,'cache_hits/op':0,'retained_raw_B':0})
    if name.endswith('/cold_open_map'):m.update({'mmap_hits/op':int(rev!='base'),'fallbacks/op':int(rev=='base'),'cache_stores/op':1})
    # Separate name/number lines imitate interleaved constructor output.
    lines.extend([name+'-4', '100 1000 ns/op '+ ' '.join(f'{v} {k}' for k,v in m.items())])
   (root/f'repair-{block}-{rev}-{rep}.txt').write_text('\n'.join(lines)+'\n')
   (root/f'repair-{block}-{rev}-{rep}.stderr.txt').write_text('SYNTHETIC stderr only\n')
   (root/f'repair-{block}-{rev}-{rep}.rss.txt').write_text('Maximum resident set size (kbytes): 1000\nExit status: 0\n')
for phase in ['before','after']:(root/f'repair-freeze-{phase}.txt').write_text('SYNTHETIC IDENTICAL FREEZE\n')
r=subprocess.run([sys.executable,str(p),str(root)],capture_output=True,text=True);assert r.returncode==0,r.stderr;print('SYNTHETIC valid complete matrix:',r.stdout.strip())
q=root/'repair-public-candidate-1.txt';original=q.read_text()
negative={
 'wrong transport':original.replace('1 mmap_hits/op','0 mmap_hits/op'),
 'unexpected benchmark':original.replace('BenchmarkDBOwnedValueLogRoute-4','BenchmarkWrong-4'),
 'duplicate row':original+original,
 'wrong CPU suffix':original.replace('-4','-2'),
 'nonfinite time':original.replace('1000 ns/op','1e999 ns/op'),
}
for name,contents in negative.items():
 q.write_text(contents)
 r=subprocess.run([sys.executable,str(p),str(root),'--cell','candidate','public','1'],capture_output=True,text=True)
 assert r.returncode!=0,name
 print('SYNTHETIC rejected:',name)
q.write_text(original)
q=root/'repair-overlay-base-1.txt';original=q.read_text();q.write_text(original.replace('512 B/op','1024 B/op'))
r=subprocess.run([sys.executable,str(p),str(root),'--cell','base','overlay','1'],capture_output=True,text=True);assert r.returncode!=0;print('SYNTHETIC rejected: redundant owned allocation');q.write_text(original)
q=root/'repair-internal-candidate-1.rss.txt';original=q.read_text();q.write_text(original.replace('Exit status: 0','Exit status: 1'))
r=subprocess.run([sys.executable,str(p),str(root),'--cell','candidate','internal','1'],capture_output=True,text=True);assert r.returncode!=0;print('SYNTHETIC rejected: process failure');q.write_text(original)
q=root/'repair-internal-candidate-1.rss.txt';original=q.read_text();q.write_text(original.replace('1000','0'))
r=subprocess.run([sys.executable,str(p),str(root),'--cell','candidate','internal','1'],capture_output=True,text=True);assert r.returncode!=0;print('SYNTHETIC rejected: zero RSS');q.write_text(original)
print('All parser-only checks PASS. No runtime/performance evidence.')
