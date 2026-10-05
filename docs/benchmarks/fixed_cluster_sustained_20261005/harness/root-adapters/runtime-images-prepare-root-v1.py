"""Root-owned Trial24 image packaging adapter; inert on import.

Reusable construction only. No final pins are embedded. Root must supply the
reviewed final build proof and exact source identities after source lands:

python3 THIS_FILE --head FINAL40 --tree FINAL40 --build-proof ABS_JSON \
 --build-proof-sha256 SHA64 --source-inventory-sha256 SHA64 \
 --build-root-name gomap-5021-sustained-runtime-build-root-vN \
 --root-name gomap-5021-sustained-runtime-images-root-vN \
 --parent-111 sha256:SHA64 --parent-185 sha256:SHA64 --go-version go1.26.0 \
 --foreign-185-cid CID64 --foreign-185-image sha256:SHA64

CLI execution performs bounded sequential packaging on 111 then 185. It does
not build Go, start a cluster, mount stores or claim runtime qualification.
Preserve failed roots/raw results; there is no automatic replay or cleanup.
"""
import argparse, hashlib, json, pathlib, re, shlex, subprocess, time

def main(argv=None):
    if not __debug__:
        raise RuntimeError('ordinary Python required; assertions are fail-closed guards')
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('head','tree','build-proof','build-proof-sha256','source-inventory-sha256','build-root-name','root-name','parent-111','parent-185','go-version','foreign-185-cid','foreign-185-image'):
        parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv)
    assert re.fullmatch('[0-9a-f]{40}',args.head) and re.fullmatch('[0-9a-f]{40}',args.tree)
    assert all(re.fullmatch('[0-9a-f]{64}',x) for x in (args.build_proof_sha256,args.source_inventory_sha256))
    assert re.fullmatch('gomap-5021-sustained-runtime-build-root-v[1-9][0-9]*',args.build_root_name)
    assert re.fullmatch('gomap-5021-sustained-runtime-images-root-v[1-9][0-9]*',args.root_name)
    assert re.fullmatch(r'go1\.[0-9]+\.[0-9]+',args.go_version)
    assert re.fullmatch('[0-9a-f]{64}',args.foreign_185_cid)
    assert re.fullmatch('sha256:[0-9a-f]{64}',args.foreign_185_image)
    FOREIGN={'111':{},'185':{args.foreign_185_cid:{'name':'/lotus-miner','image':args.foreign_185_image}}}
    PARENTS={'111':args.parent_111,'185':args.parent_185}
    assert all(re.fullmatch('sha256:[0-9a-f]{64}',x) for x in PARENTS.values())
    LOCAL=pathlib.Path('/tmp')/args.root_name
    REMOTE='/home/mikers/'+args.root_name
    BUILD='/home/mikers/'+args.build_root_name+'/receipts'
    TAG='gomap-5021:rf4trial24sustainedc1-'+args.root_name.removeprefix('gomap-5021-sustained-')
    PARENT_TAG=TAG+'-parent'
    proof_path=pathlib.Path(args.build_proof)
    assert proof_path.is_absolute() and proof_path.is_file() and not proof_path.is_symlink() and proof_path.stat().st_size<=4*1024*1024
    proof_raw=proof_path.read_bytes();assert hashlib.sha256(proof_raw).hexdigest()==args.build_proof_sha256
    proof=json.loads(proof_raw)
    assert proof['state']=='VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME' and proof['source_verified_before_after'] is True
    assert proof['head']==args.head and proof['tree']==args.tree and proof['source_inventory_sha256']==args.source_inventory_sha256
    assert proof['source']=='/home/mikers/'+args.build_root_name+'/source'
    inv_path=proof_path.parent/'git-source-inventory.json'
    assert inv_path.is_file() and not inv_path.is_symlink() and inv_path.stat().st_size<=8*1024*1024
    inv_raw=inv_path.read_bytes();assert hashlib.sha256(inv_raw).hexdigest()==args.source_inventory_sha256
    inventory=json.loads(inv_raw)
    assert inventory['head']==args.head and inventory['tree']==args.tree and len(inventory['rows'])==proof['tracked_files']
    assert set(proof['ELFs'])=={'treedb-fixed-peer','treedb-query-under-write'}
    ELFS={}
    for name,v in proof['ELFs'].items():
        assert v['path']==BUILD+'/'+name and type(v['bytes']) is int and 0<v['bytes']<=256*1024*1024
        assert re.fullmatch('[0-9a-f]{64}',v['sha256'])
        m=v['metadata'];assert set(m)=={'file','ldd','go-version-m'}
        assert m['file']['exit']==0 and 'ELF 64-bit LSB' in m['file']['stdout'] and 'x86-64' in m['file']['stdout']
        assert m['go-version-m']['exit']==0
        go=m['go-version-m']['stdout']
        assert go.splitlines()[0].endswith(': '+args.go_version)
        assert '\tpath\tgithub.com/snissn/gomap/cmd/'+name in go and '\tbuild\tGOARCH=amd64' in go and '\tbuild\tGOOS=linux' in go
        assert m['ldd']['exit']==0 or (m['ldd']['exit']==1 and ('not a dynamic executable' in m['ldd']['stderr'] or 'statically linked' in m['ldd']['stdout']))
        ELFS[name]={'sha256':v['sha256'],'bytes':v['bytes']}
    assert not LOCAL.exists() and not LOCAL.is_symlink()
    LOCAL.mkdir(exist_ok=False)
    (LOCAL/'build-proof.json').write_bytes(proof_raw)
    (LOCAL/'git-source-inventory.json').write_bytes(inv_raw)
    (LOCAL/'preparation-inputs.json').write_text(json.dumps({'head':args.head,'tree':args.tree,'build_proof_sha256':args.build_proof_sha256,'source_inventory_sha256':args.source_inventory_sha256,'build_root_name':args.build_root_name,'image_root_name':args.root_name,'parents':PARENTS,'protected_foreign':FOREIGN,'expected_go_version':args.go_version,'adapter_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()},indent=2)+'\n')

    def ssh(host,program):
        return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.'+host,shlex.join(['python3','-c',program])]

    def call(host,name,program,timeout=60):
        t=time.time();r=subprocess.run(ssh(host,program),capture_output=True,timeout=timeout)
        (LOCAL/(host+'-'+name+'.stdout')).write_bytes(r.stdout);(LOCAL/(host+'-'+name+'.stderr')).write_bytes(r.stderr)
        (LOCAL/(host+'-'+name+'.exit.json')).write_text(json.dumps({'exit':r.returncode,'started_unix':t,'finished_unix':time.time()})+'\n')
        r.check_returncode();return r.stdout

    for host in ('111','185'):
        p='import hashlib,json,os,pathlib,shutil,subprocess,sys,tarfile,time\nROOT='+repr(REMOTE)+'\nELFS='+repr(ELFS)+'\nPARENT='+repr(PARENTS[host])+'\nHOST='+repr(host)+'\nFOREIGN='+repr(FOREIGN[host])+'\nTAG='+repr(TAG)+'\nPARENT_TAG='+repr(PARENT_TAG)+'\n'
        call(host,'prepare',p+'''
root=pathlib.Path(ROOT);assert not root.exists()
disk=os.statvfs('/home/mikers');assert disk.f_bavail*disk.f_frsize>=50*1024**3
assert os.getloadavg()[0]<4
mem={l.split(':')[0]:int(l.split()[1])*1024 for l in pathlib.Path('/proc/meminfo').read_text().splitlines() if ':' in l}
assert mem['MemAvailable']>=16*1024**3
assert not subprocess.run(['pgrep','-af',r'(^|/)(go|compile|link|[^ ]+\\.test)( |$)'],capture_output=True,text=True).stdout.strip()
running=set(subprocess.check_output(['docker','ps','--no-trunc','-q'],text=True).split())
assert running<=set(FOREIGN),running
for cid in running:
 c=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
 assert c['Id']==cid and c['Name']==FOREIGN[cid]['name'] and c['Image']==FOREIGN[cid]['image']
 assert c['State']['Restarting'] is True and c['State']['Pid']==0 and c['State']['OOMKilled'] is False
print(json.dumps({'protected_foreign_running_cids':sorted(running)}))
root.mkdir();(root/'context').mkdir()
''')
        if host=='111':
            call(host,'copy',p+'BUILD='+repr(BUILD)+'\n'+'''
for name,meta in ELFS.items():
 src=pathlib.Path(BUILD)/name;assert src.is_file() and not src.is_symlink() and src.stat().st_mode&0o7777 in (0o755,0o775)
 raw=src.read_bytes();assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
 dst=pathlib.Path(ROOT)/'context'/name;subprocess.run(['cp','--reflink=auto',str(src),str(dst)],check=True);dst.chmod(0o755)
 assert hashlib.sha256(dst.read_bytes()).hexdigest()==meta['sha256']
print('VERIFIED_LOCAL_CONTEXT_ELFS')
''')
        else:
            sender='import hashlib,pathlib,sys,tarfile\nELFS='+repr(ELFS)+'\nBUILD='+repr(BUILD)+'\n'+'''
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|') as tar:
 for name,meta in ELFS.items():
  src=pathlib.Path(BUILD)/name;assert src.is_file() and not src.is_symlink() and src.stat().st_mode&0o7777 in (0o755,0o775)
  raw=src.read_bytes();assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
  tar.add(str(src),arcname=name,recursive=False)
'''
            receiver=p+'''
seen=set()
with tarfile.open(fileobj=sys.stdin.buffer,mode='r|') as tar:
 for member in tar:
  assert member.name in ELFS and member.name not in seen and member.isfile()
  meta=ELFS[member.name];assert member.size==meta['bytes']
  raw=tar.extractfile(member).read(meta['bytes']+1);assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
  dest=pathlib.Path(ROOT)/'context'/member.name
  with dest.open('xb') as f:f.write(raw)
  dest.chmod(0o755);seen.add(member.name)
assert seen==set(ELFS);print('VERIFIED_STREAMED_CONTEXT_ELFS')
'''
            with (LOCAL/'stream-source.stderr').open('xb') as err:
                src=subprocess.Popen(ssh('111',sender),stdout=subprocess.PIPE,stderr=err)
                try:
                    dst=subprocess.run(ssh('185',receiver),stdin=src.stdout,capture_output=True,timeout=90)
                    src.stdout.close();rc=src.wait(timeout=15)
                except BaseException:
                    src.stdout.close();src.terminate();src.wait(timeout=15);raise
            (LOCAL/'stream-exit.json').write_text(json.dumps({'source_exit':rc,'destination_exit':dst.returncode})+'\n')
            (LOCAL/'stream-destination.stdout').write_bytes(dst.stdout);(LOCAL/'stream-destination.stderr').write_bytes(dst.stderr)
            assert rc==0,rc;dst.check_returncode()
        package=p+'HEAD='+repr(proof['head'])+'\nTREE='+repr(proof['tree'])+'\nINVSHA='+repr(proof['source_inventory_sha256'])+'\n'+'''
root=pathlib.Path(ROOT);seq=0
def call(label,argv,timeout=60):
 global seq
 seq+=1;t=time.time();r=subprocess.run(argv,capture_output=True,timeout=timeout);name=f'{seq:02d}-{label}'
 (root/(name+'.stdout')).write_bytes(r.stdout);(root/(name+'.stderr')).write_bytes(r.stderr)
 (root/(name+'.json')).write_text(json.dumps({'argv':argv,'exit':r.returncode,'started_unix':t,'finished_unix':time.time()})+'\\n')
 return r
def inspect(image):
 r=call('inspect',['docker','image','inspect',image]);r.check_returncode();x=json.loads(r.stdout);assert len(x)==1;return x[0]
before=inspect(PARENT);assert before['Id']==PARENT
assert before['Config'].get('OnBuild') in (None,[])
tag=TAG;alias=PARENT_TAG
for image in (tag,alias):
 r=call('exclusive-tag',['docker','image','inspect',image]);assert r.returncode!=0 and b'No such image' in r.stderr
r=call('local-parent-alias',['docker','tag',PARENT,alias]);r.check_returncode();assert inspect(alias)['Id']==PARENT
context=root/'context'
for name,meta in ELFS.items():
 f=context/name;assert f.is_file() and not f.is_symlink() and f.stat().st_mode&0o777==0o755
 assert f.stat().st_size==meta['bytes'] and hashlib.sha256(f.read_bytes()).hexdigest()==meta['sha256']
dockerfile='FROM '+alias+'\\nCOPY treedb-fixed-peer /treedb-fixed-peer\\nCOPY treedb-query-under-write /treedb-query-under-write\\n'
(context/'Dockerfile').write_text(dockerfile)
r=call('build',['docker','build','--pull=false','--network=none','-t',tag,str(context)],180);r.check_returncode()
candidate=inspect(tag);after=inspect(PARENT);assert after['Id']==before['Id']==PARENT and before['RootFS']==after['RootFS']
assert candidate['RootFS']['Layers'][:-2]==before['RootFS']['Layers'] and candidate['Id']!=PARENT
receipt={'state':'SOURCE_VERIFIED_ELFS_PACKAGED_NO_CLUSTER_QUALIFICATION','host':'192.168.0.'+HOST,'image':candidate['Id'],'tag':tag,'parent':PARENT,'parent_unchanged':True,'ELFs':ELFS,'head':HEAD,'tree':TREE,'source_inventory_sha256':INVSHA,'image_root':ROOT,'dockerfile_sha256':hashlib.sha256(dockerfile.encode()).hexdigest(),'stores_mounted':False}
(root/'image-proof.json').write_text(json.dumps(receipt,indent=2)+'\\n')
raw={}
for f in sorted(root.iterdir()):
 if f.is_file():
  assert f.stat().st_size<=4*1024*1024
  raw[f.name]={'raw':f.read_text(),'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}
print(json.dumps({'receipt':receipt,'raw_receipts':raw}))
'''
        raw=call(host,'package',package,timeout=240)
        packet=json.loads(raw);(LOCAL/(host+'-image-proof.json')).write_text(json.dumps(packet['receipt'],indent=2)+'\n')
        print(json.dumps(packet['receipt']),flush=True)
    images={host:json.loads((LOCAL/(host+'-image-proof.json')).read_bytes()) for host in ('111','185')}
    (LOCAL/'receipt.json').write_text(json.dumps({'state':'BOTH_HOSTS_PACKAGED_NOT_RUNTIME','head':proof['head'],'tree':proof['tree'],'source_inventory_sha256':proof['source_inventory_sha256'],'build_proof_sha256':args.build_proof_sha256,'adapter_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'images':images},indent=2)+'\n')

if __name__=="__main__":
    main()
