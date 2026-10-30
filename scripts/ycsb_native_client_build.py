#!/usr/bin/env python3
"""Controlled future native client build; receipts are non-adversarial custody, not attestation."""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import shutil

from ycsb_evidence_json import read, same

PRODUCER='3c8148495b8d4490cf0ec30e9eb92d14a7a71bac'
WIRE='db/treedbnative/nativewire.go'
WIRE_SHA='b09334ad0f53ea49ca553953b123a2617625c04a766dbed624e4058c2399f787'
EVIDENCE=('request.json','source.patch','source-before.json','source-after.json','go-version.out','go-env.out','modules.out','packages.out','dependency-inputs-before.json','dependency-inputs-after.json','module-inputs-before.json','module-inputs-after.json','toolchain-before.json','toolchain-after.json','cc-version.out','invocation.json','build.out','build.err','buildinfo.txt','go-version.err','go-env.err','modules.err','packages.err','cc-version.err','buildinfo.err')
LIMITATIONS=['Trusted launcher, Go toolchain and C compiler; no adversarial attestation or continuous input immutability proof.', 'Package-listed source/embed files, module metadata and GOROOT are hashed; transitive system C headers/libraries, linker inputs and kernel are not fully inventoried.','Recorded environment is the exact mapping supplied to exec, not an independent procfs observation.']

def require(condition,message):
    if not condition:raise ValueError(message)

def sha(data):return hashlib.sha256(data).hexdigest()
def digest(path):return sha(pathlib.Path(path).read_bytes())
def save(path,value):
    with pathlib.Path(path).open('x') as stream:json.dump(value,stream,indent=2,allow_nan=False);stream.write('\n')
def absolute(path):
    p=pathlib.Path(path);require(p.is_absolute() and p.exists(),'absolute existing input required');return p.resolve()
def records(raw):
    decoder=json.JSONDecoder();text=raw.decode();out=[]
    while text.strip():
        text=text.lstrip();value,end=decoder.raw_decode(text);out.append(value);text=text[end:]
    return out

def inventory(paths):
    result={}
    for path in sorted(set(map(str,paths))):
        p=pathlib.Path(path);require(p.is_file() and not p.is_symlink(),'nonregular source input: '+path);result[path]=digest(p)
    return result

def tree(root):
    return inventory(p for p in pathlib.Path(root).rglob('*') if p.is_file() or p.is_symlink())

def source(repo):
    git_env=dict(PATH='/usr/bin:/bin',LC_ALL='C',GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL='/dev/null',GIT_NO_REPLACE_OBJECTS='1')
    git=lambda *a:subprocess.check_output(['/usr/bin/git','-C',str(repo),*a],env=git_env)
    require(git('rev-parse','HEAD').decode().strip()==PRODUCER,'exact merged producer required')
    require(not git('ls-files','--others','--exclude-standard').strip() and not git('ls-files','--others','--ignored','--exclude-standard').strip(),'untracked or ignored source input')
    require(git('diff','--name-only','HEAD').decode().splitlines()==[WIRE],'only native wire patch allowed')
    require(digest(repo/WIRE)==WIRE_SHA,'unexpected native wire source')
    require(not git('check-attr','-a','--',WIRE).strip(),'wire attributes are not permitted')
    entries=[]
    for entry in git('ls-tree','-rz','HEAD').split(b'\0'):
        if not entry:continue
        metadata,name=entry.split(b'\t',1);mode,kind,oid=metadata.split()
        require(mode in (b'100644',b'100755') and kind==b'blob','nonregular producer tree input')
        entries.append((os.fsdecode(name),oid))
    # Compare actual bytes with immutable producer blobs, independently of index
    # flags, worktree clean filters, or git diff's presentation of the files.
    batch=subprocess.run(['/usr/bin/git','-C',str(repo),'cat-file','--batch'],input=b''.join(oid+b'\n' for _,oid in entries),stdout=subprocess.PIPE,check=True,env=git_env).stdout
    offset=0;files=[]
    for name,oid in entries:
        end=batch.index(b'\n',offset);header=batch[offset:end].split();size=int(header[2]);start=end+1
        require(header[:2]==[oid,b'blob'],'producer blob mismatch')
        content=batch[start:start+size];offset=start+size+1
        path=repo/name;require(path.is_file() and not path.is_symlink(),'nonregular source input')
        if name!=WIRE:require(path.read_bytes()==content,'actual producer source differs: '+name)
        files.append(path)
    return git('diff','--binary','HEAD'),inventory(files)

def dependency_inputs(raw):
    paths=[]
    for package in records(raw):
        require(not package.get('Error') and not package.get('DepsErrors'),'dependency resolution failure')
        directory=pathlib.Path(package['Dir'])
        for key in ('GoFiles','CgoFiles','CFiles','CXXFiles','MFiles','HFiles','FFiles','SFiles','SwigFiles','SwigCXXFiles','SysoFiles','EmbedFiles'):
            paths.extend(directory/name for name in package.get(key,[]))
    require(paths,'empty resolved dependency inputs')
    return inventory(paths)

def build(request,out):
    require(set(request)=={'schema_version','source_dir','go_binary','cc_binary','build_id','source_patch_sha256'} and type(request['schema_version']) is int and request['schema_version']==1,'unsupported request fields')
    require(isinstance(request['build_id'],str) and request['build_id'],'missing build id')
    repo=absolute(request['source_dir']);go=absolute(request['go_binary']);cc=absolute(request['cc_binary'])
    request=dict(request,source_dir=str(repo),go_binary=str(go),cc_binary=str(cc))
    patch,before=source(repo);require(sha(patch)==request['source_patch_sha256'],'source patch mismatch')
    out=pathlib.Path(out);require(out.is_absolute(),'absolute fresh output required');out.mkdir()
    save(out/'request.json',request);(out/'source.patch').write_bytes(patch);save(out/'source-before.json',before)
    for name in ('home','tmp','cache','gopath'): (out/name).mkdir()
    env=dict(PATH='/usr/bin:/bin',HOME=str(out/'home'),TMPDIR=str(out/'tmp'),GOCACHE=str(out/'cache'),GOPATH=str(out/'gopath'),GOENV='off',GOFLAGS='',GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',CC=str(cc),LC_ALL='C',TZ='UTC')
    executable=out/'go-ycsb';argv=[str(go),'build','-p','1','-tags','libsqlite3','-o',str(executable),'./cmd/go-ycsb']
    save(out/'invocation.json',dict(argv=argv,environment=env,cwd=str(repo)))
    def run(args,stem):
        with (out/(stem+'.out')).open('xb') as stdout,(out/(stem+'.err')).open('xb') as stderr:
            subprocess.run(args,cwd=repo,env=env,stdout=stdout,stderr=stderr,check=True)
        return (out/(stem+'.out')).read_bytes()
    try:
        version=run([str(go),'version'],'go-version').decode().strip()
        goenv=json.loads(run([str(go),'env','-json'],'go-env'));goroot=absolute(goenv['GOROOT'])
        run([str(cc),'--version'],'cc-version')
        modules=run([str(go),'list','-m','-json','all'],'modules')
        module_paths=[]
        for module in records(modules):
            require(not module.get('Error'),'module resolution failure')
            if module.get('GoMod'):module_paths.append(module['GoMod'])
            if module.get('Replace',{}).get('GoMod'):module_paths.append(module['Replace']['GoMod'])
        module_inputs=inventory(module_paths);save(out/'module-inputs-before.json',module_inputs)
        packages=run([str(go),'list','-deps','-json','-tags','libsqlite3','./cmd/go-ycsb'],'packages')
        dependencies=dependency_inputs(packages);save(out/'dependency-inputs-before.json',dependencies)
        tools=tree(goroot);save(out/'toolchain-before.json',tools)
        identities={str(go):digest(go),str(cc):digest(cc)}
        run(argv,'build')
        info=run([str(go),'version','-m',str(executable)],'buildinfo');require(info.strip(),'empty build info')
        (out/'buildinfo.txt').write_bytes(info)
        after_patch,after=source(repo);save(out/'source-after.json',after)
        deps_after=inventory(dependencies);save(out/'dependency-inputs-after.json',deps_after)
        module_after=inventory(module_inputs);save(out/'module-inputs-after.json',module_after)
        require(module_inputs==module_after,'module inputs changed')
        tools_after=tree(goroot);save(out/'toolchain-after.json',tools_after)
        require(patch==after_patch and before==after and dependencies==deps_after and tools==tools_after,'build inputs changed')
        require(all(digest(path)==value for path,value in identities.items()),'compiler changed')
        receipt=dict(schema_version=1,kind='controlled_native_client_build_v1',producer_commit=PRODUCER,source_commit=PRODUCER,source_patch_sha256=sha(patch),build_id=request['build_id'],exe_sha256=digest(executable),argv=argv,environment=env,cwd=str(repo),toolchain=version,compiler_hashes=identities,limitations=LIMITATIONS,evidence={name:digest(out/name) for name in EVIDENCE})
        save(out/'receipt.json',receipt)
        manifest={key:receipt[key] for key in ('schema_version','producer_commit','source_commit','source_patch_sha256','build_id','exe_sha256','toolchain')}
        manifest.update(compatibility_configuration='tracked native wire minor 1; merged original Close unchanged; fixed controlled build without compiler overlay',controlled_build=dict(schema_version=1,receipt_sha256=digest(out/'receipt.json')))
        save(out/'client-build.json',manifest)
        validate(out,manifest)
    except BaseException as exc:
        save(out/'FAILED.json',dict(schema_version=1,error=repr(exc)));raise

def validate(packet,manifest):
    packet=pathlib.Path(packet);require(not (packet/'FAILED.json').exists(),'failed controlled build')
    binding=manifest.get('controlled_build',{})
    require(type(binding.get('schema_version')) is int and binding['schema_version']==1 and binding.get('receipt_sha256')==digest(packet/'receipt.json'),'controlled build receipt mismatch')
    receipt=read(packet/'receipt.json')
    require(receipt.get('kind')=='controlled_native_client_build_v1' and type(receipt.get('schema_version')) is int and receipt['schema_version']==1,'unsupported controlled build receipt')
    require(all(same(receipt.get(k),manifest.get(k)) for k in ('producer_commit','source_commit','source_patch_sha256','build_id','exe_sha256','toolchain')),'controlled build manifest mismatch')
    require(receipt['producer_commit']==PRODUCER and receipt['source_commit']==PRODUCER,'controlled producer mismatch')
    require(set(receipt['evidence'])==set(EVIDENCE) and all(digest(packet/name)==value for name,value in receipt['evidence'].items()),'controlled build evidence mismatch')
    request=read(packet/'request.json');env=receipt['environment'];argv=receipt['argv']
    require(argv==[request['go_binary'],'build','-p','1','-tags','libsqlite3','-o',argv[-2],'./cmd/go-ycsb'] and pathlib.Path(argv[-2]).is_absolute(),'unexpected controlled build argv')
    require(env.get('GOENV')=='off' and env.get('GOFLAGS')=='' and env.get('GOWORK')=='off' and env.get('GOTOOLCHAIN')=='local' and env.get('CC')==request['cc_binary'],'unsafe controlled build environment')
    require(set(env)=={'PATH','HOME','TMPDIR','GOCACHE','GOPATH','GOENV','GOFLAGS','GOWORK','GOTOOLCHAIN','CGO_ENABLED','CC','LC_ALL','TZ'} and env['PATH']=='/usr/bin:/bin' and env['CGO_ENABLED']=='1','unexpected compiler environment')
    require(same(read(packet/'invocation.json'),dict(argv=argv,environment=env,cwd=receipt['cwd'])),'invocation receipt mismatch')
    require(receipt['toolchain']==(packet/'go-version.out').read_text().strip(),'toolchain version mismatch')
    require(receipt['limitations']==LIMITATIONS,'missing provenance limitations')
    require(digest(packet/'source.patch')==manifest['source_patch_sha256'],'controlled source patch mismatch')
    for name in ('source','dependency-inputs','module-inputs','toolchain'):
        require(same(read(packet/(name+'-before.json')),read(packet/(name+'-after.json'))),'controlled input snapshot mismatch')
    return receipt

def retain(packet,out,manifest):
    validate(packet,manifest)
    destination=pathlib.Path(out)/'native-client-build';destination.mkdir()
    for name in (*EVIDENCE,'receipt.json'):
        with (pathlib.Path(packet)/name).open('rb') as source_file,(destination/name).open('xb') as target:
            shutil.copyfileobj(source_file,target)
    validate(destination,manifest)

def comparison(out,manifest):
    required=manifest.get('controlled_native_client_build_required',False)
    require(type(required) is bool,'invalid controlled native build requirement')
    if required:
        require('treedb_native' in manifest['targets'] and 'controlled_build' in manifest['client'],'controlled native client build required')
    if 'controlled_build' in manifest['client']:
        validate(pathlib.Path(out)/'native-client-build',manifest['client'])

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('request',type=pathlib.Path);parser.add_argument('out',type=pathlib.Path);args=parser.parse_args();build(read(args.request),args.out)
