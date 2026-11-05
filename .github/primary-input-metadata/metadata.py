"""Fixed hosted read-only PRIMARY admission-input query. Never build a product."""
import hashlib
import importlib.util
import base64
import json
import os
from pathlib import Path
import re
import stat
import shutil
import subprocess
import sys
import datetime
import threading
import time

ROOT_THREAD = '01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
SDK = Path('/opt/hostedtoolcache/go/1.26.10/x64')
CONTEXT = ('GOVERSION','GOOS','GOARCH','GOAMD64','CGO_ENABLED','CC','CXX',
           'CGO_CFLAGS','CGO_CPPFLAGS','CGO_CXXFLAGS','CGO_LDFLAGS',
           'GOFLAGS','GOEXPERIMENT','GOROOT')
started = time.monotonic()
read_bytes = 0
commands = []
inputs = {}
output = Path(os.environ['RUNNER_TEMP'])/'primary-construction-metadata'
COMMON = Path('/home/runner/work')


def census(reserve=0):
    # Fixed stage-boundary observation; the maintained guard separately samples
    # descendant RSS/free space. This is not an instantaneous filesystem quota.
    total = 0
    for directory, dirs, names in os.walk(COMMON, followlinks=False):
        need(all(not (Path(directory)/n).is_symlink() for n in dirs), 'workroot directory symlink')
        for name in names:
            p = Path(directory)/name
            try:
                item = p.lstat()
            except FileNotFoundError:
                continue
            need(stat.S_ISREG(item.st_mode), 'workroot regular files')
            total += item.st_size
    need(total+reserve <= 1024**3, 'metadata whole-workroot bound')
    need(shutil.disk_usage(COMMON).free >= 10*1024**3, 'metadata free-space floor')
    return total


def need(ok, reason):
    if not ok:
        raise RuntimeError(reason)
    if time.monotonic()-started >= 180:
        raise RuntimeError('metadata deadline')


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')


def row(path):
    global read_bytes
    p = Path(path).resolve(strict=True)
    s = p.lstat()
    need(stat.S_ISREG(s.st_mode) and not p.is_symlink(), 'regular read-only input')
    h = hashlib.sha256()
    with p.open('rb') as source:
        for b in iter(lambda: source.read(1 << 20), b''):
            read_bytes += len(b)
            need(read_bytes <= 1024**3, 'metadata read budget')
            h.update(b)
    need(p.stat().st_size == s.st_size and p.stat().st_mtime_ns == s.st_mtime_ns,
         'read input changed')
    return dict(path=str(p),bytes=s.st_size,sha256=h.hexdigest())


def remember(path):
    actual = str(Path(path).resolve(strict=True))
    if actual not in inputs:
        need(len(inputs) < 4096, 'constructor input-row bound')
        inputs[actual] = row(actual)
    return actual


def run(label, argv, env):
    # Fixed standard metadata commands only. No Go build/list/test or product.
    need(len(commands) < 128, 'bounded metadata commands')
    out = output/(label+'.stdout.log')
    err = output/(label+'.stderr.log')
    with out.open('xb') as a, err.open('xb') as b:
        result = subprocess.run(argv,env=env,cwd=output,stdout=a,stderr=b,
                                timeout=min(10,180-(time.monotonic()-started)))
    need(out.stat().st_size <= 1024**2 and err.stat().st_size <= 1024**2,
         'metadata log bound')
    commands.append(dict(label=label,argv=argv,exit=result.returncode,
                         stdout=row(out),stderr=row(err)))
    return result.returncode,out.read_text(),err.read_text()


def main():
    e = os.environ
    need(sys.flags.isolated and sys.dont_write_bytecode, 'isolated read-only query')
    need(e.get('GITHUB_ACTIONS') == 'true' and e.get('RUNNER_OS') == 'Linux'
         and e.get('GITHUB_REPOSITORY') == 'snissn/gomap'
         and e.get('GITHUB_ACTOR_ID') == '1981537' and e.get('GITHUB_RUN_ATTEMPT') == '1'
         and e.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
         and e.get('GITHUB_REF') == 'refs/heads/diagnostic/primary-construction-metadata-01'
         and e.get('GITHUB_SHA') == e.get('REVIEWED_HEAD') == e.get('WORKFLOW_COMMIT'),
         'fixed hosted metadata authority')
    need(output.resolve(strict=True) == output and e['ImageOS'] == 'ubuntu24', 'fixed metadata root')
    initial_whole_bytes = census(270532608)
    env = {k:v for k,v in e.items() if k in ('HOME','LANG','LC_ALL')}
    for name in ('tmp','gocache','modcache','gopath','xdg-cache','xdg-config'):
        (output/name).mkdir()
    env.update(PATH=str(SDK/'bin')+':/usr/bin:/bin',GOROOT=str(SDK),GOTOOLCHAIN='local',
               GOENV='off',GOWORK='off',GOTELEMETRY='off',GOCACHE=str(output/'gocache'),
               GOMODCACHE=str(output/'modcache'),GOPATH=str(output/'gopath'),
               TMPDIR=str(output/'tmp'),GOTMPDIR=str(output/'tmp'),TMP=str(output/'tmp'),
               TEMP=str(output/'tmp'),XDG_CACHE_HOME=str(output/'xdg-cache'),
               XDG_CONFIG_HOME=str(output/'xdg-config'),GIT_CONFIG_NOSYSTEM='1',
               GIT_CONFIG_GLOBAL=os.devnull,GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='Never',
               PYTHONDONTWRITEBYTECODE='1',GOMAXPROCS='2',GOMEMLIMIT='4GiB')
    for key in CONTEXT:
        if key not in ('GOVERSION','GOOS','GOARCH','GOROOT') and key in e:
            env[key] = e[key]
    paths = {'python':sys.executable,'node':e['METADATA_NODE_PATH'],
             'git':'/usr/bin/git','gcc':'/usr/bin/gcc','g++':'/usr/bin/g++',
             'readelf':'/usr/bin/readelf','ldd':'/usr/bin/ldd','bash':'/bin/bash'}
    tools = {name:row(path) for name,path in paths.items()}
    # Official archive-derived Mac receipt, not another runner's cache assertion.
    need(SDK.resolve(strict=True) == SDK, 'no SDK route fallback')
    sdkrows = []
    for directory,dirs,names in os.walk(SDK,followlinks=False):
        need(all(not (Path(directory)/n).is_symlink() for n in dirs), 'SDK directory symlink')
        for name in names:
            p = Path(directory)/name
            need(not p.is_symlink(), 'SDK file symlink')
            r = row(p)
            sdkrows.append(dict(path=p.relative_to(SDK).as_posix(),size=r['bytes'],sha256=r['sha256']))
            need(len(sdkrows) <= 15045, 'SDK extra file')
    sdkrows.sort(key=lambda r:r['path'])
    implicit = set()
    for r in sdkrows:
        implicit.update(p.as_posix() for p in Path(r['path']).parents if p != Path('.'))
    actual_dirs = {str(Path(d).relative_to(SDK).as_posix()) for d,_,_ in os.walk(SDK) if Path(d) != SDK}
    need(actual_dirs == implicit, 'SDK directory-set differs')
    sdk = dict(path=str(SDK),files=len(sdkrows),bytes=sum(r['size'] for r in sdkrows),
               sha256=hashlib.sha256(canonical(sdkrows)).hexdigest())
    need(sdk == dict(path=str(SDK),files=15045,bytes=232612406,
                    sha256='6aeaa7e94d3598c8d8b2215cd2e24bec855df3e491bcfd4dbe55a11052dcc314'),
         'cached SDK differs from official exact archive')
    need((SDK/'VERSION').read_text().splitlines()[0] == 'go1.26.10', 'SDK version metadata')
    code,text,_ = run('go-context',[str(SDK/'bin/go'),'env','-json',*CONTEXT],env)
    need(code == 0, 'Go context metadata query')
    context = json.loads(text)
    need(context['GOVERSION'] == 'go1.26.10' and context['GOOS'] == 'linux'
         and context['GOARCH'] == 'amd64' and context['GOROOT'] == str(SDK)
         and context['GOFLAGS'] == context['GOEXPERIMENT'] == ''
         and context['CC'] == 'gcc' and context['CXX'] == 'g++', 'unsupported normal context')
    executables = {str(Path(p).resolve(strict=True)) for p in paths.values() if p != '/usr/bin/ldd'}
    # Query stock compiler subordinate locations without compiling anything.
    for name in ('cc1','cc1plus','collect2','as','ld'):
        code,text,_ = run('gcc-path-'+name,['/usr/bin/gcc','-print-prog-name='+name],env)
        need(code == 0, 'compiler path metadata')
        value = text.strip()
        if not Path(value).is_absolute():
            need(value in ('as','ld'), 'unsupported compiler subordinate path')
            value = '/usr/bin/'+value
        executables.add(remember(value))
    # The existing selected Git HTTPS acquisition path, not other Git modes.
    executables.add(remember('/usr/lib/git-core/git-remote-https'))
    # Authenticated existing helpers are imported inertly, not run. Their
    # original selected methods have one deferred stdlib import: datetime in
    # Runner.now; it is imported above. Dormant campaign imports are not used.
    harness = output/'harness'
    pins = json.loads((harness/'inputs.json').read_text())
    for name, expected in pins.items():
        p = harness/name
        r = row(p)
        need((r['bytes'],r['sha256']) == (expected['bytes'],expected['sha256']), 'helper source pin')
    sys.path.insert(0,str(harness/'scripts'))
    for name,path in (('fixed_primary_runner',harness/'maintained-runner.py'),
                      ('fixed_primary_guard',harness/'scripts/ycsb_campaign_guard.py')):
        spec = importlib.util.spec_from_file_location(name,path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    import ycsb_compare_validate
    shared = set()
    startup = []
    for name,module in sorted(sys.modules.items()):
        path = getattr(module,'__file__',None)
        if path and Path(path).is_file() and not Path(path).resolve().is_relative_to(output):
            actual = remember(path)
            entry = dict(module=name,path=actual)
            cached = getattr(module,'__cached__',None)
            if cached and Path(cached).is_file():
                entry['cached'] = remember(cached)
            startup.append(entry)
            if actual.endswith('.so'):
                shared.add(actual)
    # Same maintained readelf/ldd path extraction, now applied only to stock
    # platform executables/stdlib extensions. No ELF code/disassembly parser.
    linkage = {}
    for i,path in enumerate(sorted(executables|shared)):
        remember(path)
        code,headers,_ = run('loader-header-'+str(i),['/usr/bin/readelf','-l','-d',path],env)
        need(code == 0, 'standard ELF metadata')
        interpreter = re.findall(r'Requesting program interpreter: ([^\]]+)',headers)
        dynamic = 'NEEDED' in headers or 'INTERP' in headers
        code,text,error = run('loader-path-'+str(i),['/usr/bin/ldd',path],env)
        raw = text+error
        if not dynamic:
            need(not interpreter and 'There is no dynamic section in this file.' in headers
                 and code in (0,1) and any(s in raw for s in ('not a dynamic executable','statically linked')),
                 'unsupported static metadata')
            dependencies = []
        else:
            need(code == 0 and len(interpreter) <= 1 and 'not found' not in raw,
                 'unresolved stock dependency')
            dependencies = sorted(set(re.findall(r'(/[^\s()]+)',raw))|set(interpreter))
            need(dependencies, 'missing stock dependency metadata')
            for p in dependencies:
                remember(p)
        linkage[path] = dict(interpreter=interpreter,dependencies=dependencies)
    for p in ('/etc/ld.so.cache','/etc/ld.so.conf','/etc/nsswitch.conf','/etc/resolv.conf',
              '/etc/ssl/certs/ca-certificates.crt','/etc/ssl/openssl.cnf'):
        remember(p)
    config = Path('/etc/ld.so.conf.d')
    for p in sorted(config.iterdir()):
        need(p.is_file(), 'loader config entry')
        remember(p)
    for p in paths.values():
        remember(p)
    candidate = json.loads((harness/'admission-template.json').read_text())
    candidate.update(status='ADMITTED_CONSTRUCTION_ONLY',independent_source_review_sha256='0'*64,
                     tools={k:tools[k] for k in ('node','python','git','gcc','g++')},sdk=sdk,context=context,
                     readonly_runtime_inputs=[inputs[k] for k in sorted(inputs)])
    # Complete, unchanged schema, with fixed-width placeholder only for a future
    # review digest. Never omit an authenticated origin to fit this envelope.
    decoded = len(canonical(candidate))
    encoded_admission = 4*((decoded+2)//3)
    compatible = encoded_admission <= 32768
    result = dict(schema='primary-construction-hosted-inputs-v1',scope='METADATA_ONLY_NOT_ADMISSION',
                  status='METADATA_COLLECTED' if compatible else 'ADMISSION_INPUT_TOO_LARGE',
                  head=e['GITHUB_SHA'],run_id=e['GITHUB_RUN_ID'],job_id=e['GITHUB_JOB'],
                  image={k:e.get(k) for k in ('ImageOS','ImageVersion','RUNNER_OS','RUNNER_ARCH')},
                  tools={k:tools[k] for k in ('node','python','git','gcc','g++')},sdk=sdk,context=context,
                  readonly_runtime_inputs=[inputs[k] for k in sorted(inputs)],
                  metadata_tools={k:tools[k] for k in ('readelf','ldd','bash')},linkage=linkage,
                  python_startup=startup,python_path=sys.path,commands=commands,
                  admission_decoded_bytes=decoded,admission_b64_bytes=encoded_admission,
                  admission_input_limit=32768,admission_size_compatible=compatible,
                  read_bytes=read_bytes,elapsed_seconds=time.monotonic()-started,
                  initial_whole_bytes=initial_whole_bytes,final_whole_bytes=census(4*1024**2),
                  whole_accounting='stage-boundary census plus reserved bounded command logs; no instantaneous quota',
                  no_build=True,no_product=True,future_host_identity_observed=False,
                  conditional_runtime_closure_complete=False)
    encoded = canonical(result)
    need(len(encoded) <= 2*1024**2, 'bounded complete metadata stdout')
    print(encoded.decode('ascii'),flush=True)
    return 0 if compatible else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except BaseException as error:
        print(json.dumps(dict(status='METADATA_FAILED_NO_ADMISSION',error_class=type(error).__name__)),flush=True)
        raise SystemExit(1)
