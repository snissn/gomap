#!/usr/bin/env python3
"""Offline AST/hash/source/Bash syntax inspection; never executes Go or a workload."""
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[1]
BASE='95c6ff3de473786bd98b692375cfdb9cef951668'

def require(value,message):
    if not value: raise AssertionError(message)

def sha(data): return hashlib.sha256(data).hexdigest()

def blob(name):
    return subprocess.check_output(['git','show',BASE+':'+name],cwd=REPO)

def contains(source,snippets,label):
    for snippet in snippets: require(snippet in source,label+': '+snippet)

def main():
    paths=sorted(HERE.glob('*.py'))
    for path in paths: ast.parse(path.read_text(),filename=str(path))
    accepted=json.loads((HERE/'accepted-files.json').read_text())
    require(accepted['base']==BASE,'base drift')
    for name,bound in accepted['files'].items():
        data=(HERE/'accepted'/name).read_bytes()
        require(len(data)==bound['bytes'] and sha(data)==bound['sha256'],'accepted bytes changed: '+name)
    generated=(HERE/'accepted/prototype/diagnostic_server.go.txt').read_bytes()
    require(sha(generated)=='987cd212e24c2bd1d4c28ce65501a353aacca6289b48ae3548b7eae39fb14101','producer changed')
    manifest=json.loads((HERE/'accepted/source/manifest.json').read_text())
    for name,bound in manifest['files'].items():
        require(sha(blob(name))==bound['sha256'],'pinned base source drift: '+name)
    fixture=(HERE/'hosted_profile_test.go.txt').read_bytes()
    require(sha(fixture)=='a49b78e9848a86974c70cbf9cd083224bd26855b8370d03f9368071ab37f3d38','accepted helper changed')
    require(sha((HERE/'qualification_validate_profile.py').read_bytes())=='48bd478a902db158994185b9a859daab2bcebf143494d76570685c9c1a7ba8bd','phase validator changed')
    workflow=(REPO/'.github/workflows/mongo-profile-prototype.yml').read_text()
    contains(workflow,('branches: [codex/mongo-profile-hosted-20261031]','contents: read','fetch-depth: 16',
        'persist-credentials: false','go-version: "1.26.10"','cache: false','runs-on: ubuntu-24.04',
        'timeout-minutes: 30','if: always()','Prepare bounded hosted syscall injector','PROFILE_CLIENT_ASSET_ID: "694916289"',
        'PROFILE_CLIENT_BUILD_ASSET_ID: "694917396"'), 'workflow')
    require(not any(x in workflow for x in ('workflow_dispatch:','pull_request:','schedule:','branches: [main]','ROOT_MUST_')),'unselected workflow/placeholder')
    uses=re.findall(r'uses: ([^\s]+)',workflow)
    require(len(uses)==3 and all(re.fullmatch(r'actions/(?:checkout|setup-go|upload-artifact)@[0-9a-f]{40}',x) for x in uses),'unpinned action')
    require(workflow.count('GH_TOKEN:')==1 and workflow.index('GH_TOKEN:')<workflow.index('Select exact Go toolchain'),'token scope')
    require(workflow.index('Prepare bounded hosted syscall injector')<workflow.index('Select exact Go toolchain'),'late injector preparation')
    tools=(HERE/'prepare_tools.py').read_text()
    contains(tools,("'injector-package-download'", "'injector-package-extract'", "'apt-get',*options,'download','strace'",
        "'APT::Sandbox::User='+user", "'APT::Get::AllowUnauthenticated=false'", "'Acquire::AllowInsecureRepositories=false'",
        "'dpkg-deb','--extract'", 'PACKAGE_CAP=16*1024**2','EXTRACT_CAP=64*1024**2','TOOLS_CAP=96*1024**2',
        'FILE_CAP=10000',"timeout=180",'path.is_relative_to(tools.resolve())',
        'original_sha==r.digest(origin)', 'r.digest(path)==bound', "'evidence/injector-tool.json'"),'unprivileged injector preparation')
    require(not any(word in tools for word in ("'sudo'", "'install'", "'update'", '--allow-unauthenticated', 'os.setuid', 'os.seteuid')),
            'privileged/global/unauthenticated tool preparation returned')
    driver=(HERE/'run.py').read_text()
    contains(driver,('ROOT_CHECKPOINT = "dbc659dfee648c11ad105048aa7d007caf8cb097"','ROOT_CHECKPOINT + "^"',
        '["git", "merge-base", "--is-ancestor", ROOT_CHECKPOINT, sha]', 'sha == os.environ["GITHUB_SHA"]',
        '["git", "diff", "--name-only", BASE, sha]','FLOOR = 8 * 1024**3','TOOLCHAIN_BYTE_CAP = 1024**3',
        'TOOLCHAIN_STAGE_SECONDS = 300','guard(info.st_size)','guard(len(block))','FLOOR + required_bytes',
        'staged_hashes == original_hashes','write(self.out / "toolchain-stage.json", receipt)',
        'goenv["GOVERSION"] == PINNED_GO','Path(goenv["GOROOT"]).resolve(strict=True) == goroot',
        'tooldir == (goroot / "pkg/tool/linux_amd64").resolve(strict=True)',
        'compiler_version == selected_compiler_version == "compile version " + PINNED_GO',
        'GOTOOLCHAIN="local"','GOWORK="off"','GOPROXY="off"','GOSUMDB="off"','"--ignored=matching"',
        '"-mod=readonly"','"build-actual-standalone"','qualify.execute(runner, binary)',
        '"GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN"','24 * 1024**3','32 * 1024**2',
        'total <= 1024**3','excluded_database(path, self.out)','excluded_database(path, runner.out)',
        'self.kill_descendants(proc)', 'QUALIFIED_PREDECESSOR, sha]','int(fields[19]) == rows[pid][1]','self.disk_floor = FLOOR'), 'driver guard')
    for item in ('runner.command("compiler-version"','runner.command("go-tool-compiler-version"'):
        require(driver.index(item)<driver.index('runner.command("prepare-pinned-dependencies"'),'late compiler guard')
    require(driver.index('runner.stage_toolchain()')<driver.index('runner.command("go-version"'),'early Go execution')
    require('RUNNER_TOOL_CACHE' not in driver and '-vet=off' not in driver,'toolcache/vet regression')
    require('GOMAXPROCS="' not in driver and '"GOMAXPROCS"] =' not in driver,'forced GOMAXPROCS')
    require('for case in HELPER_CASES:' not in driver,'broad helper repetition returned')
    tree=ast.parse(driver)
    cleanup=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='helper_probe_directory')
    segment=ast.get_source_segment(driver,cleanup)
    contains(segment,('path.mkdir(mode=0o700)','not any(path.iterdir())','path.rmdir()',
        'current.st_dev, current.st_ino','not path.is_symlink()','runner.clean_base()',
        '"helper-probe-directory.json"','receipt["cleanup_error"]'),'vet cwd cleanup')
    require('shutil.rmtree' not in segment and 'unlink(' not in segment,'unsafe cwd removal')
    contains(driver,('str(helper_entry_path): str(generated)','str(fixture_path): str(fixture)',
        'str(cessation_path): str(cessation)','str(helper_entry_path), str(fixture_path), str(cessation_path)',
        'generated_sha256=GENERATED, retained_binary=True','"-test.run=^TestHostedCessation$"'), 'exact test source/ELF')
    qualification=(HERE/'qualify.py').read_text()
    contains(qualification,('CAMPAIGN_FLOOR = 10 * 1024**3',"RECORDCOUNT='1000',OPERATIONCOUNT='1000',THREADCOUNT='16',RUN_REPEATS='1'",
        "CLIENT_SOURCE = '3c8148495b8d4490cf0ec30e9eb92d14a7a71bac'","CLIENT_TREE = 'b64dd21261421d580ee72fe61711120a7f56c878'",
        'controller.outcomes(directory/\'treedb_mongo\',profiles)','module.validate(directory,profiles,mode)',
        "'prepare-fixture-pinned-dependencies'","'go.mod','go.sum'","runner.disk_floor=CAMPAIGN_FLOOR",
        "'write:retval=0'","'fsync:error=EIO'","'close:error=EIO'","'write:error=EIO'",
        "'run_1.cpu.pprof','phase.stop.json'","'O_RDONLY' in line and 'DELAYED' in line",
        "'ENOENT' in line", "c.observation(opened", "c.observation(before", "c.schema(closed)",
        "c.same(closed['process'],opened['process'])", "negative original close chronology",
        "'+++ exited with '+str(expected_exit)+' +++'", "syscall+'(' in line and str(target) in line",
        "r.require(bool(chunk),'proxy request EOF", 'controller.accepted_outcome(',
        "expected_exit=0", "'real disconnect not detected by server response path'",
        "'empty-symlink-target'", "'symlink-custody.json'"), 'qualification contract')
    require('write:retval=1' not in qualification and 'r.exchange=' not in qualification,'retry/global interception returned')
    proxy_tree=ast.parse(qualification)
    proxy_function=next(node for node in proxy_tree.body if isinstance(node,ast.FunctionDef) and node.name=='dropped_response')
    proxy_source=ast.get_source_segment(qualification,proxy_function)
    proxy_final=next(node for node in proxy_function.body if isinstance(node,ast.Try))
    require(bool(proxy_final.finalbody),'proxy cleanup must run in finally')
    contains(proxy_source,('info=proxy.lstat()', 'stat.S_ISSOCK(info.st_mode)', 'bound=(info.st_dev,info.st_ino)',
        "receipt['listener_closed'] and receipt['thread_settled']", 'listener.close()', 'thread.join(20)',
        'stat.S_ISSOCK(current.st_mode) and (current.st_dev,current.st_ino)==bound', 'proxy.unlink()',
        "'proxy-cleanup.json'", 'original_failure=', 'cleanup_errors=', 'BaseExceptionGroup'), 'checked proxy teardown')
    require('rmtree' not in proxy_source,'broad proxy cleanup returned')
    phase=(HERE/'phase_controller.py').read_text()
    contains(phase,('response.status == 200','int(value[\'content_length\']) == len(raw)',
        'def accepted_outcome','accepted_outcome(outcome)','c.same(outcome[\'body\']','r.write(Path(receipt), value)'), 'strict controller')
    cessation=(HERE/'cessation_test.go.txt').read_text()
    contains(cessation,('func cessationBefore()','func cessationAfter()','//go:noinline','before==0 || after!=0',
        'finalAfter!=0','runtime.SetMutexProfileFraction(-1)!=prior','pprof.StartCPUProfile(&second)',
        'reflect.DeepEqual(artifacts,originalHashes)'), 'cessation')
    fetch=(HERE/'fetch_client.py').read_text()
    contains(fetch,('RELEASE_ID = 429832319','99134712','599','694916289','694917396',
        '90e51911687d97efdc00295462d518192fe8b15c6485f8611454bb4cdfcd5fc0',
        "'Accept: application/octet-stream'",'guard.guard()','r.FLOOR + size','time.monotonic() - start < 180',
        'guard.kill_descendants(proc)','temporary.rename(target)'), 'input custody')
    # Pure source transform plus Bash parser; no launcher command is executed.
    sys.path.insert(0,str(HERE))
    import qualify
    original=blob('scripts/ycsb_compare_mongodb_treedb.sh')
    require(sha(original)==qualify.LAUNCHER_SHA,'launcher hash')
    shadow=qualify.launcher_text(original.decode())
    subprocess.run(['bash','-n'],input=shadow.encode(),check=True)
    require(shadow.count('"$PROFILE_CONTROLLER"')==2,'phase hooks not exactly start/stop')
    contains(shadow,('env -u GOMAXPROCS -u GOMAP_DIAGNOSTIC_PROFILE_DIR',
        'live_helper snapshot "$OUT_DIR/$target" "$target" --addr "$addr" --stage reopened',
        'live_helper verify "$OUT_DIR/$target" "$target" --addr "$addr"'), 'shadow lifecycle')
    print(json.dumps(dict(status='offline-source-contracts-passed',python_ast_files=len(paths),
        copied_packet_files=len(accepted['files']),generated_sha256=sha(generated),
        reused_helper_fixture_sha256=sha(fixture),bash_shadow_syntax=True,go_parse=False,
        go_build=False,go_runtime=False,local_workload=False,network=False),indent=2))

if __name__=='__main__': main()
