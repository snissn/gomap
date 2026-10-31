#!/usr/bin/env python3
"""Early unprivileged, task-owned injector preparation; no package installation."""
import os
from pathlib import Path
import pwd
import shutil
import stat
import run as r

PACKAGE_CAP=16*1024**2
EXTRACT_CAP=64*1024**2
TOOLS_CAP=96*1024**2
FILE_CAP=10000


class ToolRunner(r.Runner):
    def guard(self):
        super().guard()
        tools=self.root/'tools'
        total=0; extracted=0; count=0
        if not tools.exists(): return
        for path in tools.rglob('*'):
            info=path.lstat()
            count+=1
            r.require(count<=FILE_CAP,'injector preparation file-count cap')
            if stat.S_ISLNK(info.st_mode):
                r.require(path.resolve().is_relative_to(tools.resolve()),'extracted injector symlink escapes owned tools')
            elif stat.S_ISREG(info.st_mode):
                total+=info.st_size
                if path.is_relative_to(tools/'extracted'): extracted+=info.st_size
                if path.suffix=='.deb': r.require(info.st_size<=PACKAGE_CAP,'injector package exceeds 16 MiB cap')
            else: r.require(stat.S_ISDIR(info.st_mode),'unexpected injector special file')
        r.require(total<=TOOLS_CAP and extracted<=EXTRACT_CAP,'injector preparation byte cap')


def main():
    r.hosted_only()
    root=Path(os.environ['PROFILE_ROOT']).resolve(strict=True)
    r.require(root.parent==Path(os.environ['RUNNER_TEMP']).resolve(strict=True),'tool namespace mismatch')
    r.require(os.getuid()!=0 and os.geteuid()==os.getuid(),'injector preparation requires ordinary runner user')
    runner=ToolRunner(root)
    tools=root/'tools'; tools.mkdir()
    receipt=dict(start_utc=r.now(),status='preparing',scope='unprivileged task-owned tool only; no installation',
        uid=os.getuid(),package_cap=PACKAGE_CAP,extraction_cap=EXTRACT_CAP,total_cap=TOOLS_CAP,file_cap=FILE_CAP)
    try:
        selected=shutil.which('strace',path=runner.env['PATH'])
        if selected is not None:
            origin=Path(selected).resolve(strict=True)
            r.require(origin.is_file() and origin.stat().st_size<=PACKAGE_CAP,'existing injector executable size/type refused')
            runner.guard()
            (tools/'bin').mkdir(); path=tools/'bin/strace'
            original_sha=r.digest(origin)
            shutil.copyfile(origin,path); path.chmod(0o500)
            r.require(r.digest(path)==original_sha==r.digest(origin),'existing/copied injector hash disagreement')
            receipt.update(prepared_from='existing-image-executable-copy',origin=str(origin),origin_sha256=original_sha)
        else:
            downloads=tools/'downloads'; downloads.mkdir()
            cache=tools/'cache'; cache.mkdir()
            user=pwd.getpwuid(os.getuid()).pw_name
            # Read the image's existing signed apt metadata. Do not update global
            # lists, weaken authentication, change UID or execute install scripts.
            options=['-o','APT::Sandbox::User='+user,'-o','Dir::Cache='+str(cache),
                '-o','Dir::Cache::pkgcache=','-o','Dir::Cache::srcpkgcache=',
                '-o','APT::Get::AllowUnauthenticated=false',
                '-o','Acquire::AllowInsecureRepositories=false',
                '-o','Acquire::AllowDowngradeToInsecureRepositories=false']
            runner.command('injector-package-download',['apt-get',*options,'download','strace'],cwd=downloads,timeout=180)
            runner.guard()
            packages=list(downloads.glob('*.deb'))
            r.require(len(packages)==1 and len(list(downloads.iterdir()))==1,'expected exactly one downloaded injector package')
            package=packages[0]
            r.require(package.is_file() and not package.is_symlink() and 0<package.stat().st_size<=PACKAGE_CAP,'invalid injector package')
            metadata=runner.command('injector-package-metadata',['dpkg-deb','--show',
                '--showformat=${Package}\t${Architecture}\t${Version}\t${Installed-Size}\n',package],cwd=tools,timeout=10).strip().split('\t')
            r.require(len(metadata)==4 and metadata[0]=='strace' and metadata[1]=='amd64' and
                metadata[3].isdecimal() and int(metadata[3])*1024<=EXTRACT_CAP,'injector package metadata/cap mismatch')
            extracted=tools/'extracted'; extracted.mkdir()
            runner.command('injector-package-extract',['dpkg-deb','--extract',package,extracted],cwd=tools,timeout=60)
            runner.guard()
            path=(extracted/'usr/bin/strace').resolve(strict=True)
            r.require(path.is_relative_to(extracted.resolve()) and path.is_file(),'extracted injector escaped namespace')
            receipt.update(prepared_from='normal-authenticated-apt-download-and-unprivileged-extraction',
                package_sha256=r.digest(package),package_bytes=package.stat().st_size,package_metadata=metadata,
                apt_sandbox_user=user,metadata_policy='existing image signed apt metadata; unavailable index/dependencies is early RED; no update/install')
        path=path.resolve(strict=True)
        r.require(path.is_relative_to(tools.resolve()) and path.is_file(),'prepared injector must resolve inside owned namespace')
        runner.guard()
        bound=r.digest(path)
        version=runner.command('injector-version',[path,'--version'],cwd=root,timeout=10)
        r.require(r.digest(path)==bound,'injector executable changed during version probe')
        receipt.update(path=str(path),sha256=bound,version=version,status='available')
    except BaseException as error:
        receipt['status'],receipt['error']='failed',str(error)
        raise
    finally:
        receipt.update(end_utc=r.now(),minimum_free_bytes=runner.min_free,commands=runner.commands)
        r.write(root/'evidence/injector-tool.json',receipt)

if __name__=='__main__': main()
