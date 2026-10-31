#!/usr/bin/env python3
"""Dedicated authenticated input step. GH_TOKEN never reaches qualification children."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

RELEASE_ID = 429832319
FILES = (
    ('go-ycsb', 'PROFILE_CLIENT_ASSET_ID', 99134712, '2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b'),
    ('client-build.json', 'PROFILE_CLIENT_BUILD_ASSET_ID', 599, '90e51911687d97efdc00295462d518192fe8b15c6485f8611454bb4cdfcd5fc0'),
)

def main():
    import run as r
    r.hosted_only()
    root = Path(os.environ['PROFILE_ROOT']).resolve(strict=True)
    r.require(root.parent == Path(os.environ['RUNNER_TEMP']).resolve(strict=True), 'input namespace mismatch')
    out = root / 'inputs'
    out.mkdir()
    guard=r.Runner(root)
    receipt = dict(schema_version=1, status='fetching', release_id=RELEASE_ID, unpublished_input_only=True, files={}, start_utc=r.now())
    try:
        for name, variable, size, expected in FILES:
            asset = os.environ[variable]
            expected_id = '694916289' if name == 'go-ycsb' else '694917396'
            r.require(asset == expected_id, 'input asset ID drift')
            r.require(asset.isdecimal() and int(asset) > 0, 'frozen numeric input asset ID required')
            target = out / name
            temporary = out / (name + '.partial')
            start = time.monotonic()
            r.require(shutil.disk_usage(root).free >= r.FLOOR + size, 'input fetch would consume reserve')
            error_log=root/'evidence'/('client-fetch-'+name+'.log')
            with temporary.open('xb') as stream, error_log.open('xb') as errors:
                proc = subprocess.Popen(['gh', 'api', f'repos/snissn/gomap/releases/assets/{asset}',
                    '-H', 'Accept: application/octet-stream'], stdout=stream, stderr=errors, start_new_session=True)
                try:
                    while proc.poll() is None:
                        guard.guard()
                        r.require(time.monotonic() - start < 180, 'input fetch deadline')
                        r.require(temporary.stat().st_size <= size, 'input exceeds exact size')
                        r.require(error_log.stat().st_size <= 1024 * 1024, 'input error log cap')
                        time.sleep(.1)
                    r.require(proc.returncode == 0, 'authenticated input fetch failed')
                finally:
                    if proc.poll() is None:
                        guard.kill_descendants(proc)
            r.require(temporary.stat().st_size == size and r.digest(temporary) == expected, 'input exact size/hash mismatch')
            temporary.rename(target)
            target.chmod(0o500 if name == 'go-ycsb' else 0o400)
            receipt['files'][name] = dict(asset_id=asset, bytes=size, sha256=expected)
        receipt['status'] = 'exact-inputs-retained'
    except BaseException as error:
        receipt['status'], receipt['error'] = 'failed', str(error)
        raise
    finally:
        receipt['end_utc'] = r.now()
        receipt['minimum_free_bytes']=guard.min_free
        r.write(root / 'evidence/client-fetch.json', receipt)

if __name__ == '__main__':
    main()
