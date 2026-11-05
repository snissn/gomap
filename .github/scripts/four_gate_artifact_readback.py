"""Fixed original M0 artifact text/state projection. No tool/product execution."""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import time
import zipfile

EXPECTED_BYTES=1854589
EXPECTED_SHA='f5db8af8fd294fb9bef55344586a7fc68d0520061c7e6d6838d1f9d28ac7669b'
ROOT_THREAD='01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
OUTPUT_ADMISSION_SHA='1d575edcdf372d7d3c2bd8e6a57a7b97ee1bd9200c2c7743785ec92c2023c029'
MEMBERS={
    'output/trace-parsed.stdout.log':(44418608,'8f6eceef8174622de3f158cfe8fc22de2e60972696dfeb8d6400e0642eefa271'),
    'output/trace-bijection.json':(947024,'929e20fe7dfd924f50257e51886ce1efd2ac8e7e4da3513d7a64478f824b4329'),
    'output/arm/m0-ordinals.json':(1082997,'a88eb5acd0282d6c33065ff631a13cbc02a7e49d838077c6466332edc3a88e91'),
}
# Same small-text selection as4100; large originals above are STREAMED, not
# extracted or copied into readback/console JSON.
NAMES={
    'entry.stderr.log','entry.stdout.log','guard/result.json','guard/stderr.log','post-guard-output.json',
    'output/completion.json','output/stages.json','output/admission.json','output/controls-pass.json',
    'output/binding-review.json','output/input-bindings.json','output/normal-ELF.json',
    'output/normal-buildinfo.stdout.log','output/normal-buildinfo.stderr.log','output/compile-context.json',
    'output/runtime.json','output/hosted-tools.json','output/cgo-tool-identities.json',
    'output/trace-tool.json','output/trace-tool-sources.json','output/trace-tool-buildinfo.stdout.log',
    'output/trace-tool-buildinfo.stderr.log','output/trace-tool-path.stdout.log','output/trace-tool-path.stderr.log',
    'output/consumer-controls.stdout.log','output/consumer-controls.stderr.log','output/one-M0.stdout.log',
    'output/one-M0.stderr.log','output/failure.json','output/resource-first-failure.json',
    'output/final-file-manifest.json','output/build-normal.stdout.log','output/build-normal.stderr.log',
    'output/source-C-identity.stdout.log','output/source-C-identity.stderr.log','output/go-version.stdout.log',
    'output/go-version.stderr.log','output/go-context.stdout.log','output/go-context.stderr.log',
    'output/gofmt.stdout.log','output/gofmt.stderr.log','output/trace-parsed.stderr.log','output/m0-C.overlay.json',
}
MIB=1024**2


def need(ok,message):
    if not ok:
        raise ValueError(message)


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(MIB),b''):
            h.update(chunk)
    return h.hexdigest()


def authority():
    head=os.environ['GITHUB_SHA']
    need(head==os.environ['EXPECTED_HEAD']==os.environ['WORKFLOW_SHA'],'immutable reviewed reader head')
    need(os.environ['GITHUB_ACTOR_ID']=='1981537' and os.environ['GITHUB_RUN_ATTEMPT']=='1'
         and os.environ['GITHUB_REF']=='refs/heads/diagnostic/m0-state-wall-projection-01','ROOT actor/ref/firstattempt')
    encoded=os.environ['M0_DATA_ADMISSION_B64'];need(len(encoded)<=24000,'bounded admission')
    raw=base64.b64decode(encoded,validate=True);need(len(raw)<=16384,'bounded admission bytes')
    admission=json.loads(raw)
    for key,value in {'authority':'ROOT','authority_thread':ROOT_THREAD,'actor_id':'1981537',
        'schema':'root-m0-state-wall-projection-v1','status':'ROOT_DATA_PROJECTION_ADMITTED',
        'reader_head':head,'artifact':13377421539,'artifact_sha256':EXPECTED_SHA,
        'original_output_admission_sha256':OUTPUT_ADMISSION_SHA,'no_recapture':True}.items():
        need(type(admission.get(key)) is type(value) and admission[key]==value,'admission: '+key)
    encoded=os.environ['M0_DATA_REVIEW_B64'];need(len(encoded)<=24000,'bounded review')
    review=base64.b64decode(encoded,validate=True);need(0<len(review)<=16384,'review bytes')
    need(hashlib.sha256(review).hexdigest()==admission.get('independent_review_sha256'),
         'actual independent-review bytes')
    # ROOT must authenticate source readiness/authorship before dispatch. Hash
    # equality binds actual bytes; it is NOT cryptographic proof of authorship.
    return admission,review


class RetainedOutputs:
    """Fixed output writer; no framework/API beyond this one data projection."""
    LIMITS={'operations':8*MIB,'segments':16*MIB,'stacks':6*MIB}
    def __init__(self,root):
        self.root=root;self.handles={};self.sizes={};self.console_bytes=0;self.started=time.monotonic()
    def check(self,extra=0,adverse=False):
        need(adverse or time.monotonic()-self.started<30,'projection30s deadline')
        total=sum(p.stat().st_size for p in self.root.rglob('*')if p.is_file()and p.name!='original.zip')
        need(total+self.console_bytes+extra+(0 if adverse else 4096)<=32*MIB,'32MiB generated outputs/console cap')
    def emit(self,name,row):
        raw=(json.dumps(row,sort_keys=True,separators=(',',':'))+'\n').encode()
        need(name in self.LIMITS,'fixed projection output')
        need(self.sizes.get(name,0)+len(raw)<=self.LIMITS[name],'projection file cap: '+name)
        self.check(len(raw))
        if name not in self.handles:
            self.handles[name]=(self.root/(name+'.jsonl')).open('xb',buffering=0)
        need(self.handles[name].write(raw)==len(raw),'retained projection short write')
        self.sizes[name]=self.sizes.get(name,0)+len(raw)
    def write(self,name,value,limit=MIB,adverse=False):
        raw=(json.dumps(value,indent=2,sort_keys=True)+'\n').encode()
        need(len(raw)<=limit,'fixed receipt size cap')
        self.check(len(raw),adverse=adverse)
        with (self.root/name).open('xb',buffering=0) as f:
            need(f.write(raw)==len(raw),'retained receipt short write');f.flush();os.fsync(f.fileno())
        return raw
    def console(self,raw):
        self.check(len(raw));self.console_bytes+=len(raw)
        need(os.write(1,raw)==len(raw),'console short write')
    def close(self):
        errors=[]
        for f in self.handles.values():
            try:f.flush();os.fsync(f.fileno())
            except BaseException as error:errors.append(error)
            try:f.close()
            except BaseException as error:errors.append(error)
        self.handles.clear()
        if errors:raise BaseExceptionGroup('retained output close failures',errors)


def main():
    admission,review=authority()
    root=Path(os.environ['RUNNER_TEMP'])/'four-gate-readback';archive=root/'original.zip'
    need(archive.stat().st_size==EXPECTED_BYTES,'archive size mismatch')
    need(digest(archive)==EXPECTED_SHA,'archive digest mismatch')
    writer=RetainedOutputs(root);error=None
    try:
        # Source manifest is bound by exact reviewed workflow/checkout before
        # download; every imported local helper is additionally hashed here.
        here=Path(__file__).resolve().parent
        inputs=json.loads((here.parent/'m0-state-projection-inputs.json').read_text())
        for name,row in inputs['files'].items():
            p=here.parent.parent/name
            need(p.stat().st_size==row['bytes'] and digest(p)==row['sha256'],'helper input binding: '+name)
        spec=importlib.util.spec_from_file_location('m0_trace_consumer',here/'m0_trace_consumer.py')
        consumer=importlib.util.module_from_spec(spec);spec.loader.exec_module(consumer)
        writer.write('admission.json',admission)
        python=Path(sys.executable).resolve(strict=True)
        writer.write('reader-runtime.json',{'python':str(python),'bytes':python.stat().st_size,
            'sha256':digest(python),'version':sys.version,'new_hosted_data_context':True,
            'full_stdlib_runtime_equivalence_claimed':False})
        writer.write('review-binding.json',{'sha256':hashlib.sha256(review).hexdigest(),
            'raw_bytes_base64':base64.b64encode(review).decode(),'authorship_verified_by':'ROOT_before_dispatch'})
        result={'source_head':os.environ['GITHUB_SHA'],'producer_head':'e605124818d57d3c3a351c51fa945ae4c729c562',
            'artifact':13377421539,'archive_sha256':EXPECTED_SHA,'scope':'original text/state wall only; no gate alteration',
            'entries':[],'texts':{},'missing':[]}
        with zipfile.ZipFile(archive) as z:
            names=z.namelist();need(len(names)==len(set(names)),'duplicate ZIP name')
            for item in z.infolist():
                parts=item.filename.split('/')
                need(not item.filename.startswith('/') and '..' not in parts and '\\' not in item.filename,'unsafe ZIP name')
                need(not stat.S_ISLNK(item.external_attr>>16),'ZIP link')
                result['entries'].append({'name':item.filename,'size':item.file_size})
            total=0
            for name in sorted(NAMES):
                if name not in names:result['missing'].append(name);continue
                info=z.getinfo(name);need(info.file_size<=131072,'selected metadata file too large')
                total+=info.file_size;need(total<=MIB,'selected metadata aggregate too large')
                data=z.read(name)
                result['texts'][name]={'size':len(data),'sha256':hashlib.sha256(data).hexdigest(),'text':data.decode('utf-8')}
            need(set(result['missing'])<={'output/failure.json','output/resource-first-failure.json'},'missing original required metadata')
            need(not any(n in result['texts'] for n in ('output/failure.json','output/resource-first-failure.json')),'original adverse receipt')
            writer.write('readback.json',result,8*MIB)
            # No large member is extracted or embedded in console/metadata.
            # First authenticate all3 exact uncompressed contents+CRC by reading
            # toEOF; then original consume and projection independently stream.
            for name,(size,expected) in MEMBERS.items():
                need(z.getinfo(name).file_size==size,'fixed large member size')
                h=hashlib.sha256();n=0
                with z.open(name) as f:
                    for chunk in iter(lambda:f.read(65536),b''):
                        writer.check();n+=len(chunk);need(n<=size,'decoded member byte bound');h.update(chunk)
                need(n==size and h.hexdigest()==expected,'fixed large member digest/CRC')
            owner=json.loads(z.read('output/arm/m0-ordinals.json'))
            aligned=json.loads(z.read('output/trace-bijection.json'))
            stages=json.loads(result['texts']['output/stages.json']['text'])
            decoder=next(r for r in stages if r['label']=='trace-parsed')
            with z.open('output/trace-parsed.stdout.log') as stream:
                reproduced=consumer.consume(owner,stream,decoder)
            need(reproduced==aligned,'original alignment reproduction mismatch')
            with z.open('output/trace-parsed.stdout.log') as stream:
                summary=consumer.project_states(owner,aligned,stream,writer.emit)
            need(summary['owner_duration_sum_ns']==826033351 and summary['p99_ns']==1156554,
                 'original stdout owner sum/p99 mismatch')
        writer.close()
        summary.update(artifact=13377421539,archive_sha256=EXPECTED_SHA,original_members=MEMBERS,
            source_head=os.environ['GITHUB_SHA'],original_output_admission_sha256=OUTPUT_ADMISSION_SHA,
            scope='DIAGNOSTIC_ORIGINAL_STATE_WALL_NOT_CAUSAL_OR_CV_ACCEPTANCE')
        summary_raw=writer.write('summary.json',summary,131072)
        files={p.name:{'bytes':p.stat().st_size,'sha256':digest(p)}for p in sorted(root.iterdir())
               if p.is_file()and p.name!='original.zip'}
        receipt={'status':'CLOSED_ORIGINAL_DATA_PROJECTION_PENDING_ROOT_REVIEW','files':files,
            'console_bytes_before_final':writer.console_bytes,'all4096':True,'acceptance':False,
            'elapsed_seconds':time.monotonic()-writer.started,'normal_projection_return':True,
            'checked_projection_writes_closed':True,'original_payload_never_extracted':True,
            'self_hash':'this receipt and hosted console are not self-hashed'}
        receipt_raw=writer.write('closed-output-receipt.json',receipt,131072)
        writer.console(summary_raw);writer.console(receipt_raw)
    except BaseException as caught:
        error=caught
    finally:
        try:writer.close()
        except BaseException as caught:
            error=caught if error is None else BaseExceptionGroup('original failure then close failure',[error,caught])
    if error is not None:
        try:
            writer.write('projection-failure.json',{'status':'INCOMPLETE_OR_REFUSED',
                'error':str(error)[:2048],'full_error':'original stderr retained; this bounded message may be truncated'},4096,adverse=True)
        except BaseException as receipt_error:
            error=BaseExceptionGroup('original failure then receipt failure',[error,receipt_error])
        raise error


if __name__=='__main__':
    main()
