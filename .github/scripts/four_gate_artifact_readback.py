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

EXPECTED_BYTES=2733520
EXPECTED_SHA='8bc1f60e2762200e52724b73eb1cf06284d59c7ad1c1936e3f06a7fcb2a275cf'
ROOT_THREAD='01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
OUTPUT_ADMISSION_SHA='cf53e72fa93362e98acefc42e2584b3f72700939f2d6ec3f6f0f19b66e0328ec'
MEMBERS={'dispatch-admission.json': (1814, '50813e34b2ae25f796391efe98ae34621f179488d04c974a7b7d68ea9567677e'), 'entry.stderr.log': (0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'), 'entry.stdout.log': (0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'), 'guard/result.json': (37146, '69b76d385ad2b407cfddd65b79f067990ff2b3ffdc048db512a294637a5de510'), 'output/admission.json': (1920, 'cf53e72fa93362e98acefc42e2584b3f72700939f2d6ec3f6f0f19b66e0328ec'), 'output/arm/m0-combiner-service.json': (8041756, 'fa6eabe15d7c88c699187c5b187f13a596005dfb21dd6010c0840bd43eacd1f8'), 'output/arm/m0-ordinals.json': (1083451, '5934178b757b382d9bcabd48b79aa8e2e71ef6115b2317e23153aed8e3a31634'), 'output/binding-review.json': (3472, 'c36ebce4684f38d60148659e74251a5b240c1ae4a39710b366c37654c95aaf3f'), 'output/cgo-tool-identities.json': (336, '5abf2e250502ac827c8af21aa1f163bc806408a29c611e003d7101120fcae98e'), 'output/combiner-service-validation.json': (2441122, 'ef4b58c76ed718f928701fb0d2615393a19000d2558438a52cf2963701f89cff'), 'output/compile-context.json': (548, 'cee464dccdbb80d2f623e204f62798893bdfd49ab7eebeadca104dba4a821024'), 'output/completion.json': (369, '0f122ed1f93dfee08e8bfc688f1bf70b78366274d0d38be04b49d513725f9671'), 'output/controls-pass.json': (1660, '35cc3e14bca3973a16906fd8b4a8bbaf8e9e538819e41b7747f5ed1ee0e238c6'), 'output/final-file-manifest.json': (7738, '766ef5d6c9d555a86c7271cd5957e40907c69d34fd2f13cdaaf10ca6da02a310'), 'output/hosted-tools.json': (662, '5af691a996e5edf5b5236cc1eb15a2fa722cc73d18fb449fbf96cd2439967b5f'), 'output/input-bindings.json': (6553, '7dfc785ce30573d678d2b13a079e77eb5faca1b24174b36573a66b7a90114b23'), 'output/normal-ELF.json': (389, 'a480562d5f0b35b451916e8f326eccede4c5ca3fbef871a82e0b4fddadade895'), 'output/one-M0.stdout.log': (1301, '252c3399913acdbfcc9844057d8bea0972433ff76c301a0bf42207ba4b10618b'), 'output/runtime.json': (214, 'b8b26a340f7c06375b1aa6f77589bb7ed91543d420cd1098439c9579be07f807'), 'output/stages.json': (10287, '1d626523bc5b2e13baeee6fc36b7f2458f22c8d6207606eca6a7e3771d3151b8'), 'output/trace-bijection.json': (947481, '005f336d4d83c9a7ffcdd8bfcb787f4a1be24a7be880f5f49e419f60797d0fad'), 'output/trace-tool.json': (258, '83fe8ef65291b229755975345e8315d1d042c5c2b00e28904b98d5a6ee88c3e1'), 'output/trace-parsed.stdout.log': (63854681, '2ebb7047be980786273172c1b2fe997fa19aa45898576058c31fc2071ffdd6f0')}
NAMES={'output/normal-ELF.json', 'guard/result.json', 'output/binding-review.json', 'output/admission.json', 'entry.stderr.log', 'output/stages.json', 'output/hosted-tools.json', 'output/one-M0.stdout.log', 'output/completion.json', 'output/runtime.json', 'dispatch-admission.json', 'output/controls-pass.json', 'entry.stdout.log', 'output/input-bindings.json', 'output/cgo-tool-identities.json', 'output/final-file-manifest.json', 'output/compile-context.json', 'output/trace-tool.json'}
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
         and os.environ['GITHUB_REF']=='refs/heads/diagnostic/m0-common-write-original-data-01','ROOT actor/ref/firstattempt')
    encoded=os.environ['M0_DATA_ADMISSION_B64'];need(len(encoded)<=24000,'bounded admission')
    raw=base64.b64decode(encoded,validate=True);need(len(raw)<=16384,'bounded admission bytes')
    admission=json.loads(raw)
    for key,value in {'authority':'ROOT','authority_thread':ROOT_THREAD,'actor_id':'1981537',
        'schema':'root-m0-state-wall-projection-v1','status':'ROOT_DATA_PROJECTION_ADMITTED',
        'reader_head':head,'artifact':13390292576,'artifact_sha256':EXPECTED_SHA,
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
    LIMITS={'operations':8*MIB,'segments':16*MIB,'stacks':6*MIB,'links':MIB}
    def __init__(self,root):
        self.root=root;self.handles={};self.sizes={};self.console_bytes=0;self.started=time.monotonic();self.sampled=0;self.receipt_bytes=0;self.max_whole_sampled=0
    def check(self,extra=0,adverse=False):
        now=time.monotonic()
        need(adverse or now-self.started<30,'projection30s deadline')
        need(sum(self.sizes.values())+extra+2*MIB<=32*MIB,'32MiB declared stream reservation')
        if now-self.sampled<.2 and not adverse:return
        self.sampled=now
        total=sum(p.stat().st_size for p in self.root.rglob('*')if p.is_file()and p.name!='original.zip')
        need(total+self.console_bytes+extra+(0 if adverse else 4096)<=32*MIB,'32MiB generated outputs/console cap')
        work=Path(os.environ.get('M0_COUNTED_ROOT',str(self.root)))
        whole=sum(p.lstat().st_size for p in work.rglob('*')if not p.is_dir())
        self.max_whole_sampled=max(self.max_whole_sampled,whole)
        need(whole+extra+4096<=1024*MIB,'1GiB counted source/task/tmp/action root')
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
        need(self.receipt_bytes+len(raw)<=2*MIB,'2MiB other receipt reservation')
        self.check(len(raw),adverse=adverse)
        with (self.root/name).open('xb',buffering=0) as f:
            need(f.write(raw)==len(raw),'retained receipt short write');f.flush();os.fsync(f.fileno())
        self.receipt_bytes+=len(raw)
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


def authenticate_members(z,writer):
    total=sum(z.getinfo(n).file_size for n in MEMBERS)
    need(total<=256*MIB,'original selected uncompressed aggregate')
    for name,(size,expected) in MEMBERS.items():
        limit=128*MIB if name=='output/trace-parsed.stdout.log' else 32*MIB
        need(size<=limit and z.getinfo(name).file_size==size,'fixed member size/contract')
        h=hashlib.sha256();n=0
        with z.open(name) as f:
            for chunk in iter(lambda:f.read(65536),b''):
                writer.check();n+=len(chunk);need(n<=size,'decoded member byte bound');h.update(chunk)
        need(n==size and h.hexdigest()==expected,'fixed member digest/CRC')


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
        allowed=os.sched_getaffinity(0);need(bool(allowed),'CPU affinity unavailable')
        os.sched_setaffinity(0,{min(allowed)})
        inputs=json.loads((here.parent/'m0-common-write-inputs.json').read_text())
        for name,row in inputs['files'].items():
            p=here.parent.parent/name
            need(p.stat().st_size==row['bytes'] and digest(p)==row['sha256'],'helper input binding: '+name)
        spec=importlib.util.spec_from_file_location('m0_trace_consumer',here/'m0_trace_consumer.py')
        consumer=importlib.util.module_from_spec(spec);sys.modules['m0_trace_consumer']=consumer;spec.loader.exec_module(consumer)
        spec=importlib.util.spec_from_file_location('m0_common_write_projection',here/'m0_common_write_projection.py')
        companion=importlib.util.module_from_spec(spec);spec.loader.exec_module(companion)
        writer.write('admission.json',admission)
        python=Path(sys.executable).resolve(strict=True)
        writer.write('reader-runtime.json',{'python':str(python),'bytes':python.stat().st_size,
            'sha256':digest(python),'version':sys.version,'new_hosted_data_context':True,
            'full_stdlib_runtime_equivalence_claimed':False})
        writer.write('review-binding.json',{'sha256':hashlib.sha256(review).hexdigest(),
            'raw_bytes_base64':base64.b64encode(review).decode(),'authorship_verified_by':'ROOT_before_dispatch'})
        result={'source_head':os.environ['GITHUB_SHA'],'producer_head':'6e00893d9b7e43507b5c8172713389517155b327',
            'artifact':13390292576,'archive_sha256':EXPECTED_SHA,'scope':'original text/state wall only; no gate alteration',
            'entries':[],'texts':{},'missing':[]}
        with zipfile.ZipFile(archive) as z:
            names=z.namelist();need(len(names)==len(set(names)),'duplicate ZIP name')
            for item in z.infolist():
                parts=item.filename.split('/')
                need(not item.filename.startswith('/') and '..' not in parts and '\\' not in item.filename,'unsafe ZIP name')
                need(not stat.S_ISLNK(item.external_attr>>16),'ZIP link')
                result['entries'].append({'name':item.filename,'size':item.file_size})
            # First full bounded pass authenticates every exact selected member
            # and CRC before parsing owner/region/service data or deriving states.
            authenticate_members(z,writer)
            for name in sorted(NAMES):
                data=z.read(name)
                result['texts'][name]={'size':len(data),'sha256':hashlib.sha256(data).hexdigest(),'text':data.decode('utf-8')}
            writer.write('readback.json',result,MIB)
            owner=json.loads(z.read('output/arm/m0-ordinals.json'))
            aligned=json.loads(z.read('output/trace-bijection.json'))
            stages=json.loads(result['texts']['output/stages.json']['text'])
            decoder=next(r for r in stages if r['label']=='trace-parsed')
            def lines():
                with z.open('output/trace-parsed.stdout.log') as stream:
                    while True:
                        raw=stream.readline(MIB+1)
                        if not raw:break
                        writer.check();need(len(raw)<=MIB and raw.endswith(b'\n'),'bounded complete parsed line')
                        yield raw
            reproduced=consumer.consume(owner,lines(),decoder)
            need(reproduced==aligned,'original alignment reproduction mismatch')
            need(aligned['goroutine']==27 and aligned['task']==0,'original caller/task identity')
            service=json.loads(z.read('output/arm/m0-combiner-service.json'))
            companion.validate_service(owner,service)
            top41=sorted(range(4096),key=lambda i:(-owner['Records'][i]['DurationNS'],i))[:41]
            writer.write('original-top41.json',{'ordinals':top41,'sum_ns':sum(owner['Records'][i]['DurationNS'] for i in top41)})
            def foreground(name,row):
                row=dict(row);row['population']='foreground-caller'
                if name=='operations':row['at_original_top41']=row['ordinal'] in top41
                writer.emit(name,row)
            summary=consumer.project_states(owner,aligned,lines(),foreground)
            link=companion.link_worker(aligned,lines(),writer.emit)
            writer.write('worker-link.json',link,131072)
            if link['status']=='POSITIVELY_LINKED_ORIGINAL_WORKER':
                worker=companion.project_worker(aligned,link['worker_goroutine'],lines(),writer.emit)
            else:
                worker=companion.unknown_worker(aligned,link['reason'],writer.emit)
            summary['worker_in_set_caller_envelopes']=worker
            summary['status']='DIAGNOSTIC_COMPLETE_BOTH_STATE_POPULATIONS' if worker['status'].startswith('COMPLETE_') else 'INCONCLUSIVE_STOP_WORKER_EVIDENCE'
            summary['historical_CV_FAIL_unchanged']=True
            summary['overlapping_populations_not_additive']=True
            summary['no_CPU_exclusive_Write_or_cause_inference']=True
        writer.sampled=0;writer.check()
        writer.close()
        summary.update(artifact=13390292576,archive_sha256=EXPECTED_SHA,original_members=MEMBERS,
            source_head=os.environ['GITHUB_SHA'],original_output_admission_sha256=OUTPUT_ADMISSION_SHA,
            scope='DIAGNOSTIC_ORIGINAL_STATE_WALL_NOT_CAUSAL_OR_CV_ACCEPTANCE')
        summary_raw=writer.write('summary.json',summary,131072)
        files={p.name:{'bytes':p.stat().st_size,'sha256':digest(p)}for p in sorted(root.iterdir())
               if p.is_file()and p.name!='original.zip'}
        receipt={'status':'CLOSED_ORIGINAL_DATA_PROJECTION_PENDING_ROOT_REVIEW','files':files,
            'console_bytes_before_final':writer.console_bytes,'all4096':True,'acceptance':False,
            'max_whole_root_sampled_bytes':writer.max_whole_sampled,
            'whole_interval_verified':False,'elapsed_seconds':time.monotonic()-writer.started,'normal_projection_return':True,
            'checked_projection_writes_closed':True,'original_payload_never_extracted':True,
            'self_hash':'this receipt and hosted console are not self-hashed'}
        receipt_raw=writer.write('closed-output-receipt.json',receipt,131072)
        need(len(summary_raw)+len(receipt_raw)<=131072,'128KiB summary console cap')
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
                'error_class':type(error).__name__,'error':'fixed data projection incomplete; original stderr retained'},4096,adverse=True)
        except BaseException as receipt_error:
            error=BaseExceptionGroup('original failure then receipt failure',[error,receipt_error])
        raise error


if __name__=='__main__':
    main()
