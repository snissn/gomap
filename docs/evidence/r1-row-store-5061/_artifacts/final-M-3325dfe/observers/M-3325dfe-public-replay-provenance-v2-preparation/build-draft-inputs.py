#!/usr/bin/env python3
"""V2 mechanical UNAPPROVED draft; bind exact original frozen A/C/D validator argv."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import urllib.parse

M='3325dfe77940fec8587d8b61b1ac4e0b2f72caca'
TREE='0600b47bf0a3a609917d224d7fd06c40737bcd8e'
PREFIX='final-M-3325dfe'
TEMPLATE_SHA='7386b8a571eb6c0831f8004f5e6753bd0a07a2ba970b56d2840b3599369d0ac4'
HIST=['comparator/baseline/0216e2e','comparator/historical-candidate/f95ea1a','comparator/final-typed-scope/00d2c370']
LANES={'A':'comparator/final-actual-M/3325dfe','C':'mutation-sweep/final-actual-M/3325dfe','D':'lifecycle/final-actual-M/3325dfe'}
OBS=['build-completed.json','build-start.json','independent-expected-source.json','independent-validation.log','observer-before.json','root-frozen-inputs.json','run-exit.json','run-start.json','runner-after.json','runner-before.json','validation-exit.json','validation-start.json']


def need(ok,msg):
    if not ok: raise ValueError(msg)

def hx(s,n=64):
    need(isinstance(s,str) and re.fullmatch('[0-9a-f]{%d}'%n,s),'missing/invalid independent pin')
    return s

def relative(s):
    need(isinstance(s,str) and s and all(re.fullmatch('[A-Za-z0-9_.-]+',p) and p not in ('.','..') for p in s.split('/')),'unsafe relative path')
    return s

def absolute(s,touch=True):
    p=Path(s); need(p.is_absolute() and '..' not in p.parts,'canonical absolute path required')
    if touch:
        for q in (p,*p.parents): need(not q.is_symlink(),'symlink path: '+str(q))
    return p

def regular(p):
    absolute(str(p)); need(stat.S_ISREG(p.lstat().st_mode),'nonregular input: '+str(p)); return p

def sha(p):
    h=hashlib.sha256()
    with regular(p).open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''): h.update(b)
    return h.hexdigest()

def pairs(xs):
    d={}
    for k,v in xs: need(k not in d,'duplicate JSON key'); d[k]=v
    return d

def load(p):
    return json.loads(regular(p).read_text(encoding='utf-8'),object_pairs_hook=pairs,
                      parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite JSON')))

def checkpin(p,h):
    need(sha(p)==hx(h),'independent byte pin mismatch: '+str(p)); return p

def write(p,v):
    with p.open('x',encoding='utf-8') as f: json.dump(v,f,sort_keys=True,indent=2,allow_nan=False); f.write('\n')

def ledger(p):
    result={}
    for line in regular(p).read_text().splitlines():
        need(re.fullmatch('[0-9a-f]{64}  .+',line),'invalid ledger entry')
        h,n=line.split('  ',1); relative(n); need(n not in result,'duplicate ledger entry')
        checkpin(p.parent/n,h); result[n]=h
    need(result,'empty ledger'); return result

def identity(record,label):
    # Only explicitly selected source fields; comparison baseline identities remain separate.
    keys=('M','commit','source_commit','actual_M','selected_actual_M_commit')
    observed=[record[k] for k in keys if k in record]
    for k in ('source','source_identity','external_pins'):
        if isinstance(record.get(k),dict) and 'commit' in record[k]: observed.append(record[k]['commit'])
    need(observed and all(x==M for x in observed),label+' missing explicit actual M identity or wrong M')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage',required=True)
    p.add_argument('--template',required=True)
    p.add_argument('--bootstrap',required=True)
    p.add_argument('--expected-bootstrap-sha256',required=True)
    p.add_argument('--out',required=True)
    a=p.parse_args()
    stage=absolute(a.stage); need(stage.is_dir(),'already-staged original text tree required')
    template=checkpin(absolute(a.template),TEMPLATE_SHA)
    bootstrap=checkpin(absolute(a.bootstrap),a.expected_bootstrap_sha256)
    b=load(bootstrap); need(b['schema']=='gomap-r1-public-replay-builder-bootstrap-v1','wrong bootstrap schema')
    c=copy.deepcopy(load(template))
    need(c['schema']=='gomap-r1-public-replay-root-inputs-v2' and c['root_approved'] is False,'wrong/unapproved template required')
    hx(b['public_artifact_git_sha'],40)
    url=urllib.parse.urlsplit(b['release_https_url'])
    need(url.scheme=='https' and url.hostname=='github.com' and url.username is None and not url.fragment
         and url.path.startswith('/snissn/gomap/releases/download/'),'actual public HTTPS release asset URL required')
    pieces=url.path.split('/')
    need(len(pieces)==7 and pieces[5]==b['release_tag'] and pieces[6]=='binary-overlay.tar.gz'
         and b['release_tag'],'actual release tag/URL disagree')
    need(b['public_repository_https']=='https://github.com/snissn/gomap.git','wrong public repo')
    for k in ('checkout_absolute','restored_absolute','proof_absolute','timed_lock','actual_host_identity_record'):
        absolute(b['fresh_linux111'][k],touch=False)
    destinations=[absolute(b['fresh_linux111'][k],touch=False) for k in
                  ('checkout_absolute','restored_absolute','proof_absolute')]
    for i,x in enumerate(destinations):
        for y in destinations[:i]:
            need(x!=y and x not in y.parents and y not in x.parents,'overlapping future public/replay/proof paths')
    hx(b['fresh_linux111']['actual_host_identity_record_sha256'])
    need(b['fresh_linux111']['no_private_capture_source'] is True,'private capture input prohibited')
    need(type(b['command_timeout_seconds']) is int and 1<=b['command_timeout_seconds']<=1800,'invalid bounded timeout')
    out=absolute(a.out)
    need(not out.exists() and out!=stage and out not in stage.parents and stage not in out.parents,'new disjoint output required')
    inventory={}
    def tracked(name):
        name=relative(name); q=regular(stage/name)
        mode=q.stat().st_mode&0o777; need(not mode&0o111,'executable staged text input: '+name)
        data=q.read_bytes()
        need(b'\0' not in data,'nontext staged input: '+name); data.decode('utf-8')
        record={'original_absolute':str(q),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),
                'mode':oct(mode),'symlink':False,'classification':'original bytes inspected mechanically; no acceptance decision'}
        if name in inventory: need(inventory[name]==record,'input changed while building')
        inventory[name]=record
        return q
    def read(name): return load(tracked(name))
    def hashed(name): tracked(name); return inventory[name]['sha256']
    core=ledger(tracked('SHA256SUMS'))
    pub=ledger(tracked('PUBLICATION_SHA256SUMS'))
    # Hashes remain a draft for root review; authentication anchors are independently supplied.
    checkpin(stage/'SHA256SUMS',b['authenticated_core_SHA256SUMS_sha256'])
    allfiles={}
    for base,dirs,names in os.walk(stage,followlinks=False):
        for n in dirs: need(not (Path(base)/n).is_symlink(),'symlink directory in stage')
        for n in names:
            q=regular(Path(base)/n); name=q.relative_to(stage).as_posix(); relative(name); allfiles[name]=sha(q)
    need(set(pub)==set(allfiles)-{'PUBLICATION_SHA256SUMS'},'PUBLICATION ledger is not complete for physical stage')
    for name in sorted(allfiles):
        if name=='binary-overlay.tar.gz': continue
        tracked(name)
    c['core_SHA256SUMS_sha256']=hashed('SHA256SUMS')
    c['PUBLICATION_SHA256SUMS_sha256']=hashed('PUBLICATION_SHA256SUMS')
    c['support_ledgers']=[]
    for name in sorted(n for n in allfiles if Path(n).name=='SUPPORT_SHA256SUMS'):
        ledger(stage/name); c['support_ledgers'].append({'stage_relative':name,'sha256':hashed(name)})
    need(c['support_ledgers'],'no staged SUPPORT ledgers')
    c['support_ledger_list_complete']=False
    index=read('binary-member-index.json'); binaries={}
    for row in index:
        need(set(row)=={'path','size','sha256'},'wrong binary-index schema')
        name=relative(row['path']); need(name not in binaries and name not in core,'duplicate indexed binary')
        hx(row['sha256']); need(type(row['size']) is int and row['size']>0,'invalid indexed ELF size')
        binaries[name]=row
    overlay=regular(stage/'binary-overlay.tar.gz'); overlay_sha=sha(overlay)
    need(core['binary-overlay.tar.gz']==overlay_sha,'overlay/core pin disagreement')
    c['overlay'].update(release_https_url=b['release_https_url'],sha256=overlay_sha,bytes=overlay.stat().st_size,
                         member_index_sha256=hashed('binary-member-index.json'))
    landing_name=PREFIX+'/5071-actual-M-landing-original.json'; landing=read(landing_name)
    need(landing['actual_main_merge_sha']==M and landing['actual_main_tree_sha']==TREE
         and landing['repo']=='snissn/gomap' and landing['pr']==5071 and landing['mergedAt'],
         'missing/wrong actual M landing original')
    c['actual_M_landing_record_stage_relative']=landing_name
    c['actual_M_landing_record_sha256']=hashed(landing_name)
    c['selected_actual_M_commit']=M; c['selected_actual_M_tree']=TREE
    c['public_artifact_git_sha']=b['public_artifact_git_sha']
    c['public_artifact_landing_reachable']=False
    c['fresh_linux111']=copy.deepcopy(b['fresh_linux111'])
    c['command_timeout_seconds']=b['command_timeout_seconds']
    base_name=PREFIX+'/selection/base13.json'; final_name=PREFIX+'/selection/final16.json'
    base=read(base_name); final=read(final_name)
    checkpin(stage/base_name,b['authenticated_base13_sha256'])
    need(len(base)==13 and len(final)==16 and final[:13]==base,'original thirteen rows not preserved as final prefix')
    need([x['path'] for x in base if x['status']=='accepted']==HIST,'original three accepted A identities/order changed')
    rejected={x['path'] for x in c['historical_rejected_D']}
    need({x['path'] for x in base if x['status']=='rejected'}==rejected and len(rejected)==10,'historical rejected D classification changed')
    need({x['path'] for x in final[13:]}==set(LANES.values()) and all(x['status']=='accepted' for x in final[13:]),
         'actual three root-selected rows absent')
    c['required_final_selection'].update(base13_stage_relative=base_name,base13_sha256=hashed(base_name),
        selection_stage_relative=final_name,selection_sha256=hashed(final_name),record_count=16,
        fresh_actual_A_C_D_records_added=False)
    bypath={x['path']:x for x in final}; need(len(bypath)==16,'duplicate selected paths')
    for row in c['historical_accepted_A']:
        path=row['path']; selected=bypath[path]
        need(selected['kind']=='comparator' and selected['expected']==row['expected'],'historical A expected source changed')
        source_name=path+'/source.json'; source=read(source_name); after=read(path+'/source-after.json')
        packet=read(path+'/packet.json'); frozen=read(path+'/frozen-source-accepted.json')
        need(source==after==packet['source'] and source['clean'] is True and frozen['accepted'] is True,
             'historical accepted original source sidecars disagree')
        for key in ('commit','runtime_sha256','harness_sha256'):
            need(source[key]==row['expected'][key]==frozen[key],'historical accepted source/freeze/selection identity disagrees')
        need(core[source_name]==hashed(source_name),'historical source bytes not bound by authenticated core ledger')
        if path==HIST[0]:
            for leaf,key in [('source.json','source_sha256'),('packet.json','packet_sha256'),
                ('frozen-source-accepted.json','freeze_sha256'),('capture-environment.json','environment_sha256')]:
                need(hashed(path+'/'+leaf)==c['A_certificate']['baseline'][key],'accepted original baseline byte pin changed')
        binary=binaries[path+'/collection_workload_bench']
        if path==HIST[0]:
            need(binary['sha256']==c['A_certificate']['baseline']['binary_sha256'],
                 'accepted original baseline binary index pin changed')
        row.update(binary_sha256=binary['sha256'],packet_sha256=hashed(path+'/packet.json'),
                   independently_verified_source_stage_relative=source_name,
                   independently_verified_source_sha256=hashed(source_name),fresh_public_replay_result='pending')
        # Original emitted sidecar may retain a historical absolute filename. Compare hash, not that path.
        side=(tracked(path+'/binary.sha256')).read_text().split()
        need(side and side[0]==binary['sha256'],'historical original binary sidecar/index mismatch')
    for label,capture in LANES.items():
        row=c['actual_lanes'][label]; rbase=PREFIX+'/receipts/'+label; result=PREFIX+'/results/'+label
        acceptance_name=result+'/root-cost-acceptance-original.json'; acceptance=read(acceptance_name)
        identity(acceptance,label+' root acceptance original')
        # Presence/source applicability only: no numeric policy or acceptance decision is made here.
        config=read(rbase+'/root-frozen-inputs.json'); trusted=read(rbase+'/trusted-completed-receipt.json')
        expected_name=rbase+'/independent-expected-source.json'; expected=read(expected_name)
        packet_name=capture+'/packet.json'; packet=read(packet_name)
        before_name=capture+('/source-before.json' if label=='D' else '/source.json'); after_name=capture+'/source-after.json'
        need(read(before_name)==read(after_name)==expected==packet['source_before' if label=='D' else 'source']
             and expected['clean'] is True,'actual original source sidecars/independent manifest disagree')
        need(config['source_commit']==config['landed_tooling_commit']==trusted['source_commit']==trusted['landed_tooling_commit']==expected['commit']==M,
             'actual original config/completed receipt not exact landed M')
        need(expected['runtime_sha256']==landing['runtime_sha256'],'actual source runtime differs from actual landing')
        if label!='D': need(expected['harness_sha256']==landing['A_C_harness_sha256'],'actual A/C harness differs from landing')
        pins={key:trusted['source_commit' if key=='commit' else key] for key in row['external_pins']}
        for key,value in pins.items(): hx(value,40 if key in ('commit','landed_tooling_commit') else 64)
        for key in ('runtime_sha256','harness_sha256'):
            need(pins[key]==config[key]==expected[key],'actual external source pins disagree')
        need(trusted['exit']==0 and trusted['packet_sha256']==hashed(packet_name), 'original completed packet failed/substituted')
        binary=binaries[capture+'/'+row['original_binary_name']]
        need(binary['sha256']==trusted['binary_sha256'],'actual index/original completed binary pins disagree')
        start=read(rbase+'/validation-start.json'); end=read(rbase+'/validation-exit.json')
        need(end['exit']==0 and isinstance(start['argv'],list) and start['argv']
             and all(isinstance(v,str) and v for v in start['argv']),
             'original independent validator receipt absent/failed')
        original_out=absolute(config['out'],touch=False)
        original_receipts=absolute(config['receipt_dir'],touch=False)
        original_repo=absolute(config['repo'],touch=False)
        original_packet=str(original_out/'packet.json')
        original_manifest=str(original_receipts/'independent-expected-source.json')
        if label=='A':
            expected_argv=[str(original_out/'collection_workload_bench'),'r1-validate',
                           '-source-manifest',original_manifest,original_packet]
        elif label=='C':
            expected_argv=[str(original_out/'collection_workload_bench'),
                           'r1-mutation-sweep-validate','-source-manifest',original_manifest]
        else:
            # The frozen D observer uses the same sys.executable in both original
            # run-start and validation-start. Bind those independently retained
            # receipt bytes; never inspect or run either historical absolute path.
            run=read(rbase+'/run-start.json')
            need(isinstance(run['argv'],list) and run['argv']
                 and isinstance(run['argv'][0],str),'D original capture argv missing')
            original_python=absolute(run['argv'][0],touch=False)
            need(re.fullmatch(r'python(?:3(?:\.[0-9]+)?)?',original_python.name),
                 'D original run-start argv[0] is not an absolute Python executable path')
            expected_argv=[str(original_python),str(original_repo/'scripts/r1_lifecycle_validate.py'),original_packet]
        if label in ('C','D'):
            prefix='-' if label=='C' else '--'
            for flag,key in [('expected-commit','commit'),('expected-runtime','runtime_sha256'),
                             ('expected-harness','harness_sha256'),('expected-landed-tooling-commit','landed_tooling_commit'),
                             ('expected-binary-sha256','binary_sha256'),('expected-packet-sha256','packet_sha256')]:
                expected_argv.extend([prefix+flag,pins[key]])
            if label=='C': expected_argv.append(original_packet)
        need(start['argv']==expected_argv,label+' original validator argv differs from exact frozen observer contract')
        need(start['cwd']==str(original_repo),label+' original validator cwd differs from frozen config.repo')
        need(trusted['capture_directory']==config['out'],'original completed capture root changed')
        row.update(capture_restored_relative=capture,external_pins=pins,original_capture_root=config['out'],
                   original_receipt_root=config['receipt_dir'],public_receipts_stage_relative=rbase,
                   public_independent_expected_source_stage_relative=expected_name,
                   independent_expected_source_sha256=hashed(expected_name),source_before_sha256=hashed(before_name),
                   source_after_sha256=hashed(after_name),original_validation_argv=copy.deepcopy(start['argv']),root_accepted=False)
        for key,leaf in [('trusted_completed_receipt','trusted-completed-receipt.json'),
                         ('original_validation_start','validation-start.json'),('original_validation_exit','validation-exit.json'),
                         ('original_validation_log','independent-validation.log')]:
            row[key+'_stage_relative']=rbase+'/'+leaf; row[key+'_sha256']=hashed(rbase+'/'+leaf)
        row['root_acceptance_stage_relative']=acceptance_name; row['root_acceptance_sha256']=hashed(acceptance_name)
        if label=='A':
            need(end['binary_sha256']==pins['binary_sha256'] and end['packet_sha256']==pins['packet_sha256']
                 and end['validation_log_sha256']==row['original_validation_log_sha256'],'A original independent validator binding disagrees')
    cert_name=PREFIX+'/results/A/root-approved-A-semantics-and-provenance-certificate-v2.json'
    cert=read(cert_name); certrow=c['A_certificate']
    need(cert['schema']=='gomap-r1-A-harness-equivalence-v1' and cert['approved'] is True
         and cert['selected']['commit']==M and cert['baseline']==certrow['baseline'], 'actual original approved semantic certificate missing/wrong M or baseline')
    certrow.update(stage_relative=cert_name,independently_root_approved_sha256=hashed(cert_name),approved=False,
        selected_harness_blobs=copy.deepcopy(cert['selected_harness_blobs']),
        reviewed_changed_path_diff_sha256=copy.deepcopy(cert['reviewed_changed_path_diff_sha256']))
    obs={n:hashed(PREFIX+'/receipts/A/'+n) for n in OBS}
    need(cert['selected']['observation_sha256']==obs,'twelve original observation pins differ from actual certificate')
    certrow['selected_observation_sha256']=obs
    freeze_name=PREFIX+'/results/A/frozen-source-accepted.json'; freeze=read(freeze_name)
    need(freeze['commit']==M and freeze['accepted'] is True,'actual A original accepted source freeze missing/wrong M')
    certrow['actual_accepted_freeze_stage_relative']=freeze_name; certrow['actual_accepted_freeze_sha256']=hashed(freeze_name)
    need(cert['selected']['freeze_sha256']==hashed(freeze_name)
         and cert['selected']['receipt_sha256']==c['actual_lanes']['A']['trusted_completed_receipt_sha256'],'certificate freeze/receipt pins disagree')
    certrow['approval_record_stage_relative']=c['actual_lanes']['A']['root_acceptance_stage_relative']
    certrow['approval_record_sha256']=c['actual_lanes']['A']['root_acceptance_sha256']
    for row in c['frozen_validator_public_paths'].values(): checkpin(tracked(row['stage_relative']),row['sha256'])
    c['root_approved']=False; c['status']='MECHANICAL_UNAPPROVED_DRAFT_ROOT_REVIEW_REQUIRED'
    # Recheck all inspected originals before emitting new drafts. Never rewrite a stage member.
    for name,row in inventory.items(): checkpin(stage/name,row['sha256'])
    checkpin(template,TEMPLATE_SHA); checkpin(bootstrap,a.expected_bootstrap_sha256)
    need(allfiles=={name:sha(stage/name) for name in allfiles},'stage input changed while building')
    out.mkdir(parents=True,exist_ok=False)
    write(out/'draft-root-inputs.json',c)
    write(out/'independent-input-inventory.json',{'status':'UNAPPROVED_MECHANICAL_INVENTORY_NO_ACCEPTANCE',
        'stage_absolute':str(stage),'template_sha256':sha(template),'bootstrap_sha256':sha(bootstrap),
        'draft_inputs_sha256':sha(out/'draft-root-inputs.json'),'original_staged_text_inputs':inventory,
        'overlay':{'original_absolute':str(overlay),'sha256':overlay_sha,'bytes':overlay.stat().st_size},
        'indexed_binaries':index,'all_stage_files_sha256':allfiles,
        'root_review_must_set_only_after_actual_approval':['root_approved','public_artifact_landing_reachable',
          'support_ledger_list_complete','required_final_selection.fresh_actual_A_C_D_records_added',
          'actual_lanes.A.root_accepted','actual_lanes.C.root_accepted','actual_lanes.D.root_accepted','A_certificate.approved'],
        'missing_actual_public_proof':'Builder has not fetched Git, downloaded HTTPS, or run any validator; executor/root remain sole public proof/acceptance owners.'})
    print(json.dumps({'status':c['status'],'out':str(out),'draft_sha256':sha(out/'draft-root-inputs.json'),
                      'inventory_sha256':sha(out/'independent-input-inventory.json'),'root_approved':False},sort_keys=True))

if __name__=='__main__':
    try: main()
    except Exception as exc:
        import sys
        print(type(exc).__name__+': '+str(exc),file=sys.stderr); sys.exit(1)
