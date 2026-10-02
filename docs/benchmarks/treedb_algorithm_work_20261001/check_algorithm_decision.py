#!/usr/bin/env python3
"""Bind the bounded algorithm decision to retained canonical evidence; no Go/native work."""
# Assertions are the same contract as the existing decision checker. Reject -O
# before argument parsing, imports of evidence helpers, or evidence/document I/O.
if not __debug__:
    raise RuntimeError('validation requires Python assertions; run without -O')
import argparse
import ast
import hashlib
import importlib.util
import json
import itertools
from pathlib import Path
import re
import statistics
import sys
from types import SimpleNamespace
sys.dont_write_bytecode = True
HASHES = {'algorithm-full-primary-inspection.json': 'db4dc6bc746201b7512c4d13c9a8529e8ae697bd606f1b7f2bf3b078f954fef1',
 'algorithm-full-normal-freeze.json': 'eb0419e74f6c11493087e408eeebf394bf008e86006ced5eb64acd303b1bd336',
 'algorithm-full-counter-freeze.json': '9ee522e28df829d261cd8e6a4d3a8df6edfb8ce7d3d960af7774579ddd7aaf80',
 'algorithm-full-run-order-v2.json': 'aa1444381926f0661ea14fd2936c504b766c7a546d9bc6d53a1b5fbe06e49ec6',
 'algorithm-paired-4919-732-five-repeat-analysis.json': '48d5cdedc881add5761a2c20ce8f2183b0ac073651deaa0b888986bbb3d75325',
 'algorithm-paired-4919-732-extend-reduce.py': '66d84a92cd05d5ece76537138ecfdcf0d531c3fd8bcbaac2cc9ed45c0e6502f3',
 '4919-732-five-repeat-independent-analysis.md': '69cc408c5870656a84a274162e3943917a58f964b563ef01ac969c95f87ca12f',
 '4919-singleton-five-pair-independent-screen-analysis.json': '12222bb951c1b9debb01ebb06ebf91d1d8b70a98075aeb633dc076e1405dd4a8',
 '4919-singleton-five-pair-independent-screen-review.md': '4efd9a26cbde1342d6fa851568dce8e46da34a21f3f5a7d44769a2d9571783ac',
 '4917-final-singleton-root-disposition.md': 'c2f64814de49fec11cff20100b44d85a6d8cb4a7fd16285b34ba008f60a5775a',
 '4917-post-five-pair-architecture-decision.md': 'f0d0adcd3a0715e727ed53ea27fc48548366bdd7745b5b9ad5bb6b04a934e767',
 '4916-e3-three-repeat-analysis.json': 'e9d8658b70843133d88e16d25fd2942c6efcd2d6aa067ab55ffc6e4e3c1233cb',
 '4916-e3-independent-matched-decision-review.md': '5fbd05c848d07ef8875a316a73d67852899d291941e2409eac6887f4ca43cd17',
 'algorithm-full-primary-packet/sparse-materialization/top.txt': '87b57a50c66f4d10899a786bb543ef54983ecfa61598519642d85374e4cf1b0d',
 'algorithm-full-primary-packet/sparse-materialization/callpath.txt': '01383ba932bf2621fa7de38d7541df00c0997857a39bc8e65ee68e6270b418c3',
 'algorithm-full-primary-packet/sparse-materialization/validated-profile.json': 'de7481ae18f85c616568b9a5886234ed07a1f651ff384342ec7ebb591c50562d',
 'algorithm-full-primary-packet/writes-r1/stdout.log': 'a1a0195b050fde60c52e20c2aaf567f4f16f152bc0383030e3b4264720c0470f',
 'algorithm-full-primary-packet/writes-r1/stderr.log': '7aa3fca42dae55319f16de08d8264cd7a163466e2b7ff289a56f638b6870efc6',
 'algorithm-full-primary-packet/writes-r1/execution.json': 'fcdb331903b55d52ad280695b8ebd72f8db77019696f70ac6811ea01e33e3a65',
 'algorithm-full-primary-packet/writes-r1/parsed.json': '86e190c3051ffa321b417969264898444201d4467e19237e7422f30cf46c5307',
 'algorithm-full-primary-packet/writes-r2/stdout.log': 'd587ad09663cc7b292257cdd7163eb643d692be74bbf08980fce6485c36d15d3',
 'algorithm-full-primary-packet/writes-r2/stderr.log': '9dbd07476ac04113563c4b11f15774d9ea1c55b2f6f574cd5c485ea377b76e77',
 'algorithm-full-primary-packet/writes-r2/execution.json': '0ab1a7724c0aee4461a081500857ab474fedbbb828bc8e4091dd8e56615f5678',
 'algorithm-full-primary-packet/writes-r2/parsed.json': '5fc99fc1198a0331bdcff2443fb7a5c2a0087a639373e3b526297f65640f9330',
 'algorithm-full-primary-packet/writes-r3/stdout.log': 'efae122348fab3a117ac5250c8106c56512c1e8b4fa127aff5447976309158d4',
 'algorithm-full-primary-packet/writes-r3/stderr.log': 'fe33f1f187a20c61123160f38a0b7d08746052dcd74fd9b012af9693efc0e54e',
 'algorithm-full-primary-packet/writes-r3/execution.json': '834b347d2de32f5d6d2e358eead9f73332328fec65dd0ffb9bb7b7c12bbd5309',
 'algorithm-full-primary-packet/writes-r3/parsed.json': '2e0d73ee6b64e468500e1446cdf2b1216e397c37dfd60d467203fa3225379505',
 'algorithm-full-primary-packet/many-r1/stdout.log': 'bee3a8452691035c41ee4f30898aeb5085db1ff5dc97ec5555460e84d1ec63ad',
 'algorithm-full-primary-packet/many-r1/stderr.log': 'b41873156b3031b09a206bbe195818954d280203e92fcc04b256add8df7d84ee',
 'algorithm-full-primary-packet/many-r1/execution.json': 'fc06f884024f772ecd05eaff29d93a5f2f1c9e00dfe80be279da0ca0fbd87459',
 'algorithm-full-primary-packet/many-r1/parsed.json': 'e3a537d2bae75d9c139cf5097ce24c0046b6ef594da76db622a5a0a8184003d8',
 'algorithm-full-primary-packet/many-r2/stdout.log': '9b495a16be54c75783742250af373e414300165c06a8a5f0b7e07cdf67618257',
 'algorithm-full-primary-packet/many-r2/stderr.log': '3b634bbfd549ae36a9429ea07b27561694cab99015ad2536f3f9b79c8e360ba9',
 'algorithm-full-primary-packet/many-r2/execution.json': 'e88b518fc28bc8c5f33c30fb73692ff7a3155f69e4c36ecfa6c85e98d3c63ae8',
 'algorithm-full-primary-packet/many-r2/parsed.json': 'e8f62796a5a82fe740e62f559b6b8875e4a1855987564b0da9f8fa884dcef411',
 'algorithm-full-primary-packet/many-r3/stdout.log': '1faf6015ab803c9992939d488615247d680689f8d16f189832da3d0efecf88d6',
 'algorithm-full-primary-packet/many-r3/stderr.log': 'c3ee30fcc30e329eb4278517ef2454d9ea5c360cc4c3cf8edbc0b98ddab6f786',
 'algorithm-full-primary-packet/many-r3/execution.json': 'bce40dd20c524901002fedffdb9ac12e4da319e4df6148c07ec001cb320bddd5',
 'algorithm-full-primary-packet/many-r3/parsed.json': 'ec82bcb34eff06bae8d8b3eee9612e457e33a05a04506bb781b83cad39c5681e',
 'algorithm-full-primary-packet/internal/stdout.log': '98ad25f379eb27b4307a6ee66ac910ee059f388307c7ed6c78aff2c2f8d9efc9',
 'algorithm-full-primary-packet/internal/stderr.log': 'f584effbb1853fab14a8c8c48907c381805012d59e448796c4e7c14d2a89be63',
 'algorithm-full-primary-packet/internal/execution.json': '8921b79d0b504b94f951e86828ed35cfc3523e63f0be8a2ed10f424abedcae47',
 'algorithm-full-primary-packet/internal/parsed.json': 'ea3cefc3be99ae23730c728adfa9169f0e478edf63a2aa00fa5284f912098173',
 'algorithm-full-preparation-anchor.json': 'b904ec72486de3c51315c33085e15a631f71232f0aa2bf09369a43cf7616c80f',
 'algorithm-full-primary-packet/launch.json': '4cd842589318b1bc7fdf51b62d514565b7bee9d1560e854ad6e98f9ad3eba3ca',
 'algorithm-full-primary-packet/completed.json': '59a53d78e47a4938c03d4926d8aa81acdc1658e82d5507d144da655471b80c48',
 'algorithm-full-primary-packet/many-inline-clustered-owned/cpu.pprof': '9d0671067f5b50d7cbe9f3510fab8bf53d9f2fba4f9b228605de845ed2b17979',
 'algorithm-full-primary-packet/many-inline-clustered-owned/validated-profile.json': '651f1b1d23eb8aaa8da94fe4b846f2ca403a433b39bba2f2fddb6e6f354c12ee',
 'algorithm-full-primary-packet/many-inline-clustered-owned/profile.json': '82b63e5dbb84e93fc299720b3c28a3ab537075710f8ad9a1841a15e84d8572cb',
 'algorithm-full-primary-packet/many-inline-clustered-view/cpu.pprof': '85d82df5ff2b9d8d5c99ab1f4cbd8cc357a4fd19bb08bd99ab1aee1a7a8731dc',
 'algorithm-full-primary-packet/many-inline-clustered-view/validated-profile.json': 'e353cafb5c6fd3dd774046e273dac808f4c86fb86541e8fef770d7ac20d60903',
 'algorithm-full-primary-packet/many-inline-clustered-view/profile.json': 'a2c13751dcea458043e6772c602abc9cb495bbb920eb692ebc100d28320bf30f',
 'algorithm-full-primary-packet/many-view-internal/cpu.pprof': '05e52cae70af44f331f0620e90161e91013249d982fbc8f995a069c12e57f669',
 'algorithm-full-primary-packet/many-view-internal/validated-profile.json': '3bad267bafa22d4896b8f2af28f83c496edb739becc7e8581b42901d61c44908',
 'algorithm-full-primary-packet/many-view-internal/profile.json': '7ff5f376b638165097101cab2b29d2a3078f93ef2c5407f175d3b62f06c6d0ca',
 'algorithm-full-primary-packet/many-view-internal/top.txt': '3ad4ad9538251c27d89994a28266b7eac31b9b9a22ee1a444fb1487c27aa5516',
 'algorithm-full-primary-packet/many-view-internal/callpath.txt': 'ce38dda4aba020774b859fc6c2ba3b020705fcb655a3c150bbd5852ae3e72962',
 'algorithm-full-normal-retained/algorithm-work.test': 'bb190f318f0a10f22c50d2f92a094b28cb35e2a3f44d734da42551b6777ec543',
 'algorithm-full-counter-retained/algorithm-work.test': '390367fa96037d46282795fe61ba7baf643ad98fbd697930fe273d9c0bb070ab',
 'algorithm-paired-4919-732-packet/normal-prepared/freeze.json': '482f64f7b9dd36f3acd8352ae3a29b31b32403dc7c5d1c25d1c71cd6542d9668',
 'algorithm-paired-4919-732-packet/normal-prepared/algorithm-work.test': 'a1043e3ad4bc7117114575d24b9b0acc28663d010076e8e97d687c1d51ec2a2f',
 'algorithm-paired-4919-732-packet/counter-prepared/freeze.json': '540b79511ad6709c6a27af6d123de596968a6c0e0ebb9df5233a724f299f81dd',
 'algorithm-paired-4919-732-packet/counter-prepared/algorithm-work.test': '5f4841a35cdab13d3cb30dd2eb548e0489cc4516230ed38c14369daa0c925223',
 '4919-singleton-screen-packet/control-normal-pilot-prepared/freeze.json': 'f73e788d7876df3d70cc7502d83c79ba551e8d5f3825a5395874e5fc54ebf6b5',
 '4919-singleton-screen-packet/control-normal-pilot-prepared/algorithm-work.test': '20e69fac0ad85bebc6dc00e13d1360dd841b2076050a8c8845298fe2abff88c7',
 '4919-singleton-screen-packet/candidate-normal-pilot-prepared/freeze.json': '31294d1e297c8452f11328f2635b9ea336e828457a567b285a8fa029332d7108',
 '4919-singleton-screen-packet/candidate-normal-pilot-prepared/algorithm-work.test': '210fb4d93677a46729d1c6bba55cf11326ad7ed4fa58ee97c68f9b4dfdc8c156'}
HASHES.update({'4921-selected-main-five-pair-analysis.json': '33329d7db4892f758d8092a0f3aa4444623d1f3811f0a88322dd24427baceba9', '4921-75f-independent-selected-main-source-review.md': '6ef184a02367e66d7fb3daee3e36a49eaf8a8a773b13b655c5dbcf9b17947e8c', '4921-selected-main-full-paired-plan.json': 'aedd1ed66362201ed5cc7269e0352a65aaced903c20bb1ae7f797864b893ebe4', '4921-selected-main-full-extension-plan.json': 'a1e194b192cf2bd339f65136589df62ffc7d8f4d3511f2f66a3b40059bcb0188', '4921-full-selected-main-packet/all4-preparation-anchor.json': '4bf1548ebf7bd1c29d8441c671a6fbc5c84e2b9b7bac08d88df9e5314a7f059e', '4920-selected-main-three-pair-residency-analysis.json': '7908f7ecf6c2cab50ed93985c07c6a32bebe2308c7c4778a0bf6b19e144ff99d', '4920-selected-main-residency-independent-evidence-review.md': 'cb008a67c3b32c623bd405f9da237757c62435a1f50bc5fb45efd018eadcd4a2', '4920-root-selected-main-residency-disposition.md': '896a2c23190d22e3100eef95ddd96f53bdcfe879236373cdfae6ee9cfd9479ff', '4920-residency-recorder.py': '0171eefeae1b80599b6cf1c91bcbf022671a75c01749cd6963a9150a198cab58', 'persistent-delta-root-activation-disposition.md': 'e25edc6aab176d28195c5a9c5e85378f05b289bb433d067bb3c77ae3286dac83', '4921-full-selected-main-packet/control/normal-prepared/freeze.json': 'fea5745454ddb43206a637a23e4b7317296f101a40aa0b7feb1bfb20879d4349', '4921-full-selected-main-packet/control/normal-prepared/algorithm-work.test': 'f4559a0125776a47df3ceebac35eeacbd605b3a399d9fca09be3b28baa1f5c99', '4921-full-selected-main-packet/control/counter-prepared/freeze.json': '8f8aed0810c4b9baf075b376390efa2d0bcbccc8c008453b5bb3d52ca8e12d6f', '4921-full-selected-main-packet/control/counter-prepared/algorithm-work.test': 'b372484c49583c33685879a7a95a278dfdf8afe81f8341640204c7b0c111e14a', '4920-residency-selected-main-packet/control/prepared/freeze.json': '9d9d714c98e18ee2fbcd249d2322e69a54f58293123ba3a901385745a0cf0e1b', '4920-residency-selected-main-packet/control/prepared/algorithm-work.test': 'c21bade8d841b283709c7a87c07f4a2f453e337a1d5a451d6dbd19cdd27e58fc', '4921-full-selected-main-packet/candidate/normal-prepared/freeze.json': '16b796bb06fbc25593dacf3c96de7cb917e7067afb7d93215e10248af86f658b', '4921-full-selected-main-packet/candidate/normal-prepared/algorithm-work.test': '519cec8a30c6cd365ef2e64426faf8a7bf6bf55f8e743e540b08b9f3a94e211a', '4921-full-selected-main-packet/candidate/counter-prepared/freeze.json': '845c94a226131c8fdcb4687b662d6393d5aacbb91a311bf834f4a3ead36be97f', '4921-full-selected-main-packet/candidate/counter-prepared/algorithm-work.test': '6b4a7e9f357619edc4037d546ef8f77dce741cf8b5db8ac915a60b220f968075', '4920-residency-selected-main-packet/candidate/prepared/freeze.json': 'ed4740e53791ed2d04e0ce4d8ef5468fe4d0f4f99e4a0e384ee287152ed59f39', '4920-residency-selected-main-packet/candidate/prepared/algorithm-work.test': '6f63ba9ee94cf11f0e7491a502644ab169183b90f9791d8212f8f170f120e74c'})
HERE = Path(__file__).resolve().parent

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1<<20),b''): h.update(chunk)
    return h.hexdigest()

def read(path):
    return json.loads(path.read_text(encoding='utf-8'))

def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+
        ['| '+' | '.join(map(str,row))+' |' for row in rows])

def delta(p,key):
    return int(p['after'][key])-int(p['before'][key])

def label(p):
    return f"pointer={p['pointer_threshold']}/CP={p['checkpoints']}/wide={str(p['coalescing_wide']).lower()}"

def stats(values,scale=1):
    m=statistics.median(values)
    return f'{m/scale:.3f} ({(max(values)-min(values))/abs(m)*100:.2f}%)'

def pointer_tables(root, cap, compare, require):
    # Static retained evidence only; never import a recorder's native entry point.
    selected=read(root/'4921-selected-main-five-pair-analysis.json')
    assert (selected['repeats'],selected['primary_cells'],selected['canonical_captures_validated'])==(5,120,12)
    heads={'control':'bc9764ab92a9ceeeb4ae15706c920653e5dd8bbc',
           'candidate':'75fe577ecc6ab81237df2cff58454bbeb205cc71'}
    freezes={}; identities=[]
    for product in heads:
        for mode in ('normal','counter'):
            prepared=root/f'4921-full-selected-main-packet/{product}/{mode}-prepared'
            f=read(prepared/'freeze.json'); freezes[(product,mode)]=f
            assert f['complete'] is True and f['runtime_head']==heads[product]
            assert sha(prepared/'algorithm-work.test')==f['binary_sha256']
            identities.append([product+'/'+mode,f['runtime_head'],f['runtime_tree'],f['harness_sha256'],f['binary_sha256']])
    normal={}; diagnostics={}; seen=set()
    for row in selected['raw_captures']:
        run=row['run_id']; assert run not in seen,run; seen.add(run)
        product=run.rsplit('-',1)[1]; family='internal' if run.startswith('internal-') else 'many'
        packet='4921-full-selected-main-extension-packet' if run.startswith(('many-r4-','many-r5-')) else '4921-full-selected-main-packet'
        directory=root/packet/'runs'/run
        assert set(row['raw_sha256'])=={'stdout.log','stderr.log','execution.json','parsed.json'}
        for name,digest in row['raw_sha256'].items(): assert sha(directory/name)==digest,(run,name)
        e=read(directory/'execution.json'); f=freezes[(product,'counter' if family=='internal' else 'normal')]
        assert e['complete'] is True and e['returncode']==0 and e['family']==family
        assert e['pilot'] is False and e['small_flush'] is False
        assert e['source_before']==e['source_after']
        assert all(e['source_before'][k]==f[k] for k in cap.IDENTITY_KEYS)
        assert e['binary_sha256']==f['binary_sha256'] and e['freeze_sha256']==sha(root/f'4921-full-selected-main-packet/{product}/{"counter" if family=="internal" else "normal"}-prepared/freeze.json')
        for stream in ('stdout','stderr'): assert e[stream+'_sha256']==sha(directory/f'{stream}.log')
        parsed=cap.validate_log((directory/'stdout.log').read_text(encoding='utf-8'),family,f,False,False)
        assert parsed==read(directory/'parsed.json')
        if family=='internal': diagnostics[product]=parsed['internal_counters']
        else: normal[(product,int(run.split('-')[1][1:]))]=parsed['rows']
    assert seen=={f'many-r{i}-{p}' for p in heads for i in range(1,6)}|{f'internal-{p}' for p in heads}
    assert set(selected['cells'])==set(normal[('control',1)]) and len(selected['cells'])==12
    rows=[]
    for name,cell in sorted(selected['cells'].items()):
        pointer,shape,view=name.split('/')[1:]
        key='/'.join([pointer.removeprefix('pointer='),shape,view.removeprefix('view=')])
        assert cell['counts']=={p:diagnostics[p][key] for p in heads}
        for metric,s in cell['metrics'].items():
            assert compare([normal[('control',i)][name]['metrics'][metric] for i in range(1,6)],
                           [normal[('candidate',i)][name]['metrics'][metric] for i in range(1,6)])==s
        s=cell['metrics']['ns/op']; b=cell['metrics']['B/op']; a=cell['metrics']['allocs/op']
        rows.append([name.removeprefix('BenchmarkAlgorithmGetMany/'),f"{s['median_change_pct']:+.3f}",
            '/'.join(f'{x:+.2f}' for x in s['paired_change_pct']),
            f"{s['control']['abs_range_over_abs_median_pct']:.2f}/{s['candidate']['abs_range_over_abs_median_pct']:.2f}",
            f"{b['control']['median']:.0f}→{b['candidate']['median']:.0f}",
            f"{a['control']['median']:.0f}→{a['candidate']['median']:.0f}",'; '.join(cell['guard_flags']) or 'none'])
    residency=read(root/'4920-selected-main-three-pair-residency-analysis.json')
    assert (residency['repeats'],residency['processes'],len(residency['cells']))==(3,6,12)
    assert residency['materiality_flags']==[]
    module=ast.parse((root/'4920-residency-recorder.py').read_text(encoding='utf-8'))
    nodes=[n for n in module.body if (isinstance(n,ast.FunctionDef) and n.name in ('integer','packets')) or
           (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('SOURCE_SHA','CACHE_KEYS','CELLS') for t in n.targets))]
    ns={'json':json,'re':re,'itertools':itertools,'d':SimpleNamespace(require=require)}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'retained residency validator','exec'),ns)
    raw={}; seen=set()
    for row in residency['retained_raw']:
        run=row['run_id']; assert run not in seen,run; seen.add(run)
        product=run.rsplit('-',1)[1]; prepared=root/f'4920-residency-selected-main-packet/{product}/prepared'
        f=read(prepared/'freeze.json'); assert f['runtime_head']==heads[product] and f['complete'] is True
        assert sha(prepared/'algorithm-work.test')==f['binary_sha256']==row['binary_sha256']
        if run.startswith('many-r1-'):
            identities.append([product+'/separate-residency',f['runtime_head'],f['runtime_tree'],f['harness_sha256'],f['binary_sha256']])
        directory=root/'4920-residency-selected-main-packet/runs'/run
        assert set(row['actual_files_sha256'])=={'diagnostic.json','diagnostic.stdout','diagnostic.stderr','parsed.json','preflight.json','postflight.json'}
        for name,digest in row['actual_files_sha256'].items(): assert sha(directory/name)==digest,(run,name)
        e=read(directory/'diagnostic.json'); pre=read(directory/'preflight.json'); post=read(directory/'postflight.json')
        assert e['exit_code']==0 and pre['source_before']==post['source_after']
        assert all(pre['source_before'][k]==f[k] for k in cap.IDENTITY_KEYS)
        assert pre['freeze_sha256']==sha(prepared/'freeze.json') and post['binary_sha256']==f['binary_sha256']
        for stream in ('stdout','stderr'): assert e[stream+'_sha256']==sha(directory/f'diagnostic.{stream}')
        parsed=read(directory/'parsed.json')
        assert ns['packets']((directory/'diagnostic.stdout').read_text(encoding='utf-8'),f,e['argv'],parsed[0]['provenance']['host'])==parsed
        for item in parsed:
            c=item['cell']; key='/'.join(['pointer' if c['pointer'] else 'inline',c['shape'],'view' if c['view'] else 'owned'])
            raw[(product,int(run.split('-')[1][1:]),key)]=c
    assert seen=={f'many-r{i}-{p}' for p in heads for i in range(1,4)}
    mem=[]
    for key,cell in sorted(residency['cells'].items()):
        fields={'post_gc_heap_alloc_bytes':('post_gc_memory','heap_alloc_bytes'),
                'phase_end_hwm_bytes':('end_proc','hwm_bytes')}
        values=[]
        for metric,(section,field) in fields.items():
            samples={p:[raw[(p,i,key)][section][field] for i in range(1,4)] for p in heads}
            s=cell['metrics'][metric]; assert compare(samples['control'],samples['candidate'])==s
            assert all(b-a<=max(4<<20,.15*a) for a,b in zip(samples['control'],samples['candidate']))
            values.append(f"{s['control']['median']/2**20:.3f}→{s['candidate']['median']/2**20:.3f}")
        mem.append([key,*values,'none'])
    return [table(['Actual retained executable','Measured source HEAD (not final merge)','Frozen TreeDB tree','Harness SHA256','Actual executable SHA256'],identities),
            table(['Selected75f cell','Median latency %','Paired r1–r5 %','Latency spread control/candidate %','B/op medians','allocs/op medians','Retained guard flags'],rows),
            table(['Separate residency cell','Post-GC HeapAlloc MiB medians','Approx phase-end VmHWM MiB medians','Material paired heap/HWM flags'],mem)]

def render(root):
    # Existing canonical log validator; its normal/counter separation is unchanged.
    spec=importlib.util.spec_from_file_location('capture',HERE/'capture.py')
    cap=importlib.util.module_from_spec(spec); spec.loader.exec_module(cap)
    freeze=read(root/'algorithm-full-normal-freeze.json')
    assert freeze['complete'] is True and freeze['runtime_head']=='a51b4795cebc194c7a158b262242668877932934'
    assert sha(root/'algorithm-full-normal-retained/algorithm-work.test')==freeze['binary_sha256']
    packets=[]
    for i in range(1,4):
        directory=root/f'algorithm-full-primary-packet/writes-r{i}'
        execution=read(directory/'execution.json')
        assert execution['returncode']==0 and execution['complete'] is True
        assert execution['source_before']==execution['source_after']
        assert all(execution['source_before'][k]==freeze[k] for k in cap.IDENTITY_KEYS)
        assert execution['binary_sha256']==freeze['binary_sha256']
        for stream in ('stdout','stderr'): assert execution[stream+'_sha256']==sha(directory/f'{stream}.log')
        parsed=cap.validate_log((directory/'stdout.log').read_text(encoding='utf-8'),'writes',freeze,False,False)
        assert parsed==read(directory/'parsed.json')
        ps=parsed['write_packets']; assert len(ps)==8 and len({label(p) for p in ps})==8
        for p in ps:
            assert (p['keys'],p['updates'],p['commits'],p['flush_threshold'])==(250000,40000,40,64<<20)
            assert p['final_close_checked'] and p['validated_all_values_and_misses']
            assert delta(p,'treedb.cache.flush_merge.applied_ops_total')==40000
            assert all(delta(p,'treedb.cache.flush_backlog_coalescing.'+k)==0 for k in
                ('admitted_runs_total','admitted_extra_memtables_total','admitted_extra_bytes_total','admitted_extra_ops_total'))
        packets.append(ps)
    work=[]; costs=[]
    for name in sorted(label(p) for p in packets[0]):
        ps=[next(p for p in run if label(p)==name) for run in packets]
        def vals(key): return [p[key] for p in ps]
        def ds(key): return [delta(p,key) for p in ps]
        fields=['treedb.flush_apply.old_leaf_read_decode.leaf_log_node_loads_total',
                'treedb.flush_apply.old_leaf_read_decode.node_loads_total',
                'treedb.flush_apply.old_leaf_read_decode.bytes_total',
                'treedb.flush_apply.merge_build.leaf_page_bytes_written_total',
                'treedb.flush_apply.read_only_prepare.spans_total']
        numbers=[ds(k) for k in fields]; assert all(len(set(x))==1 for x in numbers)
        work.append([name,*[x[0] for x in numbers],f'{numbers[2][0]/40000:.3f}',
            stats(vals('interval_ns'),1e6),stats(vals('ack_ns'),1e6),stats(vals('checkpoint_ns'),1e6)])
        costs.append([name,stats(vals('allocated_bytes'),2**20),stats(vals('allocations')),
            stats(vals('post_reopen_gc_heap_bytes'),2**20),stats(vals('read_p99_ns'),1e3),stats(vals('read_p999_ns'),1e3),
            f"{min(vals('read_samples'))}–{max(vals('read_samples'))}",
            stats([p['kernel_process_io_after']['write_bytes']-p['kernel_process_io_before']['write_bytes'] for p in ps],2**20)])
    counter=read(root/'algorithm-full-counter-freeze.json')
    assert sha(root/'algorithm-full-counter-retained/algorithm-work.test')==counter['binary_sha256']
    actual=cap.validate_log((root/'algorithm-full-primary-packet/internal/stdout.log').read_text(encoding='utf-8'),'internal',counter,False,False)
    assert actual==read(root/'algorithm-full-primary-packet/internal/parsed.json')
    assert actual['internal_counters']==read(root/'algorithm-full-primary-inspection.json')['internal_counters']
    natural=[]
    for i in range(1,4):
        directory=root/f'algorithm-full-primary-packet/many-r{i}'
        execution=read(directory/'execution.json')
        assert execution['complete'] is True and execution['returncode']==0
        assert execution['source_before']==execution['source_after']
        assert all(execution['source_before'][k]==freeze[k] for k in cap.IDENTITY_KEYS)
        assert execution['binary_sha256']==freeze['binary_sha256']
        for stream in ('stdout','stderr'): assert execution[stream+'_sha256']==sha(directory/f'{stream}.log')
        parsed=cap.validate_log((directory/'stdout.log').read_text(encoding='utf-8'),'many',freeze,False,False)
        assert parsed==read(directory/'parsed.json') and len(parsed['rows'])==12
        natural.append(parsed['rows'])
    initial=[]
    for name in sorted(natural[0]):
        cells=[run[name]['metrics'] for run in natural]
        pointer,shape,view=name.split('/')[1:]
        key='/'.join([pointer.removeprefix('pointer='),shape,view.removeprefix('view=')])
        count=actual['internal_counters'][key]
        initial.append([name.removeprefix('BenchmarkAlgorithmGetMany/'),
            stats([x['ns/op'] for x in cells],1e3),stats([x['B/op'] for x in cells]),
            stats([x['allocs/op'] for x in cells]),count['actual'],count['potential_union'],count['repeated']])
    # Reuse the existing reducer's pure arithmetic only, never its Linux/live launcher.
    module=ast.parse((root/'algorithm-paired-4919-732-extend-reduce.py').read_text(encoding='utf-8'))
    selected=ast.Module(body=[n for n in module.body if isinstance(n,ast.FunctionDef) and n.name in ('spread','compare')],type_ignores=[])
    def require(ok,message):
        if not ok: raise ValueError(message)
    ns={'statistics':statistics,'d':SimpleNamespace(require=require)}
    exec(compile(selected,'retained canonical arithmetic','exec'),ns)
    shared=read(root/'algorithm-paired-4919-732-five-repeat-analysis.json')
    singleton=read(root/'4919-singleton-five-pair-independent-screen-analysis.json')
    for product in ('control','candidate'):
        prepared=root/f'4919-singleton-screen-packet/{product}-normal-pilot-prepared'
        assert sha(prepared/'algorithm-work.test')==read(prepared/'freeze.json')['binary_sha256']
    normal={}; diagnostics={}
    candidate_freezes={mode:read(root/f'algorithm-paired-4919-732-packet/{mode}-prepared/freeze.json') for mode in ('normal','counter')}
    for mode,f in candidate_freezes.items():
        assert sha(root/f'algorithm-paired-4919-732-packet/{mode}-prepared/algorithm-work.test')==f['binary_sha256']
    for row in shared['raw_captures']:
        run=row['run_id']; product=run.rsplit('-',1)[1]
        packet='algorithm-paired-4919-732-extension-packet' if run.startswith(('many-r4-','many-r5-')) else 'algorithm-paired-4919-732-packet'
        directory=root/packet/'runs'/run
        for name,digest in row['raw_sha256'].items(): assert sha(directory/name)==digest,(run,name)
        execution=read(directory/'execution.json'); family='internal' if run.startswith('internal-') else 'many'
        f=candidate_freezes['counter' if family=='internal' else 'normal'] if product=='candidate' else counter if family=='internal' else freeze
        assert execution['complete'] is True and execution['returncode']==0 and execution['family']==family
        assert execution['binary_sha256']==f['binary_sha256']
        assert execution['source_before']==execution['source_after']
        assert all(execution['source_before'][k]==f[k] for k in cap.IDENTITY_KEYS)
        parsed=cap.validate_log((directory/'stdout.log').read_text(encoding='utf-8'),family,f,False,False)
        assert parsed==read(directory/'parsed.json')
        if family=='internal': diagnostics[product]=parsed['internal_counters']
        else:
            repeat=int(run.split('-')[1][1:])
            normal[(product,repeat)]=parsed['rows']
    assert set(normal)=={(p,i) for p in ('control','candidate') for i in range(1,6)}
    rows=[]; screen=[]
    for name,cell in sorted(shared['cells'].items()):
        pointer,shape,view=name.split('/')[1:]
        key='/'.join([pointer.removeprefix('pointer='),shape,view.removeprefix('view=')])
        assert cell['counts']=={p:diagnostics[p][key] for p in ('control','candidate')}
        for metric,s in cell['metrics'].items():
            samples={}
            for product in ('control','candidate'):
                metrics=[normal[(product,i)][name]['metrics'] for i in range(1,6)]
                samples[product]=[1e9/m['ns/op']*(64 if metric=='64keys/s' else 1) for m in metrics] if metric in ('ops/s','64keys/s') else [m[metric] for m in metrics]
            assert ns['compare'](samples['control'],samples['candidate'])==s
        s=cell['metrics']['ns/op']; b=cell['metrics']['B/op']; a=cell['metrics']['allocs/op']
        rows.append([name.removeprefix('BenchmarkAlgorithmGetMany/'),f"{s['median_change_pct']:+.3f}",
            '/'.join(f'{x:+.2f}' for x in s['paired_change_pct']),
            f"{s['control']['abs_range_over_abs_median_pct']:.2f}/{s['candidate']['abs_range_over_abs_median_pct']:.2f}",
            f"{cell['counts']['control']['actual']}→{cell['counts']['candidate']['actual']}",
            f"{b['control']['median']:.0f}→{b['candidate']['median']:.0f}",
            f"{a['control']['median']:.0f}→{a['candidate']['median']:.0f}"])
    for name,cell in sorted(singleton['cells'].items()):
        for metric,s in cell.items(): assert ns['compare'](s['control']['values'],s['candidate']['values'])==s
        s=cell['ns/op']; b=cell['B/op']; a=cell['allocs/op']
        screen.append([name.removeprefix('BenchmarkAlgorithmGetMany/'),f"{s['median_change_pct']:+.4f}",
            '/'.join(f'{x:+.2f}' for x in s['paired_change_pct']),
            f"{s['control']['abs_range_over_abs_median_pct']:.2f}/{s['candidate']['abs_range_over_abs_median_pct']:.2f}",
            '/'.join(f'{x:+.3f}' for x in b['paired_change_pct']),
            f"{a['control']['median']:.0f}→{a['candidate']['median']:.0f}"])
    return '\n\n'.join([table(['Primary cell','Leaf-log loads Δ','All old-node loads Δ','Old-node bytes Δ','Leaf output bytes Δ','Prepare spans Δ','Old-node B/update','Interval ms (spread)','Ack ms (spread)','Checkpoint ms (spread)'],work),
        table(['Primary cell','Process allocated MiB (spread)','Process allocations (spread)','Reopen GC MiB (spread)','Read p99 µs (spread)','Read p999 µs (spread)','Stride-16 samples range','Kernel write MiB Δ (spread)'],costs),
        table(['Primary natural cell','µs/64-input API+consumer (spread)','B/op (spread)','allocs/op (spread)','Actual loads/128 calls','Potential union','Repeated'],initial),
        table(['Full shared cell','Median latency %','Paired r1–r5 %','Latency spread control/candidate %','Actual internal loads','B/op medians','allocs/op medians'],rows),
        table(['Singleton pilot cell','Median latency %','Paired r1–r5 %','Latency spread control/candidate %','Paired B/op r1–r5 %','allocs/op medians'],screen),
        *pointer_tables(root,cap,ns['compare'],require)])

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root',type=Path,required=True)
    parser.add_argument('--print-tables',action='store_true')
    args=parser.parse_args(); root=args.evidence_root
    doc=HERE/'ALGORITHM_DECISION.md'; text=doc.read_text(encoding='utf-8')
    for name,digest in HASHES.items():
        assert sha(root/name)==digest,name
        row=f'| `{name}` | `{digest}` |'
        assert text.splitlines().count(row)==1,f'artifact row differs or repeats: {name}'
        assert sum(line.startswith(f'| `{name}` |') for line in text.splitlines())==1,f'duplicate artifact: {name}'
    expected=render(root)
    if args.print_tables: print(expected); return
    actual=text.split('<!-- canonical-tables:start -->\n',1)[1].split('\n<!-- canonical-tables:end -->',1)[0]
    assert actual==expected,'canonical table differs'
    print('PASS: actual retained binaries; primary and five-pair raw packets; canonical arithmetic; exact artifact rows and decision tables')

if __name__=='__main__': main()
