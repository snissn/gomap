#!/usr/bin/env python3
"""Pure source/data tests; never invoke guard, Git, Go or acquisition."""
import ast
import base64
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

HERE=Path(__file__).resolve().parent

def module(name):
    spec=importlib.util.spec_from_file_location(name,HERE/(name+'.py'))
    result=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

EVENTS=module('m0_combiner_control_events')

def complete():
    rows=[{'Action':'start','Package':EVENTS.PKG}]
    for name in sorted(EVENTS.REQUIRED):
        rows.extend([{'Action':'run','Package':EVENTS.PKG,'Test':name},
                     {'Action':'pass','Package':EVENTS.PKG,'Test':name}])
    return rows+[{'Action':'pass','Package':EVENTS.PKG}]

class FixedFixtureControls(unittest.TestCase):
    def consume(self,rows):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'raw.jsonl';p.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            return EVENTS.validate(p)

    def test_exact_population(self):
        r=self.consume(complete())
        self.assertEqual((r['tops'],r['outcomes'],r['leaves']),(24,82,74))
        self.assertEqual(r['status'],'EXACT_FIXTURE_INVENTORY_PASS')
        self.assertEqual(len(EVENTS.CHILDREN),24)

    def test_missing_duplicate_unfinished_fail_foreign_race(self):
        rows=complete()
        for changed in (rows[:-1],rows[:2]+rows[3:],rows+rows[1:2],
            [{'Action':'fail','Package':EVENTS.PKG}]+rows,
            [{'Action':'start','Package':'foreign'}]+rows,
            [{'Action':'run','Package':EVENTS.PKG,'Test':'foreign'}]+rows,
            [{'Action':'output','Package':EVENTS.PKG,'Output':'WARNING: DATA RACE'}]+rows):
            with self.assertRaises(ValueError):self.consume(changed)

    def test_selected_source_has_no_qualifying_platform_skips(self):
        for name in sorted(EVENTS.REQUIRED):
            rows=complete()
            idx=next(i for i,e in enumerate(rows) if e.get('Test')==name and e['Action']=='pass')
            rows[idx]['Action']='skip'
            with self.assertRaises(ValueError):self.consume(rows)

    def test_unknown_skip_refused(self):
        rows=complete();rows[2]['Action']='skip'
        with self.assertRaises(ValueError):self.consume(rows)

    def test_compiler_transport(self):
        self.consume([{'Action':'build-output','ImportPath':EVENTS.PKG+'.test','Output':'compiler'}]+complete())
        with self.assertRaises(ValueError):
            self.consume([{'Action':'build-output','ImportPath':'foreign','Output':'compiler'}]+complete())

    def test_original_owner_input_closure(self):
        entry=module('m0_combiner_hosted_controls')
        entry.authenticate()
        owner=HERE.parent/'m0-combiner-fixture'
        manifest=json.loads((owner/'manifest.json').read_text())
        for row in manifest['files']:
            path=owner/row['path']
            self.assertEqual(path.stat().st_size,row['bytes'])
            self.assertEqual(entry.sha(path),row['sha256'])

    def test_authority_requires_immutable_root_and_no_capture(self):
        entry=module('m0_combiner_hosted_controls');head='a'*40
        grant={'schema':'m0-combiner-hosted-controls-admission-v1','status':'ADMITTED_CONTROLS_ONLY',
            'authority':'ROOT','authority_thread':'01a18e7c-8c23-7fd2-8d55-b0486d938cd3','actor_id':'1981537',
            'expected_head':head,'run_attempt':'1','owner_manifest_sha256':entry.OWNER_MANIFEST,
            'owner_review_sha256':entry.OWNER_REVIEW,'allow_product_capture':False,
            'independent_binding_review_sha256':'b'*64}
        env={'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REPOSITORY':'snissn/gomap',
            'GITHUB_ACTOR_ID':'1981537','GITHUB_RUN_ATTEMPT':'1',
            'GITHUB_REF':'refs/heads/diagnostic/m0-combiner-controls-01',
            'REVIEWED_HEAD':head,'GITHUB_SHA':head,'WORKFLOW_COMMIT':head}
        for key,value in ((None,None),('authority_thread',None),('authority_thread','foreign'),
                          ('expected_head','c'*40),('allow_product_capture',True),('owner_review_sha256','d'*64)):
            g=dict(grant)
            if key is not None:g[key]=value
            with mock.patch.dict(entry.os.environ,dict(env,ROOT_ADMISSION_B64=base64.b64encode(json.dumps(g).encode()).decode()),clear=True):
                if key is None:self.assertEqual(entry.authority(),grant)
                else:
                    with self.assertRaises(RuntimeError):entry.authority()

    def test_exact_group_inventory_and_product_preimages(self):
        owner=HERE.parent/'m0-combiner-fixture'
        inventory=json.loads((owner/'combined-control-inventory.json').read_text())
        self.assertEqual(set(inventory['required_nodes']),EVENTS.REQUIRED)
        self.assertEqual(inventory['selector'],EVENTS.SELECTOR)
        self.assertEqual(inventory['known_platform_skips'],[])
        self.assertEqual({k:(v['tops'],v['outcomes'],v['leaves']) for k,v in inventory['groups'].items()},
            {'new_bridge':(9,44,39),'original_owning':(13,20,18),'qualified_ordinal':(2,18,17)})
        pins=json.loads((owner/'product-preimages.json').read_text())
        self.assertEqual(len(pins),7)
        self.assertEqual(sum(not r['absent'] for r in pins.values()),3)
        proof=json.loads((owner/'source-proof.json').read_text())
        for row in proof['files']:
            self.assertEqual(pins[row['path']],{'sha256':row['exact4934_preimage_sha256'],
                'absent':row['exact4934_path_absent']})
        source=(HERE/'m0_combiner_hosted_controls.py').read_text()
        self.assertIn("preimage=preimages[str(name)]",source)
        self.assertIn("len(replacements)==7",source)
        self.assertLess(source.index("preimages=json.loads"),source.index("prepare_normal_controls(go,overlay"))

    def test_single_db_compile_only_and_budget(self):
        entry=module('m0_combiner_hosted_controls')
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'tmp').mkdir();(root/'output').mkdir();calls=[];receipts=[]
            def run(label,argv,seconds,cwd):
                calls.append((label,argv,seconds,cwd));Path(argv[argv.index('-o')+1]).write_bytes(b'fixture')
            with mock.patch.object(entry.time,'monotonic',return_value=1):
                entry.prepare_normal_controls('/fixed/go',root/'overlay.json',root/'source',root,run,
                    lambda p,r:receipts.append(json.loads(json.dumps(r))))
            self.assertEqual(len(calls),1)
            self.assertEqual(calls[0][1],['/fixed/go','test','-c','-tags=treedb_test',
                '-overlay='+str(root/'overlay.json'),'-p=1','-mod=readonly','-o',
                str(root/'tmp/prepared-normal/db.test'),'./TreeDB/db'])
            self.assertEqual(calls[0][2],240)
            self.assertEqual(receipts[-1]['status'],'NORMAL_PREPARATION_COMPILED_ONLY')
            self.assertFalse(receipts[-1]['test_execution'])

    def test_source_contract_no_capture_no_primary_provenance(self):
        source=(HERE/'m0_combiner_hosted_controls.py').read_text();ast.parse(source)
        for absent in ('primary_m0','primary-m0','source-B',"'bench'",'-test.bench=','provision_go('):
            self.assertNotIn(absent,source)
        self.assertIn('WHOLE_SECONDS = 1020',source)
        self.assertIn("('normal',180,[], '150s'),('race',600,['-race'],'570s')",source)
        self.assertLess(source.index('admission=authority()'),source.index('manifest=authenticate()'))
        self.assertLess(source.index('manifest=authenticate()'),source.index("events=load("))
        self.assertIn("inventory['status']=='EXACT_FIXTURE_INVENTORY_PASS'",source)
        self.assertIn('child.kill()',source);self.assertIn("row['exit']=child.wait()",source)
        workflow=(HERE.parent/'workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
        self.assertIn("go-version: '1.26.10'",workflow)
        self.assertIn('timeout-minutes: 20',workflow)
        self.assertIn("if: always()",workflow)
        self.assertIn("'authority_thread':'01a18e7c-8c23-7fd2-8d55-b0486d938cd3'",workflow)

if __name__=='__main__':unittest.main()
