#!/usr/bin/env python3
"""Pure bounded controls for the fixed PRIMARY/M0 hosted source binding."""
import ast
import base64
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('primary_m0_control_events',HERE/'primary_m0_control_events.py')
EVENTS=importlib.util.module_from_spec(spec)
spec.loader.exec_module(EVENTS)


def complete():
    rows=[]
    for pkg in EVENTS.TOPS:
        rows.append({'Action':'start','Package':pkg})
        for p,name in sorted(EVENTS.REQUIRED):
            if p==pkg:
                rows.extend([{'Action':'run','Package':pkg,'Test':name},{'Action':'pass','Package':pkg,'Test':name}])
        rows.append({'Action':'pass','Package':pkg})
    return rows


class FixedHostedControls(unittest.TestCase):
    def consume(self,rows):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'events.jsonl'
            path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            return EVENTS.validate(path)

    def test_exact_inventory(self):
        r=self.consume(complete())
        self.assertEqual((r['tops'],r['outcomes'],r['leaves']),(6,26,24))

    def test_missing_duplicate_and_unfinished(self):
        rows=complete()
        first=next(r['Test'] for r in rows if 'Test' in r)
        missing=[r for r in rows if r.get('Test')!=first]
        for changed in (missing,rows+rows[1:2],rows[:-1],rows[:2]+rows[3:]):
            with self.assertRaises(ValueError):self.consume(changed)

    def test_failure_skip_foreign_and_race(self):
        for event in ({'Action':'fail','Package':next(iter(EVENTS.TOPS))},
                      {'Action':'skip','Package':next(iter(EVENTS.TOPS))},
                      {'Action':'start','Package':'foreign'},
                      {'Action':'output','Package':next(iter(EVENTS.TOPS)),'Output':'WARNING: DATA RACE'}):
            with self.assertRaises(ValueError):self.consume([event]+complete())

    def test_transport_and_unknown_child(self):
        pkg=next(iter(EVENTS.TOPS))
        self.consume([{'Action':'build-output','ImportPath':pkg+'.test','Output':'compiler'}]+complete())
        for event in ({'Action':'build-fail','ImportPath':pkg}, {},
                      {'Action':'build-output','ImportPath':'foreign','Output':'compiler'},
                      {'Action':'run','Package':pkg,'Test':next(iter(EVENTS.TOPS[pkg]))+'/foreign'}):
            with self.assertRaises(ValueError):self.consume([event]+complete())

    def test_authority_refuses_wrong_owner_head_capture_and_review(self):
        spec=importlib.util.spec_from_file_location('primary_m0_hosted_controls',HERE/'primary_m0_hosted_controls.py')
        entry=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(entry)  # Definitions and stdlib only; main is not invoked.
        head='a'*40
        grant={'schema':'primary-m0-hosted-controls-admission-v1','status':'ADMITTED_CONTROLS_ONLY',
               'authority':'ROOT','actor_id':'1981537','expected_head':head,'run_attempt':'1',
               'owner_manifest_sha256':entry.OWNER_MANIFEST,'owner_review_sha256':entry.OWNER_REVIEW,
               'allow_product_capture':False,'independent_binding_review_sha256':'b'*64}
        environment={'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REPOSITORY':'snissn/gomap',
                     'GITHUB_ACTOR_ID':'1981537','GITHUB_RUN_ATTEMPT':'1',
                     'GITHUB_REF':'refs/heads/diagnostic/primary-m0-controls-01',
                     'REVIEWED_HEAD':head,'GITHUB_SHA':head,'WORKFLOW_COMMIT':head}
        for key,value in ((None,None),('authority','foreign'),('expected_head','c'*40),
                          ('allow_product_capture',True),('independent_binding_review_sha256','')):
            chosen=dict(grant)
            if key is not None:chosen[key]=value
            selected=dict(environment,ROOT_ADMISSION_B64=base64.b64encode(json.dumps(chosen).encode()).decode())
            with mock.patch.dict(entry.os.environ,selected,clear=True):
                if key is None:self.assertEqual(entry.authority(),grant)
                else:
                    with self.assertRaises(RuntimeError):entry.authority()

    def test_exact_source_entry_contract(self):
        source=(HERE/'primary_m0_hosted_controls.py').read_text()
        tree=ast.parse(source)
        functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
        main=ast.get_source_segment(source,functions['main'])
        self.assertLess(main.index('admission=authority()'),main.index('manifest=authenticate()'))
        self.assertLess(main.index('manifest=authenticate()'),main.index("kernel=load("))
        self.assertIn('SOURCE_BUILD_CONTROLS_PASS_CAPTURE_NOT_ADMITTED',source)
        self.assertNotIn("'-test.bench=",source)
        self.assertNotIn('provision_go(',source)
        self.assertNotIn('args.mode',source)
        workflow=(HERE.parent/'workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
        self.assertIn('diagnostic/primary-m0-controls-01',workflow)
        self.assertIn("go-version: '1.26.10'",workflow)
        self.assertIn("if: always()",workflow)


if __name__=='__main__':
    unittest.main()
