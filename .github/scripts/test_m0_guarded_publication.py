"""Fixed pure AST/text controls; no action/guard/Node/upload execution."""
import ast
from pathlib import Path
import re
import unittest

HERE=Path(__file__).resolve().parent
SOURCE=(HERE/'four_gate_artifact_readback.py').read_text()
TREE=ast.parse(SOURCE)
FUNCTIONS={n.name:n for n in TREE.body if isinstance(n,ast.FunctionDef)}


def need(ok,reason):
    if not ok:raise ValueError(reason)


def outputs(text):
    node=FUNCTIONS['upload_outputs']
    scope={'need':need,'re':re}
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual fixed outputs parser>','exec'),scope)
    return scope['upload_outputs'](text,'snissn/gomap','123')


def output_text():
    values={'artifact-id':'42','artifact-digest':'a'*64,'artifact-url':'https://github.com/snissn/gomap/actions/runs/123/artifacts/42'}
    return ''.join(k+'<<ghadelimiter_12345678-1234-1234-1234-123456789abc\n'+v+'\nghadelimiter_12345678-1234-1234-1234-123456789abc\n'for k,v in values.items())


def completed(**overrides):
    values=dict(error=None,code=0,guard={'errors':[],'cleanup_signals':[],'status':'guard-completed-pending-independent-artifact-review'},
        pub={'status':'UPLOAD_JOINED_PENDING_OUTER_GUARD'},terminal={'status':'DATA_AND_PUBLICATION_CLOSED_PENDING_OUTER_GUARD'})
    values.update(overrides)
    expr=next(n.value for n in ast.walk(FUNCTIONS['launch'])if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='ok'for t in n.targets))
    return eval(compile(ast.Expression(expr),'<actual positive guard predicate>','eval'),values)


class Controls(unittest.TestCase):
    def test_original_output_protocol(self):
        self.assertEqual(outputs(output_text())['artifact-id'],'42')
    def test_duplicate_missing_and_foreign_outputs_refused(self):
        text=output_text()
        for bad in (text+text,'',text.replace('/runs/123/','/runs/999/'),text.replace('artifact-id','other')):
            with self.assertRaises((ValueError,IndexError)):outputs(bad)
    def test_actual_positive_guard_predicate(self):
        self.assertTrue(completed())
        for values in ({'error':ValueError()},{'code':1},{'guard':None},{'pub':None},{'terminal':None},
            {'guard':{'errors':['failed'],'cleanup_signals':[],'status':'guard-completed-pending-independent-artifact-review'}},
            {'guard':{'errors':[],'cleanup_signals':[15],'status':'guard-completed-pending-independent-artifact-review'}},
            {'guard':{'errors':[],'cleanup_signals':[],'status':'failed'}},
            {'pub':{'status':'PUBLICATION_FAILED'}},{'terminal':{'status':'DATA_OR_PUBLICATION_FAILED'}}):
            self.assertFalse(completed(**values))
    def test_no_fabricated_pending_cleanup_observation(self):
        self.assertNotIn('pending_cleanup_signals',ast.get_source_segment(SOURCE,FUNCTIONS['launch']))
    def test_m0_caps_not_primary_caps(self):
        launch=ast.get_source_segment(SOURCE,FUNCTIONS['launch'])
        self.assertIn("'--deadline','180'",launch);self.assertIn("'--cleanup-seconds','10'",launch)
        self.assertIn('MIB*1024',launch);self.assertIn('MIB*10240',launch)
        self.assertIn('writer.check(adverse=True)',launch);self.assertIn('child.wait(timeout=.2)',launch)
        self.assertIn('256*MIB+32*MIB<=1024*MIB',launch)
        self.assertNotIn('840',launch);self.assertNotIn('24*GIB',launch)
    def test_original_data_failure_not_green_publication(self):
        worker=ast.get_source_segment(SOURCE,FUNCTIONS['worker'])
        self.assertLess(worker.index('authority()'),worker.index('main()'))
        self.assertIn('original data then publication failure',worker)
        self.assertIn('if error is not None:raise error',worker)
    def test_immutable_artifact_and_later_receipts(self):
        publication=ast.get_source_segment(SOURCE,FUNCTIONS['publication'])
        self.assertLess(publication.index('paths=sorted'),publication.index('publication.stdout.log'))
        self.assertIn('uploaded immutable member changed',publication)
        self.assertIn('excludes',publication)
        self.assertIn('post-publication-terminal.json',ast.get_source_segment(SOURCE,FUNCTIONS['launch']))
    def test_one_foreground_local_action_no_separate_uploader(self):
        workflow=(HERE.parent/'workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
        self.assertIn('uses: ./.github/actions/m0-common-write',workflow)
        self.assertNotIn('uses: actions/upload-artifact',workflow)
        node=(HERE.parent/'actions/m0-common-write/index.js').read_text()
        self.assertIn("'launch'",node);self.assertIn("child.on('close'",node)
        self.assertNotIn('console.log(process.env',node)
        self.assertNotIn('detached:',node)
        entry=(HERE/'m0_common_write_entry.sh').read_text()
        self.assertLess(entry.index('M0_CONTROLS_OUTCOME'),entry.index('gh api'))
        self.assertIn('90s python3',entry)


if __name__=='__main__':unittest.main(verbosity=2)
