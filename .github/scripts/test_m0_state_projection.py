#!/usr/bin/env python3
"""Pure synthetic controls: no artifact, Go/tool, network or worker execution."""
import copy
import io
import json
import unittest
import ast
import base64
import hashlib
import os
from pathlib import Path
import tempfile
import time
from unittest.mock import patch
import m0_trace_consumer as c


def fixture():
    owner={'Schema':'m0-ordinal-observer-v1','Status':'ORDINALS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING',
           'Reason':'','DiagnosticOnly':True,'Iterations':1,'Bound':4096,'Written':4096,
           'Lost':False,'Joined':True,'TimerStopped':True,'StatsObserved':True,'ExposureChecked':True,
           'CheckedClose':True,'TraceEnabledAtStart':True,'TraceEnabledAtEnd':True,'PhaseHookCount':1,
           'Points':2048,'Ranges':2048,'ExposureMisses':0,'ForegroundError':False,'VacuumError':False,
           'OriginalDurationsMatch':True,'Stats':{'ConcurrentMutationAborts':0},'Records':[]}
    lines=[b'M=1 P=0 G=-1 StateTransition Time=0 GoID=7 Undetermined->Running Reason=""\n']
    for i in range(4096):
        kind='set' if i%2==0 else 'delete-range'
        owner['Records'].append({'Iteration':0,'Ordinal':i,'Kind':kind,'Error':False,
            'Status':'success','Region':c.identity(i),'Recorded':True,'DurationNS':i//2})
        lines.append(f'M=1 P=0 G=7 RegionBegin Time={i*1000+100} Task=0 Type="{c.identity(i)}"\n'.encode())
        if i==0:
            lines.extend([
                b'M=1 P=0 G=7 StateTransition Time=200 GoID=7 Running->Runnable Reason="preempted"\n',
                b'M=1 P=0 G=-1 StateTransition Time=220 GoID=7 Runnable->Running Reason=""\n',
                b'M=1 P=0 G=7 StateTransition Time=240 GoID=7 Running->Waiting Reason="select"\n',
                b'TransitionStack=\n',b'\tresource.wait @ 0xab\n',b'\t\tC:/source/wait.go:12\n',
                b'\n',b'Stack=\n',b'\twaker.other @ 0xcd\n',b'\t\t/other.go:15\n',
                b'\n',b'M=2 P=1 G=99 StateTransition Time=260 GoID=7 Waiting->Runnable Reason=""\n',
                b'M=1 P=0 G=-1 StateTransition Time=280 GoID=7 Runnable->Running Reason=""\n',
                b'M=1 P=0 G=7 StateTransition Time=300 GoID=7 Running->Syscall Reason=""\n',
                b'M=1 P=0 G=7 StateTransition Time=330 GoID=7 Syscall->Running Reason=""\n'])
        lines.append(f'M=1 P=0 G=7 RegionEnd Time={i*1000+500} Task=0 Type="{c.identity(i)}"\n'.encode())
    decoder=dict(normal_return=True,stdout_eof=True,stderr_eof=True,retained_writes_closed=True,exit=0)
    aligned=c.consume(owner,iter(lines),decoder)
    return owner,aligned,lines


class Controls(unittest.TestCase):
    def setUp(self):
        self.owner,self.alignment,self.lines=fixture();self.outputs={k:[]for k in ('operations','segments','stacks')}
    def run_projection(self):
        return c.project_states(self.owner,self.alignment,iter(self.lines),lambda n,r:self.outputs[n].append(r))
    def replace(self,old,new):
        self.lines=[line.replace(old,new) for line in self.lines]
    def test_complete_four_states_all_population(self):
        r=self.run_projection();self.assertEqual(r['records'],4096)
        self.assertEqual(len(self.outputs['operations']),4096)
        self.assertEqual(self.outputs['operations'][0]['state_ns'],dict(Running=310,Runnable=40,Waiting=20,Syscall=30))
        self.assertEqual(r['trace_region_sum_ns'],4096*400)
        self.assertEqual(r['envelope_ns'],r['trace_region_sum_ns']+r['between_regions_ns'])
    def test_target_resource_not_waker(self):
        r=self.run_projection();self.assertEqual(r['state_ns']['Waiting'],20)
        self.assertTrue(any(s['entry']['executing_goroutine']==99 for s in self.outputs['segments']))
    def test_other_target_ignored(self):
        self.lines.insert(2,b'M=1 P=0 G=7 StateTransition Time=150 GoID=999 Running->Waiting Reason="select"\n')
        self.assertEqual(self.run_projection()['state_ns']['Waiting'],20)
    def test_transition_stack_not_event_stack(self):
        self.run_projection();frames=self.outputs['stacks'][0]['frames']
        self.assertEqual(frames[0]['function'],'resource.wait');self.assertEqual(frames[0]['file'],'C:/source/wait.go')
    def test_missing_resource_stack_unavailable(self):
        i=self.lines.index(b'TransitionStack=\n');del self.lines[i:i+3]
        r=self.run_projection();self.assertEqual(r['stack_unavailable_ns'],50)
        self.assertEqual(r['status'],'COMPLETE_STATE_WITH_STACK_GAPS')
        self.assertEqual(self.outputs['stacks'],[])
    def test_same_state_no_reopen(self):
        self.lines.insert(2,b'M=1 P=0 G=-1 StateTransition Time=150 GoID=7 Running->Running Reason="status"\n')
        self.assertEqual(self.run_projection()['same_state_status_events'],1)
        self.assertEqual(self.outputs['operations'][0]['state_ns']['Running'],310)
    def test_rank_ties_percentile(self):
        r=self.run_projection();rows=self.outputs['operations']
        self.assertEqual(r['p99_ns'],2027);self.assertEqual(rows[4094]['rank_descending'],1)
        self.assertEqual(rows[4095]['rank_descending'],2);self.assertEqual(r['at_or_above_p99_operations'],42)
    def test_exact_transition_block_hash_offset_and_stack_changes(self):
        first=self.lines.index(b'M=1 P=0 G=7 StateTransition Time=240 GoID=7 Running->Waiting Reason="select"\n')
        last=first+1
        while last<len(self.lines) and not self.lines[last].startswith(b'M='):
            last+=1
        raw=b''.join(self.lines[first:last]);offset=sum(map(len,self.lines[:first]))
        self.run_projection()
        entry=next(r['entry'] for r in self.outputs['segments'] if r['state']=='Waiting')
        self.assertEqual(entry['parsed_byte_offset'],offset)
        self.assertEqual(entry['raw_block_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertNotEqual(entry['raw_block_sha256'],hashlib.sha256(self.lines[first]).hexdigest())
        for old,new in ((b'resource.wait @ 0xab',b'resource.wait @ 0xaf'),
                        (b'waker.other @ 0xcd',b'waker.other @ 0xce')):
            changed=[r.replace(old,new) for r in self.lines]
            block=next(b for b in c.event_blocks(changed) if b[2]==offset)
            self.assertEqual(block[0][0],self.lines[first][:-1].decode())
            self.assertEqual(block[3],hashlib.sha256(raw.replace(old,new)).hexdigest())
            self.assertNotEqual(block[3],entry['raw_block_sha256'])
        final=b'M=1 P=0 G=7 StateTransition Time=9999999 GoID=7 Running->Running Reason="status"\n\n'
        block=list(c.event_blocks(final.splitlines(keepends=True)))[0]
        self.assertEqual(block[3],hashlib.sha256(final).hexdigest())
    def test_ascending_indices_complete_and_ordinal_ties(self):
        self.run_projection();rows=self.outputs['operations']
        expected=sorted(range(4096),key=lambda i:(self.owner['Records'][i]['DurationNS'],i))
        self.assertEqual(sorted(r['index_ascending'] for r in rows),list(range(4096)))
        self.assertEqual([rows[i]['index_ascending'] for i in expected],list(range(4096)))
        self.assertEqual((rows[0]['index_ascending'],rows[1]['index_ascending']),(0,1))
        self.assertEqual((rows[4094]['index_ascending'],rows[4095]['index_ascending']),(4094,4095))
    def test_negative_interpretation(self):
        r=self.run_projection()
        for key in ('running_is_cpu','select_is_queue_service_split','phase_assignment','M0_CV_acceptance','historical_cause'):
            self.assertIs(r[key],False)
    def test_initial_unknown_refused(self):
        del self.lines[0]
        with self.assertRaises(ValueError):self.run_projection()
    def test_chain_mismatch_refused(self):
        self.replace(b'Waiting->Runnable',b'Running->Runnable')
        with self.assertRaises(ValueError):self.run_projection()
    def test_unknown_state_and_escaped_reason_refused(self):
        for old,new in ((b'Running->Runnable',b'Running->NewState'),(b'Reason="select"',b'Reason="sel\\x65ct"')):
            original=self.lines;self.lines=[l.replace(old,new)for l in original]
            with self.assertRaises(ValueError):self.run_projection()
            self.lines=original
    def test_unpaired_duplicate_excess_stack_refused(self):
        for action in ('unpaired','duplicate','excess'):
            original=self.lines;self.lines=list(original)
            i=self.lines.index(b'TransitionStack=\n')
            if action=='unpaired':del self.lines[i+2]
            elif action=='duplicate':self.lines.insert(i,b'TransitionStack=\n')
            else:self.lines[i+1:i+1]=[b'\tf @ 0xab\n',b'\t\t/f.go:1\n']*257
            with self.assertRaises(ValueError):self.run_projection()
            self.lines=original
    def test_time_regression_and_endpoint_refused(self):
        self.replace(b'Time=220',b'Time=190')
        with self.assertRaises(ValueError):self.run_projection()
    def test_truncated_stream_or_region_refused(self):
        self.lines[-1]=self.lines[-1][:-1]
        with self.assertRaises(ValueError):self.run_projection()
    def test_alignment_and_owner_mismatch_refused(self):
        self.alignment['alignment'][0]['owner_duration_ns']+=1
        with self.assertRaises(ValueError):self.run_projection()
    def test_output_failure_propagates(self):
        def fail(name,row):raise OSError('injected retained write failure')
        with self.assertRaisesRegex(OSError,'injected'):
            c.project_states(self.owner,self.alignment,iter(self.lines),fail)
    def test_state_entered_before_region_and_equal_time(self):
        # Same-time status before region begin must not invent a duration.
        self.lines.insert(1,b'M=1 P=0 G=-1 StateTransition Time=100 GoID=7 Running->Running Reason=""\n')
        self.assertEqual(self.run_projection()['records'],4096)

    def test_notexist_inside_region_refused(self):
        self.replace(b'Running->Syscall',b'Running->NotExist')
        with self.assertRaises(ValueError):self.run_projection()
    def test_reader_budget_and_failure_reserve(self):
        module=ast.parse(Path(__file__).with_name('four_gate_artifact_readback.py').read_text())
        nodes=[n for n in module.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in ('need','RetainedOutputs')]
        env={'Path':Path,'time':time,'json':json,'os':os,'MIB':1024**2}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'extracted-fixed-output-controls','exec'),env)
        with tempfile.TemporaryDirectory() as tmp:
            writer=env['RetainedOutputs'](Path(tmp))
            with self.assertRaises(ValueError):writer.check(32*1024**2)
            writer.started-=31
            with self.assertRaises(ValueError):writer.write('normal.json',{})
            with patch.object(os,'statvfs',return_value=type('FS',(),{'f_bavail':3000000,'f_frsize':4096})()):
                writer.write('failure.json',{'status':'INCOMPLETE'},4096,adverse=True)
            with self.assertRaises(ValueError):writer.emit('unknown',{})
            writer.close()
    def test_authority_missing_foreign_and_review_mismatch(self):
        module=ast.parse(Path(__file__).with_name('four_gate_artifact_readback.py').read_text())
        nodes=[n for n in module.body if isinstance(n,ast.FunctionDef) and n.name in ('need','authority')]
        env={'os':os,'base64':base64,'json':json,'hashlib':hashlib,
             'ROOT_THREAD':'01a18e7c-8c23-7fd2-8d55-b0486d938cd3',
             'EXPECTED_SHA':'artifact','OUTPUT_ADMISSION_SHA':'receipt'}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'extracted-fixed-authority-controls','exec'),env)
        review=b'actual reviewed source bytes';head='a'*40
        admission=dict(authority='ROOT',authority_thread=env['ROOT_THREAD'],actor_id='1981537',
            schema='root-m0-state-wall-projection-v1',status='ROOT_DATA_PROJECTION_ADMITTED',
            reader_head=head,artifact=13390292576,artifact_sha256='artifact',
            original_output_admission_sha256='receipt',no_recapture=True,
            independent_review_sha256=hashlib.sha256(review).hexdigest())
        values=dict(GITHUB_SHA=head,EXPECTED_HEAD=head,WORKFLOW_SHA=head,GITHUB_ACTOR_ID='1981537',
                    GITHUB_RUN_ATTEMPT='1',GITHUB_REF='refs/heads/diagnostic/m0-common-write-original-data-01',
                    M0_DATA_REVIEW_B64=base64.b64encode(review).decode())
        for change in ('match','missing','foreign','review'):
            row=dict(admission)
            if change=='missing':del row['authority_thread']
            elif change=='foreign':row['authority_thread']='foreign'
            elif change=='review':row['independent_review_sha256']='0'*64
            values['M0_DATA_ADMISSION_B64']=base64.b64encode(json.dumps(row).encode()).decode()
            with patch.dict(os.environ,values,clear=True):
                if change=='match':self.assertEqual(env['authority']()[1],review)
                else:
                    with self.assertRaises(ValueError):env['authority']()
    def test_exact_reader_inputs_and_small_member_scope(self):
        source=Path(__file__).with_name('four_gate_artifact_readback.py').read_text()
        module=ast.parse(source)
        constants={n.targets[0].id:ast.literal_eval(n.value) for n in module.body
                   if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)
                   and n.targets[0].id in ('MEMBERS','NAMES','EXPECTED_BYTES','EXPECTED_SHA')}
        self.assertEqual(constants['EXPECTED_BYTES'],2733520)
        self.assertEqual(len(constants['NAMES']),18)
        self.assertEqual(len(constants['MEMBERS']),23)
        self.assertTrue(set(constants['NAMES'])<=set(constants['MEMBERS']))
        self.assertNotIn('trace.out', ''.join(constants['MEMBERS']))
        self.assertIn('need(reproduced==aligned',source)
        self.assertIn("companion.validate_service(owner,service)",source)
        root=Path(__file__).resolve().parents[2]
        manifest=json.loads((root/'.github/m0-common-write-inputs.json').read_text())
        workflow=(root/'.github/workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
        sparse=workflow.split('          sparse-checkout: |\n',1)[1].split('          sparse-checkout-cone-mode:',1)[0]
        self.assertTrue(set(manifest['files'])<=set(sparse.split()))


if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(Controls)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful() or result.skipped or suite.countTestCases()!=23:
        raise SystemExit(1)
    print(json.dumps({'status':'M0_STATE_PROJECTION_PURE_CONTROLS_PASS','methods':23},sort_keys=True))
