#!/usr/bin/env python3
"""Finite pure fixtures for the fixed parsed-debug-text consumer; no Go/tools."""
import ast
import copy
import base64
import hashlib
import json
import os
import re
from unittest import mock
import importlib.util
import io
from pathlib import Path
import unittest

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('m0_trace_consumer',HERE/'m0_trace_consumer.py')
C=importlib.util.module_from_spec(spec);spec.loader.exec_module(C)


def owner():
    r={'Schema':'m0-ordinal-observer-v1','Status':'ORDINALS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING',
       'Reason':'','DiagnosticOnly':True,'Iterations':1,'Bound':4096,'Written':4096,'Lost':False,
       'Joined':True,'TimerStopped':True,'StatsObserved':True,'ExposureChecked':True,'CheckedClose':True,
       'TraceEnabledAtStart':True,'TraceEnabledAtEnd':True,'PhaseHookCount':1,'Points':2048,'Ranges':2048,
       'ExposureMisses':0,'ForegroundError':False,'VacuumError':False,'OriginalDurationsMatch':True,
       'Stats':{'ConcurrentMutationAborts':0},'Records':[]}
    for n in range(4096):
        r['Records'].append({'Iteration':0,'Ordinal':n,'Kind':'set' if n%2==0 else 'delete-range',
            'DurationNS':n+1,'Error':False,'Status':'success','Region':C.identity(n),'Recorded':True})
    return r


def lines():
    result=[]
    for n in range(4096):
        for event,t in (('RegionBegin',n*10),('RegionEnd',n*10+5)):
            result.append(f'M=12 P=0 G=17 {event} Time={t} Task=0 Type="{C.identity(n)}"\n'.encode())
    return result


def decoder():
    return {'exit':0,'normal_return':True,'stdout_eof':True,'stderr_eof':True,'retained_writes_closed':True}


def extracted_authority():
    # Execute only the pure admission predicate in hosted controls, never import
    # the worker/main or launch helpers. Fixture digest override below is test
    # isolation; the production literal is separately asserted unchanged.
    tree=ast.parse((HERE/'m0_operation_trace.py').read_text())
    names={'ROOT_THREAD','OWNER_MANIFEST','OWNER_REVIEW','CONTROLS_PASS_SHA','CONTROLS_HEAD','CONTROLS_TREE'}
    nodes=[n for n in tree.body if isinstance(n,ast.Assign) and
           any(isinstance(t,ast.Name) and t.id in names for t in n.targets) or
           isinstance(n,ast.FunctionDef) and n.name in ('need','authority')]
    scope={'os':os,'re':re,'base64':base64,'json':json,'hashlib':hashlib}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<extracted-admission-predicate>','exec'),scope)
    return scope


def authority_fixture():
    scope=extracted_authority();head='a'*40;thread=scope['ROOT_THREAD']
    controls={'schema':'root-primary-m0-controls-pass-v1','status':'ROOT_NORMAL_RACE_CONTROLS_ADMITTED',
      'actor_id':'1981537','authority_thread':thread,'producer_head':scope['CONTROLS_HEAD'],
      'producer_tree':scope['CONTROLS_TREE'],'owner_manifest_sha256':scope['OWNER_MANIFEST'],
      'normal':{'tops':24,'outcomes':82,'leaves':74},'race':{'tops':24,'outcomes':82,'leaves':74},
      'literal_normal_exit':0,'literal_race_exit':0,'positive_outer_guard_cleanup':True,
      'product_capture':False,'run_id':1,'artifact_id':1,'artifact_sha256':'b'*64,
      'independent_review_sha256':'958811fd4b5c0a25ada2595567b1c70f5fa2fbf90cff67c1e6b76da1dd39b9e4',
      'owner_source_review_sha256':scope['OWNER_REVIEW']}
    raw=json.dumps(controls,sort_keys=True).encode();review=b'Fixture independent source review; not a runtime receipt.\n'
    fixture_sha=hashlib.sha256(raw).hexdigest()
    assert scope['CONTROLS_PASS_SHA']=='ed5c3761d40fb3319b8303eaf62a3546ab9d6c4e4c631464de23b0be6f0cfd2b'
    scope['CONTROLS_PASS_SHA']=fixture_sha  # Only this isolated pure-fixture namespace.
    admission={'schema':'m0-operation-trace-admission-v1','status':'ADMITTED_ONE_M0_TRACE_ONLY',
      'authority':'ROOT','authority_thread':thread,'actor_id':'1981537','expected_head':head,'run_attempt':'1',
      'owner_manifest_sha256':scope['OWNER_MANIFEST'],'owner_review_sha256':scope['OWNER_REVIEW'],
      'allow_product_capture':True,'only_capture':'M0_bytes_1x_N1_4096','controls_pass_sha256':fixture_sha,
      'independent_binding_review_sha256':hashlib.sha256(review).hexdigest(),
      'independent_binding_review_head':head,'independent_binding_review_status':'READY_SOURCE_ONLY'}
    env={'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REPOSITORY':'snissn/gomap',
      'GITHUB_ACTOR_ID':'1981537','GITHUB_RUN_ATTEMPT':'1','GITHUB_REF':'refs/heads/diagnostic/m0-combiner-operation-capture-01',
      'REVIEWED_HEAD':head,'GITHUB_SHA':head,'WORKFLOW_COMMIT':head,
      'M0_CONTROLS_PASS_B64':base64.b64encode(raw).decode(),
      'M0_BINDING_REVIEW_B64':base64.b64encode(review).decode()}
    return scope,admission,controls,env,review


class FixedM0Consumer(unittest.TestCase):
    def refuse(self,rows=None,receipt=None,transport=None):
        with self.assertRaises((ValueError,UnicodeError)):
            C.consume(owner() if receipt is None else receipt,lines() if rows is None else rows,
                      decoder() if transport is None else transport)

    def test_complete_positive(self):
        result=C.consume(owner(),lines(),decoder())
        self.assertEqual((result['regions'],result['selected_events']),(4096,8192))
        self.assertFalse(result['region_duration_is_original_duration'])
        self.assertFalse(result['M0_CV_acceptance'])
        self.assertEqual(len(result['alignment']),4096)
        self.assertEqual(result['alignment'][0]['owner_duration_ns'],1)
        self.assertEqual(result['alignment'][4095]['begin_trace_ns'],40950)

    def test_missing_begin(self):self.refuse(lines()[1:])
    def test_missing_end(self):self.refuse(lines()[:-1])
    def test_duplicate_begin_or_end(self):
        for n in (0,1):
            rows=lines();rows.insert(n,rows[n]);self.refuse(rows)
    def test_extra_ordinal(self):
        rows=lines();rows.append(b'M=12 P=0 G=17 RegionBegin Time=999999 Task=0 Type="m0/extra"\n')
        self.refuse(rows)
    def test_wrong_kind_iteration_or_name(self):
        for old,new in ((b'/set"',b'/delete-range"'),(b'iteration/0000',b'iteration/0001'),
                        (b'operation/0000',b'operation/0001')):
            rows=lines();rows[0]=rows[0].replace(old,new);self.refuse(rows)
    def test_cross_goroutine_or_task(self):
        for old,new in ((b'G=17',b'G=18'),(b'Task=0',b'Task=1')):
            rows=lines();rows[1]=rows[1].replace(old,new);self.refuse(rows)
    def test_reversed_time_or_order(self):
        rows=lines();rows[3]=rows[3].replace(b'Time=15',b'Time=9');self.refuse(rows)
        rows=lines();rows[0],rows[2]=rows[2],rows[0];self.refuse(rows)
    def test_truncated_decoder_or_failed_exit(self):
        rows=lines();rows[-1]=rows[-1].rstrip(b'\n');self.refuse(rows)
        for key in ('exit','normal_return','stdout_eof','stderr_eof','retained_writes_closed'):
            d=decoder();d[key]=1 if key=='exit' else False;self.refuse(transport=d)
    def test_owner_join_close_error_or_incomplete(self):
        for key,value in (('Joined',False),('CheckedClose',False),('ForegroundError',True),
                          ('Status','INCOMPLETE'),('Written',4095)):
            r=owner();r[key]=value;self.refuse(receipt=r)
    def test_original_duration_refusal(self):
        r=owner();r['OriginalDurationsMatch']=False;self.refuse(receipt=r)
        r=owner();r['Records'][0]['DurationNS']=-1;self.refuse(receipt=r)
    def test_unknown_selected_event_or_grammar(self):
        for row in (b'M=12 P=0 G=17 Unknown Time=0 Name="m0/unknown"\n',
                    b'M=12 P=0 G=17 RegionBegin Time=0 Task=0 Type="m0/unknown" Extra=1\n'):
            self.refuse([row]+lines())
    def test_owner_duplicate_record(self):
        r=owner();r['Records'][1]=copy.deepcopy(r['Records'][0]);self.refuse(receipt=r)
    def test_retained_io_failure(self):
        class Short:
            def write(self,data):return len(data)-1
        with self.assertRaisesRegex(ValueError,'short write'):C.checked_write(Short(),b'abc')
        class Broken:
            def write(self,data):raise OSError('retained write failure')
        with self.assertRaisesRegex(OSError,'retained write failure'):C.checked_write(Broken(),b'abc')
        def broken_read():
            yield lines()[0]
            raise OSError('retained reader failure')
        with self.assertRaisesRegex(OSError,'retained reader failure'):C.consume(owner(),broken_read(),decoder())
        sink=io.BytesIO();C.checked_write(sink,b'abc');self.assertEqual(sink.getvalue(),b'abc')
    def test_fixed_entry_source_contract(self):
        source=(HERE/'m0_operation_trace.py').read_text();ast.parse(source)
        self.assertIn("'-d=parsed'",source)
        self.assertIn('consumer.checked_write(outputs[name],chunk)',source)
        self.assertIn("row[name+'_eof']=True",source)
        self.assertIn('stream.flush();os.fsync(stream.fileno())',source)
        self.assertIn('WHOLE_SECONDS = 1020',source)
        self.assertIn("GOMEMLIMIT='8GiB',GOMAXPROCS='2'",source)
        self.assertIn("spec=descriptions",source)
        self.assertNotIn('descriptions[\'focused_controls\']',source)
        self.assertNotIn("'./TreeDB/mvcc'",source)
        self.assertNotIn('shell=True',source)
        self.assertNotIn('HOME=',source)
        workflow=(HERE.parent/'workflows/treedb-mvcc-maintenance-diagnostic.yml').read_text()
        thread='01a18e7c-8c23-7fd2-8d55-b0486d938cd3'
        self.assertEqual(workflow.count("'authority_thread':'"+thread+"'"),2)
        self.assertIn("hashlib.sha256(review_raw).hexdigest()==admission.get('independent_binding_review_sha256')",workflow)

    def call_authority(self,scope,admission,env):
        env=dict(env,ROOT_ADMISSION_B64=base64.b64encode(json.dumps(admission).encode()).decode())
        with mock.patch.dict(os.environ,env,clear=True):return scope['authority']()

    def test_matching_root_thread_and_actual_review_bytes(self):
        scope,admission,controls,env,review=authority_fixture()
        actual=self.call_authority(scope,admission,env)
        self.assertEqual(actual,(admission,controls,review))

    def thread_refusal(self,where,value):
        scope,admission,controls,env,review=authority_fixture()
        target=admission if where=='admission' else controls
        if value is None:target.pop('authority_thread')
        else:target['authority_thread']=value
        if where=='controls':env['M0_CONTROLS_PASS_B64']=base64.b64encode(json.dumps(controls).encode()).decode()
        with self.assertRaisesRegex(RuntimeError,where+' immutable ROOT authority thread'):
            self.call_authority(scope,admission,env)

    def test_missing_admission_thread(self):self.thread_refusal('admission',None)
    def test_foreign_admission_thread(self):self.thread_refusal('admission','foreign-thread')
    def test_missing_controls_thread(self):self.thread_refusal('controls',None)
    def test_foreign_controls_thread(self):self.thread_refusal('controls','foreign-thread')

    def test_review_bytes_or_head_mismatch(self):
        scope,admission,controls,env,review=authority_fixture()
        env['M0_BINDING_REVIEW_B64']=base64.b64encode(review+b'altered').decode()
        with self.assertRaisesRegex(RuntimeError,'actual final independent review bytes'):
            self.call_authority(scope,admission,env)
        scope,admission,controls,env,review=authority_fixture()
        admission['independent_binding_review_head']='b'*40
        with self.assertRaisesRegex(RuntimeError,'ROOT one-M0 diagnostic admission'):
            self.call_authority(scope,admission,env)


def service_fixture():
    original=owner()
    value={'Schema':'m0-combiner-service-observer-v1',
        'Status':'SERVICE_ROWS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING','Reason':'',
        'DiagnosticOnly':True,'OriginalWriterJoined':True,'OriginalDoneObserved':True,
        'OriginalCloseReturned':True,'OriginalCloseError':'NIL','Interpretation':'fixture','Rows':[]}
    for n in range(4096):
        c={k:{'NS':0,'Valid':False} for k in ('Entry','EnqueueBefore','EnqueueAfter','SelectBefore','SelectAfter','Return')}
        c.update(EnqueueStatus='',Branch='',Error='',Entered=False,Invalid=False)
        w={k:{'NS':0,'Valid':False} for k in ('Dequeue','ApplyBefore','ApplyAfter','WriteBefore','WriteAfter','CloseBefore','CloseAfter','SendBefore','SendAfter')}
        w.update(BatchSerial=0,BatchIndex=0,BatchSize=0,Selected=0,Dispatch='',WriteKind='',WriteError='',CloseError='',IgnoredCloseError=False,Terminal=False,Invalid=False)
        if n%2==0:
            c.update({k:{'NS':t,'Valid':True} for k,t in zip(('Entry','EnqueueBefore','EnqueueAfter','SelectBefore','SelectAfter','Return'),(0,1,5,6,10,11))})
            c.update(EnqueueStatus='ENQUEUED',Branch='RESULT',Error='NIL',Entered=True)
            w.update({k:{'NS':t,'Valid':True} for k,t in zip(('Dequeue','ApplyBefore','WriteBefore','WriteAfter','CloseBefore','CloseAfter','ApplyAfter','SendBefore','SendAfter'),(2,3,4,5,6,7,8,9,12))})
            w.update(BatchSerial=n//4+1,BatchIndex=(n%4)//2,BatchSize=2,Selected=2,Dispatch='APPLIED',WriteKind='WRITE_SYNC',WriteError='NIL',CloseError='NIL',Terminal=True)
        value['Rows'].append(dict(Iteration=0,Ordinal=n,Kind=original['Records'][n]['Kind'],OriginalRegion=C.identity(n),OriginalDurationNS=n+1,Status='NON_COMBINER' if n%2 else 'APPLIED_RESULT_RECEIVED',Caller=c,Worker=w))
    return original,value


class FixedServiceConsumer(unittest.TestCase):
    def test_complete_shared_overlap_zero_origin(self):
        o,s=service_fixture();r=C.consume_service(o,s)
        self.assertEqual(r['populations'],{'APPLIED_RESULT_RECEIVED':2048,'NON_COMBINER':2048})
        self.assertEqual(len(r['unique_batches']),1024)
        self.assertEqual(r['rows'][0]['caller_select_ns'],4)
        self.assertFalse(r['M0_CV_acceptance'])
    def test_valid_all_zero_timestamps(self):
        o,s=service_fixture()
        for row in s['Rows']:
            for obj in (row['Caller'],row['Worker']):
                for stamp in obj.values():
                    if type(stamp) is dict:stamp['NS']=0
        C.consume_service(o,s)
    def test_lifecycle_refusals(self):
        for key,val in (('OriginalDoneObserved',False),('OriginalWriterJoined',False),
                        ('OriginalCloseReturned',False),('OriginalCloseError','OTHER'),('Status','INCOMPLETE')):
            o,s=service_fixture();s[key]=val
            with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_original_identity_duration_population(self):
        for key,val in (('Ordinal',2),('OriginalDurationNS',999),('OriginalRegion','wrong'),('Kind','delete-range')):
            o,s=service_fixture();s['Rows'][0][key]=val
            with self.assertRaises(ValueError):C.consume_service(o,s)
        o,s=service_fixture();s['Rows'].pop()
        with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_odd_worker_fields_refused(self):
        o,s=service_fixture();s['Rows'][1]['Worker']['BatchSerial']=1
        with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_absent_regressing_or_bad_timestamp(self):
        for val in ({'NS':0,'Valid':False},{'NS':-1,'Valid':True},{'NS':True,'Valid':True},{'NS':100,'Valid':True}):
            o,s=service_fixture();s['Rows'][0]['Caller']['SelectBefore']=val
            with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_unknown_error_dispatch_or_category(self):
        for key,val in (('Dispatch','UNKNOWN'),('WriteKind','SETUP'),('CloseError','OTHER'),('Terminal',False),('Invalid',True)):
            o,s=service_fixture();s['Rows'][0]['Worker'][key]=val
            with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_shared_conflict_duplicate_or_missing_selected(self):
        for key,val in (('BatchIndex',0),('Selected',1),('ApplyAfter',{'NS':9,'Valid':True})):
            o,s=service_fixture();s['Rows'][2]['Worker'][key]=val
            with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_direct_fallback_legal_and_no_false_worker(self):
        o,s=service_fixture()
        for n in (0,2):
            row=s['Rows'][n];row['Status']='DIRECT_FALLBACK'
            row['Caller']['EnqueueStatus']='NO_CHANNEL';row['Caller']['Branch']='DIRECT_FALLBACK'
            for k in ('EnqueueBefore','EnqueueAfter','SelectBefore','SelectAfter'):row['Caller'][k]={'NS':0,'Valid':False}
            row['Worker']=copy.deepcopy(s['Rows'][1]['Worker'])
        C.consume_service(o,s)
        s['Rows'][0]['Worker']['Dispatch']='APPLIED'
        with self.assertRaises(ValueError):C.consume_service(o,s)
    def test_source_export_after_join_and_close_not_new_capture(self):
        source=(HERE/'m0_operation_trace.py').read_text()
        self.assertIn("consumer.consume_service(owner_receipt,service)",source)
        self.assertIn("'overlay_entries':7",source)
        self.assertIn("'dependencies.json'",source)
        self.assertEqual(source.count("run('one-M0'"),1)


if __name__=='__main__':unittest.main()
