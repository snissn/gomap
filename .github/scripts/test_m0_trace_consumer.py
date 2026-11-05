#!/usr/bin/env python3
"""Finite pure fixtures for the fixed parsed-debug-text consumer; no Go/tools."""
import ast
import copy
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
        self.assertIn("spec=descriptions['m0']['C']",source)
        self.assertNotIn('descriptions[\'focused_controls\']',source)
        self.assertNotIn("'./TreeDB/mvcc'",source)
        self.assertNotIn('shell=True',source)
        self.assertNotIn('HOME=',source)


if __name__=='__main__':unittest.main()
