"""Offline framing controls, including execution with Python assertions disabled."""
import pathlib
import struct
import subprocess
import sys
import tempfile
import unittest

import ycsb_reference_wire as wire


def raw_document(elements):
    return struct.pack('<i', len(elements)+5)+elements+b'\0'


class Socket:
    def __init__(self, response):
        self.response=response

    def recv(self, size):
        result,self.response=self.response[:size],self.response[size:]
        return result

    def sendall(self, data):
        self.sent=data


class WireValidation(unittest.TestCase):
    def test_supported_document_round_trip(self):
        value=dict(ok=1, text='binary', payload=b'\0\xff', enabled=True,
                   nested=dict(n=2), items=[1,'two'])
        raw=wire.document(value)
        got,end=wire.decode(raw)
        self.assertEqual(end,len(raw))
        self.assertEqual(got,dict(value,payload=dict(binary_hex='00ff',subtype=0)))

    def test_duplicate_framing_array_and_value_bounds_reject(self):
        integer=b'\x10n\0'+struct.pack('<i',1)
        malformed=[raw_document(integer+integer), b'\x04\0\0\0\0',
                   wire.document(dict(n=1))[:-1]+b'x',
                   raw_document(b'\x08b\0\x02'),
                   raw_document(b'\x02s\0'+struct.pack('<i',4)+b'x\0'),
                   raw_document(b'\x05b\0'+struct.pack('<i',5)+b'\0x')]
        for raw in malformed:
            with self.subTest(raw=raw),self.assertRaises((ValueError,AssertionError,struct.error)):
                wire.decode(raw)
        with self.assertRaises((ValueError,AssertionError)):
            wire.decode(wire.document({'1':1}),array=True)

    def test_response_header_section_trailing_and_EOF_reject(self):
        def frame(body,to=1,opcode=2013):
            return struct.pack('<iiii',len(body)+16,2,to,opcode)+body
        body=b'\0'*5+wire.document(dict(ok=1))
        for raw in [frame(body,to=2),frame(body,opcode=1),
                    frame(b'\1'+body[1:]),frame(body[:4]+b'\1'+body[5:]),
                    frame(body+b'\0'),frame(body)[:-1]]:
            with self.subTest(raw=raw),tempfile.TemporaryDirectory() as directory:
                client=wire.Client.__new__(wire.Client)
                client.sock=Socket(raw);client.out=pathlib.Path(directory);client.counter=0
                with self.assertRaises((ValueError,AssertionError,struct.error)):
                    client.call(dict(ping=1))

    def test_optimized_interpreter_keeps_duplicate_and_framing_rejections(self):
        code='''
import pathlib, struct, tempfile
import ycsb_reference_wire as wire
from test_ycsb_reference_wire import raw_document, Socket
integer=b'\\x10n\\0'+struct.pack('<i',1)
try:
    wire.decode(raw_document(integer+integer))
except ValueError:
    pass
else:
    raise SystemExit('optimized interpreter accepted duplicate BSON key')
body=b'\\0'*5+wire.document(dict(ok=1))
wrong=struct.pack('<iiii',len(body)+16,2,99,2013)+body
with tempfile.TemporaryDirectory() as directory:
    client=wire.Client.__new__(wire.Client)
    client.sock=Socket(wrong);client.out=pathlib.Path(directory);client.counter=0
    try:
        client.call(dict(ping=1))
    except ValueError:
        pass
    else:
        raise SystemExit('optimized interpreter accepted response-to mismatch')
print('optimized duplicate and framing controls rejected')
'''
        result=subprocess.run([sys.executable,'-B','-O','-c',code],cwd=pathlib.Path(__file__).parent,
                              text=True,capture_output=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('optimized duplicate and framing controls rejected',result.stdout)


if __name__=='__main__':
    unittest.main()
