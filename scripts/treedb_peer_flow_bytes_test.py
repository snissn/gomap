import unittest
from treedb_peer_ec2 import Refused,make_plan
from treedb_peer_ec2_test import spec
from treedb_peer_flow_bytes import summarize

FIELDS='${bytes} ${az-id} ${flow-direction} ${interface-id} ${srcaddr} ${dstaddr} ${pkt-srcaddr} ${pkt-dstaddr} ${log-status}'
class FlowTests(unittest.TestCase):
 def test_direction_az_unknown_and_delivery_gaps_remain_distinct(self):
  plan=make_plan(spec())
  result=summarize(plan,[
   '100 use1-az1 egress eni-a 10.0.0.10 10.0.0.11 - - OK\n',
   '100 use1-az2 ingress eni-b 10.0.0.10 10.0.0.11 - - OK\n',
   '50 use1-az1 egress eni-a 10.0.0.10 10.1.0.99 - - OK\n',
   '- - - - - - - - SKIPDATA\n',
  ],FIELDS)
  self.assertEqual(result['nodes']['node-a']['cross_az_egress_bytes'],100)
  self.assertEqual(result['nodes']['node-a']['egress_bytes'],150)
  self.assertEqual(result['nodes']['node-a']['unknown_destination_egress_bytes'],50)
  self.assertEqual(result['nodes']['node-b']['ingress_bytes'],100)
  self.assertEqual(result['incomplete_records'],1)
  self.assertFalse(result['complete'])
 def test_bad_length_and_format_refuse(self):
  plan=make_plan(spec())
  with self.assertRaises(Refused):summarize(plan,['x'*65537],FIELDS)
  with self.assertRaises(Refused):summarize(plan,['100 invalid'],FIELDS)

if __name__=='__main__':unittest.main()
