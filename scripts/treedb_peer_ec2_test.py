import copy
import datetime as dt
import unittest

import treedb_peer_ec2 as deploy


def spec():
    now = dt.datetime.now(dt.timezone.utc)
    nodes = []
    for index, zone in enumerate('abc'):
        nodes.append(dict(node_id='node-' + zone, private_ip='10.0.0.' + str(index + 10), az='us-east-1' + zone, az_id='use1-az' + str(index+1),
                          subnet_id='subnet-' + str(index+1), image_id='ami-123', image_owner_account='123456789012', security_group_ids=['sg-123'], instance_type='m7i.large', instance_profile='treedb-nodes',
                          vcpus=2, gomaxprocs=2, memory_bytes=8 << 30, go_memory_limit_bytes=6 << 30, inflight_bytes=256 << 20,
                          data_gib=32, raft_gib=16, required_data_bytes=8 << 30, required_raft_bytes=1 << 30, temp_bytes=1 << 30, required_temp_bytes=1 << 30,
                          data_root='/mnt/data', raft_root='/mnt/raft', temp_root='/mnt/temp', config_sha256='c'*64, all_in_hourly_usd=1, user_data='#!/bin/sh\nexit 0\n'))
    return dict(schema_version=1, account='123456789012', region='us-east-1', run_id='fixture-12345678', binary_architecture='amd64', binary_sha256='b'*64, shared_config_sha256='d'*64,
                source_revision='a'*40, expires_at=(now + dt.timedelta(hours=1)).isoformat(), duration_hours=2, max_cost_usd=10,
                network_reserve_usd=1, retained_storage_reserve_usd=1, price_checked_at=now.isoformat(), expiry_role_arn='arn:aws:iam::123456789012:role/treedb-expiry',
                cloudformation_role_arn='arn:aws:iam::123456789012:role/treedb-cloudformation', flow_log_id='fl-123', peer_ports=[7100,7200], nodes=nodes, groups=[dict(group_id='catalog', voters=['node-a', 'node-b', 'node-c'])])


class FakeAWS:
    def __init__(self, plan, state='ROLLBACK_COMPLETE'):
        self.plan = plan
        self.state = state
        self.stack = 'arn:aws:cloudformation:us-east-1:123456789012:stack/treedb-fixture-12345678/unique-id'
        self.receipt = dict(plan_sha256=plan['plan_sha256'], stack_id=self.stack, change_set_id='change-set')
        self.tags = deploy.tags(plan)
        self.mutations = []

    def call(self, service, operation, value):
        if operation == 'get-caller-identity': return dict(Account='123456789012')
        if operation == 'describe-stacks':
            return dict(Stacks=[dict(StackId=self.stack, StackStatus=self.state, RoleARN=self.plan["inventory"]["cloudformation_role_arn"], Tags=[dict(Key=k, Value=v) for k,v in self.tags.items()])])
        if operation == 'describe-change-set':
            return dict(StackId=self.stack, Status='CREATE_COMPLETE', ExecutionStatus='AVAILABLE')
        if operation == 'get-template': return dict(TemplateBody=self.plan['template'])
        if operation == 'delete-stack':
            self.mutations.append((operation,value)); self.state='DELETE_IN_PROGRESS'; return {}
        if operation == 'execute-change-set': self.mutations.append((operation,value));return {}
        raise AssertionError((service,operation,value))


class LiveInventoryAWS(FakeAWS):
    def __init__(self, plan):
        super().__init__(plan)
        self.bad_az = False
        self.missing_flow_fields = False

    def call(self, service, operation, value):
        if operation == 'describe-subnets':
            n=next(n for n in self.plan['inventory']['nodes'] if n['subnet_id']==value['SubnetIds'][0])
            return dict(Subnets=[dict(AvailabilityZone=n['az'], AvailabilityZoneId='wrong' if self.bad_az else n['az_id'], CidrBlock='10.0.0.0/24', VpcId='vpc-123')])
        if operation == 'describe-instance-types':
            return dict(InstanceTypes=[dict(VCpuInfo=dict(DefaultVCpus=2), MemoryInfo=dict(SizeInMiB=8192), ProcessorInfo=dict(SupportedArchitectures=['x86_64']))])
        if operation == 'describe-images':
            return dict(Images=[dict(State='available', OwnerId='123456789012', Architecture='x86_64')])
        if operation == 'describe-security-groups':
            return dict(SecurityGroups=[dict(VpcId='vpc-123', IpPermissions=[dict(IpProtocol='tcp', FromPort=7100, ToPort=7100, UserIdGroupPairs=[dict(GroupId='sg-123',UserId='123456789012')])])])
        if operation == 'describe-flow-logs':
            fields='${bytes} ${az-id} ${flow-direction} ${interface-id} ${srcaddr} ${dstaddr} ${pkt-srcaddr} ${pkt-dstaddr} ${log-status}'
            return dict(FlowLogs=[dict(ResourceId='vpc-123', TrafficType='ALL', FlowLogStatus='ACTIVE', DeliverLogsStatus='SUCCESS', LogFormat='' if self.missing_flow_fields else fields)])
        if operation == 'create-change-set':
            self.mutations.append((operation,value))
            return dict(StackId=self.stack,Id='change-set')
        return super().call(service,operation,value)


class DeploymentTests(unittest.TestCase):
    def test_plan_has_persistent_pair_private_nic_and_expiry_before_compute(self):
        plan=deploy.make_plan(spec())
        deploy.verify_plan(plan)
        resources=plan['template']['Resources']
        self.assertEqual(resources['Data0']['DeletionPolicy'],'Retain')
        self.assertEqual(resources['Raft0']['DeletionPolicy'],'Retain')
        self.assertEqual(resources['Node0']['DependsOn'],'Expiry')
        self.assertFalse(resources['Node0']['Properties']['NetworkInterfaces'][0]['AssociatePublicIpAddress'])
        self.assertIn('AWS::StackId',resources['Expiry']['Properties']['Target']['Input']['Fn::Sub'])

    def test_failure_domain_capacity_disposable_roots_and_budget_refuse(self):
        for field in ['az','memory','disk','temp','cost','public']:
            s=spec()
            if field=='az':
                for n in s['nodes']:n['az_id']='use1-az1'
            if field=='memory':s['nodes'][0]['go_memory_limit_bytes']=9<<30
            if field=='disk':s['nodes'][0]['data_gib']=1
            if field=='temp':s['nodes'][0]['temp_root']='/mnt/data/temp'
            if field=='cost':s['max_cost_usd']=1
            if field=='public':s['nodes'][0]['private_ip']='8.8.8.8'
            with self.subTest(field=field),self.assertRaises(deploy.Refused):deploy.make_plan(s)

    def test_partial_provisioning_cleanup_is_scoped_and_idempotent(self):
        plan=deploy.make_plan(spec());aws=FakeAWS(plan,'ROLLBACK_COMPLETE')
        deploy.cleanup(aws,plan,aws.receipt)
        deploy.cleanup(aws,plan,aws.receipt)
        aws.state='DELETE_COMPLETE';deploy.cleanup(aws,plan,aws.receipt)
        self.assertEqual(len(aws.mutations),1)
        self.assertEqual(aws.mutations[0][1]['StackName'],aws.stack)
        self.assertNotIn('RetainResources',aws.mutations[0][1])

    def test_wrong_tags_or_receipt_refuse_every_mutation(self):
        for operation in (deploy.apply,deploy.cleanup):
            plan=deploy.make_plan(spec());aws=FakeAWS(plan)
            aws.tags['TreeDBRun']='someone-elses-run'
            with self.assertRaises(deploy.Refused):operation(aws,plan,aws.receipt)
            self.assertFalse(aws.mutations)
            aws.tags=deploy.tags(plan)
            receipt=copy.deepcopy(aws.receipt);receipt['stack_id']=receipt['stack_id'].replace('123456789012','999999999999')
            with self.assertRaises(deploy.Refused):operation(aws,plan,receipt)
            self.assertFalse(aws.mutations)

    def test_live_inventory_checked_before_changeset_and_exact_apply(self):
        for missing in ('bad_az','missing_flow_fields'):
            plan=deploy.make_plan(spec());aws=LiveInventoryAWS(plan)
            setattr(aws,missing,True)
            with self.assertRaises(deploy.Refused):deploy.stage(aws,plan)
            self.assertFalse(aws.mutations)
        plan=deploy.make_plan(spec());aws=LiveInventoryAWS(plan)
        receipt=deploy.stage(aws,plan)
        self.assertEqual([m[0] for m in aws.mutations],['create-change-set'])
        deploy.apply(aws,plan,receipt)
        self.assertEqual([m[0] for m in aws.mutations],['create-change-set','execute-change-set'])
        self.assertEqual(aws.mutations[-1][1]['StackName'],aws.stack)

    def test_template_mutation_refuses_before_aws(self):
        plan=deploy.make_plan(spec());aws=FakeAWS(plan)
        plan['template']['Resources']['Data0']['DeletionPolicy']='Delete'
        with self.assertRaises(deploy.Refused):deploy.apply(aws,plan,aws.receipt)
        self.assertFalse(aws.mutations)


if __name__=='__main__':unittest.main()
