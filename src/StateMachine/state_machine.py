#
# aws state machine
#

import boto3

from json import loads, dumps
from copy import deepcopy
from deepdiff import DeepDiff 

from functools import cached_property

REGION = 'us-west-2'

CAPACITY = 0

if CAPACITY == 0:
    BILLING_MODE = 'PAY_PER_REQUEST'
else:
    BILLING_MODE = 'PROVISIONED'

class StateMachine:
    region = REGION

    @property
    def credentials(self):
        sts_client = boto3.client('sts')

        assume = sts_client.assume_role(
            RoleArn=self.role,
            RoleSessionName="StateMachineSession"
        )

        return assume['Credentials']

    @cached_property
    def dynamo(self):
        creds = self.credentials
            
        return boto3.resource('dynamodb',
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )

    def create_machine_table(self, environment, machine, **kwargs):
        definition = {
            'AttributeDefinitions': [
                {
                    'AttributeName': 'instance',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'    
                },
                {
                    'AttributeName': 'state',
                    'AttributeType': 'S'
                }
            ],
            'TableName': machine + "_" + environment,
            'KeySchema': [
                {
                    'AttributeName': 'instance',
                    'KeyType': 'HASH'
                }
            ],
            'BillingMode': BILLING_MODE
        }

    if BILLING_MODE == 'PROVISIONED':
        definition['ProvisionedThroughput'] = {
            'ReadCapacityUnits': CAPACITY,
            'WriteCapacityUnits': CAPACITY,
        }

    if kwargs:
        definition['Tags'] = []

        for tag_key, tag_value in kwargs:
            definition['Tags'].append({
                'Key': tag_key,
                'Value': tag_value,
            })


    def __init__(self, machine, profile, region=REGION):
        session_default = {
            'profile_name': profile
        }
            
        self.region = region

        boto3.setup_default_session(**session_default)




        if dynamodb_resource is None:

            machine_table=dynamodb_resource.Table(TABLE)

        return machine_table

        response = dynamodb_client.create_table(

class MachineStorageError(Exception):
    def __init__(self, machine, system):
        super().__init__("machine: %s" % machine, "system: %s" % system)


class StateMachine:
    machines = None
    lazy = False

    machine_key = None
    machine_system = None

    state = None

    loaded = None

    def fetch_or_create(self, state):
        fetched = self.machines.get_item(Key={primaryKeyName: self.machine_key,
                                              sortKeyName: self.machine_system})
        if not fetched:
            new_data = {'machine': self.machine_key,
                       'system': self.machine_system,
                       'state': state,
                       'data': {state: {}}}

            response = self.machines.put_item(Item=new_data)

            if not response:
                raise MachineStorageError(self.machine_key, self.machine_system)

            self.loaded = new_data['data'][state]

        else:
            fetched = json.loads(fetched)
            self.loaded = fetched['data'][state]

        return self.loaded


    def persist(self, Force=False):
        dirty = False
        if DeepDiff(self.loaded, self.__dict__):
            dirty = True

        if Force or (self.lazy is False and dirty is True):
            updated_data = self.loaded
            updated_data.update(self.__dict__)

            response = self.machines.update_item(Key={'machine': self.machine_key},
                                                 UpdateExpression="SET #state = :state, #data[\"%\"] = :data" % self.state,
                                                 ExpressionAttributeValues={':state': self.state,
                                                                            ':data': updated_data},
                                                 ReturnValues='UPDATED_NEW')

            if not response and updated_data:
                raise MachineStorageError(self.machine_key, self.machine_system)

           self.loaded[self.state] = updated_data


    def flush(self):
        self.persist(Force=True)

    def __init__(self, state, machine, system, lazy=False):
        self.machines = dynamo_machines()
        self.lazy = lazy

        self.state = state

        self.machine_key = machine
        self.machine_group = system

        self.loaded = self.fetch_or_create(state)

        self.__dict__ = copy.deepcopy(self.loaded)


    def __assign__(self, state):
            self.persist()
            loaded = self.fetch_or_create(state)

            self.state = state
            self.__dict__ = copy.deepcopy(loaded)






