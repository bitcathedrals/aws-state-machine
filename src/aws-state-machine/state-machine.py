#
# aws state machine
#

from boto3 import resource Table
from json import loads dumps
from copy import deepcopy
from deepdiff import DeepDiff 

TABLE = 'machines'
REGION = 'us-west-2'

dynamodb_resource = None
machine_table = None

def dynamo_machines():
    if dynamodb_resource is None:
        dynamodb_resource = boto3.resource('dynamodb', region_name=REGION)

    if machine_table is None:
        machine_table=dynamodb_resource.Table(TABLE)

    return machine_table


class StateMachine:
    machines = None
    lazy = False

    machine_key = None
    machine_group = None

    state = None

    loaded = None

    def fetch_or_create(self, state):
        fetched = self.machines.get_item(Key={primaryKeyName: self.machine_key,
                                              sortKeyName: self.machine_group})
        if not fetched:
            fetched = {'state': state,
                       'data': {state, {}}}

            response = self.machines.put_item(Item=fetched)

        else:
            fetched = json.loads(fetched)

        return fetched['data'][fetched[state]]


    def persist(self, Force=False)
        dirty = False
        if DeepDiff(self.loaded, self.__dict__):
            dirty = True

        if Force or (self.lazy is False and dirty is True): 
            response = self.machines.update_item(Key={'state': self.state},
                                                 ExpressionAttributeValues={'state': self.state,
                                                                            'data': {self.state: self.__dict__}})
           self.loaded['data'][self.state] = self.__dict__


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






