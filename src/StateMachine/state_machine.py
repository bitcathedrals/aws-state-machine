#
# aws state machine
#

import boto3

from json import loads, dumps
from datetime import datetime

from collections import namedtuple
from copy import deepcopy

from functools import cached_property, wraps

from deepdiff import DeepDiff

from config import REGION, MACHINE_TABLE

StateMachineEvent = namedtuple('StateMachineEvent', ['event', 'data'])

StateMachineHalt = namedtuple('StateMachineHalt', ['state', 'data', 'value'])

class MachineStorageError(Exception):
    def __init__(self, machine_name, machine_instance, error):
        super().__init__(self, f'Machine Storage Error: [%s,%s] -> %s', (machine_name,
                                                                         machine_instance,
                                                                         error))

def state(state, other_states=[]):

    def machine_generator(handler):

        @wraps(handler)
        def machine(self, state, **kwargs):
            value, state, data = handler(self, state, **kwargs)

            if state in other_states:
                next = getattr(self, state)

                if next:
                    self.__dict__ = data
                    self.state = state 

                    next_value, next_state, next_data = next(self, state, **kwargs)

                    return next_value, next_state, next_data

                else:
                    raise ValueError('StateMachine -> Unknown state %s' % state)
            else:
                raise ValueError('StateMachine -> handler %s cannot handle state %s' % (str(handler), state))
    
        return machine

    return machine_generator


class StateMachine:
    def update_current_state(self):
        current_machines = self._machines
        current_object = deepcopy(self.__dict__)

        del current_object['_machines']

        old_object = current_machines[self._state]

        if DeepDiff(current_object, old_object):
            current_object['_timestamp'] = datetime.now()  

            current_machines[self._state] = current_object

            self.__dict__ = current_object
            self._machines = current_machines

            return True

        return False
 
    def set_new_data(self, data, timestamp):
        self.update_current_state()

        new_state = data

        new_state['_state'] = self._state
        new_state['_timestamp'] = timestamp
        
        new_machines = self._machines
        new_machines[self._state] = new_state

        new_state['_machines'] = new_machines

        self.__dict__ = new_state

    def switch_new_state(self, new_state):
        self.update_current_state()

        new_data = self._machines[new_state]
        old_machines = self._machines

        new_data['_machines'] = old_machines

        self.__dict__ = new_state

    def __init__(self, machine_name, machine_instance, initial_state, data={}, credentials={}):
        self._credentials = credentials
        self._machine_name = machine_name
        self._machine_instance = machine_instance

        self._state = initial_state

        self._timestamp = None

        self._machines = {}
        self._machines[initial_state] = data
        self.__dict__.update(self._machines[initial_state])

        self.pull_from_database()

    def __get_item__(self, event):
        try:
            self.__dict__ = self.pull_from_database()

            handler = getattr(self, self.state)
 
            if handler:
                value, state, data = handler(self, event.event, **event.data)

                self.__dict__ = data
                self.state = state

                self.write_to_database(self.__dict__)

                return value
            else:
                raise Exception("StateMachine -> unknown state %s" % self.state)

        except StateMachineHalt as halt:
            value, state, data = halt.value, halt.state, halt.data

            self.__dict__ = data
            self.state = state

            self.write_to_database(self.__dict__)

            return value
        
    def __set_item__(self, state):
        self.switch_new_state(state)

    @cached_property
    def dynamo(self, **kwargs):            
        return boto3.resource('dynamodb', **self.credentials)

    @cached_property
    def machine_table(self):
        return self.dynamo.Table(MACHINE_TABLE)

    def pull_from_database(self, initialize={}):
        fetch = self.machine_table.get_item(TableName=MACHINE_TABLE,
                                            Key={'instance': {'N': self.machine_instance},
                                                 'state': {'S': self._state},
                                                 'machine': {'S': self.machine_name}})

        if fetch:
            parsed = loads(fetch)

            database_timestamp = datetime.fromisoformat(parsed['timestamp'])
            
            database_data = parsed['data']
            database_data['_timestamp'] = database_timestamp

            if self._timestamp and database_timestamp > self._timestamp:
                self.set_new_data(database_data, 
                                  database_timestamp)

            return self._state, database_data
        else:
            self.set_new_data(initialize, datetime.now())

            written = self.write_to_database(try_load=False)
            
            return self._state, written

    def write_to_database(self, try_load=True):
        if try_load:
            try_state, try_data = self.pull_from_database()

            if try_state:
                return try_state, try_data

        timestamp = datetime.now()

        data = deepcopy(self.__dict__)
        del data['_machines']
        del data['_timestamp']

        entry = {'instance': self.machine_instance,
                 'state': self._state,
                 'machine': self.machine_name,
                 'timestamp': timestamp.isoformat(),
                 'data': dumps(data)}

        try:
            response = self.machines.put_item(Item=entry)

            if response:
                self._timestamp = timestamp
                
                return self._state, data
            else:
                raise MachineStorageError(self.machine_name,
                                          self.machine_instance,
                                          f'No response writing dynamo table.')
        
        except Exception as error:
            raise MachineStorageError(self.machine_name,
                                      self.machine_instance,
                                      "Exception %s writing dynamo table." % str(error))







