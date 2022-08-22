#
# aws state machine
#

import boto3

from json import loads, dumps
from datetime import datetime

from collections import namedtuple

from functools import cached_property, wraps

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
    def store_state(self, state, data, timestamp):
        self.__dict__ = data
        self.state = state
        self.timestamp = timestamp     

        return state, data

    def __init__(self, machine_name, machine_instance, initial_state, credentials={}):
        self.credentials = credentials
        self.machine_name = machine_name
        self.machine_instance = machine_instance

        self.state = initial_state

        self.timestamp = None

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
        
    def __set_item__(self, event):
        return self.__get_item(self, event)

    @cached_property
    def dynamo(self, **kwargs):            
        return boto3.resource('dynamodb', **self.credentials)

    @cached_property
    def machine_table(self):
        return self.dynamo.Table(MACHINE_TABLE)

    def pull_from_database(self, initialize={}):
        fetched = self.machine_table.get_item(Key={'primaryKeyName': self.machine_instance,
                                                   'sortKeyName': self.machine_name})

        if fetched:
            fetched = loads(fetched)

            database_data = fetched['data']
            database_state = fetched['state']

            database_timestamp = datetime.fromisoformat(fetched['timestamp'])

            if self.timestamp and database_timestamp > self.timestamp:
                self.store_state(database_data['state'], 
                                 database_data, 
                                 database_timestamp)

            return database_state, database_data
        else:
            self.write_to_database(try_load=False)
            
            return self.state, self.__dict__

    def write_to_database(self, try_load=True):
        if try_load:
            try_state, try_data = self.pull_from_database()

            if try_state:
                return try_state, try_data

        new_timestamp = datetime.now()

        entry = {'instance': self.machine_instance,
                 'machine': self.machine_name,
                 'timestamp': new_timestamp.isoformat(),
                 'state': self.state,
                 'data': dumps(self.__dict__)}

        try:
            response = self.machines.put_item(Item=entry)

            if response:
                self.timestamp = new_timestamp
                return self.state, self.__dict__
            else:
                raise MachineStorageError(self.machine_name,
                                          self.machine_instance,
                                          f'No response writing dynamo table.')
        except Exception as error:
            raise MachineStorageError(self.machine_name,
                                      self.machine_instance,
                                      "Exception %s writing dynamo table." % str(error))







