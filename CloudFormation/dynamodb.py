from os import path

from cfconfig.cloud_config import CloudConfig

cf_config = CloudConfig(".", "devConfigDeployUser")

cf_config.write_module(path.dirname(__file__) + "/src/StatMachine", "config")

REGION = 'us-west-2'

CAPACITY = 0

if CAPACITY == 0:
    BILLING_MODE = 'PAY_PER_REQUEST'
else:
    BILLING_MODE = 'PROVISIONED'

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

