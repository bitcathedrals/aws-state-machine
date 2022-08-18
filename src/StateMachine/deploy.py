from os import path

from cfconfig.cloud_config import CloudConfig

cf_config = CloudConfig(".", "devConfigDeployUser")

cf_config.write_module(path.dirname(__file__) + "/src/StatMachine", "config")
