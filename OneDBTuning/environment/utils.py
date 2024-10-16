# -*- coding: utf-8 -*-



import sys
import time
import json


import requests

# import xmlrpclib

# import service.client as client


# from configs import instance_config
# from warnings import filterwarnings



from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import get_return_value


class SparkClient:
    def __init__(self):
        pass

    def say_hello(server_ip, port):
        gateway = JavaGateway(gateway_parameters=GatewayParameters(address=server_ip))
        hello_app = gateway.entry_point
        value = hello_app.sayHello("qt")
        return value

    def modify_configurations(server_ip, port, configuration):
        gateway = JavaGateway(gateway_parameters=GatewayParameters(address=server_ip))
        hello_app = gateway.entry_point
        # 'SAMPLE_SIZE' = 10485760
        # 'GLOBAL_INDEXED_PIVOT_COUNT' = 9
        # 'RTREE_GLOBAL_MAX_ENTRIES_PER_NODE' = 5
        # 'RTREE_LOCAL_MAX_ENTRIES_PER_NODE' = 5
        # 'RTREE_GLOBAL_NUM_PARTITIONS' = 20
        # 'RTREE_LOCAL_NUM_PARTITIONS' = 20
        value = hello_app.runSpark(
            configuration["SAMPLE_SIZE"],
            configuration["GLOBAL_INDEXED_PIVOT_COUNT"],
            configuration["RTREE_GLOBAL_MAX_ENTRIES_PER_NODE"],
            configuration["RTREE_LOCAL_MAX_ENTRIES_PER_NODE"],
            configuration["RTREE_GLOBAL_NUM_PARTITIONS"],
            configuration["RTREE_LOCAL_NUM_PARTITIONS"],
        )

        
        return list(value)




def time_start():
    return time.time()


def time_end(start):
    end = time.time()
    delay = end - start
    return delay


def get_onedb_state(server_ip, port):
    response = SparkClient.say_hello(server_ip, port)
    if response == "Hello, hi":
        print("Greeter client received: " + response)
        return True
    else:
        return False


def modify_configurations(server_ip, server_port, configuration):
    response = SparkClient.modify_configurations(server_ip, server_port, configuration)
    return response


if __name__ == "__main__":
    a = get_onedb_state("10.214.131.7", 111)
    c = 0
