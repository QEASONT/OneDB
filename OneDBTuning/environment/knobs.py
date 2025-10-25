# -*- coding: utf-8 -*-
"""
desciption: Knob information

"""

import utils
import environment.configs
import collections

# 700GB
memory_size = 360*1024*1024
#
disk_size = 8*1024*1024*1024
instance_name = ''


KNOBS = [
    'spark.odb.globalIndexedPivotCount',
    'spark.odb.sampleSize',
    'spark.sql.odb.shuffle.partitions',
    'spark.sql.shuffle.partitions',
    'spark.odb.estimatedRate',
]



KNOB_DETAILS = None
EXTENDED_KNOBS = None
num_knobs = len(KNOBS)


def init_knobs(num_more_knobs):
    # global instance_name
    global memory_size
    global disk_size
    global KNOB_DETAILS
    global EXTENDED_KNOBS

    KNOB_DETAILS = {
        'spark.odb.globalIndexedPivotCount': ('integer', [300, 400, 500]),
        'spark.odb.sampleSize': ('integer', [10000, 20000, 30000, 50000]),
        'spark.sql.odb.shuffle.partitions': ('integer', [50, 100, 200, 300, 400, 500]),
        'spark.sql.shuffle.partitions': ('integer', [50, 100, 200, 400, 500]),
        'spark.odb.estimatedRate': ('enum', [0.1, 0.3, 0.5, 0.7, 0.9]),
    }

    # TODO: ADD Knobs HERE! Format is the same as the KNOB_DETAILS
    UNKNOWN = 0
    EXTENDED_KNOBS = {

    }
    # ADD Other Knobs, NOT Random Selected
    i = 0
    EXTENDED_KNOBS = dict(sorted(EXTENDED_KNOBS.items(), key=lambda d: d[0]))
    for k, v in EXTENDED_KNOBS.items():
        if i < num_more_knobs:
            KNOB_DETAILS[k] = v
            KNOBS.append(k)
            i += 1
        else:
            break


def get_init_knobs():

    knobs = {}

    for name, value in KNOB_DETAILS.items():
        knob_value = value[1]
        knobs[name] = knob_value[-1]

    return knobs


def gen_continuous(action):
    knobs = {}

    for idx in range(len(KNOBS)):
        name = KNOBS[idx]
        value = KNOB_DETAILS[name]

        knob_type = value[0]
        knob_value = value[1]
        min_value = knob_value[0]

        if knob_type == 'integer':
            max_val = knob_value[1]
            eval_value = int(max_val * action[idx])
            eval_value = max(eval_value, min_value)
        else:
            enum_size = len(knob_value)
            enum_index = int(enum_size * action[idx])
            enum_index = min(enum_size - 1, enum_index)
            eval_value = knob_value[enum_index]

        knobs[name] = eval_value

    return knobs


def save_knobs(knob, metrics, knob_file):
    """ Save Knobs and their metrics to files
    Args:
        knob: dict, knob content
        metrics: list, tps and latency
        knob_file: str, file path
    """
    # format: tps, latency, knobstr: [#knobname=value#]
    knob_strs = []
    for kv in knob.items():
        knob_strs.append('{}:{}'.format(kv[0], kv[1]))
    result_str = '{},{},{},'.format(metrics[0], metrics[1], metrics[2])
    knob_str = "#".join(knob_strs)
    result_str += knob_str

    with open(knob_file, 'a+') as f:
        f.write(result_str+'\n')
