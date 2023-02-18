#!/usr/bin/env python3

from sensorhub.hub import SensorHub
import pymongo
import time
from datetime import datetime
import bson
import argparse
import sys
import os
import yaml
import pprint

__HUB__:SensorHub = None
__LOG_SAMPLE__ = lambda x: x

def collect_sample():
    global __HUB__
    if __HUB__ is None:
        __HUB__ = SensorHub()

    return {
        'timestamp': datetime.utcnow(),
        'sensor': 'pi4',
        'data': {
            'temperature': __HUB__.get_off_board_temperature(),
            'humidity': __HUB__.get_humidity(),
            'pressure': __HUB__.get_barometer_pressure(),
            'board.temperature': __HUB__.get_temperature(),
            'barometer.temperature': __HUB__.get_barometer_temperature(),
            'motion': __HUB__.is_motion_detected(),
            'brightness': __HUB__.get_brightness(),
        }  
    }

def collect_samples(samplespacing: int, samplenumber: int):
    global __LOG_SAMPLE__
    samples = []
    
    samples.append(collect_sample()) # Initial sample. If sampling size = 1, we don't wait.
    __LOG_SAMPLE__(samples[::-1])
    
    while len(samples) < samplenumber:
        time.sleep(samplespacing)
        samples.append(collect_sample())
        __LOG_SAMPLE__(samples[::-1])
        
    return samples

def submit_samples(samples: list, config: dict, secrets: dict):
    # Replace the uri string with your MongoDB deployment's connection string.
    conn_str = secrets['mongodb_connection_string']
    # set a 5-second connection timeout
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    db = client[config['database_name']]
    if config['collection_name'] not in db.list_collection_names():
        db.command('create', config['collection_name'], timeseries={ 'timeField': 'timestamp', 'granularity': 'seconds' })
    
    col = db[config['collection_name']]
    col.insert_many(samples)

def get_parser():
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Sample the Raspberry Pi SensorHub and upload the records',
    )
    
    parser.add_argument('-c', '--config', help='Set config file', type=str ,default='config.yaml')
    parser.add_argument('-s', '--secrets', help='Set secrets file', type=str ,default='secrets.yaml')
    return parser

def load_settings(configfile:str, secretsfile:str):
    with open(configfile, 'r') as c:
        with open(secretsfile, 'r') as s:
            return (
                yaml.safe_load(c),
                yaml.safe_load(s)
            )
    

def main():
    global __LOG_SAMPLE__
    parser = get_parser()
    print(parser)
    
    args = parser.parse_args()
    print(args)

    config, secrets = load_settings(args.config, args.secrets)
    
    if config['hub']['print_records']:
        __LOG_SAMPLE__ = lambda x: pprint.pprint(x, indent=2)
        
    samples = collect_samples(config['hub']['sample_spacing'], config['hub']['samples'])
    submit_samples(samples, config['hub'], secrets['secrets'])
        
if __name__ == '__main__':
    main()
    