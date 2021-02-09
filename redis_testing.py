import json
import redis
import numpy as np
import yaml
import time
import re

with open('/Users/Bart/meerkat_target_selector/test/channels.txt', 'r') as f:
    channel = f.read()
    
with open('/Users/Bart/meerkat_target_selector/test/messages.txt', 'r') as f:
    messages = f.read()

chnls = channel.split('\n')
msgs = messages.split('\n')

def publish_key(chan, key, val):
    r.set(key, val)
    r.publish(chan, key)
    return True

r = redis.StrictRedis()

pool_resources = 'bluse_1,cbf_1,m001,m005,m012,m015,sdp_1'

publish_key('sensor_alerts', 'array_1:subarray_1_pool_resources', pool_resources)

for i in range(len(msgs)-3):
    if msgs[i].startswith('m0'):
        continue
    r.publish(chnls[i], msgs[i])
    time.sleep(0.05)