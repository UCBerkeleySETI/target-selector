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

pool_resources = 'bluse_1,cbf_1,fbfuse_1,m000,m001'

#'bluse_1,cbf_1,fbfuse_1,m000,m001,m002,m003,m004,m005,m006,m007,m008,m009,m010,m011,m015,m017,m018,m019,m020,m021,m023,m024,m025,m026,m027,m028,m029,m030,m031,m032,m033,m034,m035,m036,m037,m038,m039,m040,m041,m042,m043,m044,m045,m046,m048,m049,m050,m051,m052,m053,m054,m055,m056,m057,m058,m059,m060,m061,m063,ptuse_4,sdp_1,tuse_'

frequency = '816000000'

publish_key('sensor_alerts', 'array_1:subarray_1_pool_resources', pool_resources)

publish_key('sensor_alerts', 'array_1:subarray_1_streams_wide_antenna_channelised_voltage_centre_frequency', frequency)

for i in range(len(msgs)-1):
    if msgs[i].startswith('m0'):
        continue
    r.publish(chnls[i], msgs[i])
    time.sleep(0.05)
