import time
import json
import redis
import numpy as np
import yaml
import re
from datetime import datetime 
from random import seed
from random import randint
from random import choice

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

with open('random_seed.csv') as f:
    for n in f:
        # seed random number generator
        seed(n)

        # generate integers for coordinates
        if choice([True, False]) == True:
            rand_ra_pos = '-'
        else:
            rand_ra_pos = ''
        rand_ra_h = format(randint(0,11), '02d')
        rand_ra_m = format(randint(0,59), '02d')
        rand_ra_s = format(randint(0,59), '02d')
        rand_ra_cs = format(randint(0,99), '02d')

        if choice([True, False]) == True:
            rand_dec_pos = '-'
        else:
            rand_dec_pos = ''
        rand_dec_d = format(randint(0,89), '02d')
        rand_dec_m = format(randint(0,59), '02d')
        rand_dec_s = format(randint(0,59), '02d')
        rand_dec_cs = format(randint(0,9))

        a = '{}{}:{}:{}.{}, {}{}:{}:{}.{}'.format(rand_ra_pos,rand_ra_h,rand_ra_m,rand_ra_s,rand_ra_cs,rand_dec_pos,rand_dec_d,rand_dec_m,rand_dec_s,rand_dec_cs)

        print('[{}] {}'.format(datetime.now(),a))

        # set one of a number of frequency values
        if choice([True, False]) == True:
            rand_freq = '-'
        else:
            rand_freq = ''
        rand_freq_ind = randint(1,3)
        if rand_freq_ind == 1:
            b = 600000000
        elif rand_freq_ind == 2:
            b = 700000000
        elif rand_freq_ind == 3:
            b = 800000000
        else:
            b = 900000000

        pool_resources = 'bluse_1,cbf_1,fbfuse_1,m000,m001'

        publish_key('sensor_alerts', 'array_1:subarray_1_pool_resources', pool_resources)

        publish_key('sensor_alerts', 'array_1:subarray_1_streams_wide_antenna_channelised_voltage_centre_frequency', b)

        coords = 'array_1:target:radec, {}'.format(a)

        final_messages = []
        for d, line in enumerate(msgs):
            if d <= 15:
                final_messages.append(line)
            elif d == 16:
                final_messages.append(coords)
            elif d >= 17:
                final_messages.append(line)

        for i in range(len(final_messages)-1):
            if final_messages[i].startswith('m0'):
                continue
            #print(chnls[i], final_messages[i])
            r.publish(chnls[i], final_messages[i])
            time.sleep(0.05)

        time.sleep(5)