import time
import json
import redis
import codecs
import numpy as np
import yaml
import re
import pandas as pd
from io import StringIO
from datetime import datetime
from functools import reduce
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
        rand_ra_h = format(randint(0, 11), '02d')
        rand_ra_m = format(randint(0, 59), '02d')
        rand_ra_s = format(randint(0, 59), '02d')
        rand_ra_cs = format(randint(0, 99), '02d')

        if choice([True, False]) is True:
            rand_dec_pos = '-'
            rand_dec_d = format(randint(0, 89), '02d')
        else:
            rand_dec_pos = ''
            rand_dec_d = format(randint(0, 45), '02d')
        # rand_dec_d = format(randint(0, 89), '02d')
        rand_dec_m = format(randint(0, 59), '02d')
        rand_dec_s = format(randint(0, 59), '02d')
        rand_dec_cs = format(randint(0, 9))

        a = '{}{}:{}:{}.{}, {}{}:{}:{}.{}'.format(rand_ra_pos, rand_ra_h, rand_ra_m, rand_ra_s, rand_ra_cs,
                                                  rand_dec_pos, rand_dec_d, rand_dec_m, rand_dec_s, rand_dec_cs)

        # set one of a number of frequency values
        if choice([True, False]) is True:
            rand_freq = '-'
        else:
            rand_freq = ''
        rand_freq_ind = randint(1, 3)
        if rand_freq_ind == 1:
            b = 1500000000
        elif rand_freq_ind == 2:
            b = 650000000
        elif rand_freq_ind == 3:
            b = 3000000000
        else:
            b = 10000000000

        if b < 1000000000:
            print('[{}] {} MHz ({})'.format(datetime.now(), (b/1e6), a))
        elif b >= 1000000000:
            print('[{}] {} GHz ({})'.format(datetime.now(), (b/1e9), a))

        pool_resources = 'bluse_1,cbf_1,fbfuse_1,m000,m001,m002,m003,m004,m005,m006,m007,m008,m009,m010,m011,m015,m017,m018,' \
                         'm019,m020,m021,m023,m024,m025,m026,m027,m028,m029,m030,m031,m032,m033,m034,m035,m036,m037,m038,m039,' \
                         'm040,m041,m042,m043,m044,m045,m046,m048,m049,m050,m051,m052,m053,m054,m055,m056,m057,m058,m059,m060,' \
                         'm061,m063,ptuse_4,sdp_1,tuse_'

        r.set('array_1:subarray_1_pool_resources', pool_resources)

        r.set('array_1:subarray_1_streams_wide_antenna_channelised_voltage_centre_frequency', b)

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
            elif final_messages[i].endswith('False'):
                if final_messages[i+4].endswith('True'):
                    print(chnls[i], final_messages[i])
                    r.publish(chnls[i], final_messages[i])
                    print("Observing for 15 seconds...")
                    time.sleep(15)
                # try:
                #     key_glob_remaining = '*:*:remaining_to_process'
                #     for j in r.scan_iter(key_glob_remaining):
                #         targets = str(r.get(j), 'utf-8')
                #         data = pd.read_csv(
                #             StringIO(targets), sep=",", index_col=0)
                #         print("Waiting for {} seconds...".format(np.sqrt(len(data.index))))
                #         time.sleep(np.sqrt(len(data.index)))
                # except Exception as k:
                #     print(k)
                #     pass
            elif final_messages[i+1].startswith('deconfigure'):
                try:
                    key_glob = '*:*:processing_beams'
                    for k in r.scan_iter(key_glob):
                        time.sleep(0.05)
                        product_id = (str(k)[1:].replace("\'", "")).split(':')[0]
                        data = pd.DataFrame.from_dict(json.loads(r.get(k).decode("utf-8")))
                        print("\n{}\n".format(data))
                        # df = pd.read_csv(data, header=None, index_col=0, float_precision='round_trip')
                        # targetsFinal = df.transpose()
                        # print("\n",targetsFinal)
                        for s in data.index:
                            publish_key('sensor_alerts', '{}:acknowledge_{:0.4f}_{:0.4f}'
                                        .format(product_id, float(data['ra'][s]), float(data['decl'][s])), "True")
                            print('sensor_alerts', '{}:acknowledge_{:0.4f}_{:0.4f}'
                                  .format(product_id, float(data['ra'][s]), float(data['decl'][s])), "True")
                            time.sleep(0.05)
                        time.sleep(0.05)
                        for s in data.index:
                            publish_key('sensor_alerts', '{}:success_{:0.4f}_{:0.4f}'
                                        .format(product_id, float(data['ra'][s]), float(data['decl'][s])), "True")
                            print('sensor_alerts', '{}:success_{:0.4f}_{:0.4f}'
                                  .format(product_id, float(data['ra'][s]), float(data['decl'][s])), "True")
                            time.sleep(0.05)
                        time.sleep(0.05)
                except TypeError:  # array_1:pointing_0:targets empty (NoneType)
                    pass
                except Exception as k:
                    print(k)
                    pass
                print(chnls[i], final_messages[i])
                r.publish(chnls[i], final_messages[i])
                time.sleep(0.05)
            elif final_messages[i].startswith('deconfigure'):
                time.sleep(0.05)
                print(chnls[i], final_messages[i])
                r.publish(chnls[i], final_messages[i])
                time.sleep(0.05)
            else:
                print(chnls[i], final_messages[i])
                r.publish(chnls[i], final_messages[i])
                time.sleep(0.05)
