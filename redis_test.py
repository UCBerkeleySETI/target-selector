import re
import json
import yaml
import time
import threading
import redis
import csv
import itertools
import pandas as pd
from random import seed
from random import randint
from random import choice

from mk_target_selector.logger import log as logger
from mk_target_selector.mk_db import Triage
from mk_target_selector.redis_tools import (publish,
                                            get_redis_key,
                                            write_pair_redis,
                                            connect_to_redis,
                                            delete_key)

r = redis.StrictRedis()


class Listen(threading.Thread):

    """

    ADD IN DOCUMENTATION

    Examples:
        >>> client = Listen(['bluse:///set'])
        >>> client.start()

    """

    def __init__(self, chan=None):
        if not chan:
            chan = ['bluse:///set']

        threading.Thread.__init__(self)

        # Initialize redis connection
        self.redis_server = connect_to_redis()

        # Subscribe to channel
        self.p = self.redis_server.pubsub(ignore_subscribe_messages=True)
        self.p.psubscribe(chan)

    def run(self):
        """Runs continuously to listen for messages that come in from specific
           redis channels. Main function that handles the processing of the
           messages that come through redis.
        """

        # GENERATE RANDOM COORDINATES AND FREQUENCY VALUES

        with open('test/channels_copy.txt', 'r') as f:
            channel = f.read()

        with open('test/messages_copy.txt', 'r') as f:
            messages = f.read()

        chnls = channel.split('\n')
        msgs = messages.split('\n')

        with open('random_seed.csv') as f:

            tot_lines = 0
            for line in f:
                tot_lines = tot_lines + 1

            n = 0
            while n <= tot_lines:
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

                # if b < 1000000000:
                #     print('[{}] {} MHz ({})'.format(datetime.now(), (b / 1e6), a))
                # elif b >= 1000000000:
                #     print('[{}] {} GHz ({})'.format(datetime.now(), (b / 1e9), a))

                pool_resources = 'bluse_1,cbf_1,fbfuse_1,m000,m001,m002,m003,m004,m005,m006,m007,m008,m009,m010,' \
                                 'm011,m015,m017,m018,m019,m020,m021,m023,m024,m025,m026,m027,m028,m029,m030,m031,' \
                                 'm032,m033,m034,m035,m036,m037,m038,m039,m040,m041,m042,m043,m044,m045,m046,m048,' \
                                 'm049,m050,m051,m052,m053,m054,m055,m056,m057,m058,m059,m060,m061,m063,ptuse_4,' \
                                 'sdp_1,tuse_'

                r.set('array_1:subarray_1_pool_resources', pool_resources)
                r.set('array_1:subarray_1_streams_wide_antenna_channelised_voltage_centre_frequency', b)
                coords = 'array_1:target:radec, {}'.format(a)

                new_msgs = []
                for d, line in enumerate(msgs):
                    if d == 0:
                        new_msgs.append(coords)
                    else:
                        new_msgs.append(line)

                for q in range(len(new_msgs)-1):
                    print(new_msgs[q])
                    r.publish(chnls[q], new_msgs[q])

                for item in self.p.listen():
                    print("capture-stop:array_1")
                    r.publish("alerts", "capture-stop:array_1")
                    try:
                        key_glob = '*:*:processing_beams'
                        for k in r.scan_iter(key_glob):
                            product_id = (str(k)[1:].replace("\'", "")).split(':')[0]
                            data = pd.DataFrame.from_dict(json.loads(r.get(k).decode("utf-8")))
                            print("\n{}\n".format(data))
                            for s in data.index:
                                r.publish('sensor_alerts', '{}:acknowledge_{:0.4f}_{:0.4f}'
                                          .format(product_id, float(data['ra'][s]), float(data['decl'][s])))
                                print('sensor_alerts', '{}:acknowledge_{:0.4f}_{:0.4f}'
                                      .format(product_id, float(data['ra'][s]), float(data['decl'][s])))
                            for s in data.index:
                                r.publish('sensor_alerts', '{}:success_{:0.4f}_{:0.4f}'
                                          .format(product_id, float(data['ra'][s]), float(data['decl'][s])))
                                print('sensor_alerts', '{}:success_{:0.4f}_{:0.4f}'
                                      .format(product_id, float(data['ra'][s]), float(data['decl'][s])))
                    except TypeError:  # array_1:pointing_0:targets empty (NoneType)
                        pass
                    except Exception as k:
                        print(k)
                        pass

                    n += 1
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
                                                              rand_dec_pos, rand_dec_d, rand_dec_m, rand_dec_s,
                                                              rand_dec_cs)

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

                    # if b < 1000000000:
                    #     print('[{}] {} MHz ({})'.format(datetime.now(), (b / 1e6), a))
                    # elif b >= 1000000000:
                    #     print('[{}] {} GHz ({})'.format(datetime.now(), (b / 1e9), a))

                    pool_resources = 'bluse_1,cbf_1,fbfuse_1,m000,m001,m002,m003,m004,m005,m006,m007,m008,m009,m010,' \
                                     'm011,m015,m017,m018,m019,m020,m021,m023,m024,m025,m026,m027,m028,m029,m030,m031,' \
                                     'm032,m033,m034,m035,m036,m037,m038,m039,m040,m041,m042,m043,m044,m045,m046,m048,' \
                                     'm049,m050,m051,m052,m053,m054,m055,m056,m057,m058,m059,m060,m061,m063,ptuse_4,' \
                                     'sdp_1,tuse_'

                    r.set('array_1:subarray_1_pool_resources', pool_resources)
                    r.set('array_1:subarray_1_streams_wide_antenna_channelised_voltage_centre_frequency', b)
                    coords = 'array_1:target:radec, {}'.format(a)

                    new_msgs = []
                    for d, line in enumerate(msgs):
                        if d == 0:
                            new_msgs.append(coords)
                        else:
                            new_msgs.append(line)

                    for q in range(len(new_msgs) - 1):
                        print(new_msgs[q])
                        r.publish(chnls[q], new_msgs[q])

