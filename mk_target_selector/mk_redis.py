import re
import json
import yaml
import time
import threading
import math
import random
import pandas as pd
import numpy as np
from io import StringIO
from functools import reduce
from datetime import datetime
from astropy import units as u
from astropy.coordinates import Angle, SkyCoord

try:
    from .logger import log as logger
    from .mk_db import Triage
    from .redis_tools import (publish,
                              get_redis_key,
                              write_pair_redis,
                              connect_to_redis,
                              delete_key)

except ImportError:
    from logger import log as logger
    from mk_db import Triage
    from redis_tools import (publish,
                             get_redis_key,
                             write_pair_redis,
                             connect_to_redis,
                             delete_key)


class ProcessingStatus(object):
    def __init__(self, value):
        self._proc_status = value

    @property
    def proc_status(self):
        return self._proc_status

    @proc_status.setter
    def proc_status(self, value):
        self._proc_status = value

    @proc_status.deleter
    def proc_status(self):
        del self._proc_status


pStatus = ProcessingStatus("ready")


class Listen(threading.Thread):
    """

    ADD IN DOCUMENTATION

    Examples:
        >>> client = Listen(['alerts', 'sensor_alerts'])
        >>> client.start()

    When start() is called, a loop is started that subscribes to the "alerts" and
    "sensor_alerts" channels on the Redis server. Depending on the which message
    that passes over which channel, various processes are run:

    Alerts:
        1. Configure:
            -
        2. Deconfigure
            -

    Sensor Alerts:
        1. data_suspect:
            -
        2. schedule_blocks:
            -
        3.

    Things left to do:
        1. Listen for a success message from the processing nodes. Once this
           success/failure message has been returned, then add to the database.
    """

    def __init__(self, chan=None, config_file='target_selector.yml'):

        if not chan:
            chan = ['sensor_alerts', 'alerts']

        threading.Thread.__init__(self)

        # Initialize redis connection
        self.redis_server = connect_to_redis()

        # Subscribe to channel
        self.p = self.redis_server.pubsub(ignore_subscribe_messages=True)
        self.p.psubscribe(chan)

        # Database connection and triaging
        self.engine = Triage(config_file)

        self.channel_actions = {
            'alerts': self._alerts,
            'sensor_alerts': self._sensor_alerts,
        }

        self.alerts_actions = {
            'deconfigure': self._pass,
            'configure': self._pass,
            # 'deconfigure': self._deconfigure,
            # 'configure': self._configure,
            'conf_complete': self._pass,
            'capture-init': self._pass,
            'capture-start': self._capture_start,
            'capture-stop': self._capture_stop,
            'capture-done': self._pass,
        }

        self.sensor_actions = {
            'data_suspect': self._pass,
            'schedule_blocks': self._pass,
            'pool_resources': self._pool_resources,
            'observation_status': self._status_update,
            'target': self._target_query,
            'frequency': self._frequency,
            'processing_success': self._processing_success,
            'acknowledge': self._acknowledge,
            'new_obs': self._pass,
            'current_obs': self._pass
        }

    def run(self):
        """Runs continuously to listen for messages that come in from specific
           redis channels. Main function that handles the processing of the
           messages that come through redis.
        """

        for item in self.p.listen():
            self._message_to_func(item['channel'], self.channel_actions)(item['data'])

            # product_id = self._parse_sensor_name(item['data'])[0]
            # obs_start_time = self._get_sensor_value(product_id, "current_obs:obs_start_time")
            # obs_end_time = self._get_sensor_value(product_id, "current_obs:obs_end_time")
            # if "None" not in str(self._get_sensor_value(product_id, "current_obs:proc_start_time")):
            # proc_start_time = self._get_sensor_value(product_id, "current_obs:proc_start_time")
            # time_elapsed = (datetime.now()
            #                 - datetime.strptime(proc_start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
            # observation_time = (datetime.strptime(obs_end_time, "%Y-%m-%d %H:%M:%S.%f")
            #                     - datetime.strptime(obs_start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()

            # self.abort_criteria(product_id, time_elapsed, observation_time)

    def fetch_data(self, product_id, mode):
        """Fetches telescope status data and selects targets when telescope status data is stored

        Parameters:
            product_id: (str)
                Product ID parsed from redis message
            mode: (str)
                Mode in which function is to be run; "current_obs" fetches the data and writes it to current_obs redis
                keys, while "new_obs" fetches the data for comparison against the currently processing block, without
                overwriting keys relating to the currently processing observations
        Returns:
            None
        """
        if ("None" not in str(self._get_sensor_value(product_id, "new_obs:coords")))\
                and ("None" not in str(self._get_sensor_value(product_id, "new_obs:frequency")))\
                and ("None" not in str(self._get_sensor_value(product_id, "new_obs:pool_resources"))):
            try:
                # create redis key-val pairs to store current observation data & current telescope status data
                new_coords = self._get_sensor_value(product_id, "new_obs:coords")
                coords_ra = float(new_coords.split(", ")[0])
                coords_dec = float(new_coords.split(", ")[1])
                new_freq = self._get_sensor_value(product_id, "new_obs:frequency")
                new_pool = self._get_sensor_value(product_id, "new_obs:pool_resources")

                if mode == "current_obs":
                    # logger.info("Writing values of [{}:new_obs:*] to [{}:current_obs:*]"
                    #             .format(product_id, product_id))
                    write_pair_redis(self.redis_server, "{}:{}:coords".format(product_id, mode), new_coords)
                    # logger.info("Fetched [{}:{}:coords]: [{}]"
                    #             .format(product_id, mode, self._get_sensor_value(product_id, "{}:coords"
                    #                                                              .format(mode))))
                    write_pair_redis(self.redis_server, "{}:{}:frequency".format(product_id, mode), new_freq)
                    # logger.info("Fetched [{}:{}:frequency]: [{}]"
                    #             .format(product_id, mode, self._get_sensor_value(product_id, "{}:frequency"
                    #                                                              .format(mode))))
                    write_pair_redis(self.redis_server, "{}:{}:pool_resources".format(product_id, mode), new_pool)
                    # logger.info("Fetched [{}:{}:pool_resources]: [{}]"
                    #             .format(product_id, mode, self._get_sensor_value(product_id, "{}:pool_resources"
                    #                                                              .format(mode))))

                check_flag = False
                if mode == "current_obs":
                    check_flag = False
                elif mode == "new_obs":
                    check_flag = True

                targets = self\
                    .engine.select_targets(np.deg2rad(coords_ra),
                                           np.deg2rad(coords_dec),
                                           current_freq=self._get_sensor_value(product_id, "{}:frequency".format(mode)),
                                           beam_rad=self._beam_radius(self._get_sensor_value(product_id,
                                                                                             "{}:frequency"
                                                                                             .format(mode))),
                                           check_flag=check_flag)

                columns = ['ra', 'decl', 'source_id', 'dist_c', 'table_name', 'priority']
                targ_dict = targets.loc[:, columns].to_dict('list')

                if len(targets.index) == 0:
                    self.coord_error(coords=self._get_sensor_value(product_id, "{}:coords".format(mode)),
                                     frequency=self._get_sensor_value(product_id, "{}:frequency".format(mode)),
                                     mode=mode,
                                     product_id=product_id)
                else:
                    write_pair_redis(self.redis_server, "{}:{}:target_list"
                                     .format(product_id, mode), json.dumps(targ_dict))

            except KeyError:
                pass

    """

    Alerts Functions

    """

    def _alerts(self, message):
        """Response to message from the Alerts channel. Runs a function depending
        on the message sent.

        Parameters:
            message: (str)
                Message passed over the alerts channel

        Returns:
            None
        """
        sensor, product_id = self._parse_sensor_name(message)
        self._message_to_func(sensor, self.alerts_actions)(product_id)

    def _pass(self, item):
        """Temporary function to handle alerts that we don't care about responding
        to at the moment
        """
        return 0

    def _deconfigure(self, product_id):
        """Function to remove redis key-value pairs related to completed or aborted processing blocks.

        Parameters:
            product_id: (str)
                product_id for this particular subarray

        Returns:
            None
        """
        sensor_list = ['processing', 'targets']

        for sensor in sensor_list:
            key_glob = ('{}:*:{}'.format(product_id, sensor))
            success_key_glob = ('{}:success_source_id*'.format(product_id))
            ackn_key_glob = ('{}:acknowledge_source_id*'.format(product_id))
            new_obs_key_glob = ("{}:new_obs:*".format(product_id))
            current_key_glob = ("{}:current_obs:*".format(product_id))
            for a in self.redis_server.scan_iter(key_glob):
                # logger.info("Key: {}".format(get_redis_key(self.redis_server, a)))
                logger.info('Removing key: {}'.format(a))
                delete_key(self.redis_server, a)
            for b in self.redis_server.scan_iter(success_key_glob):
                delete_key(self.redis_server, b)
            for c in self.redis_server.scan_iter(ackn_key_glob):
                delete_key(self.redis_server, c)
            for d in self.redis_server.scan_iter(new_obs_key_glob):
                delete_key(self.redis_server, d)
            for e in self.redis_server.scan_iter(current_key_glob):
                delete_key(self.redis_server, e)

        # TODO: update the database with information inside the sensor_info

    def _capture_start(self, message):
        """Function that responds to capture start updates. Upon receipt the target list is generated, triaged and
         published & observation start time taken.

       Parameters:
            message: (str)
                Message passed over the alerts channel
        Returns:
            None
        """

        logger.info("Capture start message received: {}".format(message))
        product_id = message.split(":")[-1]

        if pStatus.proc_status == "ready":
            self.fetch_data(product_id, mode="current_obs")
            if "None" not in str(self._get_sensor_value(product_id, "current_obs:target_list")):
                sub_arr_id = "0"  # TODO: CHANGE TO HANDLE SUB-ARRAYS
                pulled_targets = json.loads(self._get_sensor_value(product_id, "current_obs:target_list"))
                pulled_coords = self._get_sensor_value(product_id, "current_obs:coords")
                pulled_freq = self.engine.freq_format(self._get_sensor_value(product_id, "current_obs:frequency"))

                obs_start_time = datetime.now()
                write_pair_redis(self.redis_server,
                                 "{}:current_obs:obs_start_time".format(product_id), str(obs_start_time))

                self.beam_number(product_id)

                self._publish_targets(pulled_targets, product_id, sub_arr_id)

        elif pStatus.proc_status == "processing":
            logger.info("Still processing previous pointing. Checking abort criteria")
            self.fetch_data(product_id, mode="new_obs")
            if "None" not in str(self._get_sensor_value(product_id, "new_obs:target_list")):

                # read in and format new target list from redis key to dataframe
                new_target_list = pd.DataFrame.from_dict(json.loads(
                    self._get_sensor_value(product_id, "new_obs:target_list")))
                appended_new = self.append_tbdfm(new_target_list)

                # read in and format list of targets which remain to be processed from redis key to dataframe
                remaining_to_process = pd.DataFrame.from_dict(json.loads(
                    self._get_sensor_value(product_id, "current_obs:remaining_to_process")))
                appended_remaining = self.append_tbdfm(remaining_to_process)

                # maximum achievable TBDFM parameter for sources in the new target list
                max_new_tbdfm = appended_new['tbdfm_param'].max()
                # number of sources in the new target list
                n_new_obs = len(appended_new.index)
                # the priority value for the maximum achievable TBDFM parameter for sources in the new target list
                max_new_tbdfm_prio = appended_new.loc[appended_new['tbdfm_param']
                                                      == max_new_tbdfm]['priority'].item()
                # the N_sources value for the maximum achievable TBDFM parameter for sources in the new target list
                max_new_tbdfm_num = appended_new.loc[appended_new['tbdfm_param']
                                                     == max_new_tbdfm].index.values[0]+1

                # maximum achievable TBDFM parameter for sources remaining to process
                max_remaining_tbdfm = appended_remaining['tbdfm_param'].max()
                # number of sources remaining to process
                n_remaining_obs = len(appended_remaining.index)
                # the priority value for the maximum achievable TBDFM parameter for sources remaining to process
                max_remaining_tbdfm_prio = appended_remaining.loc[appended_remaining['tbdfm_param']
                                                                  == max_remaining_tbdfm]['priority'].item()
                # the N_sources value for the maximum achievable TBDFM parameter for sources remaining to process
                max_remaining_tbdfm_num = appended_remaining.loc[appended_remaining['tbdfm_param']
                                                                 == max_remaining_tbdfm].index.values[0]+1

                logger.info("Maximum TBDFM parameter (N_sources)^(1/priority) for {} targets in new pointing: "
                            "{} ** 1/{} = {}"
                            .format(n_new_obs, max_new_tbdfm_num, max_new_tbdfm_prio, max_new_tbdfm))
                logger.info("Maximum TBDFM parameter (N_sources)^(1/priority) for {} targets remaining to process: "
                            "{} ** 1/{} = {}"
                            .format(n_remaining_obs, max_remaining_tbdfm_num, max_remaining_tbdfm_prio, max_remaining_tbdfm))

                # if (max_new_tbdfm < max_remaining_tbdfm) and (mean_new_priority <= mean_remaining_priority):
                if max_new_tbdfm > max_remaining_tbdfm:
                    self.abort_criteria(product_id)
                    write_pair_redis(self.redis_server, "{}:current_obs:coords".format(product_id),
                                     self._get_sensor_value(product_id, "new_obs:coords"))
                    write_pair_redis(self.redis_server, "{}:current_obs:frequency".format(product_id),
                                     self._get_sensor_value(product_id, "new_obs:frequency"))
                    write_pair_redis(self.redis_server, "{}:current_obs:pool_resources".format(product_id),
                                     self._get_sensor_value(product_id, "new_obs:pool_resources"))
                    write_pair_redis(self.redis_server, "{}:current_obs:target_list".format(product_id),
                                     self._get_sensor_value(product_id, "new_obs:target_list"))

                    if "None" not in str(self._get_sensor_value(product_id, "current_obs:target_list")):
                        sub_arr_id = "0"  # TODO: CHANGE TO HANDLE SUB-ARRAYS
                        pulled_targets = json.loads(self._get_sensor_value(product_id, "current_obs:target_list"))

                        obs_start_time = datetime.now()
                        write_pair_redis(self.redis_server,
                                         "{}:current_obs:obs_start_time".format(product_id), str(obs_start_time))

                        self.beam_number(product_id)

                        self._publish_targets(pulled_targets, product_id, sub_arr_id)

                elif max_new_tbdfm <= max_remaining_tbdfm:
                    logger.info("New pointing does not contain sources with a higher maximum achievable TBDFM "
                                "parameter. Abort criteria not met. Continuing")
                    pass

    def _capture_stop(self, message):
        """Function that responds to capture stop updates. Takes observing end time and stores metadata

       Parameters:
            message: (str)
                Message passed over the alerts channel
        Returns:
            None
        """
        logger.info("Capture stop message received: {}".format(message))
        product_id = message.split(":")[-1]
        self.store_metadata(product_id, mode="new_sample")

    """

    Sensor Alerts Functions

    """

    def _sensor_alerts(self, message):
        """Response to sensor_alerts channel. Runs a function based on the input
        message

        Parameters:
            message: (str)
                Message received from listening to the sensor_alerts channel

        Returns:
            None
        """
        product_id, sensor = self._parse_sensor_name(message)

        if sensor.endswith('pool_resources'):
            sensor = 'pool_resources'

        if sensor.endswith('frequency'):
            sensor = 'frequency'

        if sensor.startswith('success'):
            sensor = 'processing_success'

        if sensor.startswith('acknowledge'):
            sensor = 'acknowledge'

        if sensor.startswith('new_obs'):
            sensor = 'new_obs'

        if sensor.startswith('current_obs'):
            sensor = 'current_obs'

        self._message_to_func(sensor, self.sensor_actions)(message)

    def _target_query(self, message):
        """Response to message from the Sensor Alerts channel. If both the right
        ascension and declination are stored, then the database is queried
        for

        Parameters:
            message: (str)
                Message passed over the sensor alerts channel

        Returns:
            None
        """

        product_id, sensor, value = message.split(':', 2)

        if value == 'unavailable':
            return

        else:
            logger.info("Target coordinate message received: {}".format(message))
            coords = SkyCoord(' '.join(value.split(', ')[-2:]), unit=(u.hourangle, u.deg))
            coord_key = "{}:new_obs:coords".format(product_id)
            coord_value = "{}, {}".format(coords.ra.deg, coords.dec.deg)
            write_pair_redis(self.redis_server, coord_key, coord_value)
            # logger.info("Wrote [{}] to [{}]".format(coord_value, coord_key))

    def _schedule_blocks(self, key, target_pointing, beam_rad):
        """Block that responds to schedule block updates. Searches for targets
           and publishes the information to the processing channel

       Parameters:
            key: (dict)
                Redis channel message received from listening to a channel

        Returns:
            None
        """

        message = get_redis_key(self.redis_server, key)
        product_id = key.split(':')[0]
        schedule_block = self.load_schedule_block(message)

        if isinstance(schedule_block, list):
            if isinstance(schedule_block[0], list):
                target_pointing = schedule_block[0]
            elif isinstance(schedule_block[0], dict):
                target_pointing = schedule_block

        if isinstance(schedule_block, dict):
            target_pointing = schedule_block['targets']

        start = time.time()
        for i, t in enumerate(target_pointing):
            targets = self.engine.select_targets(*self.pointing_coords(t),
                                                 beam_rad,
                                                 current_freq=self._get_sensor_value(product_id,
                                                                                     "current_obs:frequency"))
            self._publish_targets(targets, product_id=product_id, sub_arr_id=i)

        logger.info('{} pointings processed in {} seconds'.format(len(target_pointing),
                                                                  time.time() - start))

    def _pool_resources(self, message):
        """Response to a pool_resources message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            None
        """
        logger.info("Pool resources message received: {}".format(message))

        product_id, _ = message.split(':')
        pool_resources_value = get_redis_key(self.redis_server, message)

        pool_resources_key = "{}:new_obs:pool_resources".format(product_id)
        write_pair_redis(self.redis_server, pool_resources_key, pool_resources_value)
        # logger.info("Wrote [{}] to [{}]".format(pool_resources_value, pool_resources_key))

    def _frequency(self, message):
        """Response to a frequency message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            None
        """
        logger.info("Frequency message received: {}".format(message))

        product_id, sensor_name = message.split(':')
        frequency_value = get_redis_key(self.redis_server, message)

        frequency_key = "{}:new_obs:frequency".format(product_id)
        write_pair_redis(self.redis_server, frequency_key, frequency_value)
        # logger.info("Wrote [{}] to [{}]".format(frequency_value, frequency_key))

    def _processing_success(self, message):
        """Response to a successful processing success message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channel.

        Returns:
            None
        """

        product_id = message.split(':')[0]
        sensor_name = message.split(':')[-1]
        source_id = sensor_name.split('source_id_')[1]

        # update observation_status with success message
        self.engine.update_obs_status(source_id,
                                      obs_start_time=str(self.round_time
                                                         (self._get_sensor_value
                                                          (product_id, "current_obs:obs_start_time"))),
                                      processed='TRUE')

        target_list = json.loads(self._get_sensor_value(product_id, "current_obs:target_list"))
        remaining_64 = json.loads(self._get_sensor_value(product_id, "current_obs:processing_64"))
        number_to_process = len(target_list['source_id'])
        number_processed = target_list['source_id'].index(source_id) + 1
        number_remaining = len(remaining_64['source_id'])
        fraction_processed = number_processed / number_to_process

        # remaining_list = pd.read_csv(
        #     StringIO(
        #         self._get_sensor_value(
        #             product_id, "current_obs:remaining_to_process")),
        #     sep=",", index_col=0, dtype={'source_id': str})
        # remaining = remaining_list[remaining_list.source_id != str(source_id)].reset_index(drop=True)
        # remaining_to_process = pd.DataFrame.to_csv(remaining)
        # write_pair_redis(self.redis_server,
        #                  "{}:current_obs:remaining_to_process".format(product_id), remaining_to_process)

        # TODO: CHECK REDIS HASHES AND REDIS LISTS, check regex first

        # FIND INDEX WHERE SOURCE_ID == SOURCE_ID
        target_list = json.loads(self._get_sensor_value(product_id, "current_obs:remaining_to_process"))
        index_to_rm = target_list['source_id'].index(source_id)

        # REMOVE INDEX FROM LISTS UNDER ALL KEYS
        keys = ['ra', 'decl', 'source_id', 'dist_c', 'table_name', 'priority']
        for i in keys:
            to_rm = target_list.get(i)
            del to_rm[index_to_rm]

        write_pair_redis(
            self.redis_server,
            "{}:current_obs:remaining_to_process".format(product_id), json.dumps(target_list))
        remaining = json.loads(self._get_sensor_value(product_id, "current_obs:remaining_to_process"))
        processing_64 = json.loads(self._get_sensor_value(product_id, "current_obs:processing_64"))

        # proc_start_time = self._get_sensor_value(product_id, "current_obs:proc_start_time")
        # time_elapsed = (datetime.now() - datetime.strptime(proc_start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()

        if number_processed == number_to_process:
            logger.info("Successful processing of all {} remaining targets confirmed by processing nodes"
                        .format(number_remaining))
            self._deconfigure(product_id)
            pStatus.proc_status = "ready"
            logger.info(
                "-------------------------------------------------------------------------------------------------")
            logger.info("Processing state set to \'ready\'")

        elif processing_64['source_id'][-1] in message:
            logger.info("Successful processing of 64 targets confirmed by processing nodes. "
                        "Writing next {} to {}:current_obs:processing_64"
                        .format(len(processing_64.get('source_id')), product_id))
            next_64 = {k: v[:64] for k, v in remaining.items()}
            self.beam_number(product_id)
            self._publish_targets(remaining, product_id)
            write_pair_redis(self.redis_server, "{}:current_obs:processing_64".format(product_id), json.dumps(next_64))
            self.store_metadata(product_id, mode="next_64")

        # self.abort_criteria(product_id, time_elapsed, fraction_processed)

    def _acknowledge(self, message):
        """Response to a successful acknowledgement message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channel.

        Returns:
            None
        """

        product_id = message.split(':')[0]

        # if message contains final target source_id,
        remaining_all = json.loads(self._get_sensor_value(product_id, "current_obs:remaining_to_process"))
        remaining_64 = {k: v[:64] for k, v in remaining_all.items()}

        if remaining_all['source_id'][-1] in message:
            # all sources have been received by the processing nodes
            # begin processing time
            logger.info("Receipt of all {} remaining targets confirmed by processing nodes"
                        .format(len(remaining_all.get('source_id'))))
            # write_pair_redis(self.redis_server,
            #                  "{}:current_obs:proc_start_time".format(product_id),
            #                  str(datetime.now()))

        elif remaining_64['source_id'][-1] in message:
            # 64 sources have been received by the processing nodes
            # begin processing time
            logger.info("Receipt of next 64 targets confirmed by processing nodes")
            # write_pair_redis(self.redis_server,
            #                  "{}:current_obs:proc_start_time".format(product_id),
            #                  str(datetime.now()))

    """

    Internal Methods

    """

    def append_tbdfm(self, table):
        """Function to calculate and append TBDFM values to tables containing targets for both the new pointing and
        those currently remaining to process

        TBDFM = To Be Determined Figure of Merit; = (N_sources)^(1/priority)

        Parameters:
            table: (dataframe)
                Table from which to calculate TBDFM and to which the values are appended
        Returns:
            table: (dataframe)
                Dataframe with appended TBDFM values for each row
        """
        # create empty array for TBDFM parameter values in the target list
        tbdfm_param = np.full(table.shape[0], 0, dtype=float)
        # fill this array with the (N_sources)^(1/priority) value for each row
        # table = table.sort_values('dist_c', ignore_index=True)
        tbdfm_param[table.index] = np.float_power((table.index + 1), (1/table['priority']))
        # append this array to the target list dataframe
        table['tbdfm_param'] = tbdfm_param
        return table

    def abort_criteria(self, product_id, time_elapsed=None, observation_time=None, fraction_processed=None):
        """Function to abort processing if certain conditions are met

        Parameters:
            product_id: (str)
                Subarray ID received from redis message
            time_elapsed: (str)
                Total time elapsed since start of processing
            observation_time: (str)
                Total recorded observation time from the processing nodes (t_obs)
            fraction_processed: (str)
                Fraction of sources successfully processed from the currently processing block

        Returns:
            None
        """
        if (not fraction_processed) and (not observation_time):
            # processing aborted based on priority of new sources & optimising TBDFM values
            # (larger TBDFM = better)
            logger.info("New pointing contains sources with a higher maximum achievable TBDFM parameter. Aborting")
            # self._deconfigure(product_id)
            pStatus.proc_status = "ready"
            logger.info(
                "-------------------------------------------------------------------------------------------------")
            logger.info("Processing state set to \'ready\'")

        elif not fraction_processed:  # processing aborted based on observation time (t_obs)
            if (time_elapsed > 1200) and (time_elapsed > (2 * observation_time) - 300):
                logger.info("Processing time has exceeded both 20 and (2t_obs - 5) minutes."
                            " Aborting")
                self._deconfigure(product_id)
                pStatus.proc_status = "ready"
                logger.info(
                    "-------------------------------------------------------------------------------------------------")
                logger.info("Processing state set to \'ready\'")

        elif not observation_time:  # processing aborted based on absolute processing time
            if (fraction_processed > 0.9) and (time_elapsed > 600):
                logger.info("Processing time has exceeded 10 minutes, with >90% of targets processed successfully."
                            " Aborting")
                self._deconfigure(product_id)
                pStatus.proc_status = "ready"
                logger.info(
                    "-------------------------------------------------------------------------------------------------")
                logger.info("Processing state set to \'ready\'")

    def reformat_table(self, table):
        """Function to reformat the table of targets pushed to the backend

        Parameters:
            table: (str)
                A pandas DataFrame containing target list information, parsed as a string

        Returns:
            targets_to_process: (pandas.DataFrame)
                Reformatted pandas DataFrame containing information from the given table
        """
        replace_chars = ("\"", ""), (":", ","), ("[", ""), ("], ", "\n"), ("]", ""), \
                        ("{", ""), ("}", "")
        formatted = reduce(lambda a, kv: a.replace(*kv), replace_chars, table)
        data = StringIO(formatted)
        df = pd.read_csv(data, header=None, index_col=0, float_precision='round_trip')
        targets_to_process = df.transpose()
        return targets_to_process

    def coord_error(self, product_id, coords, frequency, mode):
        """Function to handle errors due to coordinate values (empty pointings or out of range)

        Parameters:
            product_id: (str)
                subarray ID
            coords: (str)
                string to parse containing erroneous coordinates
            frequency: (str)
                the central frequency of observation
            mode: (str)
                current_obs or new_obs, the keys concerning erroneous or unavailable coordinates, to be deleted

        Returns:
            None
        """
        arr_ra_dec = coords.split(', ')
        dec_coord = arr_ra_dec[1]
        if float(dec_coord) > 45:  # coordinates out of MeerKAT's range
            logger.info('Selected coordinates ({}) unavailable. Waiting for new coordinates'
                        .format(coords))
        else:  # no sources from target list in beam
            logger.info('No targets visible for coordinates ({}) at {} Hz. Waiting for new coordinates'
                        .format(coords, frequency))

        # DELETE mode:target_list redis key
        target_key = ('{}:{}:target_list'.format(product_id, mode))
        if mode == "new_obs" and "None" not in str(self._get_sensor_value(product_id, "{}:target_list".format(mode))):
            delete_key(self.redis_server, target_key)

        if mode == "current_obs":
            pStatus.proc_status = "ready"

    def round_time(self, timestamp):
        """Function to round timestamp values to nearest second for database matching

        Parameters:
            timestamp: (datetime)
                Timestamp to round to the nearest second (for matching success messages against the table of
                previous observations)

        Returns:
            rounded: (datetime)
                Timestamp rounded to the nearest second
        """
        dt = str(timestamp)
        date = dt.split()[0]
        h, m, s = [dt.split()[1].split(':')[0],
                   dt.split()[1].split(':')[1],
                   str(round(float(dt.split()[1].split(':')[-1])))]
        rounded = "{} {}:{}:{}".format(date, h, m, s)
        return rounded

    def load_schedule_block(self, message):
        """Reformats schedule block messages and reformats them into dictionary format

        Parameters:
            message: (str)
                asdf

        Returns:
            None
        """

        message = message.replace('"[', '[')
        message = message.replace(']"', ']')
        return yaml.safe_load(message)

    def _get_sensor_value(self, product_id, sensor_name):
        """Returns the value for a given sensor and product id number

        Parameters:
            product_id: (str)
                ID received from redis message
            sensor_name: (str)
                Name of the sensor to be queried

        Returns:
            value: (str, int)
                Value attached to the key in the redis database
        """

        key = '{}:{}'.format(product_id, sensor_name)
        value = get_redis_key(self.redis_server, key)
        return value

    def _message_to_func(self, channel, action):
        """Function that selects a function to run based on the channel entered

        Parameters:
            channel: (str)
                channel/sensor name

        Returns:
            Function attached to a particular sensor_name

        """

        return action.get(channel, self._other)

    def _other(self, channel):
        """Function that handles unrecognized requests from redis server

        Parameters:
            channel: (dict)
                channel over which message is passed

        Returns:
            None
        """
        logger.info('Unrecognized channel style: {}'.format(channel))

    def _status_update(self, msg):
        """Function to test the status_update from the processing nodes.

        Parameters:
            msg: (str)
                string formatted like a

        Returns:
            None
        """
        status_msg = self.load_schedule_block(msg)
        if status_msg['success']:
            self.engine.update_obs_status(**status_msg)

    def _unsubscribe(self, channels=None):
        """Unsubscribe from the redis server

        Parameters:
            channels: (str, list)
                List of channels you wish to unsubscribe to

        Returns:
            None
        """

        if channels is None:
            self.p.unsubscribe()
        else:
            self.p.unsubscribe(channels)

        logger.info('Unsubscribed from channel(s)')

    def store_metadata(self, product_id, mode):
        """Stores observation metadata in database.

        Parameters:
            product_id: (str)
                Product ID of the subarray from which status metadata is pulled to add to the table of previously
                 completed observations
            mode: (str)
                Either new_sample or next_64; if mode=new_sample, the observation end time is set, else the previously
                stored value is used
        Returns:
            None
        """

        if "None" not in str(self._get_sensor_value(product_id, "current_obs:target_list")):
            if (pStatus.proc_status == "ready") or (mode == "next_64"):
                if mode == "new_sample":
                    try:
                        obs_end_time = datetime.now()
                        write_pair_redis(self.redis_server,
                                         "{}:current_obs:obs_end_time".format(product_id), str(obs_end_time))
                        pStatus.proc_status = "processing"
                        logger.info("Processing state set to 'processing\'")
                    except Exception as e:
                        logger.info(e)

                pool_resources = self._get_sensor_value(product_id, "current_obs:pool_resources")

                antennas = ','.join(re.findall(r'm\d{3}', pool_resources))
                proxies = ','.join(re.findall(r'[a-z A-Z]+_\d', pool_resources))
                start = self._get_sensor_value(product_id, "current_obs:obs_start_time")
                end = self._get_sensor_value(product_id, "current_obs:obs_end_time")
                # TODO: query frequency band sensor
                current_freq = self._get_sensor_value(product_id, "current_obs:frequency")
                bands = self.engine._freqBand(current_freq)
                # antenna count
                n_antennas = antennas.count(',') + 1
                # TODO: ask Daniel/Dave about unique file-id
                file_id = 'filler_file_id'

                # TODO: Change this to handle specific pointing in subarray
                targets = pd.DataFrame.from_dict(
                    json.loads(self._get_sensor_value(product_id, "current_obs:processing_64")))

                self.engine.add_sources_to_db(targets, start, end, proxies, antennas, n_antennas, file_id, bands)

    def _beam_radius(self, current_freq, dish_size=13.5):
        """Returns the beam radius based on the frequency band used in the
           observation

       Parameters:
            current_freq: (float)
                Central frequency for the current observation in Hertz
        Returns:
            beam_rad: (float)
                Radius of the beam in radians
        """

        # TODO: change this to the real name
        beam_rad = (2.998e8 / float(current_freq)) / dish_size
        return beam_rad

    def _publish_targets(self, targets, product_id, columns=None,
                         sub_arr_id=0, sensor_name='targets'):
        """Reformat the table returned from target searching

        Parameters:
            targets: (pandas.DataFrame)
                DataFrame containing data for the triaged target list to be processed
            product_id: (str)
                product_id for the given subarray
            sub_arr_id: (int)
                ASDF pointing number for the given schedule block
            sensor_name: (str)
                name of the sensor to be queried; in this case the targets sensor
            columns: (list)
                list of columns for the published list of targets to have
            channel: (str)
                channel over which to publish the targets

        Returns:
            None
        """

        key = '{}:pointing_{}:{}'.format(product_id, sub_arr_id, sensor_name)
        key_current_obs = '{}:current_obs:remaining_to_process'.format(product_id)
        key_64 = '{}:current_obs:processing_64'.format(product_id)

        first_64 = {k: v[:64] for k, v in targets.items()}
        init_targets = len(first_64['source_id'])
        n_spare = 64 - init_targets
        if n_spare >= 1:
            coords = self._get_sensor_value(product_id, "current_obs:coords")
            c_ra = math.radians(float(coords.split(", ")[0]))
            c_dec = math.radians(float(coords.split(", ")[1]))
            beam_rad = self._beam_radius(self._get_sensor_value(product_id, "current_obs:frequency"))
            for n in range(1, n_spare + 1):
                # random angle
                alpha = 2 * math.pi * random.random()
                # random radius
                u = random.random() + random.random()
                r = beam_rad * (2 - u if u > 1 else u)
                # calculating coordinates
                x = r * math.cos(alpha) + c_ra
                if x > math.pi:
                    x = x - (2 * math.pi)
                y = r * math.sin(alpha) + c_dec
                source_id = "spare_{}_{}".format(math.degrees(x), math.degrees(y))
                first_64['source_id'].append(source_id)
                first_64['ra'].append(math.degrees(x))
                first_64['decl'].append(math.degrees(y))
                first_64['dist_c'].append(0)
                first_64['priority'].append(7)
                first_64['table_name'].append('spare_beams')

        # logger.info("{}".format(len(first_64['source_id'])))
        # logger.info("{}".format(len(first_64['ra'])))
        # logger.info("{}".format(len(first_64['decl'])))
        # logger.info("{}".format(len(first_64['dist_c'])))
        # logger.info("{}".format(len(first_64['priority'])))
        # logger.info("{}".format(len(first_64['table_name'])))

        channel = "bluse:///set"

        write_pair_redis(self.redis_server, key, json.dumps(targets))
        write_pair_redis(self.redis_server, key_64, json.dumps(first_64))

        if n_spare >= 1:
            logger.info('{} of {} targets plus {} random coordinates for spare beams published to {}'
                        .format(init_targets, len(targets.get('source_id')), n_spare, channel))
            write_pair_redis(self.redis_server, key_current_obs, json.dumps(first_64))
            write_pair_redis(self.redis_server, "{}:current_obs:target_list".format(product_id), json.dumps(first_64))
        elif n_spare < 1:
            logger.info('{} of {} targets published to {}'
                        .format(init_targets, len(targets.get('source_id')), channel))
            write_pair_redis(self.redis_server, key_current_obs, json.dumps(targets))
        publish(self.redis_server, channel, key)

    def pointing_coords(self, t_str):
        """Function used to clean up run loop and parse pointing information

        Parameters:
            t_str: (dict)
                schedule block telescope pointing information

        Returns:
            c_ra, c_dec: (float)
                pointing coordinates of the telescope
        """

        pointing = t_str['target'].split(', ')
        c_ra = Angle(pointing[-2], unit=u.hourangle).rad
        c_dec = Angle(pointing[-1], unit=u.deg).rad
        return c_ra, c_dec

    def _parse_sensor_name(self, message):
        """Parse channel name sent over redis channel

        Parameters:
            message: (str)
                Message received over the ASDF channel

        Returns:
            product_id: (str)
                product_id of the given subarray
            sensor: (str)
                Name of the particular sensor
        """

        try:
            if len(message.split(':')) == 3:
                product_id, sensor, _ = message.split(':')

            elif len(message.split(':')) == 2:
                product_id, sensor = message.split(':')

            else:
                temp = message.split(", ", maxsplit=1)  # TRYING TO FIGURE OUT ERROR
                temp[0] = temp[0].split(":")  # TRYING TO FIGURE OUT ERROR
                message = temp[0] + [temp[1]]  # TRYING TO FIGURE OUT ERROR

                product_id = message[0]
                sensor = message[1]

                # product_id, sensor = message.split(':', 2)[:2]

            return product_id, sensor

        except:
            logger.warning('Parsing sensor name failed. Unrecognized message '
                           'style: {}'.format(message))
            return False

    def beam_number(self, product_id):
        """Function to calculate the maximum number of targets observable with 64 beams

        Parameters:
            product_id: (str)
                ASDF

        Returns:
            ASDF: (float)
                ASDF
        """

        # pointing_coords = self._get_sensor_value(product_id, "current_obs:coords")
        # pointing_ra, pointing_dec = pointing_coords.split(", ")

        pulled_targets = json.loads(self._get_sensor_value(product_id, "current_obs:target_list"))
        entries_to_remove = ('source_id', 'priority', 'table_name', 'dist_c')
        for k in entries_to_remove:
            pulled_targets.pop(k, None)

        targets = pd.DataFrame.from_dict(pulled_targets)
        points = []

        for n in targets.index:
            targets['ra'][n] = Angle(targets['ra'][n] * u.deg).wrap_at(360 * u.deg).degree
            # coords = (round(targets['ra'][n], 4), round(targets['decl'][n], 4))
            coords = (targets['ra'][n], targets['decl'][n])
            points.append(coords)

        obs_freq = self._get_sensor_value(product_id, "current_obs:frequency")
        beamform_rad = ((2.998e8 / float(obs_freq)) / 1000) * 180 / math.pi

        total_circles = []

        for n in points:
            x_point = Angle(n[0] * u.deg).wrap_at(360 * u.deg).degree
            y_point = n[1]
            for i in range(1, 11):
                r_new = np.random.uniform(0, beamform_rad)
                x_max_r = Angle((x_point + r_new) * u.deg).wrap_at(360 * u.deg).degree
                x_min_r = Angle((x_point - r_new) * u.deg).wrap_at(360 * u.deg).degree
                x_max = x_point + beamform_rad
                x_min = x_point - beamform_rad
                y_max = y_point + beamform_rad
                y_min = y_point - beamform_rad
                x_new = np.random.uniform(x_min_r, x_max_r)
                # (x_new - x_point)**2 + (y - y_point)**2 = r_new**2
                y_new = np.sqrt((r_new ** 2) - ((x_new - x_point)**2)) + y_point
                contained_points = []
                filtered_points = [item for item in points if not (item[0] > x_max or item[0] < x_min or
                                                                   item[1] > y_max or item[1] < y_min)]
                for m in filtered_points:
                    x_point = Angle(m[0] * u.deg).wrap_at(360 * u.deg).degree
                    y_point = m[1]
                    if ((x_point - x_new) ** 2) + ((y_point - y_new) ** 2) <= (beamform_rad ** 2):
                        contained_points.append(m)
                logger.info("{} points contained within circle centre ({}, {}) of radius {} degrees"
                            .format(len(contained_points), x_new, y_new, beamform_rad))
                total_circles.append([x_new, y_new, len(contained_points), contained_points])

        circles_df = pd.DataFrame(sorted(total_circles, key=lambda x: x[2], reverse=True),
                                  columns=['ra', 'decl', 'n_contained', 'contained'])
        circles_df.to_csv('circles_df_init.csv')

        circles_containing = []
        while len(points) != 0:
            if len(circles_df['contained'][0]) != 0:
                covered = circles_df['contained'][0][0]
                circles_containing_ra = [item[0] for item in circles_containing]
                circles_containing.append([circles_df['ra'][0], circles_df['decl'][0],
                                           covered, max(1, len(set(circles_containing_ra)) + 1)])
                x_max = covered[0] + beamform_rad
                x_min = covered[0] - beamform_rad
                y_max = covered[1] + beamform_rad
                y_min = covered[1] - beamform_rad
                filtered_circles = circles_df[(circles_df['ra'] < x_max) & (circles_df['ra'] > x_min)
                                             & (circles_df['decl'] < y_max) & (circles_df['decl'] > y_min)]\
                    .reset_index(drop=True)
                for q in range(0, len(filtered_circles.index)):
                    circles_df = circles_df[(circles_df['ra'] != filtered_circles['ra'][q])
                                            & (circles_df['decl'] != filtered_circles['decl'][q])]
                    if covered in filtered_circles['contained'][q]:
                        filtered_circles['contained'][q].remove(covered)
                        filtered_circles.loc[q, 'n_contained'] = len(filtered_circles['contained'][q])
                circles_df = pd.concat([filtered_circles, circles_df]).reset_index(drop=True)
                if covered in points:
                    points.remove(covered)
                logger.info("Number of points remaining: {}".format(len(points)))
            else:
                circles_df = circles_df[circles_df['n_contained'] != 0]\
                    .sort_values('n_contained', ascending=False)\
                    .reset_index(drop=True)

        points_df = pd.DataFrame(circles_containing, columns=['circle_ra', 'circle_dec', 'point_coord', 'n_circles'])
        logger.info("\n\n{}\n".format(points_df))
        logger.info("{} visible targets can be observed with a minimum of {} beams"
                    .format(len(targets.index), points_df['n_circles'].max()))
        circles_df.to_csv('circles_df_fin.csv')
        points_df.to_csv('beamform_points.csv')
        beamform_64 = points_df[points_df['n_circles'] <= 64]
        logger.info("{} targets can be observed with {} beams"
                    .format(len(beamform_64.index), beamform_64['n_circles'].max()))

        # POSSIBLY WHEN INCLUDING PRIORITY, ADD WEIGHTING TO CIRCLES
        # MAKE SOME PLOTS TO VERIFY FUNCTIONALITY

    def _found_aliens(self):
        """You found aliens! Alerting slack

        Parameters:
            None

        Returns:
            None
        """
        try:
            from .slack_tools import notify_slack
        except ImportError:
            from slack_tools import notify_slack

        notify_slack("Congratulations! You found aliens!")
