import re
import json
import yaml
import time
import threading
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

            product_id = self._parse_sensor_name(item['data'])[0]
            if "None" not in str(self._get_sensor_value(product_id, "current_obs:proc_start_time")):
                proc_start_time = self._get_sensor_value(product_id, "current_obs:proc_start_time")
                time_elapsed = (datetime.now()
                                - datetime.strptime(proc_start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
                obs_start_time = self._get_sensor_value(product_id, "current_obs:obs_start_time")
                obs_end_time = self._get_sensor_value(product_id, "current_obs:obs_end_time")
                observation_time = (datetime.strptime(obs_end_time, "%Y-%m-%d %H:%M:%S.%f")
                                    - datetime.strptime(obs_start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()

                self.abort_criteria(product_id, time_elapsed, observation_time)

    def fetch_data(self, product_id, mode):
        """Fetches telescope status data and select targets if and when
           the status of the targets selector is ready & telescope status data is stored

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
                    write_pair_redis(self.redis_server, "{}:{}:coords".format(product_id, mode), new_coords)
                    logger.info("Fetched {}:{}:coords: {}"
                                .format(product_id, mode, self._get_sensor_value(product_id, "{}:coords"
                                                                                 .format(mode))))
                    write_pair_redis(self.redis_server, "{}:{}:frequency".format(product_id, mode), new_freq)
                    logger.info("Fetched {}:{}:frequency: {}"
                                .format(product_id, mode, self._get_sensor_value(product_id, "{}:frequency"
                                                                                 .format(mode))))
                    write_pair_redis(self.redis_server, "{}:{}:pool_resources".format(product_id, mode), new_pool)
                    logger.info("Fetched {}:{}:pool_resources: {}"
                                .format(product_id, mode, self._get_sensor_value(product_id, "{}:pool_resources"
                                                                                 .format(mode))))

                targets = self\
                    .engine.select_targets(np.deg2rad(coords_ra),
                                           np.deg2rad(coords_dec),
                                           current_freq=self._get_sensor_value(product_id, "{}:frequency".format(mode)),
                                           beam_rad=self._beam_radius(self._get_sensor_value(product_id,
                                                                                             "{}:frequency"
                                                                                             .format(mode))))

                targets_table = pd.DataFrame.to_csv(targets)

                if len(targets.index) == 0:
                    self.coord_error(coords=self._get_sensor_value(product_id, "{}:coords".format(mode)),
                                     frequency=self._get_sensor_value(product_id, "{}:frequency".format(mode)))
                else:
                    write_pair_redis(self.redis_server, "{}:{}:target_list".format(product_id, mode), targets_table)

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
                pulled_targets = StringIO(self._get_sensor_value(product_id, "current_obs:target_list"))
                pulled_coords = self._get_sensor_value(product_id, "current_obs:coords")
                pulled_freq = self.engine.freq_format(self._get_sensor_value(product_id, "current_obs:frequency"))

                targets_to_publish = pd.read_csv(pulled_targets, sep=",", index_col=0)
                logger.info("Targets to publish for {} at {}:\n\n{}\n".format(pulled_coords,
                                                                              pulled_freq, targets_to_publish))

                obs_start_time = datetime.now()
                write_pair_redis(self.redis_server,
                                 "{}:current_obs:obs_start_time".format(product_id), str(obs_start_time))

                self._publish_targets(targets_to_publish, product_id, sub_arr_id)

        elif pStatus.proc_status == "processing":
            self.fetch_data(product_id, mode="new_obs")
            if "None" not in str(self._get_sensor_value(product_id, "new_obs:target_list")):

                new_target_list = pd.read_csv(StringIO(self._get_sensor_value(product_id,
                                                                              "new_obs:target_list")), sep=",",
                                              index_col=0)

                remaining_to_process = pd.read_csv(StringIO(self._get_sensor_value(product_id,
                                                                                   "current_obs:remaining_to_process")),
                                                   sep=",", index_col=0)

                n_remaining = len(remaining_to_process.index)
                n_new_list = len(new_target_list.index)
                r_med_remaining = remaining_to_process['dist_c'].median()
                r_med_new_list = new_target_list['dist_c'].median()

                if (n_remaining < n_new_list) and (r_med_new_list < r_med_remaining):
                    self.abort_criteria(product_id, n_remaining, n_new_list, r_med_remaining, r_med_new_list)
                    self.fetch_data(product_id, mode="current_obs")
                    if "None" not in str(self._get_sensor_value(product_id, "current_obs:target_list")):
                        sub_arr_id = "0"  # TODO: CHANGE TO HANDLE SUB-ARRAYS
                        pulled_targets = StringIO(self._get_sensor_value(product_id, "current_obs:target_list"))
                        pulled_coords = self._get_sensor_value(product_id, "current_obs:coords")
                        pulled_freq = self.engine.freq_format(
                            self._get_sensor_value(product_id, "current_obs:frequency"))

                        targets_to_publish = pd.read_csv(pulled_targets, sep=",", index_col=0)
                        logger.info("Targets to publish for {} at {}:\n\n{}\n".format(pulled_coords,
                                                                                      pulled_freq, targets_to_publish))

                        obs_start_time = datetime.now()
                        write_pair_redis(self.redis_server,
                                         "{}:current_obs:obs_start_time".format(product_id), str(obs_start_time))

                        self._publish_targets(targets_to_publish, product_id, sub_arr_id)

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

        # LATEST CHANGES BELOW: won't need after we have tracking messages
        if "None" not in str(self._get_sensor_value(product_id, "current_obs:target_list")) \
                and "None" in str(self._get_sensor_value(product_id, "current_obs:obs_end_time")):  # HERE
            try:
                obs_end_time = datetime.now()
                write_pair_redis(self.redis_server, "{}:current_obs:obs_end_time".format(product_id), str(obs_end_time))
                self.store_metadata(product_id)
            except Exception as e:
                logger.info(e)

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
            logger.info("Wrote [{}] to [{}]".format(coord_value, coord_key))

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
        logger.info("Wrote [{}] to [{}]".format(pool_resources_value, pool_resources_key))

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
        logger.info("Wrote [{}] to [{}]".format(frequency_value, frequency_key))

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
        source_id = sensor_name.split('_')[-1]

        # update observation_status with success message
        self.engine.update_obs_status(source_id,
                                      obs_start_time=str(self.round_time
                                                         (self._get_sensor_value
                                                          (product_id, "current_obs:obs_start_time"))),
                                      processed='TRUE')

        target_list = pd.read_csv(StringIO(self._get_sensor_value(product_id,
                                                                  "current_obs:target_list")), sep=",", index_col=0)
        number_to_process = len(target_list.index)
        number_processed = target_list[target_list['source_id'] == float(source_id)].index.values[0]+1
        fraction_processed = number_processed / number_to_process

        remaining_to_process = pd.DataFrame.to_csv(target_list.drop(
            target_list[target_list.index < number_processed - 1].index))

        write_pair_redis(self.redis_server,
                         "{}:current_obs:remaining_to_process".format(product_id), remaining_to_process)

        proc_start_time = self._get_sensor_value(product_id, "current_obs:proc_start_time")
        time_elapsed = (datetime.now() - datetime.strptime(proc_start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()

        if number_processed == number_to_process:
            logger.info("Confirmation of successful processing of all sources received from processing nodes")
            self._deconfigure(product_id)
            pStatus.proc_status = "ready"
            logger.info("Processing state set to \'ready\'")

        self.abort_criteria(product_id, time_elapsed, fraction_processed)

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
        key_glob = ('{}:*:{}'.format(product_id, 'targets'))
        for k in self.redis_server.scan_iter(key_glob):
            q = get_redis_key(self.redis_server, k)
            if get_redis_key(self.redis_server, "{}:current_obs:proc_start_time".format(product_id)) is None\
                    and self.reformat_table(q)['source_id'].iloc[-1].lstrip() in message:
                # all sources have been received by the processing nodes
                # begin processing time
                logger.info("Receipt of all targets confirmed by processing nodes")
                proc_start_time = datetime.now()
                write_pair_redis(self.redis_server,
                                 "{}:current_obs:proc_start_time".format(product_id), str(proc_start_time))

    """

    Internal Methods

    """

    def abort_criteria(self, product_id, time_elapsed=None, observation_time=None, fraction_processed=None,
                       n_remaining=None, n_new_list=None, r_med_remaining=None, r_med_new_list=None):
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
            n_remaining: (str)
                Number of sources remaining to be processed in the current block
            n_new_list: (str)
                Number of sources in the new target list
            r_med_remaining: (str)
                Median distance of sources remaining to be processed in the current block
            r_med_new_list: (str)
                Median distance of sources in the new target list

        Returns:
            None
        """
        if (not fraction_processed) and (not observation_time):
            # processing aborted based on greater number of sources within a given distance in the new pointing compared
            # with the remaining unprocessed sources
            logger.info("{} sources with a median distance of {} pc left to process"
                        .format(n_remaining, r_med_remaining))
            logger.info("{} sources with a median distance of {} pc contained in new pointing"
                        .format(n_new_list, r_med_new_list))
            logger.info("New pointing contains a greater number of sources with a lower median distance."
                        " Aborting processing of current pointing")
            self._deconfigure(product_id)
            pStatus.proc_status = "ready"
            logger.info("Processing state set to \'ready\'")

        elif not fraction_processed:  # processing aborted based on observation time (t_obs)
            if (time_elapsed > 1200) and (time_elapsed > (2 * observation_time) - 300):
                logger.info("Processing time has exceeded both 20 and (2t_obs - 5) minutes."
                            " Aborting processing of current pointing")
                self._deconfigure(product_id)
                pStatus.proc_status = "ready"
                logger.info("Processing state set to \'ready\'")

        elif not observation_time:  # processing aborted based on absolute processing time
            if (fraction_processed > 0.9) and (time_elapsed > 600):
                logger.info("Processing time has exceeded 10 minutes, with >90% of targets processed successfully."
                            " Aborting processing of current pointing")
                self._deconfigure(product_id)
                pStatus.proc_status = "ready"
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

    def coord_error(self, coords, frequency):
        """Function to handle errors due to coordinate values (empty pointings or out of range)

        Parameters:
            coords: (str)
                string to parse containing erroneous coordinates
            frequency: (str)
                the central frequency of observation

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

    def store_metadata(self, product_id):
        """Stores observation metadata in database.

        Parameters:
            product_id: (str)
                Product ID of the subarray from which status metadata is pulled to add to the table of previously
                 completed observations

        Returns:
            None
        """

        pool_resources = self._get_sensor_value(product_id, "current_obs:pool_resources")

        antennas = ','.join(re.findall(r'm\d{3}', pool_resources))
        proxies = ','.join(re.findall(r'[a-z A-Z]+_\d', pool_resources))
        start = self._get_sensor_value(product_id, "current_obs:obs_start_time")
        end = self._get_sensor_value(product_id, "current_obs:obs_end_time")

        # TODO: Change this to handle specific pointing in subarray
        targets = pd.read_csv(StringIO(self._get_sensor_value(product_id, "current_obs:target_list")),
                              sep=",", index_col=0)

        # TODO: query frequency band sensor
        current_freq = self._get_sensor_value(product_id, "current_obs:frequency")
        bands = self.engine._freqBand(current_freq)

        # antenna count
        n_antennas = antennas.count(',') + 1

        # TODO: ask Daniel/Dave about unique file-id
        file_id = 'filler_file_id'
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

    def _publish_targets(self, targets, product_id, channel, columns=None,
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

        if not columns:
            columns = ['source_id', 'ra', 'decl', 'priority']

        channel = "bluse:///set"
        targ_dict = targets.loc[:, columns].to_dict('list')
        key = '{}:pointing_{}:{}'.format(product_id, sub_arr_id, sensor_name)
        write_pair_redis(self.redis_server, key, json.dumps(targ_dict))
        publish(self.redis_server, channel, key)

        logger.info('Targets published to {}'.format(channel))
        pStatus.proc_status = "processing"
        logger.info("Processing state set to 'processing\'")

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
