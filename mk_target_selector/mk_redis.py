import re
import json
import yaml
import time
import threading
import pandas as pd
import numpy as np
from io import StringIO
from functools import reduce
from datetime import datetime, timedelta
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


final_target = "0"
total_targets = 0
pStatus = ProcessingStatus("ready")
proc_start_time = datetime.now()


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

        self.sensor_info = {}

        self.channel_actions = {
            'alerts': self._alerts,
            'sensor_alerts': self._sensor_alerts,
        }

        self.alerts_actions = {
            'deconfigure': self._deconfigure,
            'configure': self._pass,
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
            'target_selector': self._pass
        }

    def run(self):
        """Runs continuously to listen for messages that come in from specific
           redis channels. Main function that handles the processing of the
           messages that come through redis.
        """

        # target_key = '*:target_selector:*'
        # for m in self.redis_server.scan_iter(target_key):
        #     delete_key(self.redis_server, m)

        for item in self.p.listen():
            time_elapsed = (datetime.now() - proc_start_time).total_seconds()

            try:
                self._message_to_func(item['channel'], self.channel_actions)(item['data'])
            except Exception as e:
                logger.info(e, type(e))
            #     self.coord_error(item['data'])

            if ("target_selector:target_list" in item['data']) or ("deconfigure" in item['data']):
                pass
            else:
                self.fetch_data(item['data'])

    def fetch_data(self, message):
        """Runs continuously to fetch telescope status data and select targets if and when
           the status of the targets selector is ready & telescope status data is stored
        """
        product_ids = []
        target_selector_key = "*:target_selector:*"
        for k in self.redis_server.scan_iter(target_selector_key):
            product_id = k.split(":")[0]
            product_ids.append(product_id)

        for k in set(product_ids):
            if ("None" not in str(get_redis_key(self.redis_server, "{}:target_selector:coords".format(k))))\
                    and ("None" not in str(get_redis_key(self.redis_server, "{}:target_selector:frequency".format(k))))\
                    and ("None" not in str(get_redis_key(self.redis_server, "{}:target_selector:pool_resources".format(k))))\
                    and (pStatus.proc_status == "ready"):
                try:
                    coords = get_redis_key(self.redis_server, "{}:target_selector:coords".format(k))
                    coords_ra = float(coords.split(", ")[0])
                    coords_dec = float(coords.split(", ")[1])
                    self.sensor_info[k]['target'] = get_redis_key(self.redis_server, "{}:target_selector:coords".format(k))
                    logger.info("Fetched {}:target_selector:coords: {}".format(k, self.sensor_info[k]['target']))
                    self.sensor_info[k]['frequency'] = get_redis_key(self.redis_server, "{}:target_selector:frequency"
                                                                     .format(k))
                    logger.info("Fetched {}:target_selector:frequency: {}".format(k, self.sensor_info[k]['frequency']))
                    self.sensor_info[k]['pool_resources'] = get_redis_key(self.redis_server,
                                                                          "{}:target_selector:pool_resources"
                                                                          .format(k))
                    logger.info("Fetched {}:target_selector:pool_resources: {}"
                                .format(k, self.sensor_info[k]['pool_resources']))

                    targets = self.engine.select_targets(np.deg2rad(coords_ra), np.deg2rad(coords_dec),
                                                         beam_rad=self._beam_radius(k),
                                                         current_freq=self.sensor_info[k]['frequency'])
                    targets_table = pd.DataFrame.to_csv(targets)

                    if len(targets.index) == 0:
                        self.coord_error(coords=self.sensor_info[k]['target'],
                                         frequency=self.sensor_info[k]['frequency'])
                    else:
                        write_pair_redis(self.redis_server, "{}:target_selector:target_list".format(k), targets_table)
                        publish(self.redis_server, "sensor_alerts", "{}:target_selector:target_list".format(k))

                except KeyError as e:
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

    def _configure(self, product_id):
        """Response to a configure message from the redis alerts channel. This
           sets up a dictionary which stores sensor information for a particular
           product_id

        Parameters:
            product_id: (str)
                product_id for this particular subarray

        """
        logger.info("Configure message received: {}".format(product_id))
        if product_id not in self.sensor_info:
            self.sensor_info[product_id] = {'data_suspect': True, 'pointings': 0,
                                            'targets': [], 'pool_resources': '',
                                            'frequency': ''}

    def _deconfigure(self, product_id):
        """Response to deconfigure message from the redis alerts channel

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
            selector_key_glob = ("{}:target_selector:*".format(product_id))
            for a in self.redis_server.scan_iter(key_glob):
                logger.info('Deconfigure message received. Removing key: {}'.format(a))
                delete_key(self.redis_server, a)
            for b in self.redis_server.scan_iter(success_key_glob):
                # logger.info('Deconfigure message. Removing key: {}'.format(b))
                delete_key(self.redis_server, b)
            for c in self.redis_server.scan_iter(ackn_key_glob):
                # logger.info('Deconfigure message. Removing key: {}'.format(c))
                delete_key(self.redis_server, c)
            for d in self.redis_server.scan_iter(selector_key_glob):
                # logger.info('Deconfigure message. Removing key: {}'.format(c))
                delete_key(self.redis_server, d)

        # TODO: update the database with information inside the sensor_info
        try:
            del self.sensor_info[product_id]
        except KeyError:
            logger.info('Deconfigure message received before configure message')

    def _capture_start(self, message):
        """Block that responds to capture start updates. Upon receipt the target list is published & observation
         start time taken

       Parameters:
            message: (...)
                ...
        Returns:
            None
        """
        logger.info("Capture start message received: {}".format(message))
        product_id = message.split(":")[-1]
        sub_arr_id = self.sensor_info[product_id]['pointings']
        # print(self.sensor_info[product_id]['targets'][0])
        try:
            pulled_targets = StringIO(get_redis_key(self.redis_server,
                                                    "{}:target_selector:target_list".format(product_id)))

            targets_df = pd.read_csv(pulled_targets, sep=",", index_col=0)
            self.sensor_info[product_id]['targets'] = targets_df
            targets_to_publish = self.sensor_info[product_id]['targets']
            logger.info("Targets to publish:\n{}\n".format(targets_to_publish))
            self.sensor_info[product_id]['start_time'] = datetime.now()
            self._publish_targets(targets_to_publish, product_id, sub_arr_id)
        except pd.errors.EmptyDataError:  # no targets in table to parse
            pass

    def _capture_stop(self, message):
        """Block that responds to capture stop updates. Takes observing end time and stores metadata

       Parameters:
            message: (...)
                ...
        Returns:
            None
        """
        logger.info("Capture stop message received: {}".format(message))
        product_id = message.split(":")[-1]

        try:
            self.sensor_info[product_id]['end_time'] = datetime.now()
            self.store_metadata(product_id)
        except KeyError:  # no observation start time stored, ergo no target sources in FoV
            pass

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

        if sensor.startswith('target_selector'):
            sensor = 'target_selector'

        if product_id not in self.sensor_info.keys():
            self._configure(product_id)

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
            # p_num = self.sensor_info[product_id]['pointings']
            coord_key = "{}:target_selector:coords".format(product_id)
            coord_value = "{}, {}".format(coords.ra.deg, coords.dec.deg)
            write_pair_redis(self.redis_server, coord_key, coord_value)
            publish(self.redis_server, "sensor_alerts", coord_key)
            logger.info("Published {} to sensor_alerts: {}".format(coord_key, coord_value))

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
                                                 beam_rad, current_freq=self.sensor_info[product_id].get('frequency',
                                                                                                         'unknown'))
            self.sensor_info[product_id]['pointing_{}'.format(i)] = targets
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
        value = get_redis_key(self.redis_server, message)

        pool_resources_key = "{}:target_selector:pool_resources".format(product_id)
        write_pair_redis(self.redis_server, pool_resources_key, value)
        publish(self.redis_server, "sensor_alerts", pool_resources_key)
        logger.info("Published {} to sensor_alerts: {}".format(pool_resources_key, value))

    def _frequency(self, message):
        """Response to a frequency message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            current_freq:.....
                Current central frequency of observation
            current_band:.....
                Current frequency band of observation
        """
        logger.info("Frequency message received: {}".format(message))

        product_id, sensor_name = message.split(':')
        value = get_redis_key(self.redis_server, message)

        frequency_key = "{}:target_selector:frequency".format(product_id)
        write_pair_redis(self.redis_server, frequency_key, value)
        publish(self.redis_server, "sensor_alerts", frequency_key)
        logger.info("Published {} to sensor_alerts: {}".format(frequency_key, value))
        current_freq = value

        return current_freq

    def _processing_success(self, message):
        """Response to a successful processing success message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            asdf:.....
                ...
        """

        product_id = message.split(':')[0]
        sensor_name = message.split(':')[-1]
        source_id = sensor_name.split('_')[-1]
        # value = get_redis_key(self.redis_server, message)
        # self.sensor_info[product_id]['processing_success'] = value

        # update observation_status with success message
        self.engine.update_obs_status(source_id,
                                      obs_start_time=str(self.round_time(self.sensor_info[product_id]['start_time'])),
                                      processed='TRUE')

        if final_target in message:
            logger.info("Confirmation of successful processing of all sources received from processing nodes")
            pStatus.proc_status = "ready"
            logger.info("Processing state set to \'ready\'")

    def _acknowledge(self, message):
        """Response to a successful acknowledgement message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            asdf:.....
                ...
        """
        global proc_start_time

        product_id = message.split(':')[0]

        # if message contains final target source_id,
        key_glob = ('{}:*:{}'.format(product_id, 'targets'))
        for k in self.redis_server.scan_iter(key_glob):
            q = get_redis_key(self.redis_server, k)
            if self.reformat_table(q)['source_id'].iloc[-1].lstrip() in message:
                # all sources have been received by the processing nodes
                # begin processing time
                # pStatus.proc_status = "processing"
                logger.info("Receipt of all targets confirmed by processing nodes")
                # logger.info("Processing state set to \'processing\'")
                proc_start_time = datetime.now()
                # logger.info("pStatus.proc_status: {}".format(pStatus.proc_status))




    """

    Internal Methods

    """

    def reformat_table(self, table):
        """Function to reformat the table of targets pushed to the backend

        Parameters:
            table: (asdf)
                asdf

        Returns:
            targets_to_process: (asdf)
                asdf
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
            data: (string)
                string to parse containing erroneous coordinates
            coords: (string)
                string to parse containing erroneous coordinates
            frequency: (string)
                string to parse containing erroneous coordinates

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
        # logger.info("Processing state set to \'ready\'")

    def round_time(self, timestamp):
        """Function to round timestamp values to nearest second for database matching

        Parameters:
            timestamp: (datetime)
                asdf

        Returns:
            rounded: (datetime)
                asdf
        """
        dt = str(timestamp)
        date = dt.split()[0]
        h, m, s = [dt.split()[1].split(':')[0],
                   dt.split()[1].split(':')[1],
                   str(round(float(dt.split()[1].split(':')[-1])))]
        rounded = "{} {}:{}:{}".format(date, h, m, s)
        return rounded

    def load_schedule_block(self, message):
        """Reformats schedule block messages and reformats them into dictionary
           format

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
        """

        pool_resources = self.sensor_info[product_id]['pool_resources']

        antennas = ','.join(re.findall(r'm\d{3}', pool_resources))
        proxies = ','.join(re.findall(r'[a-z A-Z]+_\d', pool_resources))
        start = self.sensor_info[product_id]['start_time']
        end = self.sensor_info[product_id]['end_time']

        # TODO: Change this to handle specific pointing in subarray
        targets = self.sensor_info[product_id]['targets']

        # TODO: query frequency band sensor
        current_freq = self.sensor_info[product_id]['frequency']
        bands = self.engine._freqBand(current_freq)

        # antenna count
        n_antennas = antennas.count(',') + 1

        # TODO: ask Daniel/Dave about unique file-id
        file_id = 'filler_file_id'
        self.engine.add_sources_to_db(targets, start, end, proxies, antennas, n_antennas, file_id, bands)

    def _beam_radius(self, product_id, dish_size=13.5):
        """Returns the beam radius based on the frequency band used in the
           observation

       Parameters:
            product_id: (str)
                product ID for the given sub-array

        Returns:
            beam_rad: (float)
                Radius of the beam in radians
        """

        # TODO: change this to the real name
        current_freq = float(get_redis_key(self.redis_server, "{}:target_selector:frequency".format(product_id)))
        beam_rad = (2.998e8 / current_freq) / dish_size
        return beam_rad

    def _publish_targets(self, targets, product_id, channel, columns=None,
                         sub_arr_id=0, sensor_name='targets'):
        """Reformat the table returned from target searching

        Parameters:
            targets: (pandas.DataFrame)
                Target information
            product_id: (...)
                ...
            sub_arr_id: (...)
                ...
            sensor_name: (...)
                ...
            columns: (...)
                ...
            channel: (...)
                ...

        Returns:
            None
        """
        global final_target
        global total_targets

        if not columns:
            columns = ['source_id', 'ra', 'decl', 'priority']

        channel = "bluse:///set"
        targ_dict = targets.loc[:, columns].to_dict('list')
        key = '{}:pointing_{}:{}'.format(product_id, sub_arr_id, sensor_name)
        write_pair_redis(self.redis_server, key, json.dumps(targ_dict))
        publish(self.redis_server, channel, key)
        final_target = self.reformat_table(str(targ_dict))['\'source_id\''].iloc[-1].replace("\'", "").lstrip()
        total_targets = len(self.reformat_table(str(targ_dict))['\'source_id\''].index)
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
            channel: (str)
                Name of the channel coming over the

        Returns:
            sensor: (str)
                Name of the sensor attached to the message
            value: (str)
                Value of the particular sensor
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


def str_to_bool(value):
    """Returns a boolean value corresponding to

    Parameters:
        value: (str)
            String with

    Returns:
        boolean
    """

    if value == 'True':
        return True
    elif value == 'False':
        return False
    else:
        raise ValueError
