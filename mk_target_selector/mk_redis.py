import os
import re
import json
import yaml
import time
import threading
import sys
import numpy as np
from datetime import datetime
from astropy import units as u
from astropy.coordinates import Angle, SkyCoord
from astropy.config import get_config_dir

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

    def __init__(self, chan = ['sensor_alerts', 'alerts'], config_file = 'target_selector.yml'):
     
        threading.Thread.__init__(self)

        # Initialize redis connection
        self.redis_server = connect_to_redis()

        # Subscribe to channel
        self.p = self.redis_server.pubsub(ignore_subscribe_messages = True)
        self.p.psubscribe(chan)

        # Database connection and triaging
        self.engine = Triage(config_file)

        self.sensor_info = {}

        self.channel_actions = {
            'alerts': self._alerts,
            'sensor_alerts': self._sensor_alerts,
        }

        self.alerts_actions = { 
            'deconfigure'  : self._deconfigure,
            'configure': self._configure,
            'conf_complete': self._pass,
            'capture-init': self._pass,
            'capture-start': self._pass,
            'capture-stop': self._pass,
            'capture-done': self._pass
        }

        self.sensor_actions = {
            'data_suspect': self._data_suspect,
            'schedule_blocks': self._pass,
            'pool_resources': self._pool_resources,
            'observation_status': self._status_update,
            'target': self._target_query
        }

    def run(self):
        """Runs continuously to listen for messages that come in from specific
           redis channels. Main function that handles the processing of the
           messages that come through redis.
        """

        #for item in self.p.listen():
        #    self._message_to_func(item['channel'], self.channel_actions)(item['data'])

        for item in self.p.listen():
            try:
                self._message_to_func(item['channel'], self.channel_actions)(item['data'])
            except IndexError:
                if 'target' in item['data']:
                    arr_item_data = item['data'].split(', ')
                    logger.info('Selected coordinates ({}, {}) unavailable. Waiting for new coordinates'.format(arr_item_data[1], arr_item_data[2]))

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
        self.sensor_info[product_id] = {'data_suspect': True, 'pointings': 0,
                                        'targets': [], 'pool_resources': ''}

    def _deconfigure(self, product_id):
        """Response to deconfigure message from the redis alerts channel

        Parameters:
            item: (str)
                product_id for this particular subarray

        Returns:
            None
        """
        sensor_list = ['processing', 'targets']

        for sensor in sensor_list:
            key_glob = '{}:*:{}'.format(product_id, sensor)
            for k in self.redis_server.scan_iter(key_glob):
                logger.info('Deconfigure message. Removing key: {}'.format(k))
                delete_key(self.redis_server, k)

        # TODO: update the database with information inside the sensor_info
        try:
            del self.sensor_info[product_id]
        except KeyError:
            logger.info('Deconfigure message received before configure message')


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
            coords = SkyCoord(' '.join(value.split(', ')[-2:]), unit=(u.hourangle, u.deg))
            p_num = self.sensor_info[product_id]['pointings']
            self.sensor_info[product_id][sensor] = coords
            targets = self.engine.select_targets(coords.ra.rad, coords.dec.rad,
                                                 beam_rad = np.deg2rad(0.5))
            self.sensor_info[product_id]['pointings'] += 1
            self.sensor_info[product_id]['targets'].append(targets)
            self._publish_targets(targets, product_id = product_id,
                                  sub_arr_id = p_num)

    def _schedule_blocks(self, key):
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
                                                  beam_rad = np.deg2rad(0.5))
            self.sensor_info[product_id]['pointing_{}'.format(i)] = targets
            self._publish_targets(targets, product_id = product_id, sub_arr_id = i)

        logger.info('{} pointings processed in {} seconds'.format(len(target_pointing),
                                                             time.time() - start))

    def _data_suspect(self, message):
        """Response to a data_suspect message from the sensor_alerts channel.
        """

        product_id, _, value = message.split(':')
        value = str_to_bool(value)

        # If data_suspect is currently True and the new value is False, update the dictionary
        if self.sensor_info[product_id]['data_suspect'] and not value:
            self.sensor_info[product_id]['data_suspect'] = False
            self.sensor_info[product_id]['start_time'] = datetime.now()

        # If data_suspect is current False and the new value is True, set end time
        elif not self.sensor_info[product_id]['data_suspect'] and value:
            self.sensor_info[product_id]['data_suspect'] = True
            self.sensor_info[product_id]['end_time'] = datetime.now()
            self.store_metadata(product_id)

    def _pool_resources(self, message):
        """Response to a pool_resources message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            None
        """

        product_id, _ = message.split(':')
        value = get_redis_key(self.redis_server, message)
        self.sensor_info[product_id]['pool_resources'] = value

    """

    Internal Methods

    """

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
            item: (dict)
                message passed over redis channel

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

    def _unsubscribe(self, channels = None):
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

        antennas = ','.join(re.findall('m\d{3}', pool_resources))
        proxies = ','.join(re.findall('[a-z A-Z]+_\d', pool_resources))
        start = self.sensor_info[product_id]['start_time']
        end = self.sensor_info[product_id]['end_time']

        # TODO: Change this to handle specific pointing in subarray
        targets = self.sensor_info[product_id]['targets'][0]

        # TODO: query frequency band sensor
        bands = 'L BAND'

        # TODO: ask Daniel/Dave about unique file-id
        file_id = 'filler_file_id'
        self.engine.add_sources_to_db(targets, start, end, proxies, antennas,
                                      file_id, bands)

    def _beam_radius(self, product_id, dish_size = 13.5):
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
        sensor_name = 'max_freq'
        key = '{}:{}'.format(product_id, sensor_name)
        max_freq = get_redis_key(self.redis_server, key)
        return (2.998e8 / max_freq) / dish_size

    def _publish_targets(self, targets, product_id, sub_arr_id = 0, sensor_name = 'targets',
                         columns = ['ra', 'decl', 'priority'] , channel = 'bluse:///set'):
        """Reformat the table returned from target searching

        Parameters:
            targets: (pandas.DataFrame)
                Target information
            t: (dict)
                Information about the telescope pointing
            start_time: (str)
                Beginning of the observation

        Returns:
            None
        """

        targ_dict = targets.loc[:, columns].to_dict('list')
        key = '{}:pointing_{}:{}'.format(product_id, sub_arr_id, sensor_name)
        write_pair_redis(self.redis_server, key, json.dumps(targ_dict))
        publish(self.redis_server, channel, key)
        logger.info('Targets published to {}'.format(channel))

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
                temp = message.split(", ", maxsplit=1) # TRYING TO FIGURE OUT ERROR
                temp[0] = temp[0].split(":") # TRYING TO FIGURE OUT ERROR
                message = temp[0] + [temp[1]] # TRYING TO FIGURE OUT ERROR
                lenMessage = len(message)

                product_id = message[0]
                sensor = message[1]

                #product_id, sensor = message.split(':', 2)[:2]

            return product_id, sensor

        except:
            logger.warning('Parsing sensor name failed. Unrecognized message ' \
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

        notify_slack()


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
