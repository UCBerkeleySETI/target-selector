import os
import re
import json
import yaml
import time
import threading
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
        print("Running init function...") # TRYING TO FIGURE OUT ERROR
     
        threading.Thread.__init__(self)

        # Initialize redis connection
        print("Initialising redis connection...") # TRYING TO FIGURE OUT ERROR
        self.redis_server = connect_to_redis()
        print("...redis connection initialised.") # TRYING TO FIGURE OUT ERROR

        # Subscribe to channel
        print("Subscribing to channel...") # TRYING TO FIGURE OUT ERROR
        self.p = self.redis_server.pubsub(ignore_subscribe_messages = True)
        self.p.psubscribe(chan)
        print("...Subscribed to channel.") # TRYING TO FIGURE OUT ERROR

        # Database connection and triaging
        print("Triaging according to config file...") # TRYING TO FIGURE OUT ERROR
        self.engine = Triage(config_file)
        print("...Triaged according to config file.") # TRYING TO FIGURE OUT ERROR

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
            'target': self._target
        }

    def run(self):
        """Runs continuously to listen for messages that come in from specific
           redis channels. Main function that handles the processing of the
           messages that come through redis.
        """

        for item in self.p.listen():
            print("Running run function for item...") # TRYING TO FIGURE OUT ERROR
            print("run(self): Item: ", item) # TRYING TO FIGURE OUT ERROR
            print("run(self): Item type: ", type(item)) # TRYING TO FIGURE OUT ERROR
            print("run(self): Channel item: ", item['channel']) # TRYING TO FIGURE OUT ERROR
            print("run(self): Data item: ", item['data']) # TRYING TO FIGURE OUT ERROR
            self._message_to_func(item['channel'], self.channel_actions)(item['data'])
            print("...Run function ran for item.") # TRYING TO FIGURE OUT ERROR

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
        print("Running _alerts function...") # TRYING TO FIGURE OUT ERROR
        sensor, product_id = self._parse_sensor_name(message)
        print("_alerts(self, message): Sensor: ", sensor) # TRYING TO FIGURE OUT ERROR
        print("_alerts(self, message): Product ID: ", product_id) # TRYING TO FIGURE OUT ERROR
        self._message_to_func(sensor, self.alerts_actions)(product_id)
        print("..._alerts function ran.") # TRYING TO FIGURE OUT ERROR

    def _pass(self, item):
        print("Running _pass function...") # TRYING TO FIGURE OUT ERROR
        """Temporary function to handle alerts that we don't care about responding
        to at the moment
        """
        return 0
        print("..._pass function ran.") # TRYING TO FIGURE OUT ERROR

    def _configure(self, product_id):
        """Response to a configure message from the redis alerts channel. This
           sets up a dictionary which stores sensor information for a particular
           product_id

        Parameters:
            product_id: (str)
                product_id for this particular subarray

        """
        print("Running _configure function...") # TRYING TO FIGURE OUT ERROR
        self.sensor_info[product_id] = {'data_suspect': True, 'pointings': 0,
                                        'targets': [], 'pool_resources': ''}
        print("..._configure function ran.") # TRYING TO FIGURE OUT ERROR

    def _deconfigure(self, product_id):
        """Response to deconfigure message from the redis alerts channel

        Parameters:
            item: (str)
                product_id for this particular subarray

        Returns:
            None
        """
        print("Running _deconfigure function...") # TRYING TO FIGURE OUT ERROR
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

        print("..._deconfigure function ran.") # TRYING TO FIGURE OUT ERROR


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
        print("Running _sensor_alerts function...") # TRYING TO FIGURE OUT ERROR
        product_id, sensor = self._parse_sensor_name(message)
        print("_sensor_alerts(self, message): Sensor: ", sensor) # TRYING TO FIGURE OUT ERROR
        print("_sensor_alerts(self, message): Product ID: ", product_id) # TRYING TO FIGURE OUT ERROR

        if sensor.endswith('pool_resources'):
            sensor = 'pool_resources'

        if product_id not in self.sensor_info.keys():
            self._configure(product_id)

        print("_sensor_alerts(self, message): Message: ", message) # TRYING TO FIGURE OUT ERROR
        print("_sensor_alerts(self, message): Message type: ", type(message)) # TRYING TO FIGURE OUT ERROR
        self._message_to_func(sensor, self.sensor_actions)(message)
        print("..._sensor_alerts function ran.") # TRYING TO FIGURE OUT ERROR

    def _target(self, message):
        """Response to message from the Sensor Alerts channel. If both the right
        ascension and declination are stored, then the database is queried
        for

        Parameters:
            message: (str)
                Message passed over the sensor alerts channel

        Returns:
            None
        """
        print("Running _target function...") # TRYING TO FIGURE OUT ERROR
        print("_target(self, message): Target message passed: ", message) # TRYING TO FIGURE OUT ERROR
        product_id, sensor, value = message.split(':', 2)
        print("_target(self, message): Target: ", 'target') # TRYING TO FIND ERROR

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

        print("..._target function ran.") # TRYING TO FIGURE OUT ERROR

    def _schedule_blocks(self, key):
        """Block that responds to schedule block updates. Searches for targets
           and publishes the information to the processing channel

       Parameters:
            key: (dict)
                Redis channel message received from listening to a channel

        Returns:
            None
        """
        print("Running _schedule_blocks function...") # TRYING TO FIGURE OUT ERROR
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

        print("..._schedule_blocks function ran.") # TRYING TO FIGURE OUT ERROR

    def _data_suspect(self, message):
        """Response to a data_suspect message from the sensor_alerts channel.
        """

        print("Running _data_suspect function...") # TRYING TO FIGURE OUT ERROR

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

        print("..._data_suspect function ran.") # TRYING TO FIGURE OUT ERROR

    def _pool_resources(self, message):
        """Response to a pool_resources message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channels. Acts as the key to
                query Redis in the case of this function.

        Returns:
            None
        """
        print("Running _pool_resources function...") # TRYING TO FIGURE OUT ERROR

        product_id, _ = message.split(':')
        value = get_redis_key(self.redis_server, message)
        self.sensor_info[product_id]['pool_resources'] = value

        print("..._pool_resources function ran.") # TRYING TO FIGURE OUT ERROR


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
        print("Running load_schedule_block function...") # TRYING TO FIGURE OUT ERROR

        message = message.replace('"[', '[')
        message = message.replace(']"', ']')
        return yaml.safe_load(message)

        print("...load_schedule_block function ran.") # TRYING TO FIGURE OUT ERROR


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
        print("Running get_sensor_value function...") # TRYING TO FIGURE OUT ERROR

        key = '{}:{}'.format(product_id, sensor_name)
        value = get_redis_key(self.redis_server, key)
        return value

        print("...get_sensor_value function ran.") # TRYING TO FIGURE OUT ERROR

    def _message_to_func(self, channel, action):
        """Function that selects a function to run based on the channel entered

        Parameters:
            channel: (str)
                channel/sensor name

        Returns:
            Function attached to a particular sensor_name

        """
        print("Running _message_to_func function...") # TRYING TO FIGURE OUT ERROR
        print("_message_to_func(self, channel, action): Channel: ", channel) # TRYING TO FIGURE OUT ERROR
        print("_message_to_func(self, channel, action): Action: ", action) # TRYING TO FIGURE OUT ERROR

        return action.get(channel, self._other)

        print("..._message_to_func function ran.") # TRYING TO FIGURE OUT ERROR

    def _other(self, channel):
        """Function that handles unrecognized requests from redis server

        Parameters:
            item: (dict)
                message passed over redis channel

        Returns:
            None
        """
        print("Running _other function...") # TRYING TO FIGURE OUT ERROR
        logger.info('Unrecognized channel style: {}'.format(channel))
        print("..._other function ran.") # TRYING TO FIGURE OUT ERROR

    def _status_update(self, msg):
        """Function to test the status_update from the processing nodes.

        Parameters:
            msg: (str)
                string formatted like a

        Returns:
            None
        """
        print("Running _status_update function...") # TRYING TO FIGURE OUT ERROR
        status_msg = self.load_schedule_block(msg)
        if status_msg['success']:
            self.engine.update_obs_status(**status_msg)

        print("..._status_update function ran.") # TRYING TO FIGURE OUT ERROR

    def _unsubscribe(self, channels = None):
        """Unsubscribe from the redis server

        Parameters:
            channels: (str, list)
                List of channels you wish to unsubscribe to

        Returns:
            None
        """
        print("Running _unsubscribe function...") # TRYING TO FIGURE OUT ERROR

        if channels is None:
            self.p.unsubscribe()
        else:
            self.p.unsubscribe(channels)

        logger.info('Unsubscribed from channel(s)')
        print("..._unsubscribe function ran.") # TRYING TO FIGURE OUT ERROR

    def store_metadata(self, product_id):
        """Stores observation metadata in database.
        """
        print("Running store_metadata function...") # TRYING TO FIGURE OUT ERROR

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

        print("...store_metadata function ran.") # TRYING TO FIGURE OUT ERROR

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
        print("Running _beam_radius function...") # TRYING TO FIGURE OUT ERROR

        # TODO: change this to the real name
        sensor_name = 'max_freq'
        key = '{}:{}'.format(product_id, sensor_name)
        max_freq = get_redis_key(self.redis_server, key)
        return (2.998e8 / max_freq) / dish_size

        print("..._beam_radius function ran.") # TRYING TO FIGURE OUT ERROR

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
        print("Running _publish_targets function...") # TRYING TO FIGURE OUT ERROR

        targ_dict = targets.loc[:, columns].to_dict('list')
        key = '{}:pointing_{}:{}'.format(product_id, sub_arr_id, sensor_name)
        write_pair_redis(self.redis_server, key, json.dumps(targ_dict))
        publish(self.redis_server, channel, key)
        logger.info('Targets published to {}'.format(channel))

        print("..._publish_targets function ran.") # TRYING TO FIGURE OUT ERROR

    def pointing_coords(self, t_str):
        """Function used to clean up run loop and parse pointing information

        Parameters:
            t_str: (dict)
                schedule block telescope pointing information

        Returns:
            c_ra, c_dec: (float)
                pointing coordinates of the telescope
        """
        print("Running pointing_coords function...") # TRYING TO FIGURE OUT ERROR

        pointing = t_str['target'].split(', ')
        c_ra = Angle(pointing[-2], unit=u.hourangle).rad
        c_dec = Angle(pointing[-1], unit=u.deg).rad
        return c_ra, c_dec

        print("...pointing_coords function ran.") # TRYING TO FIGURE OUT ERROR

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
        print("Running _parse_sensor_name function...") # TRYING TO FIGURE OUT ERROR

        try:
            if len(message.split(':')) == 3:
                product_id, sensor, _ = message.split(':')
                print("_parse_sensor_name(self, message): Length of split message = 3") # TRYING TO FIGURE OUT ERROR
                print("_parse_sensor_name(self, message): Product ID: ", product_id) # TRYING TO FIGURE OUT ERROR
                print("_parse_sensor_name(self, message): Sensor: ", sensor) # TRYING TO FIGURE OUT ERROR

            elif len(message.split(':')) == 2:
                product_id, sensor = message.split(':')
                print("_parse_sensor_name(self, message): Length of split message = 2") # TRYING TO FIGURE OUT ERROR
                print("_parse_sensor_name(self, message): Product ID: ", product_id) # TRYING TO FIGURE OUT ERROR
                print("_parse_sensor_name(self, message): Sensor: ", sensor) # TRYING TO FIGURE OUT ERROR

            else:
                print("_parse_sensor_name(self, message): Message to split: ", message) # TRYING TO FIGURE OUT ERROR

                #print("_parse_sensor_name(self, message): Split message 0 times: ", message.split(':', 0)) # TRYING TO FIGURE OUT ERROR
                #print("_parse_sensor_name(self, message): Split message 1 time: ", message.split(':', 1)) # TRYING TO FIGURE OUT ERROR
                #print("_parse_sensor_name(self, message): Split message 2 times: ", message.split(':', 2)) # TRYING TO FIGURE OUT ERROR
                #print("_parse_sensor_name(self, message): Split message 3 times: ", message.split(':', 3)) # TRYING TO FIGURE OUT ERROR

                temp = message.split(", ", maxsplit=1) # TRYING TO FIGURE OUT ERROR
                temp[0] = temp[0].split(":") # TRYING TO FIGURE OUT ERROR
                message = temp[0] + [temp[1]] # TRYING TO FIGURE OUT ERROR
                lenMessage = len(message)

                print("_parse_sensor_name(self, message): Split message: ", message) # TRYING TO FIGURE OUT ERROR
                print("_parse_sensor_name(self, message): Length of split message: ", lenMessage) # TRYING TO FIGURE OUT ERROR

                product_id = message[0]
                sensor = message[1]

                #product_id, sensor = message.split(':', 2)[:2]

                print("_parse_sensor_name(self, message): Product ID: ", product_id) # TRYING TO FIGURE OUT ERROR
                print("_parse_sensor_name(self, message): Sensor: ", sensor) # TRYING TO FIGURE OUT ERROR

            return product_id, sensor

        except:
            logger.warning('Parsing sensor name failed. Unrecognized message ' \
                           'style: {}'.format(message))
            return False

        print("..._parse_sensor_name function ran.") # TRYING TO FIGURE OUT ERROR

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

    print("Running str_to_bool function...") # TRYING TO FIGURE OUT ERROR

    if value == 'True':
        return True
    elif value == 'False':
        return False
    else:
        raise ValueError

    print("...str_to_bool function ran.") # TRYING TO FIGURE OUT ERROR
