import re
import json
import yaml
import time
import threading
import math
import random
import pytz
import scipy.constants as con
import pandas as pd
import numpy as np
from io import StringIO
from functools import reduce
from datetime import datetime
from geometry import Target, great_circle_distance, cosine_attenuation
from target_selector_variables import priority_decay, primary_sensitivity_exponent, number_beams
from optimizer import Optimizer
from test_plot import test_plot

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


successfully_processed = []


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

    def fetch_data(self, product_id, mode):
        """Fetches telescope status data and selects targets when telescope status data is stored

        Parameters:
            product_id: (str)
                Product ID parsed from redis message
            mode: (str)
                Mode in which function is to be run; "recalculating" is used to update parameters for the currently
                processing sample, "current_obs" fetches the data and writes it to current_obs redis keys, while
                "new_obs" fetches the data for comparison against the currently processing block, without overwriting
                 keys relating to the currently processing observations
        Returns:
            None
        """
        if mode == "recalculating":
            # mode = recalculating: used to update priority values for the currently processing sample
            current_coords = self._get_sensor_value(product_id, "current_obs:coords")
            coords_ra = float(current_coords.split(", ")[0])
            coords_dec = float(current_coords.split(", ")[1])
            current_freq = self._get_sensor_value(product_id, "current_obs:frequency")
            targets = self \
                .engine.select_targets(np.deg2rad(coords_ra),
                                       np.deg2rad(coords_dec),
                                       current_freq=current_freq,
                                       beam_rad=self._beam_radius(current_freq))
            columns = ['ra', 'decl', 'source_id', 'dist_c', 'table_name', 'priority']
            targ_dict = targets.loc[:, columns].to_dict('list')
            write_pair_redis(self.redis_server, "{}:current_obs:target_list"
                             .format(product_id), json.dumps(targ_dict))
        else:
            # mode = new_obs: fetches data for comparison against currently processing block
            # mode = current_obs: fetches data and writes it to current_obs redis keys
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

                    targets = self\
                        .engine.select_targets(np.deg2rad(coords_ra),
                                               np.deg2rad(coords_dec),
                                               current_freq=self._get_sensor_value(product_id, "{}:frequency".format(mode)),
                                               beam_rad=self._beam_radius(self._get_sensor_value(product_id,
                                                                                                 "{}:frequency"
                                                                                                 .format(mode))))

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
            success_key_glob = ('{}:success_*'.format(product_id))
            ackn_key_glob = ('{}:acknowledge_*'.format(product_id))
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

                obs_start_time = datetime.now(pytz.utc)
                write_pair_redis(self.redis_server,
                                 "{}:current_obs:obs_start_time".format(product_id), str(obs_start_time))

                self._publish_targets(pulled_targets, product_id, sub_arr_id)

        elif pStatus.proc_status == "processing":
            logger.info("Still processing previous pointing. Checking abort criteria")
            self.fetch_data(product_id, mode="new_obs")
            if "None" not in str(self._get_sensor_value(product_id, "new_obs:target_list")):

                # read in and format new target list from redis key to dataframe
                new_target_list = pd.DataFrame.from_dict(json.loads(
                    self._get_sensor_value(product_id, "new_obs:target_list")))
                new_coords = self._get_sensor_value(product_id, "new_obs:coords")
                new_frequency = self._get_sensor_value(product_id, "new_obs:frequency")
                appended_new = self.append_tbdfm(new_target_list, new_coords, new_frequency)

                # read in and format list of targets which remain to be processed from redis key to dataframe
                current_target_list = pd.DataFrame.from_dict(json.loads(
                    self._get_sensor_value(product_id, "current_obs:target_list")))
                current_coords = self._get_sensor_value(product_id, "current_obs:coords")
                current_frequency = self._get_sensor_value(product_id, "current_obs:frequency")
                appended_current = self.append_tbdfm(current_target_list, current_coords, current_frequency)

                # total TBDFM parameter for sources in the new target list
                tot_new_tbdfm = appended_new['tbdfm_param'].sum()
                # number of sources in the new target list
                n_new_obs = len(appended_new.index)

                # total TBDFM parameter for sources currently processing
                tot_current_tbdfm = appended_current['tbdfm_param'].sum()
                # number of sources currently processing
                n_current_obs = len(appended_current.index)

                logger.info("Total TBDFM parameter Σ((sensitivity ** {}) * {} ** (7 - priority)) "
                            "for {} targets in new pointing = {}"
                            .format(primary_sensitivity_exponent(), priority_decay(),
                                    n_new_obs, tot_new_tbdfm))
                logger.info("Total TBDFM parameter Σ((sensitivity ** {}) * {} ** (7 - priority)) "
                            "for {} targets in current pointing = {}"
                            .format(primary_sensitivity_exponent(), priority_decay(),
                                    n_current_obs, tot_current_tbdfm))

                if tot_new_tbdfm > tot_current_tbdfm:
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

                        obs_start_time = datetime.now(pytz.utc)
                        write_pair_redis(self.redis_server,
                                         "{}:current_obs:obs_start_time".format(product_id), str(obs_start_time))

                        self._publish_targets(pulled_targets, product_id, sub_arr_id)

                elif tot_new_tbdfm <= tot_current_tbdfm:
                    logger.info("New pointing does not contain sources with a higher total TBDFM "
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
            # coords = SkyCoord(' '.join(value.split(', ')[-2:]), unit=(u.hourangle, u.deg))
            coord_key = "{}:new_obs:coords".format(product_id)
            coord_string = value.split(', ', 1)[1]
            coord_value = "{}, {}".format(self.hms_dms_decimal_convert(coord_string)[0],
                                          self.hms_dms_decimal_convert(coord_string)[-1])
            write_pair_redis(self.redis_server, coord_key, coord_value)
            # logger.info("Wrote [{}] to [{}]".format(coord_value, coord_key))

    # def _schedule_blocks(self, key, target_pointing, beam_rad):
    #     """Block that responds to schedule block updates. Searches for targets
    #        and publishes the information to the processing channel
    #
    #    Parameters:
    #         key: (dict)
    #             Redis channel message received from listening to a channel
    #
    #     Returns:
    #         None
    #     """
    #
    #     message = get_redis_key(self.redis_server, key)
    #     product_id = key.split(':')[0]
    #     schedule_block = self.load_schedule_block(message)
    #
    #     if isinstance(schedule_block, list):
    #         if isinstance(schedule_block[0], list):
    #             target_pointing = schedule_block[0]
    #         elif isinstance(schedule_block[0], dict):
    #             target_pointing = schedule_block
    #
    #     if isinstance(schedule_block, dict):
    #         target_pointing = schedule_block['targets']
    #
    #     start = time.time()
    #     for i, t in enumerate(target_pointing):
    #         targets = self.engine.select_targets(*self.pointing_coords(t),
    #                                              beam_rad,
    #                                              current_freq=self._get_sensor_value(product_id,
    #                                                                                  "current_obs:frequency"))
    #         self._publish_targets(targets, product_id=product_id, sub_arr_id=i)
    #
    #     logger.info('{} pointings processed in {} seconds'.format(len(target_pointing),
    #                                                               time.time() - start))

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
        frequency_value = get_redis_key(self.redis_server, message)
        logger.info("Frequency message received: {}, {}".format(message, frequency_value))

        product_id, sensor_name = message.split(':')
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

        # message = "array_1:success_XX.XXXX_YY.YYYY"
        product_id = message.split(':')[0]
        ra = "{:0.4f}".format(float(message.split("_")[2]))
        decl = "{:0.4f}".format(float(message.split("_")[3]))

        self.engine.update_obs_status(obs_start_time=str(self._get_sensor_value(product_id,
                                                                                "current_obs:obs_start_time")),
                                      beamform_ra=str(ra),
                                      beamform_decl=str(decl),
                                      processed='TRUE')

        processing_beams = json.loads(self._get_sensor_value(product_id, "current_obs:processing_beams"))

        matching_ra = ["{:0.4f}".format(float(item)) for item in processing_beams['ra']]
        if ra in matching_ra:
            idx_ra = matching_ra.index(ra)
            keys_64 = ['ra', 'decl', 'source_id', 'contained_dist_c', 'contained_table', 'contained_priority']
            for j in keys_64:
                to_rm = processing_beams.get(j)
                del to_rm[idx_ra]
        else:
            logger.info("Was not able to remove successfully processed beamforming coordinates {}, {} "
                        "from list of beams to process".format(ra, decl))

        write_pair_redis(
            self.redis_server,
            "{}:current_obs:processing_beams".format(product_id), json.dumps(processing_beams))

        if not len(processing_beams['ra']):
            logger.info("Successful processing of 64 beamforming coordinates confirmed by processing nodes. "
                        "Calculating next 64")
            self.fetch_data(product_id, mode="recalculating")
            self._publish_targets(json.loads(self._get_sensor_value(product_id, "current_obs:target_list")),
                                  product_id)
            self.store_metadata(product_id, mode="next_64")

    def _acknowledge(self, message):
        """Response to a successful acknowledgement message from the sensor_alerts channel.

        Parameters:
            message: (str)
                Message passed over sensor_alerts channel.

        Returns:
            None
        """

        product_id = message.split(':')[0]
        # message = "array_1:acknowledge_XX.XXXX_YY.YYYY"
        ra = message.split("_")[2]
        decl = message.split("_")[3]

        unacknowledged_beams = pd.DataFrame.from_dict(
            json.loads(self._get_sensor_value(product_id, "current_obs:unacknowledged_beams")))\
            .astype({'ra': float, 'decl': float})

        format_mapping = {'ra': '{:0.4f}', 'decl': '{:0.4f}'}
        for key, value in format_mapping.items():
            unacknowledged_beams[key] = unacknowledged_beams[key].apply(value.format)

        unacknowledged_beams = unacknowledged_beams.loc[~((unacknowledged_beams['ra'] == ra) |
                                                          (unacknowledged_beams['decl'] == decl))]\
            .reset_index(drop=True).to_dict(orient="list")
        write_pair_redis(self.redis_server, "{}:current_obs:unacknowledged_beams".format(product_id),
                         json.dumps(unacknowledged_beams))

        if not len(unacknowledged_beams['ra']):
            # All target coordinates have been received by the processing nodes
            logger.info("Receipt of next {} beamforming coordinates confirmed by processing nodes"
                        .format(number_beams()))

    """

    Internal Methods

    """

    def hms_dms_decimal_convert(self, coords):
        """Converts H:M:S, D:M:S RA/Dec values to decimal for processing

        Parameters:
            coords: (str)
                Coordinate string

        Returns:
            coords_ra_deg, coords_dec_deg: (float, float)
                Tuple of RA/Dec coordinates in decimal format
        """

        coords_ra_hms, coords_dec_dms = coords.split(', ')
        coords_ra_h = int(coords_ra_hms.split(':')[0])
        coords_ra_m = int(coords_ra_hms.split(':')[-2])
        coords_ra_s = float(coords_ra_hms.split(':')[-1])
        coords_dec_d = int(coords_dec_dms.split(':')[0])
        coords_dec_m = int(coords_dec_dms.split(':')[-2])
        coords_dec_s = float(coords_dec_dms.split(':')[-1])

        if coords_ra_h < 0:
            coords_ra_deg = float(15 * (coords_ra_h - (coords_ra_m / 60) - (coords_ra_s / 3600)))
        else:
            coords_ra_deg = float(15 * (coords_ra_h + (coords_ra_m / 60) + (coords_ra_s / 3600)))
        if coords_dec_d < 0:
            coords_dec_deg = float(coords_dec_d - (coords_dec_m / 60) - (coords_dec_s / 3600))
        else:
            coords_dec_deg = float(coords_dec_d + (coords_dec_m / 60) + (coords_dec_s / 3600))

        if coords_ra_deg < 0:
            coords_ra_deg = 360 + coords_ra_deg

        return coords_ra_deg, coords_dec_deg

    def append_tbdfm(self, table, coords, frequency):
        """Function to calculate and append TBDFM values to tables containing targets

        TBDFM = To Be Determined Figure of Merit

        Parameters:
            table: (dataframe)
                Table from which to calculate TBDFM and to which the values are appended
            coords: TODO: ADD DESCRIPTION
                TODO: ADD DESCRIPTION
            frequency: TODO: ADD DESCRIPTION
                TODO: ADD DESCRIPTION
        Returns:
            table: (dataframe)
                Dataframe with appended TBDFM values for each row
        """
        # create empty array for TBDFM parameter values in the target list
        tbdfm_param = np.full(table.shape[0], 0, dtype=float)

        for q in table.index:
            # calculate primary beam sensitivity for each target based on cosine attenuation pattern
            point_1 = table['ra'][q], table['decl'][q]
            point_2 = float(coords.split(", ")[0]), float(coords.split(", ")[1])
            gcd = great_circle_distance(point_1, point_2)
            beam_fwhm = np.rad2deg((con.c / float(frequency)) / 13.5)
            proportional_offset = gcd / beam_fwhm
            primary_sensitivity = cosine_attenuation(proportional_offset)
            # One target of priority n is worth priority_decay targets of priority n+1.
            tbdfm_param[q] = int((primary_sensitivity ** primary_sensitivity_exponent())
                                 * priority_decay() ** (7 - table['priority'][q]))
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
            logger.info("New pointing contains sources with a higher total TBDFM parameter. Aborting")
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
        formatted_freq = self.engine.freq_format(frequency)
        if float(dec_coord) > 45:  # coordinates out of MeerKAT's range TODO: UPDATE TO ACTUAL VALUE
            logger.info('Selected coordinates ({}) unavailable. Waiting for new coordinates'
                        .format(coords))
        else:  # no sources from target list in beam
            logger.info('No targets visible for coordinates ({}) at {}. Waiting for new coordinates'
                        .format(coords, formatted_freq))

        # DELETE mode:target_list redis key
        target_key = ('{}:{}:target_list'.format(product_id, mode))
        if mode == "new_obs" and "None" not in str(self._get_sensor_value(product_id, "{}:target_list".format(mode))):
            delete_key(self.redis_server, target_key)

        if mode == "current_obs":
            pStatus.proc_status = "ready"

    # def round_time(self, timestamp):
    #     """Function to round timestamp values to nearest second for database matching
    #
    #     Parameters:
    #         timestamp: (datetime)
    #             Timestamp to round to the nearest second (for matching success messages against the table of
    #             previous observations)
    #
    #     Returns:
    #         rounded: (datetime)
    #             Timestamp rounded to the nearest second
    #     """
    #     dt = str(timestamp)
    #     date = dt.split()[0]
    #     h, m, s = [dt.split()[1].split(':')[0],
    #                dt.split()[1].split(':')[1],
    #                str(round(float(dt.split()[1].split(':')[-1])))]
    #     rounded = "{} {}:{}:{}".format(date, h, m, s)
    #     return rounded

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
                        obs_end_time = datetime.now(pytz.utc)
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
                    json.loads(self._get_sensor_value(product_id, "current_obs:processing_beams")))

                coords = self._get_sensor_value(product_id, "current_obs:coords")
                self.engine.add_sources_to_db(targets, coords, start, end, proxies, antennas, n_antennas, file_id, bands)

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
        beam_rad = 0.5 * (con.c / float(current_freq)) / dish_size
        return beam_rad

    def _publish_targets(self, targets, product_id, columns=None,
                         sub_arr_id=0, sensor_name='targets'):
        """Reformat the table returned from target searching

        Parameters:
            targets: (dict)
                dict containing data for the triaged target list to be processed
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

        # calculate list containing the maximum number of targets observable with 64 beams
        self.optimize(product_id, targets)

        # fetch list of beamforming coordinates
        beamform_beams = json.loads(self._get_sensor_value(product_id, "current_obs:beamform_beams"))
        str_ra = [str(i) for i in beamform_beams['ra']]
        str_decl = [str(i) for i in beamform_beams['decl']]
        beamform_beams['ra'] = str_ra
        beamform_beams['decl'] = str_decl

        # fetch list of targets contained within beams centred on the above beamforming coordinates
        beamform_targets = json.loads(self._get_sensor_value(product_id, "current_obs:beamform_targets"))
        # total number of beamforming coordinates
        init_coords = len(beamform_beams['ra'])
        # total number of unique contained targets
        init_targets = len(beamform_targets['source_id'])
        init_unique = len(set(beamform_targets['source_id']))

        n_spare = number_beams() - init_coords
        # if all targets can be observed with fewer than the maximum number of beams,
        if n_spare >= 1:
            # fetch current observation coordinates
            coords = self._get_sensor_value(product_id, "current_obs:coords")
            c_ra = math.radians(float(coords.split(", ")[0]))
            c_dec = math.radians(float(coords.split(", ")[1]))
            # calculate the beam radius from the current observation frequency
            beam_rad = self._beam_radius(self._get_sensor_value(product_id, "current_obs:frequency"))
            # for each "spare" beam,
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
                source_id = "spare_{:0.4f}_{:0.4f}".format(math.degrees(x), math.degrees(y))
                # append values for spare beams to beams table
                beamform_beams['source_id'].append(source_id)
                # beamform_beams['ra'].append(float("{:0.4f}".format(math.degrees(x))))
                # beamform_beams['decl'].append(float("{:0.4f}".format(math.degrees(y))))
                beamform_beams['ra'].append("{:0.4f}".format(float(math.degrees(x))))
                beamform_beams['decl'].append("{:0.4f}".format(float(math.degrees(y))))
                beamform_beams['contained_dist_c'].append(0)
                beamform_beams['contained_priority'].append(7)
                beamform_beams['contained_table'].append('spare_beams')
                # append values for spare beams to targets table
                beamform_targets['source_id'].append(source_id)
                beamform_targets['ra'].append(math.degrees(x))
                beamform_targets['decl'].append(math.degrees(y))
                beamform_targets['beam_ra'].append(math.degrees(x))
                beamform_targets['beam_decl'].append(math.degrees(y))
                beamform_targets['dist_c'].append(0)
                beamform_targets['priority'].append(7)
                beamform_targets['table_name'].append('spare_beams')

        key = '{}:pointing_{}:{}'.format(product_id, sub_arr_id, sensor_name)
        key_beams_processing = '{}:current_obs:processing_beams'.format(product_id)
        key_beams_unacknowledged = '{}:current_obs:unacknowledged_beams'.format(product_id)
        channel = "bluse:///set"
        # write tables to redis
        write_pair_redis(self.redis_server, key, json.dumps(targets))
        write_pair_redis(self.redis_server, key_beams_processing, json.dumps(beamform_beams))
        write_pair_redis(self.redis_server, key_beams_unacknowledged, json.dumps(beamform_beams))

        pd.DataFrame.to_csv(pd.DataFrame.from_dict(targets), "targets.csv")

        if n_spare >= 1:
            # if there are spare beams,
            logger.info('{} beamforming coordinates containing {} of {} unique targets ({} total) plus '
                        '{} random coordinates for spare beams published to {}'
                        .format(init_coords,
                                init_unique, len(targets.get('source_id')), init_targets, n_spare, channel))
            # write tables to redis
            write_pair_redis(self.redis_server, "{}:current_obs:target_list".format(product_id),
                             json.dumps(beamform_targets))
        elif n_spare < 1:
            # if there are no spare beams,
            logger.info('{} beamforming coordinates containing {} of {} unique targets ({} total) published to {}'
                        .format(init_coords,
                                init_unique, len(targets.get('source_id')), init_targets, channel))
        publish(self.redis_server, channel, key)

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

    def optimize(self, product_id, targets):

        obs_time = str(self._get_sensor_value(product_id, "current_obs:obs_start_time"))
        pool_resources = str(self._get_sensor_value(product_id, "current_obs:pool_resources"))
        coordinates = str(self._get_sensor_value(product_id, "current_obs:coords"))
        frequency = str(self._get_sensor_value(product_id, "current_obs:frequency"))
        pointing_ra, pointing_dec = map(float, coordinates.split(", "))
        possible_targets = Target.parse_targets(targets, pointing_ra, pointing_dec, frequency)
        opt = Optimizer(frequency, coordinates, pool_resources, possible_targets, time=obs_time)
        opt.optimize()
        opt.show_attenuation_stats()
        beams, targets = opt.write_csvs()
        pd.DataFrame.to_csv(pd.DataFrame.from_dict(beams), "beamform_beams.csv")
        write_pair_redis(self.redis_server, "{}:current_obs:beamform_beams"
                         .format(product_id), json.dumps(beams))
        pd.DataFrame.to_csv(pd.DataFrame.from_dict(targets), "beamform_targets.csv")
        write_pair_redis(self.redis_server, "{}:current_obs:beamform_targets"
                         .format(product_id), json.dumps(targets))

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
