import matplotlib.pyplot as plt  # for location plot
import astropy.coordinates as coord  # for location plot
import astropy.units as u  # for location plot
from dateutil import parser
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import yaml
import math
import numpy as np
import pandas as pd
import matplotlib  # for location plot

matplotlib.use('Agg')  # for location plot

try:
    from .logger import log as logger
    from .redis_tools import get_redis_key

except ImportError:
    from logger import log as logger
    from redis_tools import get_redis_key


class DatabaseHandler(object):
    """
    Class to handle the connection to the source database as well as querying
    the database for astronomical sources within the field of view.

    Examples:
        # db = DatabaseHandler()
        # db.select_targets(c_ra, c_dec, beam_rad)
    """

    def __init__(self, config_file):
        """
        __init__ function for the DatabaseHandler class

        Parameters:
            config_file: (str)
                asdf

        Returns:
            None
        """
        self.cfg = self.configure_settings(config_file)
        # self.priority_sources = np.array(self.cfg['priority_sources'])
        self.conn = self.connect_to_db(self.cfg['mysql'])

    def configure_settings(self, config_file):
        """Sets configuration settings

        Parameters:
            config_file: (str)
                Name of the yaml configuration file to be opened

        Returns:
            cfg: (dict)
                Dictionary containing the values of the configuration file
        """
        try:
            with open(config_file, 'r') as f:
                try:
                    cfg = yaml.safe_load(f)
                    return cfg
                except yaml.YAMLError as E:
                    logger.error(E)
        except IOError:
            logger.error('Config file not found')

    def connect_to_db(self, cred):
        """
        Connects to the Breakthrough Listen database

        Parameters:
            cred: (dict)
                Dictionary containing information on the source list database

        Returns:
            conn : sqlalchemy connection
                SQLalchemy connection to the database containing sources for
                triaging
        """
        url = URL(**cred)
        # self.engine = create_engine(name_or_url = url)
        self.engine = create_engine(url)
        return self.engine.connect()

    def close_conn(self):
        """Close the connection to the database

        Parameters:
            None

        Returns:
            None
        """
        self.conn.close()
        self.engine.dispose()


class Triage(DatabaseHandler):
    """

    ADD IN DOCUMENTATION

    Examples:
        # conn = Triage()
        # conn.select_targets(ra, dec, beam_rad)

    When start() is called, a loop is started that subscribes to the "alerts" and
    "sensor_alerts" channels on the Redis server. Depending on the which message
    that passes over which channel, various processes are run:
    """

    def __init__(self, config_file):
        super(Triage, self).__init__(config_file)

    def add_sources_to_db(self, df, start_time, end_time, proxies, antennas, n_antennas,
                          file_id, bands, mode=0, table='observation_status'):
        """
        Adds a pandas DataFrame to a specified table

        Parameters:
            df: (pandas.DataFrame)
                DataFrame containing information on the sources within the field
                of views
            start_time: (datetime)
                Datetime start time object of the observation
            end_time: (datetime)
                Datetime end time object of the observation
            proxies: (str)
                names of the proxies used for the observation
            antennas: (str)
                antennas used for the observation
            n_antennas: (int)
                number of antennas used for the observation
            file_id: (str)
                unique identifier for the file containing observation data ASDF
            bands: (str)
                frequency band of the observation
            mode: (ASDF)
                ASDF
            table: (str)
                name of the observation metadata table to add sources to

        Returns
            bool:
                If sources were successfully added to the database, returns True.
                Else, returns False.
        """

        source_tb = df.loc[:, ['source_id']]
        source_tb['duration'] = (datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f")
                                 - datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
        source_tb['time'] = start_time
        source_tb['mode'] = mode
        source_tb['file_id'] = file_id
        source_tb['proxies'] = proxies
        source_tb['bands'] = bands
        source_tb['antennas'] = antennas
        source_tb['n_antennas'] = n_antennas

        try:
            source_tb.to_sql(table, self.conn, if_exists='append', index=False)
            return True

        except Exception as e:
            logger.info(e)
            logger.warning('Was not able to add sources to the database!')
            return False

    def update_obs_status(self, source_id, obs_start_time,
                          processed, table='observation_status'):
        """
        Function to update the status of the observation. For now, it only updates
        the success column, but should be flexible enough to update things like
        length of observation time, observation start time, etc.

        Parameters:
            source_id: (str)
                ID of the source being accessed
            obs_start_time: (datetime)
                Start time of observation
            processed: (bool)
                Status of the observation that is to be updated
            table: (str)
                Name of the table to be updated with processing success

        Returns:
            None
        """

        update = """\
                    UPDATE {table}
                    SET processed = {processed}
                    WHERE (source_id = {id} AND time = '{time}');
                 """.format(table=table, id=source_id,
                            time=parser.parse(obs_start_time),
                            processed=processed)
        self.conn.execute(update)

    def _box_filter(self, c_ra, c_dec, beam_rad, table, cols, current_freq):
        """Returns a string which acts as a pre-filter for the more computationally
        intensive search

        Reference:
            #document 404s
            http://janmatuschek.de/LatitudeLongitudeBoundingCoordinate

        Parameters:
            c_ra, c_dec: (float)
                Pointing coordinates of the telescope in radians
            beam_rad: (float)
                Angular radius of the primary beam in radians
            table: (str)
                Table within database where
            cols: (list)
                Columns to select within the table
            current_freq: (str)
                Current central frequency of observation in Hz
        Returns:
            query: (str)
                SQL query string

        """
        current_band = self._freqBand(current_freq)
        beam_rad_arcmin = beam_rad * (180 / math.pi) * 60
        logger.info("Beam radius at {} ({}): {} radians = {} arc minutes".format(self.freq_format(current_freq),
                                                                               current_band, beam_rad, beam_rad_arcmin))
        if c_dec - beam_rad <= - np.pi / 2.0:
            ra_min, ra_max = 0.0, 2.0 * np.pi
            dec_min = -np.pi / 2.0
            dec_max = c_dec + beam_rad

        elif c_dec + beam_rad >= np.pi / 2.0:
            ra_min, ra_max = 0.0, 2.0 * np.pi
            dec_min = c_dec - beam_rad
            dec_max = np.pi / 2.0

        else:
            ra_offset = np.arcsin(np.sin(beam_rad) / np.cos(c_dec))
            ra_min = c_ra - ra_offset
            ra_max = c_ra + ra_offset
            dec_min = c_dec - beam_rad
            dec_max = c_dec + beam_rad

        bounds = np.rad2deg([ra_min, ra_max, dec_min, dec_max])

        query = """
                SELECT {cols}
                FROM {table}
                UNION ALL
                SELECT {cols}
                FROM adhoc_list
                UNION ALL
                SELECT {cols}
                FROM exotica_list
                WHERE ({ra_min} < ra  AND ra < {ra_max}) AND
                      ({dec_min} < decl AND decl < {dec_max})
                """.format(cols=', '.join(cols), table=table,
                           ra_min=bounds[0], ra_max=bounds[1],
                           dec_min=bounds[2], dec_max=bounds[3])
        return query

    def _freqBand(self, current_freq):
        """
        Calculates the current frequency band of observation

        Parameters:
            current_freq: (str)
                Current central frequency of observation in Hz
        Returns:
            current_band: (str)
                Current frequency band of observation

        """

        if (float(current_freq) > 300000000) and (float(current_freq) < 1000000000):
            current_band = "UHF band"
        elif (float(current_freq) > 1000000000) and (float(current_freq) < 2000000000):
            current_band = "L band"
        elif (float(current_freq) > 2000000000) and (float(current_freq) < 4000000000):
            current_band = "S band"
        elif (float(current_freq) > 8000000000) and (float(current_freq) < 12000000000):
            current_band = "X band"
        else:
            current_band = "FAIL"

        return current_band

    def triage(self, tb, current_freq, table='observation_status'):
        """
        Returns an array of priority values (or maybe the table with priority values
        appended)

        Parameters:
            tb: (pandas.DataFrame)
                table containing sources within the field of view of MeerKAT's pointing
            current_freq: (str)
                Current central frequency of observation in Hz
            table: (str)
                Name of the MySQL table of previous observations to be used for triaging

        Returns:
            tb: (pandas.DataFrame)
                table containing the sources to be beamformed on
        """

        # initially, all sources assigned a priority of 2
        priority = np.full(tb.shape[0], 2, dtype=int)

        query = """
                SELECT source_id, antennas, n_antennas, bands, processed, max(duration) AS duration
                FROM {} 
                GROUP BY source_id, antennas, n_antennas, bands, processed
                """.format(table)

        # TODO replace these with sqlalchemy queries

        # list of previous observations
        prev_obs = pd.read_sql(query, con=self.conn)
        # logger.info("Previous observations:\n{}\n".format(prev_obs.drop('antennas', axis=1)))
        # prev_obs.to_csv('prev_obs.csv')
        successfully_processed = prev_obs.drop('antennas', axis=1).loc[prev_obs['processed'].isin(['1'])]

        # exotica sources
        priority[tb['table_name'].str.contains('exotica')] = 3

        # sources previously observed & successfully processed
        priority[tb['source_id'].isin(successfully_processed['source_id'])] = 6

        # sources previously observed, but at a different frequency
        prev_freq = successfully_processed.groupby('source_id').agg(lambda x: ', '.join(x.values))
        longest_obs = successfully_processed.groupby('source_id')['duration'].max()
        most_antennas = successfully_processed.groupby('source_id')['n_antennas'].max()
        current_band = self._freqBand(current_freq)

        for p in tb['source_id']:
            try:
                if current_band in prev_freq.loc[p]['bands']:
                    pass
                else:
                    priority[tb['source_id'] == p] = 5
            except KeyError:  # chosen source is not in prev_freq table
                pass
            except IndexError:  # prev_freq table is empty
                pass
            # sources previously observed, but for < 5 minutes, or with < 58 antennas
            try:
                if (longest_obs[p] < 300) or (most_antennas[p] < 58):
                    priority[tb['source_id'] == p] = 4
            except KeyError:  # chosen source is not in prev_obs table
                pass
            except IndexError:  # prev_obs table is empty
                pass

        # ad-hoc sources
        priority[tb['table_name'].str.contains('adhoc')] = 1

        tb['priority'] = priority
        return tb.sort_values('priority')

    def select_targets(self, c_ra, c_dec, beam_rad, current_freq='Unknown', table='target_list', cols=None):
        """Returns a string to query the 1 million star database to find sources
           within some primary beam area

        Parameters:
            c_ra : (float)
                Pointing coordinates of the telescope in radians (right ascension)
            c_dec : (float)
                Pointing coordinates of the telescope in radians (declination)
            beam_rad: (float)
                Angular radius of the primary beam in radians
            current_freq: (str)
                Current central frequency of observation in Hz
            table: (str)
                Name of the MySQL table that is being queried
            cols: (list)
                Columns of table to output

        Returns:
            target_list: (DataFrame)
                Returns a pandas DataFrame containing the objects meeting the filter
                criteria, sorted in order of priority

        """

        if not cols:
            cols = ['ra', 'decl', 'source_id', 'project', 'dist_c', 'table_name']

        mask = self._box_filter(c_ra, c_dec, beam_rad, table, cols, current_freq)

        query = """
                SELECT *
                FROM ({mask}) as T
                WHERE ACOS( SIN(RADIANS(decl)) * SIN({c_dec}) + COS(RADIANS(decl)) *
                COS({c_dec}) * COS({c_ra} - RADIANS(ra))) < {beam_rad}; \
                """.format(mask=mask, c_ra=c_ra,
                           c_dec=c_dec, beam_rad=beam_rad)
        logger.info('Query:\n {}\n'.format(query))

        # TODO: replace with sqlalchemy queries
        tb = pd.read_sql(query, con=self.conn)
        sorting_priority = self.triage(tb, current_freq)
        target_list = sorting_priority.sort_values(by=['priority', 'dist_c']).reset_index()
        # self.output_targets(target_list, c_ra, c_dec, current_freq)
        return target_list

    # def output_targets(self, target_list, c_ra, c_dec, current_freq='Unknown'):
    #     """Function to plot selected targets & output the source list
    #
    #     Parameters:
    #         target_list: (DataFrame)
    #             A pandas DataFrame containing the objects meeting the filter criteria
    #         c_ra : float
    #             Pointing coordinates of the telescope in radians (right ascension)
    #         c_dec : float
    #             Pointing coordinates of the telescope in radians (declination)
    #         current_freq:.....
    #             Current central frequency of observation in Hz
    #     Returns:
    #         None
    #     """
    #
    #     current_band = self._freqBand(current_freq)
    #
    #     # TESTING plot locations of target sources
    #     ra_plot = coord.Angle(target_list['ra'] * u.degree)
    #     ra_plot = ra_plot.wrap_at(180 * u.degree)
    #     dec_plot = coord.Angle(target_list['decl'] * u.degree)
    #     location_fig = plt.figure(figsize=(8, 6))
    #     ax = location_fig.add_subplot(111, projection="mollweide")
    #     ax.scatter(ra_plot.radian, dec_plot.radian, marker="+")
    #     ax.grid(True)
    #
    #     location_fig.savefig("test_plot.pdf")
    #     plt.close()
    #     pointing_coord = coord.SkyCoord(ra=c_ra * u.rad, dec=c_dec * u.rad, frame='icrs')
    #
    #     logger.info('Plot of targets for pointing coordinates ({}, {}) at {} ({}) saved successfully'.format(
    #         pointing_coord.ra.wrap_at('180d').to_string(unit=u.hour, sep=':', pad=True),
    #         pointing_coord.dec.to_string(unit=u.degree, sep=':', pad=True),
    #         self.freq_format(current_freq), current_band))
    #
    #     print('\nTarget list for pointing coordinates ({}, {}) at {} ({}):\n {}\n'.format(
    #         pointing_coord.ra.wrap_at('180d').to_string(unit=u.hour, sep=':', pad=True),
    #         pointing_coord.dec.to_string(unit=u.degree, sep=':', pad=True), self.freq_format(current_freq),
    #         current_band, target_list))
    #     return target_list

    def freq_format(self, current_freq):
        """Function to format current frequency to either MHz or GHz for output

        Parameters:
            current_freq: (str)
                Current central frequency of observation in Hz
        Returns:
            freq_formatted: (str)
                Formatted central frequency of observation, in either MHz or GHz
        """

        if float(current_freq) > 1000000000:
            gigahertz = float(current_freq) * 10**-9
            freq_formatted = "{} GHz".format(gigahertz)
        else:
            megahertz = float(current_freq) * 10**-6
            freq_formatted = "{} MHz".format(megahertz)

        return freq_formatted
