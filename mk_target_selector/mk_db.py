import yaml
import numpy as np
import pandas as pd
from dateutil import parser
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

try:
    from .logger import log as logger

except ImportError:
    from logger import log as logger

class Database_Handler(object):
    """
    Class to handle the connection to the source database as well as querying
    the database for astronomical sources within the field of view.

    Examples:
        >>> db = Database_Handler()
        >>> db.select_targets(c_ra, c_dec, beam_rad)
    """
    def __init__(self, config_file = 'config.yml'):
        """
        __init__ function for the DataBase_Handler class

        Parameters:
            config_file: (str)
                asdf

        Returns:
            None
        """
        self.cfg = self.configure_settings(config_file)
        #self.priority_sources = np.array(self.cfg['priority_sources'])
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
        self.engine = create_engine(name_or_url = url)
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





class Triage(Database_Handler):
    """

    ADD IN DOCUMENTATION

    Examples:
        >>> conn = Triage()
        >>> conn.select_targets(ra, dec, beam_rad)

    When start() is called, a loop is started that subscribes to the "alerts" and
    "sensor_alerts" channels on the Redis server. Depending on the which message
    that passes over which channel, various processes are run:
    """
    def __init__(self, config_file = 'config.yml'):
        super(Triage, self).__init__(config_file)

    def add_sources_to_db(self, df, start_time, end_time, proxies, antennas,
                          file_id, bands, mode = 0, table = 'observation_status'):
        """
        Adds a pandas DataFrame to a specified table

        Parameters:
            df: (pandas.DataFrame)
                DataFrame containing infrormation on the sources within the field
                of views
            t: (dict)
                Dictionary containing observation metadata
            start_time: (datetime)
                Datetime object of the observation
            table:
                name of the observation metadata table

        Returns
            bool:
                If sources were successfully added to the database, returns True.
                Else, returns False.
        """

        source_tb = df.loc[:, ['source_id']]
        source_tb['duration'] = (end_time - start_time).total_seconds()
        source_tb['time'] = start_time
        source_tb['mode'] = mode
        source_tb['file_id'] = file_id
        source_tb['proxies'] = proxies
        source_tb['bands'] = bands
        source_tb['antennas'] = antennas

        try:
            source_tb.to_sql(table, self.conn, if_exists='append', index=False)
            return True

        except Exception as e:
            print (e)
            logger.warning('Was not able to add sources to the database!')
            return False

    def update_obs_status(self, source_id, obs_start_time,
                          success, table = 'observation_status'):
        """
        Function to update the status of the observation. For now, it only updates
        the success column, but should be flexible enough to update things like
        length of observation time, observation start time, etc.

        Parameters:
            id: (str, int)
                ID of the source being accessed
            time: (datetime)
                Time of observation
            success: (bool)
                Status of the observation that is to be updated

        Returns:
            None
        """

        update = """\
                    UPDATE {table}
                    SET success = {success}
                    WHERE (source_id = {id} AND obs_start_time = '{time}');
                 """.format(table = table, id = source_id,
                            time = parser.parse(obs_start_time),
                            success = success)
        self.conn.execute(update)

    def _box_filter(self, c_ra, c_dec, beam_rad, table, cols):
        """Returns a string which acts as a pre-filter for the more computationally
        intensive search

        Reference:
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
        Returns:
            query: str
                SQL query string

        """
        if c_dec - beam_rad <= - np.pi / 2.0:
            ra_min, ra_max = 0.0, 2.0 * np.pi
            dec_min = -np.pi / 2.0
            dec_max = c_dec + beam_rad

        elif c_dec + beam_rad >= np.pi / 2.0:
            ra_min, ra_max = 0.0, 2.0 * np.pi
            dec_min = c_dec - beam_rad
            dec_max = np.pi / 2.0

        else:
            ra_offset = np.arcsin( np.sin(beam_rad) / np.cos(c_dec))
            ra_min  = c_ra - ra_offset
            ra_max  = c_ra + ra_offset
            dec_min = c_dec - beam_rad
            dec_max = c_dec + beam_rad

        bounds = np.rad2deg([ra_min, ra_max, dec_min, dec_max])

        query = """
                SELECT {cols}
                FROM {table}
                WHERE ({ra_min} < ra  AND ra < {ra_max}) AND
                      ({dec_min} < decl AND decl < {dec_max})\
                """.format(cols = ', '.join(cols), table = table,
                           ra_min = bounds[0], ra_max = bounds[1],
                           dec_min = bounds[2], dec_max = bounds[3])
        return query

    def triage(self, tb, table = 'observation_status'):
        """
        Returns an array of priority values (or maybe the table with priority values
        appended)

        Parameters:
            tb: (pandas.DataFrame)
                table containing sources within the field of view of MeerKAT's
                pointing

        Returns:
            tb: (pandas.DataFrame)
                table containing the sources to be beamformed on
        """
        priority = np.ones(tb.shape[0], dtype=int)

        query = 'SELECT DISTINCT source_id \
                 FROM {}'.format(table)

        # TODO replace these with sqlalchemy queries
        source_ids = pd.read_sql(query, con = self.conn)
        priority[tb['source_id'].isin(source_ids['source_id'])] += 1
        #priority[tb['source_id'].isin(self.priority_sources)] = 0
        tb['priority'] = priority
        return tb.sort_values('priority')

    def select_targets(self, c_ra, c_dec, beam_rad, table = 'target_list',
                       cols = ['ra', 'decl', 'source_id', 'Project']):
        """Returns a string to query the 1 million star database to find sources
           within some primary beam area

        Parameters:

            conn: SQLalchemy connection
                SQLalchemy connection to a database
            c_ra, c_dec : float
                Pointing coordinates of the telescope in radians
            beam_rad: float
                Angular radius of the primary beam in radians
            table : str
                Name of the table that is being queried

        Returns:
            source_list : DataFrame
                Returns a pandas DataFrame containing the objects meeting the filter
                criteria

        """
        mask = self._box_filter(c_ra, c_dec, beam_rad, table, cols)

        query = """\
                SELECT *
                FROM ({mask}) as T
                WHERE ACOS( SIN(RADIANS(decl)) * SIN({c_dec}) + COS(RADIANS(decl)) *
                COS({c_dec}) * COS({c_ra} - RADIANS(ra))) < {beam_rad}; \
                """.format(mask = mask, c_ra = c_ra,
                           c_dec = c_dec, beam_rad = beam_rad)

        # TODO: replace with sqlalchemy queries
        tb = pd.read_sql(query, con = self.conn)
        source_list = self.triage(tb)

        return source_list
