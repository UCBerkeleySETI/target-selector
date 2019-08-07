#!/usr/bin/env python

'''

This script is pretty slow for creating the database. Will work on adding support
for bulk inserting data from csv file in the future.

'''

import os
import yaml
import pandas as pd
from getpass import getpass
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import (VARCHAR, BOOLEAN, BIGINT, FLOAT,
                              TIMESTAMP, INT, BIGINT, Text)
from sqlalchemy import Index, Column
from sqlalchemy.engine.url import URL
import sys
from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter
)

data_link = 'https://www.dropbox.com/s/yklypkckc6m2xx1/1_million_sample_complete.csv?dl=1'

Base = declarative_base()
class Observation(Base):
    """Observation table data schema. Stores information on the status of the
       observation.
    """
    __tablename__ = 'observation_status'
    source_id = Column(BIGINT, primary_key = True)
    antennas = Column(Text)
    proxies = Column(Text)
    bands = Column(VARCHAR(45))
    duration = Column(FLOAT)
    file_id = Column(VARCHAR(45))
    mode = Column(INT)
    time = Column(TIMESTAMP)


def cli(prog=sys.argv[0]):
    usage = "{} [options]".format(prog)
    description = 'MeerKAT Breakthrough Listen Database Setup'

    parser = ArgumentParser(usage=usage,
                            description=description,
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-u', '--username',
        type=str,
        default="root",
        help='MySQL username')
    parser.add_argument(
        '-d', '--database',
        type=str,
        default="breakthrough_db",
        help='Name of the database to enter the data into')
    parser.add_argument(
        '-H', '--host',
        type=str,
        default="localhost",
        help='Database host')

    args = parser.parse_args()
    password = getpass('Password for {}@{}: '.format(args.username, args.host))

    main(user = args.username,
         password = password,
         host = args.host,
         schema_name = args.database)

def write_yaml(cred, filename = 'config.yml'):
    data = {"mysql": cred}

    if os.path.basename(os.getcwd()) == 'scripts':
        path = os.path.split(os.getcwd())[0]
        filename = os.path.join(path, filename)

    with open(filename, 'w') as outfile:
        yaml.dump(data, outfile, default_flow_style=False)

def main(user, password, host, schema_name):
    cred = {'username': user, 'host': 'localhost', 'password': password,
            'drivername': 'mysql'}

    source_table_name = 'target_list'
    obs_table_name = 'observation_status'
    url = URL(**cred)
    engine = create_engine(name_or_url = url)
    engine.execute('CREATE DATABASE IF NOT EXISTS {};'.format(schema_name))
    engine.execute('USE {};'.format(schema_name))

    # Create config file
    cred['database'] = schema_name
    write_yaml(cred)


    if not engine.dialect.has_table(engine, source_table_name):
        print ('Creating table: {}'.format(source_table_name))
        tb = pd.read_csv(data_link)
        tb.to_sql(source_table_name, engine, index = False,
                  if_exists = 'replace', chunksize = None)
        engine.execute('CREATE INDEX target_list_loc_idx ON \
                        {}.{} (ra, decl)'.format(schema_name, source_table_name))
        del tb

    else:
        print ('Table with the name, {}, already exists. Could not create table.'.format(source_table_name))

    if not engine.dialect.has_table(engine, obs_table_name):
        print ('Creating table: {}'.format(obs_table_name))
        engine.execute('DROP TABLE {}.{}'.format(schema_name, source_table_name)
        Base.metadata.create_all(engine)
    else:
        print ('Table with the name, {}, already exists. Could not create table.'.format(obs_table_name))

if __name__ == '__main__':
    cli()
