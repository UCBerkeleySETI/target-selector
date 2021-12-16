#!/usr/bin/env pythoni

from optparse import OptionParser
import sys
import time
import logging
from mk_target_selector.logger import log, set_logger
from mk_target_selector.target_selector import Target_Selector

def start(config):
    log = set_logger(log_level=logging.INFO)
    ts = Target_Selector(True, config)
    ts.run()
    while True:
        time.sleep(1)

def cli(prog = sys.argv[0]):
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage = usage)
    parser.add_option('-c', '--config', type = str,
                     help = 'Config file (yaml)', default = 'mk_target_selector/target_selector.yml')
    (opts, args) = parser.parse_args()
    start(config = opts.config)

if __name__ == '__main__':
    cli()
    while True:
        time.sleep(1)
