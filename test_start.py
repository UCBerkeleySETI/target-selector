#!/usr/bin/env pythoni

from optparse import OptionParser
import sys
import time
import logging
from mk_target_selector.logger import log, set_logger
from redis_test import Listen


def start():
    log = set_logger(log_level=logging.INFO)
    ts = Listen()
    ts.run()
    while True:
        time.sleep(1)


def cli():
    start()


if __name__ == '__main__':
    cli()
    while True:
        time.sleep(1)
