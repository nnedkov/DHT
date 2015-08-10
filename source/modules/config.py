#!/usr/bin/python

import logging
LOG_LEVEL = logging.DEBUG
CONFIG_PATH = '../config/dht.conf'
SHUT_DOWN = 0

HOSTNAME = 'localhost'
PORT = 10089
PEER_PORT = 10090

OVERLAY_HOSTNAME = 'something1'
HOSTLIST = 'something2'

# the duration of period servers check the SHUT_DOWN flag
SHUT_DOWN_PERIOD = 1
