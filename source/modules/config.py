#!/usr/bin/python

import logging
LOG_LEVEL = logging.DEBUG
CONFIG_PATH = '../config/dht.conf'
SHUT_DOWN = 0

HOSTNAME = 'localhost'
PORT = 50160
PEER_PORT = 50161

PUBLIC_KEY_PATH = ''
PEER_ID = '18403bb44d4dda2bdfc9487bf384e056bc72fa94377d363f94e1015f237db870'

OVERLAY_HOSTNAME = 'something1'
HOSTLIST = 'something2'

# the duration of period servers check the SHUT_DOWN flag
SHUT_DOWN_PERIOD = 1

MSG_DHT_PUT = 500
MSG_DHT_GET = 501
MSG_DHT_TRACE = 502
MSG_DHT_GET_REPLY = 503
MSG_DHT_TRACE_REPLY = 504
MSG_DHT_ERROR = 505
