#!/usr/bin/python

import logging
import os
import ConfigParser

from utilities import exit_gracefully
import config

logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)

logger = logging.getLogger('ConfParser')


def config_section_map(config_parser, section):

    section_map = {}

    options = config_parser.options(section)
    for option in options:

        try:
            section_map[option] = config_parser.get(section, option)
            if section_map[option] == -1:
                logger.warning("skiping key '%s'" % option)
        except:
            logger.warning("no value found for key '%s'" % option)
            section_map[option] = None

    return section_map


def read_conf():

    if not os.path.isfile(config.CONFIG_PATH):
        exit_gracefully("cannot read '%s': No such file or directory"
                            % config.CONFIG_PATH)

    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config.CONFIG_PATH)

    logger.debug('detected sections (%s): %s' % (config.CONFIG_PATH,
                                                 config_parser.sections()))

    dht_properties = config_section_map(config_parser, 'COMMON')
    config.PUBLIC_KEY_PATH = dht_properties['public_key_path']

    dht_properties = config_section_map(config_parser, 'KX')
    config.KX_PORT = int(dht_properties['port'])
    config.KX_HOSTNAME = dht_properties['hostname']

    dht_properties = config_section_map(config_parser, 'DHT')
    config.PORT = int(dht_properties['port'])
    config.KADEM_PORT = int(dht_properties['kadem_port'])
    config.HOSTNAME = dht_properties['hostname']
