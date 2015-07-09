#!/usr/bin/python

import config
import sys


def debug_print(message):

    if config.DEBUG_MODE:
        print message


def exit_gracefully(message):

    print message
    sys.exit(0)


def config_section_map(config_parser, section):

    section_map = {}
    
    options = config_parser.options(section)
    for option in options:

        try:
            section_map[option] = config_parser.get(section, option)
            if section_map[option] == -1:
                debug_print('skiping key: %s in config file' % option)
        except:
            debug_print('no value found for key: %s in config file' % option)
            section_map[option] = None

    return section_map
