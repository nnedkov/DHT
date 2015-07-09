#!/usr/bin/python

import sys
import os
import ConfigParser
from signal import signal, SIGINT, pause
from Queue import Queue
from threading import Thread

from utilities import debug_print, exit_gracefully, config_section_map
from module_protocol_server import ModuleProtocolRequestHandler, ModuleProtocolServer
from module_api_server import ModuleAPIRequestHandler, ModuleAPIServer
from data_server import data_server
import config


def signal_handler(signal, frame):

    debug_print('SIGINT signal detected')

    # Update global variable to induce threads to exit
    config.SHUT_DOWN = 1


def check_args():

    for i in range(len(sys.argv)):

        # configuration file flag
        if sys.argv[i] == '-c':
            try:
                config.CONFIG_PATH = sys.argv[i+1]
            except IndexError:
                message = "missing configuration file operand after '-c'\n"
                message += "Try '%s --help' for more information." % sys.argv[0]
                exit_gracefully(message)


def read_config_file():

    if not os.path.isfile(config.CONFIG_PATH):
        exit_gracefully("cannot read '%s': No such file or directory"
                            % config.CONFIG_PATH)

    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config.CONFIG_PATH)
    
    debug_print('Detected sections in config file (%s): %s' % (config.CONFIG_PATH,
                                                               config_parser.sections()))

    dht_properties = config_section_map(config_parser, 'DHT')

    config.PORT = int(dht_properties['port'])
    config.PEER_PORT = int(dht_properties['peer_port'])
    config.HOSTNAME = dht_properties['hostname']


def init_servers():

    threads = list()

    # If an exception/error occurs in any of the threads it is not detectable.
    # For this reason we use inter-thread communication via a FIFO Queue.
    queue = Queue()

    # Create a thread for inter-module communication
    address = (config.HOSTNAME, config.PORT)
    server = ModuleProtocolServer(address, ModuleProtocolRequestHandler)
    t = Thread(target=server.serve_forever, args=(queue, ))
    threads.append(t)

    # Create a thread for DHT API communication
    address = (config.HOSTNAME, config.PEER_PORT)
    server = ModuleAPIServer(address, ModuleAPIRequestHandler)
    t = Thread(target=server.serve_forever, args=(queue, ))
    threads.append(t)

    # Create a thread for k-buckets creation & maintenance
    threads.append(Thread(target=data_server, args=(queue, )))

    for t in threads:
        t.start()

    # Sleep until a signal is received
    pause()

    debug_print('Waiting for threads to exit')

    for t in threads:
        all_ok, error = queue.get(block=True)
        if not all_ok:
            raise error

        queue.task_done()

    for t in threads:
        t.join()

    exit_gracefully("successful shut down")


if __name__ == '__main__':

    # TODO: set handlers for signals other than SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler)
    check_args()
    read_config_file()
    init_servers()
