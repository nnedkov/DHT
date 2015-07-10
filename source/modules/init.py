#!/usr/bin/python

from signal import signal, SIGINT, pause
from Queue import Queue
from threading import Thread
import logging
import sys

from utilities import exit_gracefully
from conf_parser import read_conf

from kademlia_protocol_server import KademliaProtocolRequestHandler, KademliaProtocolServer
from dht_api_server import DHTAPIRequestHandler, DHTAPIServer
from data_server import data_server
import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)

logger = logging.getLogger('Init (main thread)')


def signal_handler(signal, frame):

    logger.debug('SIGINT signal detected')

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


def init_servers():

    threads = list()

    # If an exception/error occurs in any of the threads it is not detectable.
    # For this reason we use inter-thread communication via a FIFO Queue.
    queue = Queue()

    # Create a thread for inter-module communication
    address = (config.HOSTNAME, config.PORT)
    kademlia_server = KademliaProtocolServer(address, KademliaProtocolRequestHandler)
    t = Thread(target=kademlia_server.serve_forever, args=(queue, ))
    threads.append(t)

    # Create a thread for DHT API communication
    address = (config.HOSTNAME, config.PEER_PORT)
    dht_server = DHTAPIServer(address, DHTAPIRequestHandler)
    t = Thread(target=dht_server.serve_forever, args=(queue, ))
    threads.append(t)

    # Create a thread for k-buckets creation & maintenance
    threads.append(Thread(target=data_server, args=(queue, )))

    for t in threads:
        t.start()

    # Sleep until a signal is received
    pause()

    logger.info('some signal received. Waiting for threads to exit')
    
    kademlia_server.socket.close()
    dht_server.socket.close()

    for t in threads:
        all_ok, error = queue.get(block=True)
        if not all_ok:
            raise error

        queue.task_done()

    for t in threads:
        t.join()

    exit_gracefully('successful shut down')


if __name__ == '__main__':

    # TODO: set handlers for signals other than SIGINT (Ctrl+C)
    signal(SIGINT, signal_handler)
    check_args()
    read_conf()
    init_servers()
