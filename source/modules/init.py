#!/usr/bin/python

from signal import signal, SIGINT
from Queue import Queue, Empty
from threading import Thread
import logging
import sys

from utilities import exit_gracefully
from conf_parser import read_conf

from kademlia_protocol_server import KademliaProtocolRequestHandler, KademliaProtocolServer
from dht_api_server import DHTAPIRequestHandler, DHTAPIServer
import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)

logger = logging.getLogger('Init (main thread)')


def set_signal_trapping():
    # TODO: other exit signals?
    exit_signals = [SIGINT,   # (Ctrl+C)
                   ]
    for sig in exit_signals:
        # setup handler for signal "sig" received by the process
        signal(sig, signal_handler)


def signal_handler(signal, frame):

    if signal == 2:
        logger.debug('SIGINT signal detected')

    # update global variable to induce children threads to exit
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

    # A container for all thread instances
    threads = list()

    # Inter-thread communication via FIFO Queues
    # DHT ---> kademlia: queue for DHT to send requests to kademlia
    request_q = Queue()
    # kademlia ---> DHT: queue for kademlia to send responses to DHT
    response_q = Queue()
    # If an exception/error occurs in any of the threads it is not detectable.
    # Therefore we use an additional queue that feeds the mail thread with
    # potential exceptions/errors that occured in any of the children threads.
    err_q = Queue()

    # Create a thread for kademlia server
    address = (config.HOSTNAME, config.PORT)
    kademlia_server = KademliaProtocolServer(request_q,
                                             response_q,
                                             err_q,
                                             address,
                                             KademliaProtocolRequestHandler)
    t = Thread(target=kademlia_server.serve_forever)
    threads.append(t)

    # Create a thread for DHT server
    address = (config.HOSTNAME, config.PEER_PORT)
    dht_server = DHTAPIServer(request_q,
                              response_q,
                              err_q,
                              address,
                              DHTAPIRequestHandler)
    t = Thread(target=dht_server.serve_forever)
    threads.append(t)

    for t in threads:
        t.start()

    # As long as we haven't received an EXIT signal, read the queue for
    # potential exceptions/errors that occured in any of the children threads.
    # The reading of the queue is with a blocking 'get', so no CPU cycles are
    # wasted while waiting. Also, 'get' is given a timeout, so the SHUT_DOWN
    # flag is always checked, even if there's nothing in the queue.
    while not config.SHUT_DOWN:
        try:
            error, thread_name = err_q.get(block=True, timeout=0.05)
            # queue.task_done()
            raise error
        except Empty:
            continue

    logger.info('Some signal for exit was received. Waiting threads to exit')

    for t in threads:
        t.join()

    kademlia_server.socket.close()
    dht_server.socket.close()

    exit_gracefully('successful shut down')


if __name__ == '__main__':

    set_signal_trapping()
    check_args()
    read_conf()
    init_servers()
