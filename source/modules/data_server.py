#!/usr/bin/python

from time import sleep

from utilities import debug_print
import config


def data_server(queue):

    debug_print("Warm Hello from data_server! :)")
    
    while True:

        if config.SHUT_DOWN == 1:
            debug_print("data_server: going to exit")
            break

        debug_print("data_server: going to sleep for a bit")
        sleep(5)

    debug_print("data_server: exiting")

    result = (True, None)
    queue.put(result)
