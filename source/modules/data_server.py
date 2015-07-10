#!/usr/bin/python

from time import sleep
import logging

import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)

logger = logging.getLogger('DataServer')


def data_server(queue):

    logger.debug('activating')

    while True:

        if config.SHUT_DOWN == 1:
            logger.info('going to exit')
            break

        logger.debug('taking a small nap')
        sleep(5)

    logger.info('exiting')

    result = (True, None)
    queue.put(result)


if __name__ == '__main__':
    from Queue import Queue
    from threading import Thread

    queue = Queue()
    t = Thread(target=data_server, args=(queue, ))
    t.setDaemon(True)   # terminate when the main thread ends
    t.start()
    sleep(10)
