#!/usr/bin/python

import logging
import SocketServer
import Queue

import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)


class DHTAPIRequestHandler(SocketServer.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger('DHTAPIRequestHandler')
        self.logger.debug('__init__')
        self.request_q = request_q
        self.response_q = response_q
        self.err_q = err_q
        SocketServer.BaseRequestHandler.__init__(self,
                                                 request,
                                                 client_address,
                                                 server)

    def setup(self):
        self.logger.debug('setup')
        return SocketServer.BaseRequestHandler.setup(self)

    def handle(self):
        self.logger.debug('handle')

        # Echo the back to the client
        data = self.request.recv(1024)
        self.logger.debug("recv()->'%s'", data)
        self.request.send(data.upper())

    def finish(self):
        self.logger.debug('finish')
        return SocketServer.BaseRequestHandler.finish(self)


class DHTAPIServer(SocketServer.TCPServer):

    def __init__(self,
                 request_q,
                 response_q,
                 err_q,
                 server_address,
                 handler_class=DHTAPIRequestHandler):
        self.logger = logging.getLogger('DHTAPIServer')
        self.logger.debug('__init__')
        self.thread_name = 'DHTAPIServer'
        self.request_q = request_q
        self.response_q = response_q
        self.err_q = err_q
        SocketServer.TCPServer.__init__(self, server_address, handler_class)

    def server_activate(self):
        self.logger.debug('server_activate')
        SocketServer.TCPServer.server_activate(self)

    def serve_forever(self):
        try:
            self.serve_til_shutdown()
        except Exception as e:
            self.logger.debug('Exception occured: %s' % str(e))
            exception = (e, self.thread_name)
            self.err_q.put(exception)

        self.socket.close()

    def serve_til_shutdown(self):
        self.logger.debug('waiting for requests')

        # Timeout duration, measured in seconds. If handle_request() receives
        # no incoming requests within the timeout period, handle_request() will
        # return. It essentially makes handle_request() non-blocking.
        self.timeout = config.SHUT_DOWN_PERIOD

        # Test DHT-server request issuing to kademlia. Send 10 requests
        # self.test_req_issuing_to_kademlia(10)

        # As long as we haven't received an EXIT signal, read the response
        # queue for incoming responses from kademlia (lower level). In
        # addition, serve any incoming DHT requests from KX (read socket for
        # that). The reading of the queue is with a blocking 'get', so no CPU
        # cycles are wasted while waiting. Also, 'get' is given a timeout, so
        # the SHUT_DOWN flag is always checked, even if there's nothing in the
        # queue.

        while not config.SHUT_DOWN:

            try:
                response = self.response_q.get(True, 0.05)
                self.process_kademlia_response(response)
            except Queue.Empty:
                pass
            except Exception as e:
                exception = (e, self.thread_name)
                self.err_q.put(exception)

            try:
                self.handle_request()
            except Exception as e:
                exception = (e, self.thread_name)
                self.err_q.put(exception)

        self.logger.info('shutting down...')

    def process_kademlia_response(self, response):
        self.logger.debug('process_dht_request')
        self.logger.debug('received kademlia response: %s' % str(response))
        # TODO: add functionality for response processing

    def test_req_issuing_to_kademlia(self, req_num):
        # Test DHT-server request issuing to kademlia
        for req_id in range(req_num):
            self.request_q.put('Request (with id %s) from dht_api_server' % req_id)

    def handle_request(self):
        self.logger.debug('handle_request')
        return SocketServer.TCPServer.handle_request(self)

    def verify_request(self, request, client_address):
        self.logger.debug('verify_request(%s, %s)', request, client_address)
        return SocketServer.TCPServer.verify_request(self,
                                                     request,
                                                     client_address)

    def process_request(self, request, client_address):
        self.logger.debug('process_request(%s, %s)', request, client_address)
        return SocketServer.TCPServer.process_request(self,
                                                      request,
                                                      client_address)

    def server_close(self):
        self.logger.debug('server_close')
        return SocketServer.TCPServer.server_close(self)

    def finish_request(self, request, client_address):
        self.logger.debug('finish_request(%s, %s)', request, client_address)
        return SocketServer.TCPServer.finish_request(self,
                                                     request,
                                                     client_address)

    def close_request(self, request_address):
        self.logger.debug('close_request(%s)', request_address)
        return SocketServer.TCPServer.close_request(self, request_address)


if __name__ == '__main__':

    from threading import Thread
    import socket
    from time import sleep

    request_q = Queue.Queue()
    response_q = Queue.Queue()
    err_q = Queue.Queue()
    address = (config.HOSTNAME, config.PORT)
    SocketServer.TCPServer.allow_reuse_address = 1
    server = DHTAPIServer(request_q,
                          response_q,
                          err_q,
                          address,
                          DHTAPIRequestHandler)

    t = Thread(target=server.serve_forever)
    #t.setDaemon(True)   # terminate when the main thread ends
    t.start()

    # Test DHT-server request handling from KX
    logger = logging.getLogger('Client')
    logger.info('Server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    s.connect(address)

    # Send the data
    message = 'Hello, world'
    logger.debug('sending data: "%s"', message)
    len_sent = s.send(message)

    # Receive a response
    logger.debug('waiting for response')
    response = s.recv(len_sent)
    logger.debug('response from server: "%s"', response)

    # Clean up
    logger.debug('closing socket')
    s.close()
    config.SHUT_DOWN = 1
    sleep(3)
    logger.debug('done')
    server.socket.close()
