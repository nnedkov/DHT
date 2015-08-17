#!/usr/bin/python

import logging
import SocketServer
import Queue
from struct import pack, unpack, calcsize
from bitstring import BitArray
import sys


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
        # key -> RPC identifier, value -> RPC handler
        self.RPCs = { config.MSG_DHT_PUT: self.put_reply,
                      config.MSG_DHT_GET: self.get_reply,
                      config.MSG_DHT_TRACE: self.trace_reply,
                      config.MSG_DHT_ERROR: self.error }
        SocketServer.BaseRequestHandler.__init__(self,
                                                 request,
                                                 client_address,
                                                 server)

    def setup(self):
        self.logger.debug('setup')
        return SocketServer.BaseRequestHandler.setup(self)

    def handle(self):
        self.logger.debug('handle')

        try:
            msg_header = self.request.recv(4)

            amount_received = len(msg_header)
            unpacked_header = unpack('hh', msg_header)

            amount_expected = socket.ntohs(unpacked_header[0])
            self.req_type = socket.ntohs(unpacked_header[1])

            self.msg_body = ''
            while amount_received < amount_expected:
                self.msg_body += self.request.recv(16384)
                amount_received = len(msg_header) + len(self.msg_body)

        except:
            print 'Unexpected error:', sys.exc_info()[0]
            self.response = self.error()
            self.request.send(self.response, self.client_address)
            return

        RPC_handler = self.RPCs[self.req_type]
        RPC_handler()

        if self.response is not None:
            self.request.send(self.response)

        self.request.close()

    def put_reply(self):
        self.logger.debug('put_reply')

        pack_str = '256shh32s%ss' % (len(self.msg_body) - calcsize('256shh32s'))
        self.logger.debug('expected body size: %s' % calcsize(pack_str))
        self.logger.debug('received body size: %s' % len(self.msg_body))

        try:
            unpacked_body = unpack(pack_str, self.msg_body)

            self.key = unpacked_body[0]
            self.ttl = socket.ntohs(unpacked_body[1])
            self.replication = socket.ntohs(unpacked_body[2])
            self.reserved = unpacked_body[3]
            self.content = unpacked_body[4]
        except:
            self.response = self.error()

        self.logger.debug('key: %s' % self.key)
        self.logger.debug('ttl: %s' % self.ttl)
        self.logger.debug('replication: %s' % self.replication)
        self.logger.debug('reserved: %s' % self.reserved)
        self.logger.debug('content: %s' % self.content)

        # TODO: functionality for processing the request

        self.response = None

    def get_reply(self):
        self.logger.debug('get_reply')

        pack_str = '256s'
        self.logger.debug('expected body size: %s' % calcsize(pack_str))
        self.logger.debug('received body size: %s' % len(self.msg_body))

        try:
            unpacked_body = unpack(pack_str, self.msg_body)

            self.key = unpacked_body[0]
        except:
            self.response = self.error()
            return

        self.logger.debug('key: %s' % self.key)

        # TODO: functionality for processing the request

        content = BitArray(int=randrange(0, 1000), length=256)
        pack_str = 'hh256s%ss' % len(content)
        self.response = pack(pack_str,
                             socket.htons(calcsize(pack_str)),
                             socket.htons(config.MSG_DHT_GET_REPLY),
                             str(self.key),
                             str(content))

        self.logger.debug('content: %s' % str(content))
        self.logger.debug('total response size: %s' % calcsize(pack_str))

    def trace_reply(self):
        self.logger.debug('trace_reply')

        try:
            pack_str = '256s'
            unpacked_body = unpack(pack_str, self.msg_body)

            self.key = unpacked_body[0]
        except:
            self.response = self.error()
            return

        self.logger.debug('key: %s' % self.key)

        # TODO: functionality for processing the request

        self.response = None

    def error(self):
        self.logger.debug('error')

        pack_str = 'hhh16s256s'
        error_res = pack(pack_str,
                         socket.htons(calcsize('hhh16s256s')),
                         config.MSG_DHT_ERROR,
                         socket.htons(self.req_type),
                         BitArray(int=0, length=16),
                         self.key)
        return error_res

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
    from random import randrange


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
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    s.connect(address)

    # Create request data

    # DHT PUT
    key = BitArray(int=randrange(0, 100), length=256)
    ttl = 12
    replication = 4
    reserved = BitArray(int=0, length=32)
    content = BitArray(int=randrange(0, 1000), length=256)
    pack_str = 'hh256shh32s%ss' % len(content)
    size = calcsize(pack_str)

    dht_put = pack(pack_str,
                   socket.htons(size),
                   socket.htons(config.MSG_DHT_PUT),
                   str(key),
                   socket.htons(ttl),
                   socket.htons(replication),
                   str(reserved),
                   str(content))

    logger.debug('key: %s' % str(key))
    logger.debug('ttl: %s' % ttl)
    logger.debug('replication: %s' % replication)
    logger.debug('reserved: %s' % str(reserved))
    logger.debug('content: %s' % str(content))
    logger.debug('total DHT PUT request size: %s' % size)

    # Send the data
    logger.debug('sending the DHT PUT request')
    len_sent = s.send(dht_put)

    # DHT GET
    key = BitArray(int=randrange(0, 100), length=256)
    pack_str = 'hh256s'
    size = calcsize(pack_str)

    dht_get = pack(pack_str,
                   socket.htons(size),
                   socket.htons(config.MSG_DHT_GET),
                   str(key))

    logger.debug('key: %s' % str(key))
    logger.debug('total DHT GET request size: %s' % size)

    # Send the data
    logger.debug('sending the DHT GET request')
    len_sent = s.send(dht_get)

    # Receive a response
    logger.debug('waiting for response')
    response = s.recv(16384)
    print len(response)

    unpacked_response = unpack('hh256s256s', response)
    logger.debug('total response size: %s' % socket.ntohl(unpacked_response[0]))
    logger.debug('response type: %s' % socket.ntohl(unpacked_response[1]))
    logger.debug('key: %s' % unpacked_response[2])
    logger.debug('content: %s' % unpacked_response[3])

    # DHT TRACE
    key = BitArray(int=randrange(0, 100), length=256)
    pack_str = 'hh256s'
    size = calcsize(pack_str)

    dht_trace = pack(pack_str,
                     socket.htons(size),
                     socket.htons(config.MSG_DHT_TRACE),
                     str(key))

    logger.debug('key: %s' % str(key))
    logger.debug('total DHT TRACE request size: %s' % size)

    # Send the data
    logger.debug('sending the DHT TRACE request')
    len_sent = s.send(dht_trace)

    # Clean up
    logger.debug('closing socket')
    s.close()
    config.SHUT_DOWN = 1
    sleep(3)
    logger.debug('done')
    server.socket.close()
