#!/usr/bin/python

import logging
import SocketServer
import Queue
from struct import pack, unpack, calcsize
from bitstring import BitArray
import sys
from random import randrange
import socket


import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)


class DHTAPIRequestHandler(SocketServer.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger('DHTAPIRequestHandler')
        self.logger.debug('__init__')
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
            self.request.send(self.response)
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

        req = { 'type': config.MSG_DHT_PUT,
                'key': self.key,
                'ttl': self.ttl,
                'replication': self.replication }

        res = self.make_kademlia_request(req)

        if res is None or not res['all_ok']:
            self.response = self.error()
            return

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

        req = { 'type': config.MSG_DHT_GET,
                'key': self.key }

        res = self.make_kademlia_request(req)

        if res is None or not res['all_ok']:
            self.response = self.error()
            return

        content = res['content']
        self.logger.debug('get_reply.len(str(content)): %s' % len(str(content)))

        pack_str = 'hh256s%ss' % len(content)
        self.response = pack(pack_str,
                             socket.htons(calcsize(pack_str)),
                             socket.htons(config.MSG_DHT_GET_REPLY),
                             str(self.key),
                             str(content))

        self.logger.debug('pack_str: %s' % pack_str)
        self.logger.debug('total response size: %s' % calcsize(pack_str))
        self.logger.debug('response type: %s' % config.MSG_DHT_GET_REPLY)
        self.logger.debug('key: %s' % self.key)
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

        if not hasattr(self, 'req_type'):
            self.req_type = 0
            self.key = BitArray(int=0, length=256)

        pack_str = 'hhh16s256s'
        error_res = pack(pack_str,
                         socket.htons(calcsize('hhh16s256s')),
                         socket.htons(config.MSG_DHT_ERROR),
                         socket.htons(self.req_type),
                         str(BitArray(int=0, length=16)),
                         str(self.key))
        return error_res

    def make_kademlia_request(self, req):
        res = None

        self.logger.debug('placing request in queue')

        self.server.request_q.put(req)

        self.logger.debug('placed request in queue')

        try:
            res = self.server.response_q.get(True, 10)
            self.server.response_q.task_done()
            self.logger.debug('make_kademlia_request received response: %s' % str(res))
        except Queue.Empty:
            self.logger.debug('make_kademlia_request Queue.Empty')
        except Exception as e:
            exception = (e, self.thread_name)
            self.sever.err_q.put(exception)
            self.logger.debug('make_kademlia_request Exception')

        self.logger.debug('make_kademlia_request done')

        return res

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





def issue_dht_put(logger, port):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test DHT-server request handling from KX
    address = (config.HOSTNAME, port)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    s.connect(address)

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

    # Receive a response
    logger.debug('waiting for response')
    res_header = s.recv(4)

    logger.debug('response lenght: %s' % len(res_header))

    if len(res_header) == 0:
        clean_up()
    else:
        amount_received = len(res_header)
        unpacked_header = unpack('hh', res_header)

        res_size = socket.ntohs(unpacked_header[0])
        res_type = socket.ntohs(unpacked_header[1])
        logger.debug('response size: %s' % res_size)
        logger.debug('response type: %s' % res_type)
        clean_up()


def issue_dht_get(logger, port):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test DHT-server request handling from KX
    address = (config.HOSTNAME, port)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    s.connect(address)

    # DHT PUT
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
    res_header = s.recv(4)

    logger.debug('response lenght: %s' % len(res_header))

    if len(res_header) == 0:
        clean_up()
    else:
        amount_received = len(res_header)
        unpacked_header = unpack('hh', res_header)

        res_size = socket.ntohs(unpacked_header[0])
        res_type = socket.ntohs(unpacked_header[1])
        logger.debug('response size: %s' % res_size)
        logger.debug('response type: %s' % res_type)
        if res_type == config.MSG_DHT_ERROR:
            clean_up()

        amount_expected = res_size
        res_body = ''
        while amount_received < amount_expected:
            res_body += s.recv(16384)
            amount_received = len(res_header) + len(res_body)

        pack_str = '256s%ss' % (len(res_body)-calcsize('256s'))
        unpacked_response = unpack(pack_str, res_body)
        logger.debug('pack_str: %s' % pack_str)
        logger.debug('key: %s' % unpacked_response[0])
        logger.debug('content: %s' % unpacked_response[1])


if __name__ == '__main__':
    port = 42054
    logger = logging.getLogger('DHT Client')
    logger.debug('issuing DHT PUT')
    issue_dht_put(logger, port)
    print
    logger.debug('issuing DHT GET')
    issue_dht_get(logger, port)
