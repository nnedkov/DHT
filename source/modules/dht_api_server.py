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
            unpacked_header = unpack('HH', msg_header)

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

        pack_str = '32sHH4s%ss' % (len(self.msg_body) - calcsize('32sHH4s'))
        self.logger.debug('expected body size: %s' % calcsize(pack_str))
        self.logger.debug('received body size: %s' % len(self.msg_body))

        try:
            unpacked_body = unpack(pack_str, self.msg_body)

            self.key = unpacked_body[0]
            self.ttl = socket.ntohs(unpacked_body[1])
            self.replication = socket.ntohs(unpacked_body[2]) >> 8
            self.reserved = unpacked_body[3]
            self.content = unpacked_body[4]
        except:
            self.response = self.error()

        self.logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in self.key]).replace('x00', '00').replace('0x', ''))
        self.logger.debug('ttl: %s' % self.ttl)
        self.logger.debug('replication in 8 MSBits: %s (in decimal: %s)' % (self.replication << 8, self.replication))
        self.logger.debug('reserved: %s' % '0x' + ''.join([hex(ord(i)) for i in self.reserved]).replace('x00', '00').replace('0x', ''))
        self.logger.debug('content: %s' % '0x' + ''.join([hex(ord(i)) for i in self.content]).replace('x00', '00').replace('0x', ''))

        req = { 'type': config.MSG_DHT_PUT,
                'key': self.key,
                'ttl': self.ttl,
                'replication': self.replication,
                'value': self.content }

        res = self.make_kademlia_request(req)

        if res is None or not res['all_ok']:
            self.response = self.error()
            return

        self.response = None

    def get_reply(self):
        self.logger.debug('get_reply')

        pack_str = '32s'
        self.logger.debug('expected body size: %s' % calcsize(pack_str))
        self.logger.debug('received body size: %s' % len(self.msg_body))

        try:
            unpacked_body = unpack(pack_str, self.msg_body)

            self.key = unpacked_body[0]
        except:
            self.response = self.error()
            return

        self.logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in self.key]).replace('x00', '00').replace('0x', ''))

        req = { 'type': config.MSG_DHT_GET,
                'key': self.key }

        res = self.make_kademlia_request(req)

        if res is None or not res['all_ok']:
            self.response = self.error()
            return

        content = res['content']
        self.logger.debug('get_reply.len(str(content)): %s' % len(content))

        pack_str = 'HH32s%ss' % int(len(content))
        self.response = pack(pack_str,
                             socket.htons(calcsize(pack_str)),
                             socket.htons(config.MSG_DHT_GET_REPLY),
                             self.key,
                             content)

        self.logger.debug('pack_str: %s' % pack_str)
        self.logger.debug('total response size: %s' % calcsize(pack_str))
        self.logger.debug('response type: %s' % config.MSG_DHT_GET_REPLY)
        self.logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in self.key]).replace('x00', '00').replace('0x', ''))
        self.logger.debug('content: %s' % '0x' + ''.join([hex(ord(i)) for i in content]).replace('x00', '00').replace('0x', ''))


    def trace_reply(self):
        self.logger.debug('trace_reply')

        pack_str = '32s'
        self.logger.debug('expected body size: %s' % calcsize(pack_str))
        self.logger.debug('received body size: %s' % len(self.msg_body))

        try:
            unpacked_body = unpack(pack_str, self.msg_body)

            self.key = unpacked_body[0]
        except:
            self.response = self.error()
            return

        self.logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in self.key]).replace('x00', '00').replace('0x', ''))

        req = { 'type': config.MSG_DHT_TRACE,
                'key': self.key }

        res = self.make_kademlia_request(req)

        if res is None or not res['all_ok']:
            self.response = self.error()
            return

        trace_recs = res['trace']

        pack_str = 'HH32s'
        to_be_packed = [ socket.htons(config.MSG_DHT_TRACE_REPLY),
                         self.key ]

        for trace_rec in trace_recs:
            pack_str += '32sHH4s16s'
            to_be_packed += [ trace_rec['peer_ID'],
                              socket.htons(trace_rec['KX_port']),
                              socket.htons(0),
                              trace_rec['IPv4_address'],
                              trace_rec['IPv6_address'] ]

        to_be_packed.insert(0, socket.htons(calcsize(pack_str)))

        self.logger.debug('pack_str: %s' % pack_str)
        self.logger.debug('total response size: %s' % calcsize(pack_str))
        self.logger.debug('response type: %s' % config.MSG_DHT_TRACE_REPLY)
        self.logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in self.key]).replace('x00', '00').replace('0x', ''))

        for trace_rec in trace_recs:
            self.logger.debug('peer_ID: %s' % '0x' + ''.join([hex(ord(i)) for i in trace_rec['peer_ID']]).replace('x00', '00').replace('0x', ''))
            self.logger.debug('KX_port: %s' % trace_rec['KX_port'])
            self.logger.debug('IPv4_address: %s' % '0x' + ''.join([hex(ord(i)) for i in trace_rec['IPv4_address']]).replace('x00', '00').replace('0x', ''))
            self.logger.debug('IPv6_address: %s' % '0x' + ''.join([hex(ord(i)) for i in trace_rec['IPv6_address']]).replace('x00', '00').replace('0x', ''))

        self.response = pack(pack_str, *to_be_packed)


    def error(self):
        self.logger.debug('error')

        if not hasattr(self, 'req_type'):
            self.req_type = 0
            self.key = BitArray(int=0, length=256)

        pack_str = 'HHH16s256s'
        error_res = pack(pack_str,
                         socket.htons(calcsize('HHH16s256s')),
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

        SocketServer.TCPServer.allow_reuse_address = True
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





def issue_dht_put(logger):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test DHT-server request handling from KX
    address = (config.HOSTNAME, config.PORT)
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
    pack_str = 'HH32sHH4s%ss' % str(len(content)/8)
    size = calcsize(pack_str)

    logger.debug('key: %s' % str(key))
    logger.debug('ttl: %s' % ttl)
    logger.debug('replication: %s (in 8 MSBits: %s)' % (replication, replication << 8))
    logger.debug('reserved: %s' % str(reserved))
    logger.debug('content: %s' % str(content))
    logger.debug('total DHT PUT request size: %s' % size)

    dht_put = pack(pack_str,
                   socket.htons(size),
                   socket.htons(config.MSG_DHT_PUT),
                   key.tobytes(),
                   socket.htons(ttl),
                   socket.htons(replication << 8),
                   reserved.tobytes(),
                   content.tobytes())

    # Send the data
    logger.debug('sending the DHT PUT request')
    s.send(dht_put)

    # Receive a response
    logger.debug('waiting for response')
    res_header = s.recv(4)

    logger.debug('response length: %s' % len(res_header))

    if len(res_header) == 0:
        clean_up()
    else:
        amount_received = len(res_header)
        unpacked_header = unpack('HH', res_header)

        res_size = socket.ntohs(unpacked_header[0])
        res_type = socket.ntohs(unpacked_header[1])
        logger.debug('response size: %s' % res_size)
        logger.debug('response type: %s' % res_type)
        clean_up()


def issue_dht_get(logger):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test DHT-server request handling from KX
    address = (config.HOSTNAME, config.PORT)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    s.connect(address)

    # DHT GET
    key = BitArray(int=randrange(0, 100), length=256)
    pack_str = 'HH32s'
    size = calcsize(pack_str)

    dht_get = pack(pack_str,
                   socket.htons(size),
                   socket.htons(config.MSG_DHT_GET),
                   key.tobytes())

    logger.debug('key: %s' % str(key))
    logger.debug('total DHT GET request size: %s' % size)

    # Send the data
    logger.debug('sending the DHT GET request')
    s.send(dht_get)

    # Receive a response
    logger.debug('waiting for response')
    res_header = s.recv(4)

    logger.debug('response length: %s' % len(res_header))

    if len(res_header) == 0:
        clean_up()
    else:
        amount_received = len(res_header)
        unpacked_header = unpack('HH', res_header)

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

        pack_str = '32s%ss' % (len(res_body)-calcsize('32s'))
        unpacked_response = unpack(pack_str, res_body)
        logger.debug('pack_str: %s' % pack_str)
        logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in unpacked_response[0]]).replace('x00', '00').replace('0x', ''))
        logger.debug('content: %s' % '0x' + ''.join([hex(ord(i)) for i in unpacked_response[1]]).replace('x00', '00').replace('0x', ''))


def issue_dht_trace(logger):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test DHT-server request handling from KX
    address = (config.HOSTNAME, config.PORT)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    s.connect(address)

    # DHT TRACE
    key = BitArray(int=randrange(0, 100), length=256)
    pack_str = 'HH32s'
    size = calcsize(pack_str)

    dht_get = pack(pack_str,
                   socket.htons(size),
                   socket.htons(config.MSG_DHT_TRACE),
                   key.tobytes())

    logger.debug('key: %s' % str(key))
    logger.debug('total DHT TRACE request size: %s' % size)

    # Send the data
    logger.debug('sending the DHT TRACE request')
    s.send(dht_get)

    # Receive a response
    logger.debug('waiting for response')
    res_header = s.recv(4)

    logger.debug('response length: %s' % len(res_header))

    if len(res_header) == 0:
        clean_up()
    else:
        amount_received = len(res_header)
        unpacked_header = unpack('HH', res_header)

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

        trace_recs_num = (len(res_body)-calcsize('32s'))/calcsize('32sHH4s16s')
        logger.debug('trace_recs_num: %s' % trace_recs_num)
        pack_str = '32s' + trace_recs_num * '32sHH4s16s'
        unpacked_response = unpack(pack_str, res_body)
        logger.debug('pack_str: %s' % pack_str)
        logger.debug('key: %s' % '0x' + ''.join([hex(ord(i)) for i in unpacked_response[0]]).replace('x00', '00').replace('0x', ''))
        logger.debug('len(unpacked_response): %s' % str(len(unpacked_response)))

        for i in range(trace_recs_num):
            logger.debug('peer_ID: %s' % '0x' + ''.join([hex(ord(j)) for j in unpacked_response[i*5+1]]).replace('x00', '00').replace('0x', ''))
            logger.debug('KX_port: %s' % socket.ntohs(unpacked_response[i*5+2]))
            logger.debug('IPv4_address: %s' % '0x' + ''.join([hex(ord(j)) for j in unpacked_response[i*5+4]]).replace('x00', '00').replace('0x', ''))
            logger.debug('IPv6_address: %s' % '0x' + ''.join([hex(ord(j)) for j in unpacked_response[i*5+5]]).replace('x00', '00').replace('0x', ''))


if __name__ == '__main__':
    logger = logging.getLogger('DHT Client')
    logger.debug('issuing DHT PUT')
    issue_dht_put(logger)
    print
    logger.debug('issuing DHT GET')
    issue_dht_get(logger)
    print
    logger.debug('issuing DHT TRACE')
    issue_dht_trace(logger)
