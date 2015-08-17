#!/usr/bin/python

from bson.json_util import dumps, loads
import logging
import SocketServer
import Queue
from threading import Thread
import buckets, verify
import random
from data_server import DataServer

import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)


def kbuckets_maintainer(buckets, err_q):

    def maintain_til_shutdown(buckets):
        while not config.SHUT_DOWN:
            # TODO: set ping flags in the kbuckets - ping peers
            logger.debug('sleeping for a bit')
            sleep(3)

    logger = logging.getLogger('kbuckets_maintainer')
    logger.debug('starting maintainance')
    thread_name = 'kbuckets_maintainer'

    try:
        maintain_til_shutdown(buckets)
    except Exception as e:
        logger.debug('Exception occured: %s' % str(e))
        exception = (e, thread_name)
        err_q.put(exception)


class KademliaProtocolRequestHandler(SocketServer.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger('KademliaProtocolRequestHandler')
        self.logger.debug('__init__')
        self.request_q = request_q
        self.response_q = response_q
        self.err_q = err_q
        # key -> RPC identifier, value -> [RPC handler, [keys the bson obj should have]]
        self.RPCs = { 'PING': [self.pong, ['MID', 'SID', 'RID']],
                      'STORE': [self.store_reply, ['MID', 'SID', 'RID', 'Key', 'TTL', 'Value']],
                      'FIND_NODE': [self.find_node_reply, ['MID', 'SID', 'RID', 'KX_INFO', 'Key']],
                      'FIND_VALUE': [self.find_value_reply, ['MID', 'SID', 'RID', 'Key']],
                      'VERIFY': [self.verify_reply, ['MID', 'SID', 'RID', 'Challenge']] }

        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    def setup(self):
        self.logger.debug('setup')
        return SocketServer.BaseRequestHandler.setup(self)

    def handle(self):
        self.buckets = self.server.buckets
        self.data_server = self.server.data_server
        self.logger.debug('handle')

        self.req = self.request[0].strip()
        socket = self.request[1]
        self.logger.debug("%s sent->'%s'", self.client_address[0], self.req)

        is_valid, error_msg = self.req_is_valid()
        if not is_valid:
            # TODO: handle non-valid request
            res = { 'error': True, 'message': error_msg }
            socket.sendto(dumps(res), self.client_address)
            return

        RPC_handler = self.RPCs[self.RPC_id][0]
        res = RPC_handler()

        socket.sendto(dumps(res), self.client_address)

    def req_is_valid(self):
        try:
            self.req = loads(self.req)
        except ValueError:
            return False, 'non-valid bson object'

        try:
            self.RPC_id = self.req['TYPE']
            _, required_keys = self.RPCs[self.RPC_id]
        except KeyError:
            return False, 'request with no specified type'

        for key in required_keys:
            # TODO: verify value types
            if not self.req.has_key(key):
                return False, 'request with required keys missing'
        # TODO: verify SID/RID combination
        return True, ''

    def pong(self):
        res = self.prepare_reply('PONG')
        node = {'id':self.req['SID'],'ip': self.client_address[0], 'port': self.client_address[1]}
        self.buckets.add_refresh_node(node)
        return res

    def store_reply(self):
        res = self.prepare_reply('STORE_REPLY')
        node = {'id':self.req['SID'],'ip': self.client_address[0], 'port': self.client_address[1]}
        self.buckets.add_refresh_node(node)
        if self.req['TTL'] > 43200:  # Max duration
            res['Status'] = -1
            return res
        else:
            res['Status'] = self.data_server.add(self.req['Key'], self.req['Value'], self.req['TTL'])
            return res

    def find_node_reply(self):
        # TODO: change 20 to a global constant K
        res = self.prepare_reply('FIND_STORE_REPLY')
        node = {'id':self.req['SID'],'ip': self.client_address[0], 'port': self.client_address[1]}
        self.buckets.add_refresh_node(node)
        nodes = self.buckets.get_closest_nodes(self.req['Key'], 20)
        res['Nodes'] = nodes
        return res

    def find_value_reply(self):
        res = self.prepare_reply('FIND_VALUE_REPLY')
        node = {'id':self.req['SID'],'ip': self.client_address[0], 'port': self.client_address[1]}
        self.buckets.add_refresh_node(node)
        value = self.data_server.get(self.req['Key'])
        if value is None:
            nodes = self.buckets.get_closest_nodes(self.req['Key'], 20)
            res['Nodes'] = nodes
        res['Values'] = value
        return res

    def verify_reply(self):
        res = self.prepare_reply('VERIFY_REPLY')
        node = {'id':self.req['SID'],'ip': self.client_address[0], 'port': self.client_address[1]}
        self.buckets.add_refresh_node(node)
        res['Challenge_Reply'] = verify.work(self.req['Challenge'])
        return res

    def prepare_reply(self, msg_type):
        res = dict()
        res['TYPE'] = msg_type
        res['MID'] = self.req['MID']
        res['SID'] = self.req['RID']
        res['RID'] = self.req['SID']
        return res

    def finish(self):
        self.logger.debug('finish')
        return SocketServer.BaseRequestHandler.finish(self)


class KademliaProtocolServer(SocketServer.UDPServer):

    peer_id = "1BCD77AFF8391729182DC63AFFFFF319000567AA"
    def __init__(self,
                 request_q,
                 response_q,
                 err_q,
                 server_address,
                 handler_class=KademliaProtocolRequestHandler):
        self.logger = logging.getLogger('KademliaProtocolServer')
        self.logger.debug('__init__')
        self.thread_name = 'KademliaProtocolServer'
        self.request_q = request_q
        self.response_q = response_q
        self.err_q = err_q
        self.buckets = buckets.Buckets(KademliaProtocolServer.peer_id, 160, 20)
        self.data_server = DataServer()

        self.kbuckets_maintainer = Thread(target=kbuckets_maintainer, args=(self.buckets, self.err_q))
        self.kbuckets_maintainer.start()

        SocketServer.UDPServer.__init__(self, server_address, handler_class)

    def server_activate(self):
        self.logger.debug('server_activate')
        SocketServer.UDPServer.server_activate(self)

    def serve_forever(self):
        try:
            self.serve_til_shutdown()
        except Exception as e:
            self.logger.debug('Exception occurred: %s' % str(e))
            exception = (e, self.thread_name)
            self.err_q.put(exception)

        self.kbuckets_maintainer.join()
        self.socket.close()

    def serve_til_shutdown(self):
        self.logger.debug('waiting for requests')

        # Timeout duration, measured in seconds. If handle_request() receives
        # no incoming requests within the timeout period, handle_request() will
        # return. It essentially makes handle_request() non-blocking.
        self.timeout = config.SHUT_DOWN_PERIOD

        # As long as we haven't received an EXIT signal, read the request queue
        # for requests from dht (from the higher level). In addition, serve any
        # incoming kademlia requests from other peers (read socket for that).
        # The reading of the queue is with a blocking 'get', so no CPU cycles
        # are wasted while waiting. Also, 'get' is given a timeout, so the
        # SHUT_DOWN flag is always checked, even if there's nothing in the
        # queue.
        while not config.SHUT_DOWN:

            try:
                request = self.request_q.get(True, 0.05)
                response = self.process_dht_request(request)
                self.response_q.put(response)
            except Queue.Empty:
                pass
            except Exception as e:
                exception = (e, self.thread_name)
                err_q.put(exception)

            try:
                self.handle_request()
            except Exception as e:
                exception = (e, self.thread_name)
                self.err_q.put(exception)

        self.logger.info('shutting down...')

    def process_dht_request(self, request):
        self.logger.debug('process_dht_request')
        self.logger.debug('received dht request: %s' % str(request))
        # TODO: add functionality for request processing
        status = True
        message = 'Response from kademlia_protocol_server (for request: %s)' % request
        response = (status, message)

        return response

    def handle_request(self):
        self.logger.debug('handle_request')
        return SocketServer.UDPServer.handle_request(self)

    def verify_request(self, request, client_address):
        self.logger.debug('verify_request(%s, %s)', request, client_address)
        return SocketServer.UDPServer.verify_request(self,
                                                     request,
                                                     client_address)

    def process_request(self, request, client_address):
        self.logger.debug('process_request(%s, %s)', request, client_address)
        return SocketServer.UDPServer.process_request(self,
                                                      request,
                                                      client_address)

    def server_close(self):
        self.logger.debug('server_close')
        return SocketServer.UDPServer.server_close(self)

    def finish_request(self, request, client_address):
        self.logger.debug('finish_request(%s, %s)', request, client_address)
        return SocketServer.UDPServer.finish_request(self,
                                                     request,
                                                     client_address)

    def close_request(self, request_address):
        self.logger.debug('close_request(%s)', request_address)
        return SocketServer.UDPServer.close_request(self, request_address)


    #TODO: add a static method for sending through sockets

    @staticmethod
    def prepare_req(msg_type):
        req = dict()
        req['TYPE'] = msg_type
        req['MID'] = str(hex(random.getrandbits(160)))[2:-1]
        #req['SID'] = str(self.buckets.id)
        return req

    @staticmethod
    def ping(id, ip, port):
        req = KademliaProtocolServer.prepare_req('PING')
        req['RID'] = str(id)  # or ID in case ID is already a string
        #send_req(req, ip, port)
        pass

    @staticmethod
    def store(id, ip, port, key, value, ttl):
        req = self.prepare_req('STORE')
        req['RID'] = id
        req['Key'] = key
        req['Value'] = value
        req['TTL'] = ttl
        #send_req(req, ip, port)
        pass

    @staticmethod
    def find_node(id, ip, port, KX_INFO, key):
        req = prepare_req('FIND_NODE')
        req['RID'] = id
        req['KX_INFO'] = KX_INFO
        req['Key'] = key
        #send_req(req, ip, port)
        pass

    @staticmethod
    def find_value(id, ip, port, key):
        req = prepare_req('FIND_VALUE')
        req['RID'] = id
        req['Key'] = key
        #send_req(req, ip, port)
        pass

    @staticmethod
    def verify(id, ip, port, x):
        req = prepare_req('VERIFY')
        req['RID'] = id
        req['Challenge'] = x
        #send_req(req, ip, port)
        pass


if __name__ == '__main__':

    from threading import Thread
    import socket
    from time import sleep

    request_q = Queue.Queue()
    response_q = Queue.Queue()
    err_q = Queue.Queue()
    address = (config.HOSTNAME, config.PEER_PORT)
    SocketServer.UDPServer.allow_reuse_address = 1
    server = KademliaProtocolServer(request_q,
                                    response_q,
                                    err_q,
                                    address,
                                    KademliaProtocolRequestHandler)

    t = Thread(target=server.serve_forever)
    #t.setDaemon(True) # terminate when the main thread ends
    t.start()

    # Test kademlia request handling from a peer
    logger = logging.getLogger('Client')
    logger.info('Server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logger.debug('connecting to server')
    s.connect(address)

    # Send the data
    store_req = {'TYPE': 'PING',
                 'MID': '1BCD77AFF8391729182DC63',
                 'SID': '1BCD77AFF8391729182DC63AFFFFF319000567AB',
                 'RID': '1BCD77AFF8391729182DC63AFFFFF319000567AA'
                 }
    store_req_bson = dumps(store_req)
    logger.debug('sending data: "%s"', store_req)
    len_sent = s.send(store_req_bson)

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
