#!/usr/bin/python

from bson.json_util import dumps, loads
import logging
import SocketServer
import Queue
from threading import Thread
import buckets, verify
import random
from data_server import DataServer
from time import sleep


import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)


def kbuckets_maintainer(buckets, err_q):

    def maintain_til_shutdown(buckets, logger):
        while not config.SHUT_DOWN:
            # TODO: set ping flags in the kbuckets - ping peers
            logger.debug('sleeping for a bit')
            sleep(3)

    logger = logging.getLogger('kbuckets_maintainer')
    logger.debug('starting maintainance')
    thread_name = 'kbuckets_maintainer'

    try:
        maintain_til_shutdown(buckets, logger)
    except Exception as e:
        logger.debug('Exception occured: %s' % str(e))
        exception = (e, thread_name)
        err_q.put(exception)


class KademliaProtocolRequestHandler(SocketServer.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger('KademliaProtocolRequestHandler')
        self.logger.debug('__init__')
        # key -> RPC identifier, value -> [RPC handler, [keys the bson obj should have]]
        self.RPCs = { 'PING': [self.pong, ['MID', 'SID', 'RID', 'IP', 'PORT']],
                      'STORE': [self.store_reply, ['MID', 'SID', 'RID', 'Key', 'TTL', 'Value', 'IP', 'PORT']],
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

        if self.req['RID'] != str(self.buckets.id):
            return False, 'request routed to wrong peer (peer id is not %s)'
        # TODO: think if we need to check SID/IP combination by looking into buckets

        if self.req.has_key('Key'):
            try:
                int(self.req['Key'], 16)
            except ValueError:
                return False, 'given key is not in Hex format'

        return True, ''

    def pong(self):
        res = self.prepare_reply('PONG')
        node = { 'id':  self.req['SID'],
                 'ip': self.req['IP'],
                 'port': self.req['PORT'] }

        self.buckets.add_refresh_node(node)
        return res

    def store_reply(self):
        res = self.prepare_reply('STORE_REPLY')
        node = { 'id': self.req['SID'],
                 'ip': self.req['IP'],
                 'port': self.req['PORT'] }
        self.buckets.add_refresh_node(node)
        if self.req['TTL'] > 43200:  # Max duration
            res['Status'] = -1
            return res
        else:
            res['Status'] = self.data_server.add(self.req['Key'], self.req['Value'], self.req['TTL'])
            return res

    def find_node_reply(self):
        res = self.prepare_reply('FIND_STORE_REPLY')

        node = { 'id': self.req['SID'],
                 'ip': self.client_address[0],
                 'port': self.client_address[1] }
        self.buckets.add_refresh_node(node)

        if self.req.has_key('KX_INFO'):
            kx = {'ip': config.KX_HOSTNAME, 'port' : config.KX_PORT}
            res['KX_INFO'] = kx

        nodes = self.buckets.get_closest_nodes(self.req['Key'], self.buckets.ksize)
        res['Nodes'] = nodes
        return res

    def find_value_reply(self):
        res = self.prepare_reply('FIND_VALUE_REPLY')
        node = { 'id': self.req['SID'],
                 'ip': self.client_address[0],
                 'port': self.client_address[1] }
        self.buckets.add_refresh_node(node)
        value = self.data_server.get(self.req['Key'])
        if value is None:
            nodes = self.buckets.get_closest_nodes(self.req['Key'], self.buckets.ksize)
            res['Nodes'] = nodes
        res['Values'] = value
        return res

    def verify_reply(self):
        res = self.prepare_reply('VERIFY_REPLY')
        node = { 'id': self.req['SID'],
                 'ip': self.client_address[0],
                 'port': self.client_address[1] }
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
        # TODO: change 20 to a global constant K
        self.buckets = buckets.Buckets(256, 1)
        self.data_server = DataServer()

        self.kbuckets_maintainer = Thread(target=kbuckets_maintainer, args=(self.buckets, self.err_q))
        self.kbuckets_maintainer.start()

        SocketServer.UDPServer.__init__(self, server_address, handler_class)
        SocketServer.UDPServer.allow_reuse_address = True
# TODO: make sure wakeup call is in the right place
        #self.wakeup()

    def server_activate(self):
        self.logger.debug('server_activate')
        SocketServer.UDPServer.server_activate(self)

    def wakeup(self):
        # try to ping a predetermined bootstrapper node
        # the following 3 parameters are taken from config file
        id = "TEST" # TODO: read id from config file
        ip = "TEST" # TODO: read id from config file
        port = "TEST" # TODO: read id from config file
        if KademliaProtocolServer.ping(id,ip,port):
            node = {'id': id, 'ip': ip, 'port': port}
            self.buckets.add_refresh_node(node)
            self.node_lookup(self.id,3)
        # nothing to do, you are the first node to enter the network.

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
                self.request_q.task_done()
                response = self.process_dht_request(request)
                self.response_q.put(response)
            except Queue.Empty:
                pass
            except Exception as e:
                self.logger.debug('Exception occured: %s' % str(e))
                exception = (e, self.thread_name)
                self.err_q.put(exception)

            try:
                self.handle_request()
            except Exception as e:
                self.logger.debug('Exception occured: %s' % str(e))
                exception = (e, self.thread_name)
                self.err_q.put(exception)

        self.logger.info('shutting down...')

    def process_dht_request(self, request):
        def store(request):
            key = request['key']
            value = request['value']
            ttl = request['ttl']
            replication = request['replication']
            i = 0
            return {'all_ok': True}
            # 3 is called alpha in kademlia specification and is recommended by the authors to be 3
            target_nodes = self.node_lookup(id, 3)
            for node in target_nodes:
                if store(node['id'], node['ip'], node['port'], key, value, ttl):
                    # i is the number of stores successful
                    i += 1
                    if i == replication:
                        break
            if i < replication:
                return {'all_ok': False}

            return {'all_ok': True}

        def find_value(request):
            # TODO: Do the same as in store request above, and delete the lines below
            # TODO: in the algorithm of this function, the peer must decide if it is already storing the key/value pair in question
            # by issuing a self.data_server.get(key), and checking the result
            from random import randrange
            from bitstring import BitArray

            content = BitArray(int=randrange(0, 1000), length=256)
            self.logger.debug('process_dht_request->find_value.str(content): %s' % str(content))
            self.logger.debug('process_dht_request->find_value.len(str(content)): %s' % len(str(content)))
            response = { 'content': content.tobytes(),
                         'all_ok': True }
            self.logger.debug('process_dht_request->find_value.len(response["content"]): %s' % len(response['content']))

            return response

        def trace(request):
            from random import randrange
            from bitstring import BitArray

            trace_recs_num = randrange(2, 5)
            trace_recs = []
            for i in range(trace_recs_num):

                peer_ID = BitArray(int=randrange(0, 1000), length=256)
                KX_port = randrange(10000, 20000)
                IPv4_address = BitArray(int=randrange(0, 1000), length=32)
                IPv6_address = BitArray(int=randrange(0, 1000), length=128)

                trace_rec = { 'peer_ID': peer_ID.tobytes(),
                              'KX_port': KX_port,
                              'IPv4_address': IPv4_address.tobytes(),
                              'IPv6_address': IPv6_address.tobytes() }
                trace_recs.append(trace_rec)

                self.logger.debug('process_dht_request->trace.str(peer_ID): %s' % str(peer_ID))
                self.logger.debug('process_dht_request->trace.str(KX_port): %s' % str(KX_port))
                self.logger.debug('process_dht_request->trace.str(IPv4_address): %s' % str(IPv4_address))
                self.logger.debug('process_dht_request->trace.str(IPv6_address): %s' % str(IPv6_address))

            response = { 'trace': trace_recs,
                         'all_ok': True }
            self.logger.debug('process_dht_request->find_value.len(response["trace"]): %s' % len(response['trace']))

            return response

        self.logger.debug('process_dht_request')
        self.logger.debug('received dht request: %s' % str(request))

        # TODO: uncomment below code when testing is done
#        request_handlers = { config.MSG_DHT_PUT: KademliaProtocolRequestHandler.store,
#                             config.MSG_DHT_GET: KademliaProtocolRequestHandler.find_value,
#                             config.MSG_DHT_TRACE: KademliaProtocolRequestHandler.?}
        request_handlers = { config.MSG_DHT_PUT: store,
                             config.MSG_DHT_GET: find_value,
                             config.MSG_DHT_TRACE: trace }

        request_handler = request_handlers[request['type']]
        del request['type']
        response = request_handler(request)

        self.logger.debug('response (for dht request: %s): %s' % (request, response))
        self.logger.debug('process_dht_request is done')

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

    def node_lookup(self, id, alpha, include_kx_info=False):
        # TODO: when calling find_node in this function, put the KX_INFO flag, to get the KX ip/port for DHT_TRACE
        # TODO: figure out which nodes to return the KX_INFO of, read the specification of the project
        # get local neighbors in your own kbuckets
        local_peers = self.buckets.get_closest_nodes(id, alpha)
        # Super list of all nodes indexed by the distance of a node to the target id
        nodes = dict()
        for peer in local_peers:
            # peer: (id, ip, port)
            # tuple: (peer, contacted, replied)
            nodes[int(peer['id'], 16) ^ id] = (peer, False, False)

        round_nodes = []
        # begin rounds:
        for node in nodes:
            # if node has not been contacted it yet!
            if not node[1]:
                # node = tuple: (peer, contacted, replied)
                node[1] = True
                round_nodes.append(node)
                # TODO:
                # issue find_node() requests to all round_nodes
                # for each peer that replies do:
                    # round_nodes[(index of peer tha replies)][2] = True
                # for each reply add nodes to dict:
                # if (int(reply['id'], 16) ^ id) not in nodes:      << remember nodes is indexed by distance of id to target id
                    # nodes[int(reply['id'], 16) ^ id] = (reply, False, False) << add new node to nodes super list

        # BEGIN ROUND
        while 1:
            # Empty current nods list
            del round_nodes[:]
            # Sort nodes to find closest peers
            sorted_nodes = sorted(nodes.items(), key=operator.itemgetter(0))
            # if new closer nodes were found, add them to the round_nods for querying
            i = 0
            while not sorted_nodes[i][1][1] and i < alpha:
                round_nodes.append(sorted_nodes[i][1])
                round_nodes[-1][1] = True
                nodes[sorted_nodes[i][0]][1] = True
                i += 1
            # if there were new closer nodes, add old not yet queried close nodes to the round_nods for querying
            if i == 0:
                while i < self.buckets.ksize and i < len(sorted_nodes):
                    if not sorted_nodes[i][1][1]:
                        round_nodes.append(sorted_nodes[i][1])
                        round_nodes[-1][1] = True
                        nodes[sorted_nodes[i][0]][1] = True

            # check if there are new nodes to query, if not, node_lookup is complete
            if not round_nodes:
                break
            # TODO:
            # issue find_node() requests to all round_nodes
                # for each peer that replies do:
                    # round_nodes[(index of peer tha replies)][2] = True
                # for each reply add nodes to dict:
                # if (int(reply['id'], 16) ^ id) not in nodes:      << remember nodes is indexed by distance of id to target id
                    # nodes[int(reply['id'], 16) ^ id] = (reply, False, False) << add new node to nodes super list

        # END ROUND
        result = []
        sorted_nodes = sorted(nodes.items(), key=operator.itemgetter(0))
        for i in range(self.buckets.ksize):
            result.append(sorted_nodes[i])
        return result

    # TODO: add a static method for sending through sockets and getting the replies back

    @staticmethod
    def prepare_req(msg_type):
        req = dict()
        req['TYPE'] = msg_type
        req['MID'] = str(hex(random.getrandbits(160)))[2:-1]
        req['SID'] = str(config.PEER_ID)

        return req


    @staticmethod
    def ping(id, ip, port):
        req = KademliaProtocolServer.prepare_req('PING')

        req['RID'] = str(id)
        req['IP'] = config.HOSTNAME
        req['PORT'] = config.KADEM_PORT

        address = (ip, port)
        logger.info('server on %s:%s', address[0], address[1])

        # Connect to the peer
        logger.debug('creating socket')
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logger.debug('connecting to server')
        s.connect(address)

        # Send the data
        req_bson = dumps(req)
        logger.debug('sending data: "%s"', req_bson)
        s.send(req_bson)

        # Receive a response
        logger.debug('waiting for response')
        response = s.recv(16384)
        logger.debug('response from server: "%s"', response)

        s.close()

        if len(response) == 0:
            return False

        is_valid, error_msg = KademliaProtocolServer.res_is_valid(response)
        if not is_valid:
            return False

        return True

    @staticmethod
    def store(id, ip, port, key, value, ttl):
        req = KademliaProtocolServer.prepare_req('STORE')
        req['RID'] = id
        req['Key'] = key
        req['Value'] = value
        req['TTL'] = ttl
        req['IP'] = config.HOSTNAME
        req['PORT'] = config.KADEM_PORT

        # Connect to the peer
        logger.debug('creating socket')
        address = (ip, port)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logger.debug('connecting to server')
        s.connect(address)

        req_bson = dumps(req)
        logger.debug('sending data: "%s"', req_bson)
        s.send(req_bson)

        # Receive a response
        logger.debug('waiting for response')
        response = s.recv(16384)
        logger.debug('response from server: "%s"', response)

        s.close()

        if len(response) == 0:
            return False

        is_valid, error_msg = KademliaProtocolServer.res_is_valid(response)
        if not is_valid:
            return False

        return True

    @staticmethod
    def find_node(id, ip, port, key, include_kx_info=False):
        req = KademliaProtocolServer.prepare_req('FIND_NODE')
        req['RID'] = id
        req['KX_INFO'] = KX_INFO
        req['Key'] = key
        #send_req(req, ip, port)
        #retrun the answer in find_node_reply from the other peer
        #return nodes

    @staticmethod
    def find_value(id, ip, port, key):
        req = KademliaProtocolServer.prepare_req('FIND_VALUE')
        req['RID'] = id
        req['Key'] = key
        #send_req(req, ip, port)

    @staticmethod
    def verify(id, ip, port, x):
        req = KademliaProtocolServer.prepare_req('VERIFY')
        req['RID'] = id
        req['Challenge'] = x
        #send_req(req, ip, port)

    @staticmethod
    def res_is_valid(res):
        required_keys_map = { 'PONG': ['MID', 'SID', 'RID'],
                              'STORE_REPLY': ['MID', 'SID', 'RID', 'Status'],
                              'VERIFY_REPLY': ['MID', 'SID', 'RID', 'Challenge_Reply'] }
        try:
            res = loads(res)
        except ValueError:
            return False, 'non-valid bson object'

        try:
            res_type = res['TYPE']
            required_keys = required_keys_map[res_type]
        except KeyError:
            return False, 'response with no specified type'

        for key in required_keys:
            # TODO: verify value types
            if not res.has_key(key):
                return False, 'response with required keys missing'
        # TODO: verify SID/RID combination
        # TODO: verify if MID of request is returned in response
        return True, ''


def issue_ping(logger):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test Kademlia-server request handling from a Kademlia peer
    address = (config.HOSTNAME, config.KADEM_PORT)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logger.debug('connecting to server')
    s.connect(address)

    # Send the data
    store_req = { 'TYPE': 'PING',
                  'MID': '1BCD77AFF8391729182DC63',
                  'SID': str(int(config.PEER_ID.replace('d', 'a'), 16)),
                  'RID': str(int(config.PEER_ID, 16)),
                  'IP': config.HOSTNAME,
                  'PORT': config.KADEM_PORT }
    store_req_bson = dumps(store_req)
    logger.debug('sending data: "%s"', store_req)
    s.send(store_req_bson)

    # Receive a response
    logger.debug('waiting for response')
    response = s.recv(16384)
    logger.debug('response from server: "%s"', response)


def issue_store(logger, key, value):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test Kademlia-server request handling from a Kademlia peer
    address = (config.HOSTNAME, config.KADEM_PORT)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logger.debug('connecting to server')
    s.connect(address)

    # Send the data
    store_req = { 'TYPE': 'STORE',
                  'MID': '1BCD77AFF8391729182DC63',
                  'SID': str(int(config.PEER_ID.replace('d', 'a'), 16)),
                  'RID': str(int(config.PEER_ID, 16)),
                  'TTL': 10,
                  'Key': key,
                  'Value': value,
                  'IP': config.HOSTNAME,
                  'PORT': config.KADEM_PORT }

    store_req_bson = dumps(store_req)
    logger.debug('sending data: "%s"', store_req)
    s.send(store_req_bson)

    # Receive a response
    logger.debug('waiting for response')
    response = s.recv(16384)
    logger.debug('response from server: "%s"', response)


def issue_find_value(logger, key):
    def clean_up():
        # Clean up
        logger.debug('closing socket')
        s.close()
        logger.debug('done')

    # Test Kademlia-server request handling from a Kademlia peer
    address = (config.HOSTNAME, config.KADEM_PORT)
    logger.info('server on %s:%s', address[0], address[1])

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logger.debug('connecting to server')
    s.connect(address)

    # Send the data
    store_req = { 'TYPE': 'FIND_VALUE',
                  'MID': '1BCD77AFF8391729182DC63',
                  'SID': str(int(config.PEER_ID.replace('d', 'a'), 16)),
                  'RID': str(int(config.PEER_ID, 16)),
                  'Key': key }

    store_req_bson = dumps(store_req)
    logger.debug('sending data: "%s"', store_req)
    s.send(store_req_bson)

    # Receive a response
    logger.debug('waiting for response')
    response = s.recv(16384)
    logger.debug('response from server: "%s"', response)


if __name__ == '__main__':
    import socket

    logger = logging.getLogger('Kademlia Peer')
    logger.debug('issuing PING')
    issue_ping(logger)
    print
    logger.debug('issuing STORE')
    issue_store(logger, 'aaaa', 'value1')
    print
    logger.debug('issuing FIND_VALUE')
    issue_find_value(logger, 'aaaa')
    print
    logger.debug('issuing PING')
    KademliaProtocolServer.ping(str(int(config.PEER_ID, 16)), config.HOSTNAME, config.KADEM_PORT)
    print
    logger.debug('issuing STORE')
    KademliaProtocolServer.store(str(int(config.PEER_ID, 16)), config.HOSTNAME, config.KADEM_PORT, 'aaaa', 'value2', 10)
    print
    logger.debug('issuing FIND_VALUE')
    issue_find_value(logger, 'aaaa')
    print
    logger.debug('issuing FIND_VALUE')
    issue_find_value(logger, 'key2')
    print
    logger.debug('issuing STORE')
    KademliaProtocolServer.store(str(int(config.PEER_ID, 16)), config.HOSTNAME, config.KADEM_PORT, 'aaaab', 'value2', 10)
    print
    logger.debug('issuing FIND_VALUE')
    issue_find_value(logger, 'aaaa')
