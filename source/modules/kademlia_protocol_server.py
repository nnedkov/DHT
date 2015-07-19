#!/usr/bin/python

from bson.json_util import dumps, loads
import logging
import SocketServer

import config


logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s',)


class KademliaProtocolRequestHandler(SocketServer.BaseRequestHandler):

    def __init__(self, request, client_address, server, buckets):
        self.logger = logging.getLogger('KademliaProtocolRequestHandler')
        self.logger.debug('__init__')
        self.buckets = buckets
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
        return res
        
    def store_reply(self):
        res = self.prepare_reply('STORE_REPLY')
        # TODO: uncomment below when data server interface is ready
        # status = store_value(self.req['Key'], self.req['Value'], self.req['TTL'])
        # res['Status'] = status
        res['Status'] = 0   # fake success

        return res

    def find_node_reply(self):
        # TODO: change 20 to a global constant K
        nodes = self.buckets.get_closest_nodes(self.req['Key'], 20)
        
        res = self.prepare_reply('FIND_STORE_REPLY')
        pass

    def find_value_reply(self):
        res = self.prepare_reply('FIND_VALUE_REPLY')
        pass

    def verify_reply(self):
        res = self.prepare_reply('VERIFY_REPLY')
        pass

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

    def __init__(self, server_address, handler_class=KademliaProtocolRequestHandler):
        self.logger = logging.getLogger('KademliaProtocolServer')
        self.logger.debug('__init__')
        self.buckets = buckets("1BCD77AFF8391729182DC63AFFFFF319000567AA",160,20)
        SocketServer.UDPServer.__init__(self, server_address, handler_class, buckets)

    def server_activate(self):
        self.logger.debug('server_activate')
        SocketServer.UDPServer.server_activate(self)

    def serve_forever(self, queue):
        self.logger.debug('waiting for request')

        while True:
            if config.SHUT_DOWN == 1:
                self.logger.info('going to exit')
                break

            self.handle_request()

        self.logger.info('exiting')

        result = (True, None)
        queue.put(result)

    def handle_request(self):
        self.logger.debug('handle_request')
        return SocketServer.UDPServer.handle_request(self)

    def verify_request(self, request, client_address):
        self.logger.debug('verify_request(%s, %s)', request, client_address)
        return SocketServer.UDPServer.verify_request(self, request, client_address)

    def process_request(self, request, client_address):
        self.logger.debug('process_request(%s, %s)', request, client_address)
        return SocketServer.UDPServer.process_request(self, request, client_address)

    def server_close(self):
        self.logger.debug('server_close')
        return SocketServer.UDPServer.server_close(self)

    def finish_request(self, request, client_address):
        self.logger.debug('finish_request(%s, %s)', request, client_address)
        return SocketServer.UDPServer.finish_request(self, request, client_address)

    def close_request(self, request_address):
        self.logger.debug('close_request(%s)', request_address)
        return SocketServer.UDPServer.close_request(self, request_address)

if __name__ == '__main__':
    from Queue import Queue
    from threading import Thread
    import socket

    queue = Queue()
    address = (config.HOSTNAME, config.PEER_PORT)
    server = KademliaProtocolServer(address, KademliaProtocolRequestHandler)

    t = Thread(target=server.serve_forever, args=(queue, ))
    t.setDaemon(True)   # terminate when the main thread ends
    t.start()

    logger = logging.getLogger('Client')
    logger.info('Server on %s:%s', config.HOSTNAME, config.PEER_PORT)

    # Connect to the server
    logger.debug('creating socket')
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logger.debug('connecting to server')
    s.connect((config.HOSTNAME, config.PEER_PORT))

    # Send the data
    store_req = {'TYPE': 'STORE',
                 'MID': 'some mid',
                 'SID': 'some sid',
                 'RID': 'some rid',
                 'Key': 'some key',
                 'TTL': 'some key',
                 'Value': 'some value'}
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
    logger.debug('done')
    server.socket.close()
