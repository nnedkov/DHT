#!/usr/bin/python

import logging
from collections import OrderedDict
import itertools
import kademlia_protocol_server

import config

logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s', )


class Buckets:
    def __init__(self, id, length, ksize):
        self.logger = logging.getLogger('Bucket')
        self.logger.debug('__init__')

        self.ksize = ksize  # k in kademlia
        self.id = int(id, 16)
        self.length = length  # key space size
        self.buckets = dict()

    # node = {'id': , 'port': , 'ip': }
    def add_refresh_node(self, node):
        id = int(node['id'], 16)
        id_str = node['id']
        distance = id ^ self.id  # distance from peer
        index = distance.bit_length() - 1
        if index in self.buckets:
            if id_str in self.buckets[index]:
                del self.buckets[index][id_str]
                self.buckets[index][id_str] = node
            elif len(self.buckets[index]) >= self.ksize:
                oldest_unused = self.buckets[index].items()[0]
                kademlia_protocol_server.KademliaProtocolServer.ping(oldest_unused[1]['id'],
                                                                             oldest_unused[1]['ip'],
                                                                             oldest_unused[1]['port'])
                return False
            else:
                self.buckets[index][id_str] = node
        else:
            self.buckets[index] = OrderedDict()
            self.buckets[index][id_str] = node
        return True

    def del_node(self, id):
        id = int(id, 16)
        distance = id ^ self.id
        index = distance.bit_length() - 1
        if index in self.buckets:
            if id in self.buckets[index]:
                del self.buckets[index][id]
                if len(self.buckets[index]) == 0:
                    del self.buckets[index]
                return True
        return False

    # return k closest peers to the id
    def get_closest_nodes(self, id, k):
        id = int(id, 16)
        distance = id ^ self.id
        index = distance.bit_length() - 1
        neighbors = []
        i = 0
        total = 0
        while total < k and abs(i) < self.length:
            remaining = k - total
            current_index = index + i
            if current_index in self.buckets:
                length = len(self.buckets[current_index])
                # there is enough nodes or less in the bucket
                if length <= remaining:
                    total += length
                    items = length
                # there is more than enough nodes in the bucket
                else:
                    total += remaining
                    items = remaining
                # append nodes to neighbors list
                x = dict(itertools.islice(self.buckets[current_index].items(), 0, items))
                neighbors.append(x)
            # i = 0, -1, 1, -2, 2, -3, 3, -4, 4,....
            if i == 0:
                i = 1
            elif i > 0:
                i *= -1
            else:
                i = abs(i - 1)
        return neighbors

    def get_nodes(self):
        return self.buckets


if __name__ == '__main__':
    import random

    size = 160
    id = str(hex(random.getrandbits(size)))[2:-1]
    buckets = Buckets(id, size, 20)
    times = []
    id = "00000000000000000000000000000000000000"
    buckets.add_refresh_node(node={'id': id + "01", 'ip': '10.2.1.2', 'port': '1'})
    # print buckets.get_nodes()
    # buckets.del_node(id)
    # print buckets.get_nodes()
    node = {'id':'d','ip': 'w', 'port': 'q'}
    print node