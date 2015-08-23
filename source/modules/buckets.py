#!/usr/bin/python

import logging
from collections import OrderedDict
import itertools
import kademlia_protocol_server

import config

logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s', )


class Buckets:
    def __init__(self, length, ksize):
        self.logger = logging.getLogger('Bucket')
        self.logger.debug('__init__')

        self.ksize = ksize  # k in kademlia
        self.id = config.PEER_ID
        self.length = length  # key space size
        self.buckets = dict()

    # node = {'id': string, 'port': number, 'ip': number}
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

    # id is a string
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

    # id is a string
    # return k closest peers to the id as a list
    def get_closest_nodes(self, id, k):
        id = int(id, 16)
        distance = id ^ self.id
        indices = Buckets.get_indices(distance)
        neighbors = []
        i = 0
        total = 0
        while total < k and i < self.length:
            remaining = k - total
            current_index = indices[i]
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
            i += 1
        return neighbors

    def get_nodes(self):
        return self.buckets

    @staticmethod
    def get_indices(distance):
        indices = []
        temp_dist = distance
        while temp_dist != 0:
            indices.append(temp_dist.bit_length() - 1)
            temp_dist = 2**indices[-1]-1 & distance
        i = 0
        while i < 256:
            if i not in indices:
                indices.append(i)
            i += 1
        return indices

if __name__ == '__main__':
    import random
    import operator

    size = 256
    id = str(hex(random.getrandbits(size)))[2:-1]
    buckets = Buckets(id, size, 20)
    print str(buckets.id)
    #for i in range(100000):
    #    id = str(hex(random.getrandbits(size)))[2:-1]
    #    buckets.add_refresh_node(node={'id': id, 'ip': '10.2.1.2', 'port': '1'})