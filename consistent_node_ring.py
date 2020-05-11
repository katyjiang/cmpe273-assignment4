import hashlib
from hashlib import md5
from bisect import bisect
from server_config import NODES


class NodeRing(object):

    def __init__(self, servers, replicas=7):
        nodes = self.push_nodes(servers, replicas)
        hash_nodes = [self.hash(node) for node in nodes]
        hash_nodes.sort()
        self.replicas = replicas
        self.nodes = nodes
        self.hash_nodes = hash_nodes
        self.nodes_list = {self.hash(node): node.split("-")[1] for node in nodes}

    @staticmethod
    def push_nodes(servers, replicas):
        nodes = []
        for i in range(replicas):
            for node in servers:
                nodes.append("{}-{}".format(i, node))
        return nodes

    @staticmethod
    def hash(key):
        hkey = md5(key.encode('utf-8'))
        return int(hkey.hexdigest(), 16) 

    def get_node(self, key):
        pos = bisect(self.hash_nodes, self.hash(key))
        if pos == len(self.hash_nodes):
            return self.nodes_list[self.hash_nodes[0]]
        else:
            return self.nodes_list[self.hash_nodes[pos]]



ring = NodeRing(NODES)
print(ring.get_node('ed9440c44263621b608521b3f2650b8'))
print(ring.get_node('iuwiugigi122334'))
print(ring.get_node('0'))
print(ring.get_node('1234567890lkjkn/.,;....'))
print(ring.get_node('77777778991'))
print(ring.get_node('6789'))

#run the above local test via: python3 consistent_node_ring.py
