import hashlib
from server_config import NODES

       
class NodeRing():
	
    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes

    	
    def get_node(self, key):
    	maxi, champion = -1, None
    	for node in self.nodes:
    		w = compute_weighted_score(node['port'],key)
    		if w > maxi:
    			champion, maxi = node, w
    	return champion
    
       
def compute_weighted_score(server_address, key):
	a = 1103515245
	b = 12345
	return (a * ((a * server_address + b) ^ hash(key)) + b) % 2^31
   
        
	
ring = NodeRing(nodes = NODES)
champ = ring.get_node('9ad57948743a')
print(champ)
print(ring.get_node('e0b8'))
print(ring.get_node('0'))
print(ring.get_node('123456789086543'))
	

#run the above local test via: python3 rendezvous_node_ring.py
