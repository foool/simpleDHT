import md5

class HashRing(object):
	def __init__(self,nodes=None,replicas=3):
		"""initial HashRing, 
		ring store pairs of (key,node), 
		replicas represent the number of virtual nodes
		sorted_keys saves sorted keys"""
		self.ring=dict()
		self.replicas=replicas
		self.sorted_keys=[]
		if nodes:
			for node in nodes:
				self.add_node(node)
	
	def add_node(self,node):
		"""add each (virtual)node, sorted"""
		for i in xrange(0,self.replicas):
			key=self.gen_key('%s%s'%(node,i))
			self.ring[key]=node
			self.sorted_keys.append(key)
		self.sorted_keys.sort()
		
	def get_node_pos(self,str_src):
		"""return the node to store src file"""
		fHash=self.gen_key(str_src)
		node_hash=self.upper_node_hash(fHash)
		for each in self.ring:
			if each==node_hash:
				return self.ring[each]

	def gen_key(self,str_node):
		"""return (virtual)node's hash result"""
		m=md5.new()
		m.update(str_node)
		return long(m.hexdigest(),16)
		
	def upper_node_hash(self,xhash):
		"""return upper node hash for xhash"""
		for each in self.sorted_keys:
			if each>xhash:
				return each