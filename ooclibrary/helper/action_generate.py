import copy
import random
import bisect
from collections import Sequence

# put back sample
def sample(arr, n):
	res = []
	for i in xrange(n):
		res.append(arr[random.randint(0, len(arr) - 1)])
	return res

class WeightSequence(Sequence):
	def __init__(self, arr, weight):
		self.arr = arr
		self.weight = weight

		cum_weight = [0] * len(weight)
		total = 0
		for i in range(len(weight)):
			total += weight[i]
			cum_weight[i] = total

		self.cum_weight = cum_weight

	def __len__(self):
		return self.cum_weight[-1]

	def __getitem__(self, i):
		if i < 0 or i >= len(self):
			raise IndexError(i)
		return self.arr[bisect.bisect(self.cum_weight, i)]

def weight_sample(arr, weight, n):
	seq = WeightSequence(arr, weight)
	return sample(seq, n)

class ActionDocument:

	def __init__(self, small, large):
		self.small = small
		self.large = large
		self.mem_counter = 0
		self.doc = []
		self.mem_type = {}

	def l_mems(self):
		return [i for i in range(self.mem_counter) if self.mem_type.has_key(i) and self.mem_type[i] == "L"]

	def s_mems(self):
		return [i for i in range(self.mem_counter) if self.mem_type.has_key(i) and self.mem_type[i] == "S"]

	def genL(self):
		self.doc.append("L %d %dM" % (self.mem_counter, self.large))
		self.mem_type[self.mem_counter] = "L"
		self.mem_counter += 1

	def genS(self):
		self.doc.append("S %d %dM" % (self.mem_counter, self.small))
		self.mem_type[self.mem_counter] = "S"
		self.mem_counter += 1

	def freeL(self):
		if (len(self.l_mems()) == 0):
			return

		d = random.sample(self.l_mems(), 1)[0]
		self.doc.append("F %d" % d)
		del(self.mem_type[d])

	def freeS(self):
		if (len(self.s_mems()) == 0):
			return

		d = random.sample(self.s_mems(), 1)[0]
		self.doc.append("F %d" % d)
		del(self.mem_type[d])

	def accessL(self):
		d = random.sample(self.l_mems(), 1)[0]
		self.doc.append("A %d" % d)

	def accessS(self):
		d = random.sample(self.s_mems(), 1)[0]
		self.doc.append("A %d" % d)

	def generate_document(self, weight, n):
		actions = [self.genS, self.genL, self.freeS, self.freeL, self.accessS, self.accessL]

		for i in range(n):
			idx = [0, 1]

			if len(self.s_mems()) != 0:
				idx = idx + [2, 4]

			if len(self.l_mems()) != 0:
				idx = idx + [3, 5]

			action = weight_sample([actions[i] for i in idx], [weight[i] for i in idx], 1)
			action[0]()


		return "\n".join(self.doc) + "\n"

	def actions(self):

		return [
			self.genS,
			self.genL,
			self.freeS,
			self.freeL,
			self.accessS,
			self.accessL
		]

def generate_action(weight, num):
	doc = ActionDocument(2048, 2048)
	return doc.generate_document(weight, num)