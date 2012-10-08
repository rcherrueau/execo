class ProcessesFailed(Exception):

	def __init__(self, processes):
		self._processes = processes

	def __str__(self):
		s = "<ProcessesFailed - failed process(es)::"
		for p in self._processes:
			s += "  " + p.dump() + "\n"
		s += ">\n"
		return s
