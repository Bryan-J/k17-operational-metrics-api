import time

class Metric(object):
	def __init__(self, source=None, metric=None, resource=None, node=None, value=0, timestamp=None):
		self.source = source
		self.metric_type = metric
		self.resource = resource
		self.node = node
		self.value = value
		self.timestamp = (timestamp if timestamp is not None else int(round(time.time() * 1000)))
		self.ci_identifier = {
			'node': node
		}
