from hashlib import sha1
from datetime import datetime
from metric import Metric
from util import RepeatedTimer
import hmac
import requests
import jsonpickle
import time
import psutil


# Configuration variables
MID_SECRET = ''
MID_ADDRESS = ''
MID_PORT = ''

COLLECTION_INTERVAL = 60

METRIC_TYPE = 'K17'
METRIC_RESOURCE = 'Local'
METRIC_NODE = 'K17 CreatorCon Computer'
METRIC_SOURCE = 'K17Demo'

# Constants
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
DATE_FORMAT_GMT = '%a, %d %b %Y %H:%M:%S GMT'
CONTENT_TYPE = 'application/json'


# Function to generate a metric object
def createMetric(category, metric, value):
	# Build a metric type name for the given metric
	m = METRIC_TYPE + ' / ' + category + ' / ' + metric

	# Return a metric description object
	return Metric(source=METRIC_SOURCE, metric=m, resource=METRIC_RESOURCE,node=METRIC_NODE, value=value)


# Function to collect all metrics for a psutil result
def collectMetricCategory(category, result):
	# Get all stats in the current result class
	# Do this by filtering out all internal/callable attributes
	stats = [a for a in dir(result) if not a.startswith('_') and not callable(getattr(result,a))]

	# For each stat, collect a metric object
	metrics = []
	for stat in stats:
		metrics.append(createMetric(category=category, metric=stat, value=getattr(result, stat)))

	# Return a list of all the the collected metrics for the result set
	return metrics


# Function to collect all local metrics
def collectMetrics():
	data = []

	# Collect data from multiple system categories
	data += collectMetricCategory(category='cpu', result=psutil.cpu_times_percent())
	data += collectMetricCategory(category='memory', result=psutil.virtual_memory())
	data += collectMetricCategory(category='disk', result=psutil.disk_io_counters())
	data += collectMetricCategory(category='network', result=psutil.net_io_counters())

	# Return a list of all metric data
	return data


# Function to collect and print metric data
def collectAndPrint():
	print('\n\n')
	print(jsonpickle.encode(collectMetrics(), unpicklable=False))


# Function to collect and POST metric data
def collectAndSend():
	# Collect all metric data and encode as a JSON string
	metrics = collectMetrics()
	data = jsonpickle.encode(metrics, unpicklable=False)

	# Build the URL to send to and calculate headers
	url = MID_ADDRESS + ':' + MID_PORT + '/api/mid/sa/metrics';
	headers = requestHeaders()

	# Send a POST request to MID with the collected data
	r = requests.post(url, headers=headers, data=data)

	# Log a metric send event
	print('Sent ' + str(len(metrics)) + ' metrics at ' + datetime.utcnow().strftime(DATE_FORMAT)[:-3])


# Function to create request headers for sending data to MID
def requestHeaders():
	# Build a request string to be turned into an HMAC
	auth = '';
	auth += 'POST\n'
	auth += 'application/json\n'
	auth += datetime.utcnow().strftime(DATE_FORMAT)[:-3] + 'Z\n'
	auth += '/api/mid/sa/metrics'

	# Create and HMAC token using the secret and request string
	hashed = hmac.new(MID_SECRET, auth, sha1)
	encoded = hashed.digest().encode('base64').rstrip('\n')

	# Return the request headers
	return {
		'Authorization': encoded,
		'Date': datetime.utcnow().strftime(DATE_FORMAT)[:-3] + 'Z',
		'Content-Type': 'application/json'
	}



# Trigger metric collection once to avoid initial zero values
collectMetrics()

# Set up a repeating timer to collect metrics at our preferred interval
rt = RepeatedTimer(COLLECTION_INTERVAL, collectAndSend)
