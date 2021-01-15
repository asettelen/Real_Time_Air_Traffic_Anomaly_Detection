import os
import random
from datetime import datetime

def application(environ, start_response):
	query = environ['QUERY_STRING']
	reportKey = str(random.randint(1000000, 9999999))
	now = datetime.now()
	reportKey = now.strftime("%Y-%m-%d-%H-%M-%S")
	filename = reportKey + ".tmp"
	os.system("spark-submit service.py \"" + query + "\" " + reportKey)
	start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
	with open(filename, 'r') as file:
			data = file.read()
	os.remove(filename)
	return [data]

