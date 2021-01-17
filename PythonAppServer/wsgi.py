import os
from datetime import datetime
import os.path
import uwsgi
import urllib
import time
import subprocess

def application(environ, start_response):
	start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
	query = environ['QUERY_STRING']
	query2 = query.split('&')
	args = {}
	for i in range(len(query2)):
		c = query2[i].split("=")
		args[c[0]] = urllib.unquote(c[1])
	action = args['action']
	if action == "startBatch" or action == "startRealtime":
		#cmd = "spark-submit service.py \"" + query + "\"\n"
		cmd = "kill -9 `cat pid2`\nspark-submit service.py \"" + query + "\" &\n"
		cmd += "var=$!\n"
		cmd += "echo \"$var\" > \"pid2\""
		file = open("cmd.in", "w") 
		file.write(cmd)
		file.close()
		#os.system(cmd)
		print("start")
	elif action == "stop":	
		cmd = "kill -9 `cat pid2`"
		file = open("cmd.in", "w") 
		file.write(cmd)
		file.close()
		#os.system("kill `cat pid2`")
		print("stop")
		return ["Stopped"]
	else:
		"""file = open("event.in", "w") 
		file.write(query)
		file.close()"""
		with open('mypipe', 'w') as f:
			f.write(query)
		print("makeReport")
	filename = "event.out"
	while(not(os.path.isfile(filename))):
		time.sleep(1)
	with open(filename, 'r') as file:
			data = file.read()
	os.remove(filename)
	return [data]

