import time
import os

while(1):
	time.sleep(1)
	filename = "cmd.in"
	if os.path.isfile(filename):	
		with open(filename, 'r') as file:
			cmd = file.read()
		os.remove(filename)
		print(cmd)
		os.system(cmd)