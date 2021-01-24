import subprocess as sb
from threading import Thread

print("------Start thread launcher  ")

def func():
    print("thread launcher checkout ")
    sb.check_output("/spark/bin/spark-submit --conf spark.executor.cores=2 \
         --conf spark.executor.memory=5G --master spark://spark-master:7077 /entry/activus/real_time_mode.py \
              2019-05-04-12:00:00 2019-05-04-16:00:00 DSO05LM 01:00:5e:50:01:42  ",shell=True)
    
TH=Thread(target=func)
TH.start()
#TH.join()

