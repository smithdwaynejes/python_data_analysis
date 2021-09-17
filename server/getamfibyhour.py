#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt,timedelta
import threading
import time

from libs.ppsecurity import PpAmfi

def job1(unix_time, human_time):
  print ("Job1: Unix time: %s  Human time: %s \n" % (unix_time, human_time))

def job2():
  print ("ehllo....")

def auto_amfi(todate,getfor):
  time = dt.now()

  print ("Doing stuff at {}".format(time))

  ppamfi = PpAmfi(todate)
  ppamfi.suck()
#   ppamfi.read("/home/infant/20190225.txt")
  ppamfi.parse()
  ppamfi.save(isCopyRaw=True,saveOnly=getfor,includeTime=True)
#   ppamfi.save(isCopyRaw=True,saveOnly="03052019",includeTime=True)
  lastupdateFile = ppamfi.settings['path']['raw'] + ppamfi.settings['lastupdate']

  if ppamfi.rawoutput != "":
    with open(lastupdateFile,"a+") as fp:
        fp.writelines(ppamfi.rawoutput)

    
def wrapper(func, args): # with star
    func(*args)

class schedule(object):

    def __init__(self,task,task_params,start_time,end_time,delay):
        self.run_permission = False

        self.start_time = start_time # Hours, Minutes, Seconds
        self.end_time = end_time # Hours, Minutes, Seconds

        self.timer_sched_time = delay # seconds

        threading.Thread(name="time_checker", target=self.check_time, args=(self.start_time, self.end_time,)).start()
        threading.Thread(name="scheduled_job", target=self.Timer, args=(self.timer_sched_time,task,task_params )).start()

        while True:
            time.sleep(10)

    def timer_job(self, unix_time, human_time):
        print ("Unix time: %s  Human time: %s \n" % (unix_time, human_time))

    def Timer(self, delay,task,task_params):
        while True:
            try:
                time_remaining = delay-time.time()%delay
                time.sleep(time_remaining)
         
                if(self.run_permission):
                    # unix_time = str(int(round(time.time()*1000)))
                    # human_time = str(dt.now())
                    # task_params = [unix_time,human_time]
                    wrapper(task, task_params)

            except Exception as ex:
                raise ex

    def check_time(self, start_execution_time, end_execution_time):
        while True:
            now = dt.now()

            if(now.hour >= start_execution_time[0] and now.minute >= start_execution_time[1] and now.second >= start_execution_time[2]):
                self.run_permission = True

            if(now.hour >= end_execution_time[0] and now.minute >= end_execution_time[1] and now.second >= end_execution_time[2]):
                self.run_permission = False

unix_time = str(int(round(time.time()*1000)))
human_time = dt.now()

todate = human_time.strftime("%m%d%Y")

start_time = [18,0,0] # Hours, Minutes, Seconds
end_time = [23,30,0] # Hours, Minutes, Seconds
schedule(auto_amfi,[todate,todate],start_time,end_time,1800)
