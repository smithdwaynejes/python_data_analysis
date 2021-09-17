#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

# # import urllib.request
# # import threading
# # from datetime import datetime, timedelta
# # import time

# # # from libs.ppsecurity import PPamfi,PPnse

# # def domystuff():
# #   now = datetime.now
# #   print ("Doing stuff...")
# #   print("{}:{}:{}".format(now().hour,now().minute,now().second))

# # mythrd = None

# # def run_check():
# #   while (True):
    
# #     now = datetime.now
# #     if now().hour == 16 and now().minute == 30 and now().second == 30:
# #       mythrd.start()
# #       time.sleep(1)
# #     if mythrd.is_alive():
# #       domystuff()
  
# #     if datetime.now().hour == 16 and datetime.now().minute == 31:
# #         mythrd.cancel()

# # mythrd =threading.Timer(5.0, run_check)
# # run_check()

# import threading
# from threading import Thread, Timer
# import time
# from datetime import datetime, timedelta

# keep_checking = True
# keep_working = True

# def domystuff():
#   now = datetime.now()
#   print ("Doing stuff...")
#   print("{}:{}:{}".format(now.hour,now.minute,now.second))

# def worker_thread():
#     while (keep_working):
#         domystuff()

# def time_checker():
#     while (keep_checking):
#         now = datetime.now()
#         if now.hour == 17 and now.minute == 40 and (
#            now.second >= 30 and now.second < 31) and (
#            not mythrd.isAlive()):
#             mythrd.start()
#         elif now.minute >= 31 and mythrd.isAlive():
#             keep_working = False # and let it die instead to kill it
#         else:
#             time.sleep(1)

# mythrd = threading.Thread(target=time_checker)
# time_checker() # might turn this into a thread as well

from datetime import datetime as dt
import threading
import time

from libs.ppsecurity import PpAmfi

def job1(unix_time, human_time):
  print ("Job1: Unix time: %s  Human time: %s \n" % (unix_time, human_time))

def job2():
  print ("ehllo....")

def autoamfi():
  time = dt.now()

  print ("Doing stuff at {}".format(time))
  ppamfi = PpAmfi("02222019")
  ppamfi.suck()
  # ppamfi.read("/home/infant/20190220.txt")
  ppamfi.parse()
  ppamfi.save(isCopyRaw=True,saveOnly="02222019",includeTime=True)



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
human_time = str(dt.now())
 
start_time = [13,8,45] # Hours, Minutes, Seconds
end_time = [13,9,0] # Hours, Minutes, Seconds

schedule(autoamfi,[],start_time,end_time,5)