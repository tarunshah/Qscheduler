from my_queue import Queue
import pandas as pd
import time
import sys

#Input
f = open('inputs_events.csv','rt',encoding='utf8')
df1 = pd.read_csv(f,delimiter=",",quoting=3, header=0)

now = str(sys.argv[2])

#function to calculate time difference
def time_diff(row):
    return timeToEpoch(row["time_to_expire"]) - timeToEpoch(now)

#function to convert date-time to epoch
def timeToEpoch(strtTime):
    pattern = '%Y/%m/%d %H:%M'
    return int(time.mktime(time.strptime(strtTime, pattern)))

#function to convert epoch to date-time
def epocToTime(tm,count):
    tm = tm + 60*count
    return time.strftime('%Y/%m/%d %H:%M', time.localtime(tm))


if __name__ == "__main__":
    #calculating time difference between start time and end time
    df1["time_difference"] = df1.apply(time_diff, axis=1)

    #sorting dataframe according to time and priority respectively
    df1 = df1.sort_values(['time_difference','priority'], ascending=[True,True])

    #convert dataframe to dictionary
    df = df1.to_dict(orient='records')

    #Initializing Queue
    q = Queue()

    #insert dictionary to queue
    for i in range(len(df)):
        q.enqueue(df[i])

    flag = 0  #Flag to check 1st time claculation
    count = 0
    while not q.isEmpty():
         count+=1
         time.sleep(60)
         if flag ==0:
             print("--After 1 minute --")
             while timeToEpoch(q.items[-1]["time_to_expire"]) == (timeToEpoch(now) + (60*count)):
                 print("Current time [" + epocToTime(timeToEpoch(now),count) + "], Event \"" + q.items[-1]["event_name"] + "\" Processed")
                 q.dequeue()
                 flag = 1
                 if q.isEmpty():
                     break
         else:
             print("--After Another 1 minute --")
             while timeToEpoch(q.items[-1]["time_to_expire"]) == (timeToEpoch(now) + (60*count)):
                 print("Current time [" + epocToTime(timeToEpoch(now),count) + "], Event \"" + q.items[-1]["event_name"] + "\" Processed")
                 q.dequeue()
                 if q.isEmpty():
                     break