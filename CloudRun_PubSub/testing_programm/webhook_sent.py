# We want to create a programm to simulate the traffic of messages.
# We simulate the trafic for about 10000 messages per minute.
# To do so we create at start a array of 10000 values where we store random numbers from 1 to 60
# witch are going to be the random timestamps that the messages are beeing sent.
# The messages are going to contain the Number of the Message and also the time that is sent.
import requests
import json
import numpy as np
import time 
import random
import os
from multiprocessing import Process


number_of_event = 1000


time_array = np.random.uniform(low=1, high=60, size = (number_of_event,))

time_array = np.sort(time_array)

#for testing
#webhook_url = 'https://pubsub-cloudrun-tljpjyel3q-oa.a.run.app'
data = {'message': '', 'TimeSent': ''}
webhook_url = 'https://cloudrun-server-tljpjyel3q-uc.a.run.app'


def sentrequest(num):
    for i in range(10):
        #print("Programm " , num," started!")
        data['message'] = num
        data['TimeSent'] =  time.time()
        r = requests.post(webhook_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
        r.raise_for_status()
        #print("Programm " , num ," finished!")
    
    


if __name__ == "__main__":

    childs=[]
    start_time = time.time()

    for i in range(number_of_event):
        if i == 0:
            time.sleep(time_array[i])
        else: 
            time.sleep(time_array[i]-time_array[i-1])    
        pid = os.fork()
        if (pid == 0):
            sentrequest(i)
            #childs.remove(pid)
            os.kill(os.getpid(), 9)
    time_of_all_child_cr = time.time()-start_time
    for i in range(number_of_event):
        pid, exit_code = os.wait()
        
        #pid, exit_code = os.waitpid(-1, os.WNOHANG)
        #if pid == 0:
        #print("Waiting all the messages to be sent...")
          #  time.sleep(0.5)


        #threads.append(tmp)
        #os.system("gcloud pubsub topics publish myRunTopic --message %d"%i)    
        #tmp.start()

    print("All messages Sent \nTime : ", time.time()-start_time, "\nWaiting all threads to close...")

    #for thread in threads:
    #    thread.join()

    current_time = time.time()
    elapsed_time = current_time - start_time
    print("All children created in : ", time_of_all_child_cr)
    print("Finished iterating in: " + str(int(elapsed_time)) + " seconds")


