import os
from os import fork
from multiprocessing import Lock

# Lock is acquired in the parent process:
lock = Lock()
lock.acquire()
print(f"{os.getpid()} acquires lock")

if fork() == 0:
    # In the child process, try to grab the lock:
    print(f"{os.getpid()}Acquiring lock...")
    lock.acquire()
    print("Lock acquired! (This code will never run)")
    lock.release()
else:
    lock.release()
