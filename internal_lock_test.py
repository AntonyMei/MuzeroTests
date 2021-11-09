"""
This file is used for testing multiprocessing manager
"""

import multiprocessing as mp
import os
import platform
import queue
import random
import time
from multiprocessing import Lock
from multiprocessing.managers import BaseManager

import numpy as np

reader_count = 4    # 16
writer_count = 4    # 128
item_count = 10


class Buffer:
    def __init__(self, context):
        self.queue = queue.Queue()
        self.lock = context.Lock()

    def get_length(self):
        return len(self.list)

    def get(self):
        self.lock.acquire()
        try:
            item = self.queue.get(block=False)
        except queue.Empty:
            item = None
        self.lock.release()
        return item

    def set(self, item):
        self.lock.acquire()
        self.queue.put(item)
        self.lock.release()


class BufferManager(BaseManager):
    pass


def writer_proc(idx):
    """
    400MB each, call write 10 times
    """

    # connect to server and get buffer
    BufferManager.register('get_buffer')
    m = BufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # generate some data and feed into buffer
    for i in range(item_count):
        start = time.time()
        buffer.set(idx * item_count + i)
        end = time.time()
        print(f"[Writer {os.getpid()}] put {idx * item_count + i}, delay {int((end - start) * 1000)}ms, "
              f"at time {int(start * 10000) % 1000}.", flush=True)
        time.sleep(random.random() * 0.1)



def reader_proc(idx):

    # connect to server
    BufferManager.register('get_buffer')
    m = BufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # read some data
    for i in range(item_count):
        start = time.time()
        item = buffer.get()
        end = time.time()
        print(f"[Reader {os.getpid()}] get {item}, delay {int((end - start) * 1000)}ms, "
              f"at time {int(start * 10000) % 1000}.", flush=True)
        time.sleep(random.random() * 0.1)


def start_server(context):
    # start server
    buffer = Buffer(context)
    BufferManager.register('get_buffer', callable=lambda: buffer)
    print('server registered')
    manager = BufferManager(address=('localhost', 12333), authkey=b'antony')
    server = manager.get_server()
    print('server started')
    server.serve_forever()


if __name__ == '__main__':

    # fork must be used on linux and must not be used on windows
    if platform.system() == 'Windows':
        ctx = mp.get_context('spawn')
    elif platform.system() == 'Linux':
        ctx = mp.get_context('fork')
    else:
        print('OS not supported')
        assert False

    # start process
    server_proc = ctx.Process(target=start_server, args=(ctx, ))
    server_proc.start()

    # start workers
    w_workers = [ctx.Process(target=writer_proc, args=(i, )) for i in range(writer_count)]
    r_workers = [ctx.Process(target=reader_proc, args=(i, )) for i in range(reader_count)]
    start_time = time.time()
    [worker.start() for worker in w_workers]
    time.sleep(0.1)
    [worker.start() for worker in r_workers]
    [worker.join() for worker in w_workers]
    [worker.join() for worker in r_workers]
    end_time = time.time()
    print("Total time", end_time - start_time)
    server_proc.terminate()
