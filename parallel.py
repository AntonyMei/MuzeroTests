"""
This file is used for testing multiprocessing manager
"""

import multiprocessing as mp
import os
import platform
import random
import time
from multiprocessing import Lock
from multiprocessing.managers import BaseManager

import numpy as np

reader_count = 2    # 16
writer_count = 16    # 128
entry_size = 40*1024*1024


class Buffer:
    def __init__(self):
        self.val = 0

    def func(self, idx):
        if idx == 0:
            print("0", time.time())
            self.val = 10
            time.sleep(5)
            self.val = 0
        elif idx == 1:
            print("1", time.time())
            time.sleep(2)

            while not self.val == 0:
                print(f"not ready {self.val}")
                time.sleep(1)
        else:
            assert False


class BufferManager(BaseManager):
    pass


def start_server():
    # start server
    buffer = Buffer()
    BufferManager.register('get_buffer', callable=lambda: buffer)
    print('server registered')
    manager = BufferManager(address=('localhost', 12333), authkey=b'antony')
    server = manager.get_server()
    print('server started')
    server.serve_forever()


def get_buffer() -> Buffer:
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
    return m.get_buffer()


def writer_proc():
    buffer = get_buffer()
    buffer.func(0)


def reader_proc():
    buffer = get_buffer()
    buffer.func(1)


if __name__ == '__main__':

    # start process
    ctx = mp.get_context("spawn")
    server_proc = ctx.Process(target=start_server)
    server_proc.start()

    # start workers
    w_worker = ctx.Process(target=writer_proc)
    r_worker = ctx.Process(target=reader_proc)
    w_worker.start()
    r_worker.start()
    w_worker.join()
    r_worker.join()
    server_proc.terminate()
