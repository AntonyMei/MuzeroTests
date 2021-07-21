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
        self.list = []

    def get_length(self):
        return len(self.list)

    def get(self, idx):
        return self.list[idx]

    def set(self, game):
        self.list.append(game)


class BufferManager(BaseManager):
    pass


def writer_proc(lock: Lock(), idx):
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
    obj = m.get_buffer()

    # generate some data and feed into buffer
    for iteration in range(10):
        # do some work
        game = np.ones(entry_size)
        for i in range(int(0.01 * entry_size)):
            game[random.randint(0, entry_size - 1)] = random.random()
        # set into buffer
        print('writer start iter:', iteration, 'writer id', idx, os.getpid())
        lock.acquire()
        s_time = time.time()
        obj.set(game)
        e_time = time.time()
        lock.release()
        print('writer finished iter:', iteration, 'writer id', idx, os.getpid())
        print('writer', e_time - s_time)


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
    obj = m.get_buffer()

    # read some data
    for iteration in range(10):
        print('reader start iter:', iteration, 'reader id', idx, os.getpid())
        length = obj.get_length()
        pos = random.randint(0, length)
        s_time = time.time()
        array = obj.get(pos - 1)
        e_time = time.time()
        print(np.sum(array))
        print('reader finished iter:', iteration, 'reader id', idx, os.getpid())
        print('reader', e_time - s_time)


def start_server():
    # start server
    buffer = Buffer()
    game = np.ones(entry_size)
    buffer.set(game)
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
    buffer_lock = Lock()

    # start process
    server_proc = ctx.Process(target=start_server)
    server_proc.start()

    # start workers
    w_workers = [ctx.Process(target=writer_proc, args=(buffer_lock, i, )) for i in range(writer_count)]
    r_workers = [ctx.Process(target=reader_proc, args=(i, )) for i in range(reader_count)]
    start_time = time.time()
    [worker.start() for worker in w_workers]
    [worker.start() for worker in r_workers]
    [worker.join() for worker in w_workers]
    [worker.join() for worker in r_workers]
    end_time = time.time()
    print("Total time", end_time - start_time)
    server_proc.terminate()
