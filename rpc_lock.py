"""
This file is used for testing multiprocessing manager with shared memory
"""
import os
import platform
import queue
import time

import numpy as np
import multiprocessing as mp
from multiprocessing.managers import BaseManager


class RPCProtocol:

    def __init__(self):
        """
        This class represents reader-writer RPC protocol. This implementation is a balanced
        """
        self.reader_counter_lock = mp.Lock()
        self.writer_lock = mp.Lock()
        self.readwrite_lock = mp.Lock()


def write_test(a, writer_lock):

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # enter
    #writer_lock.acquire()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    print("W")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # leave
    #writer_lock.release()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    print("E")


def main():
    """
    standard r/w procedure: see write_test, read_test and delete_test
    cleanup: call shutdown to unlink
    """

    # get multiprocessing context and start server for buffer
    if platform.system() == 'Windows':
        ctx = mp.get_context('spawn')
    elif platform.system() == 'Linux':
        ctx = mp.get_context('spawn')
    else:
        print('OS not supported')
        assert False
    protocol = RPCProtocol()
    lock = mp.Lock()
    workers = []

    # write -> element 0
    w_array1 = np.array([1, 2, 3, 4, 5])
    writer1 = ctx.Process(target=write_test, args=(w_array1, lock,))
    workers.append(writer1)
    writer1.start()

    # write -> element 1
    w_array2 = np.array([[6, 6, 6, 6, 6], [7, 7, 7, 7, 7]])
    writer2 = ctx.Process(target=write_test, args=(w_array2, lock,))
    workers.append(writer2)
    writer2.start()

    # write -> new element 1
    w_array3 = np.array([[3, 3, 3, 3, 3], [1, 1, 1, 1, 1]])
    writer3 = ctx.Process(target=write_test, args=(w_array3, lock,))
    workers.append(writer3)
    writer3.start()

    [worker.join() for worker in workers]
    print("Finished")


if __name__ == '__main__':
    main()
