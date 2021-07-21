"""
This file is used for testing shared memory (speed)
"""

import os
import multiprocessing as mp
import random
import time
from multiprocessing.managers import BaseManager
from multiprocessing import Lock
from multiprocessing import shared_memory

import numpy
import numpy as np
import math
import platform
import queue


class MyBuffer:
    def __init__(self, memory_size, block_size):
        """
        memory_size must be integer times of block_size
        """
        # pretend there are some other data
        self.data1 = 567

        # prepare shared memory, these should be updated each write
        self.shm = shared_memory.SharedMemory(create=True, size=memory_size)
        self.shm_name = self.shm.name

        # initialize configurations
        self.block_size = block_size
        self.write_lock = Lock()
        self.dtype_list = []
        self.shape_list = []
        self.entry_head_list = []   # it contains which block the entry is mapped to
        self.free_block_idx = queue.Queue(maxsize=int(memory_size/block_size))  # it contains the free blocks
        for i in range(int(memory_size/block_size)):
            self.free_block_idx.put(i)

    def get_block_offset(self, block_idx):
        return self.block_size * block_idx

    def get_shm_name(self):
        return self.shm_name

    def read_item_config(self, idx):
        return [self.dtype_list[idx], self.shape_list[idx], self.entry_head_list[idx]]

    def append_item_config(self, shape, dtype):
        self.write_lock.acquire()
        try:
            block_idx = self.free_block_idx.get(block=False)
            self.entry_head_list.append(block_idx)
            self.dtype_list.append(dtype)
            self.shape_list.append(shape)
        except queue.Empty:
            print("Buffer out of memory")
            assert False
        self.write_lock.release()
        return block_idx

    def delete_item_config(self, idx):
        self.write_lock.acquire()
        try:
            block_idx = self.entry_head_list.pop(idx)
            self.free_block_idx.put(block_idx, block=False)
            self.dtype_list.pop(idx)
            self.shape_list.pop(idx)
        except queue.Full:
            print("Full buffer, there might be some duplicate")
            assert False
        self.write_lock.release()

    def shutdown(self):
        self.shm.unlink()

    def get_free_blocks(self):
        return self.free_block_idx


def main():
    """
    standard r/w procedure:

    cleanup: call shutdown to unlink
    """

    # config
    plt = platform.platform()
    if "Windows" in plt:
        shm_size = 4096 * 1024 * 1024
        block_size = 512 * 1024 * 1024
    elif "Linux" in plt:
        shm_size = 100 * 1024 * 1024 * 1024
        block_size = 8 * 1024 * 1024
    else:
        print("OS not supported")
        assert False

    buffer = MyBuffer(shm_size, block_size)
    shm_name = buffer.get_shm_name()

    # write1
    shm = shared_memory.SharedMemory(name=shm_name)
    w_array1 = np.ones(40 * 1024 * 1024)
    block_idx1 = buffer.append_item_config(w_array1.shape, w_array1.dtype)
    block_offset1 = buffer.get_block_offset(block_idx1)
    s_time = time.time()
    mm_array1 = np.ndarray(shape=w_array1.shape, dtype=w_array1.dtype, buffer=shm.buf, offset=block_offset1)
    m_time = time.time()
    mm_array1[:] = w_array1[:]
    e_time = time.time()
    shm.close()
    print("open shm time", m_time - s_time)
    print("write time", e_time - m_time)

    # write2
    shm = shared_memory.SharedMemory(name=shm_name)
    w_array2 = np.ones(40 * 1024 * 1024)
    block_idx2 = buffer.append_item_config(w_array2.shape, w_array2.dtype)
    block_offset2 = buffer.get_block_offset(block_idx2)
    s_time = time.time()
    mm_array2 = np.ndarray(shape=w_array2.shape, dtype=w_array2.dtype, buffer=shm.buf, offset=block_offset2)
    m_time = time.time()
    mm_array2[:] = w_array2[:]
    e_time = time.time()
    shm.close()
    print("open shm time", m_time - s_time)
    print("write time", e_time - m_time)

    # # read2
    # shm = shared_memory.SharedMemory(name=shm_name)
    # r2_dtype, r2_shape, r2_block_idx = buffer.read_item_config(1)
    # r2_offset = buffer.get_block_offset(r2_block_idx)
    # r2_array = np.ndarray(shape=r2_shape, dtype=r2_dtype, buffer=shm.buf, offset=r2_offset)
    # print("Read block 2\n", r2_array)
    # shm.close()
    #
    # # read2
    # shm = shared_memory.SharedMemory(name=shm_name)
    # r1_dtype, r1_shape, r1_block_idx = buffer.read_item_config(0)
    # r1_offset = buffer.get_block_offset(r1_block_idx)
    # r1_array = np.ndarray(shape=r1_shape, dtype=r1_dtype, buffer=shm.buf, offset=r1_offset)
    # print("Read block 1\n", r1_array)
    # shm.close()
    #
    # # delete
    # shm = shared_memory.SharedMemory(name=shm_name)
    # buffer.delete_item_config(0)
    # free_blocks = []
    # free_blocks_queue = buffer.get_free_blocks()
    # print("Delete block 1, free blocks\n", list(free_blocks_queue.queue))
    # shm.close()
    #
    # # read3
    # shm = shared_memory.SharedMemory(name=shm_name)
    # r3_dtype, r3_shape, r3_block_idx = buffer.read_item_config(0)
    # r3_offset = buffer.get_block_offset(r3_block_idx)
    # r3_array = np.ndarray(shape=r3_shape, dtype=r3_dtype, buffer=shm.buf, offset=r3_offset)
    # print("Read block 2\n", r3_array)
    # shm.close()

    # clean up
    buffer.shutdown()


if __name__ == '__main__':
    main()
