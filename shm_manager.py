"""
This file is used for testing multiprocessing manager with shared memory
"""

import multiprocessing as mp
import os
import platform
import queue
import time
from multiprocessing import Lock
from multiprocessing import shared_memory
from multiprocessing.managers import BaseManager

import numpy as np


class MyBuffer:
    def __init__(self, memory_size, block_size):
        """
        memory_size must be integer times of block_size, otherwise shared memory space will not be
        fully utilized (with internal fraction at most the size of one block)
        """
        # prepare shared memory, these should be updated each write
        self.shm = shared_memory.SharedMemory(create=True, size=memory_size)
        self.shm_name = self.shm.name

        # initialize configurations
        self.block_size = block_size
        self.write_lock = Lock()    # used for internal protection
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
        """
        NEVER call this remotely, this function is only meant for internal use,
        call get_free_block_idx instead
        TODO: protect this function
        """
        return self.free_block_idx

    def get_free_blocks_list(self):
        return list(self.free_block_idx.queue)


class MyBufferManager(BaseManager):
    pass


def start_server():
    """
    Start server, run this in a separate process
    """
    buffer = MyBuffer(memory_size=2048 * 1024 * 1024, block_size=256 * 1024 * 1024)
    MyBufferManager.register('get_buffer', callable=lambda: buffer)
    print('server registered')
    manager = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    server = manager.get_server()
    print('server started')
    server.serve_forever()


def write_test(w_array, buffer_lock: Lock()):
    """
    For tests call this in a separate process
    """

    # connect to server and get buffer
    MyBufferManager.register('get_buffer')
    m = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # write
    # TODO: beware of deadlock of buffer_lock and lock in class MyBuffer
    buffer_lock.acquire()
    shm = shared_memory.SharedMemory(name=buffer.get_shm_name())
    block_idx = buffer.append_item_config(w_array.shape, w_array.dtype)
    block_offset = buffer.get_block_offset(block_idx)
    mm_array = np.ndarray(shape=w_array.shape, dtype=w_array.dtype, buffer=shm.buf, offset=block_offset)
    mm_array[:] = w_array[:]
    shm.close()
    print(f"[Writer] Process {os.getpid()} Write block\n {w_array}")
    buffer_lock.release()


def read_test(idx):
    """
    For tests call this in a separate process, idx start from 0
    TODO: add reader-writer lock to avoid accidents
    """

    # connect to server and get buffer
    MyBufferManager.register('get_buffer')
    m = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # read
    shm = shared_memory.SharedMemory(name=buffer.get_shm_name())
    r_dtype, r_shape, r_block_idx = buffer.read_item_config(idx)
    r_offset = buffer.get_block_offset(r_block_idx)
    r_array = np.ndarray(shape=r_shape, dtype=r_dtype, buffer=shm.buf, offset=r_offset)
    print(f"[Reader] Process {os.getpid()} Read block {idx}\n {r_array}")
    shm.close()


def delete_test(idx):
    """
        For tests call this in a separate process, idx start from 0
        TODO: if write again will cause trouble?
        """

    # connect to server and get buffer
    MyBufferManager.register('get_buffer')
    m = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # delete
    shm = shared_memory.SharedMemory(name=buffer.get_shm_name())
    buffer.delete_item_config(idx)
    print(f"[Delete] Delete block {idx}, free blocks\n {buffer.get_free_blocks_list()}")
    shm.close()


def unlink_shm():
    # connect to server and get buffer
    MyBufferManager.register('get_buffer')
    m = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # buffer
    buffer.shutdown()


def main():
    """
    standard r/w procedure:

    cleanup: call shutdown to unlink
    """

    # get multiprocessing context and start server
    if platform.system() == 'Windows':
        ctx = mp.get_context('spawn')
    elif platform.system() == 'Linux':
        ctx = mp.get_context('fork')
    else:
        print('OS not supported')
        assert False
    buffer_lock = Lock()
    buffer_server = ctx.Process(target=start_server)
    buffer_server.start()

    # write -> 0
    w_array0 = np.array([1, 2, 3, 4, 5])
    writer0 = ctx.Process(target=write_test, args=(w_array0, buffer_lock))
    writer0.start()

    # write -> 1
    w_array1 = np.array([[6, 6, 6, 6, 6], [7, 7, 7, 7, 7]])
    writer1 = ctx.Process(target=write_test, args=(w_array1, buffer_lock))
    writer1.start()

    # wait until finished
    writer0.join()
    writer1.join()
    print("[Main] Write two objects")

    # read -> 1
    reader1 = ctx.Process(target=read_test, args=(1, ))
    reader1.start()

    # read -> 0
    reader2 = ctx.Process(target=read_test, args=(0,))
    reader2.start()

    # wait until finished
    reader1.join()
    reader2.join()
    print("[Main] Read two objects")

    # delete
    deleter = ctx.Process(target=delete_test, args=(0,))
    deleter.start()
    deleter.join()
    print("[Main] Delete block 0")

    # read -> 0
    reader3 = ctx.Process(target=read_test, args=(0,))
    reader3.start()
    reader3.join()
    print("[Main] Read after delete")

    # clean up
    unlink_shm()
    buffer_server.terminate()


if __name__ == '__main__':
    main()
