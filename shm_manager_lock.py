"""
This file is used for testing multiprocessing manager with shared memory
"""
import multiprocessing
import multiprocessing as mp
import os
import platform
import queue
import time
from multiprocessing import shared_memory
from multiprocessing.managers import BaseManager
import warnings

import numpy as np


class MyBuffer:

    def __init__(self, memory_size, block_size):
        """
        memory_size must be integer times of block_size, otherwise shared memory space will not be
        fully utilized (with internal fraction at most the size of one block)

        MyBuffer is process safe when instantiated as a Python proxy object under a multiprocessing
        manager. Therefore, no lock is needed for its data. However, the shared memory space per se
        is NOT process safe. It should be accessed under a rpc protection protocol. (For example,
        reader-writer model)

        NOTE: MyBuffer is not safe when used in multiple threads under the same process. Never use
        it that way.
        """
        # prepare shared memory and related configurations
        self.shm = shared_memory.SharedMemory(create=True, size=memory_size, name="MyShm")
        self.shm_name = self.shm.name
        self.block_size = block_size

        # initialize data description storage
        self.dtype_list = []
        self.shape_list = []
        self.entry_head_list = []   # it contains which block the entry is mapped to
        self.free_block_idx = queue.Queue(maxsize=int(memory_size/block_size))  # it contains the free blocks
        for i in range(int(memory_size/block_size)):
            self.free_block_idx.put(i)

        # reader-writer protocol
        self.reader_count = 0

        # Used for safe exit on linux
        self.remaining_shm_link_count = 0
        self.exit_flag = False

    # buffer

    def get_block_count(self):
        return len(self.entry_head_list)

    def is_buffer_full(self):
        return len(list(self.free_block_idx.queue)) == 0

    def is_buffer_empty(self):
        return len(self.entry_head_list) == 0

    def get_block_offset(self, block_idx):
        return self.block_size * block_idx

    def get_shm_name(self):
        return self.shm_name

    def read_item_config(self, idx):
        return [self.dtype_list[idx], self.shape_list[idx], self.entry_head_list[idx]]

    def append_item_config(self, shape, dtype):
        try:
            block_idx = self.free_block_idx.get(block=False)
            self.entry_head_list.append(block_idx)
            self.dtype_list.append(dtype)
            self.shape_list.append(shape)
        except queue.Empty:
            print("Buffer out of memory")
            assert False
        return block_idx

    def delete_item_config(self, idx):
        try:
            block_idx = self.entry_head_list.pop(idx)
            self.free_block_idx.put(block_idx, block=False)
            self.dtype_list.pop(idx)
            self.shape_list.pop(idx)
        except queue.Full:
            print("Full buffer, there might be some duplicate")
            assert False

    def shutdown_buffer(self):
        self.shm.close()
        self.shm.unlink()

    def get_free_blocks_list(self):
        """
        Note that return self.free_block_idx directly will cause pickle error and won't work
        """
        return list(self.free_block_idx.queue)

    # rpc protocol

    def get_reader_count(self):
        return self.reader_count

    def increase_reader(self):
        self.reader_count += 1

    def decrease_reader(self):
        self.reader_count -= 1

    # linux sync
    def register_worker(self):
        self.remaining_shm_link_count += 1

    def unregister_worker(self):
        self.remaining_shm_link_count -= 1
        assert self.remaining_shm_link_count >= 0

    def get_remaining_worker(self):
        return self.remaining_shm_link_count

    def set_exit_flag(self):
        self.exit_flag = True

    def get_exit_flag(self):
        return self.exit_flag


class RPCProtocol:

    def __init__(self):
        """
        This class represents reader-writer RPC protocol. This implementation is a balanced
        """
        self.reader_counter_lock = multiprocessing.Lock()
        self.writer_lock = multiprocessing.Lock()
        self.readwrite_lock = multiprocessing.Lock()


class MyBufferManager(BaseManager):
    pass


def start_server():
    """
    Start server, run this in a separate process
    """
    buffer = MyBuffer(memory_size=2 * 1024 * 1024, block_size=1 * 1024 * 1024)
    MyBufferManager.register('get_buffer', callable=lambda: buffer)
    print('[Server] server registered')
    manager = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    server = manager.get_server()
    print('[Server] server started')
    server.serve_forever()
    print("[Server] ERROR server drops************************************************")


def write_test(w_array, protocol: RPCProtocol):
    """
    For tests call this in a separate process
    Write uses shm, therefore, registration is required
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
            print('[Writer] Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # enter
    protocol.writer_lock.acquire()
    protocol.readwrite_lock.acquire()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # write
    if buffer.is_buffer_full():
        print(f"[Writer] Process {os.getpid()} Buffer full")
    else:
        try:
            shm = shared_memory.SharedMemory(name=buffer.get_shm_name())
            block_idx = buffer.append_item_config(w_array.shape, w_array.dtype)
            block_offset = buffer.get_block_offset(block_idx)
            mm_array = np.ndarray(shape=w_array.shape, dtype=w_array.dtype, buffer=shm.buf, offset=block_offset)
            mm_array[:] = w_array[:]
            shm.close()
            print(f"[Writer] Process {os.getpid()} Write block\n {w_array}")
        except FileNotFoundError:
            print(f"[Writer] Unable to open shm. {os.getpid()}")
            time.sleep(1)

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # leave
    protocol.readwrite_lock.release()
    protocol.writer_lock.release()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # signal main process that this worker has finished and wait for exit permission
    buffer.unregister_worker()
    while not buffer.get_exit_flag():
        time.sleep(1)
        print(f"[Writer] Process {os.getpid()} waiting for exit permission")
    print(f"[Writer] Process {os.getpid()} exit")


def read_test(idx, protocol: RPCProtocol):
    """
    For tests call this in a separate process, idx start from 0
    Read uses shm, therefore, registration is required

    _TODO: add reader-writer lock to avoid accidents
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
            print('[Reader] Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # enter
    protocol.writer_lock.acquire()
    protocol.reader_counter_lock.acquire()
    if buffer.get_reader_count() == 0:
        protocol.readwrite_lock.acquire()
    buffer.increase_reader()
    protocol.reader_counter_lock.release()
    protocol.writer_lock.release()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # read
    if idx >= buffer.get_block_count():
        print(f"[Reader] Process {os.getpid()} Data required not available (deleted or index out of range)")
    else:
        try:
            shm = shared_memory.SharedMemory(name=buffer.get_shm_name())
            r_dtype, r_shape, r_block_idx = buffer.read_item_config(idx)
            r_offset = buffer.get_block_offset(r_block_idx)
            r_array = np.ndarray(shape=r_shape, dtype=r_dtype, buffer=shm.buf, offset=r_offset)
            print(f"[Reader] Process {os.getpid()} Read block {idx}\n {r_array}")
            shm.close()
        except FileNotFoundError:
            print(f"[Reader] Unable to open shm. {os.getpid()}")
            time.sleep(1)

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # leave
    protocol.reader_counter_lock.acquire()
    buffer.decrease_reader()
    if buffer.get_reader_count() == 0:
        protocol.readwrite_lock.release()
    protocol.reader_counter_lock.release()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # signal main process that this worker has finished and wait for exit permission
    buffer.unregister_worker()
    while not buffer.get_exit_flag():
        time.sleep(1)
        print(f"[Reader] Process {os.getpid()} waiting for exit permission")
    print(f"[Reader] Process {os.getpid()} exit")


def delete_test(idx, protocol: RPCProtocol):
    """
    For tests call this in a separate process, idx start from 0
    Delete doesn't need shm, therefore, no need for registration (lazy delete)

    _TODO: if write again will cause trouble? (No)
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
            print('[Deleter] Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # enter
    protocol.writer_lock.acquire()
    protocol.readwrite_lock.acquire()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # delete
    if idx >= buffer.get_block_count():
        print(f"[Deleter] Process {os.getpid()} Data has already been deleted")
    else:
        buffer.delete_item_config(idx)
        print(f"[Deleter] Process {os.getpid()} Delete block {idx}, free blocks\n {buffer.get_free_blocks_list()}")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # leave
    protocol.readwrite_lock.release()
    protocol.writer_lock.release()
    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


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
            print('[Unlink] Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # buffer
    buffer.shutdown_buffer()


def main():
    """
    standard r/w procedure: see write_test, read_test and delete_test
    cleanup: call shutdown to unlink
    """

    # get multiprocessing context and start server for buffer
    if platform.system() == 'Windows':
        ctx = mp.get_context('spawn')
    elif platform.system() == 'Linux':
        ctx = mp.get_context('fork')
    else:
        print('OS not supported')
        assert False
    protocol = RPCProtocol()
    buffer_server = ctx.Process(target=start_server)
    buffer_server.start()
    workers = []

    # get buffer for registration
    MyBufferManager.register('get_buffer')
    m = MyBufferManager(address=('localhost', 12333), authkey=b'antony')
    connected = False
    while not connected:
        try:
            m.connect()
            connected = True
        except ConnectionRefusedError:
            print('[Main] Server not ready, retrying in 1 sec.', os.getpid())
            time.sleep(1)
    buffer = m.get_buffer()

    # write -> element 0
    w_array1 = np.array([1, 2, 3, 4, 5])
    writer1 = ctx.Process(target=write_test, args=(w_array1, protocol, ))
    workers.append(writer1)
    buffer.register_worker()
    writer1.start()

    # write -> element 1
    w_array2 = np.array([[6, 6, 6, 6, 6], [7, 7, 7, 7, 7]])
    writer2 = ctx.Process(target=write_test, args=(w_array2, protocol, ))
    workers.append(writer2)
    buffer.register_worker()
    writer2.start()

    # read -> element 1
    reader1 = ctx.Process(target=read_test, args=(1, protocol, ))
    workers.append(reader1)
    buffer.register_worker()
    reader1.start()

    # read -> element 0
    reader2 = ctx.Process(target=read_test, args=(0, protocol, ))
    workers.append(reader2)
    buffer.register_worker()
    reader2.start()

    # delete element 0 (element 1 -> new element 0)
    # delete does not open shm, no need for registration
    deleter = ctx.Process(target=delete_test, args=(0, protocol, ))
    workers.append(deleter)
    deleter.start()

    # read -> new element 0
    reader3 = ctx.Process(target=read_test, args=(0, protocol, ))
    workers.append(reader3)
    buffer.register_worker()
    reader3.start()

    # write -> new element 1
    w_array3 = np.array([[3, 3, 3, 3, 3], [1, 1, 1, 1, 1]])
    writer3 = ctx.Process(target=write_test, args=(w_array3, protocol, ))
    workers.append(writer3)
    buffer.register_worker()
    writer3.start()

    # read -> new element 1
    reader4 = ctx.Process(target=read_test, args=(1, protocol, ))
    workers.append(reader4)
    buffer.register_worker()
    reader4.start()

    # clean up
    print(f"[Main] All workers have been launched")
    while not buffer.get_remaining_worker() == 0:
        time.sleep(1)
        print(f"[Main] Waiting for workers to complete, remaining workers {buffer.get_remaining_worker()}")
    print(f"[Main] All workers have completed, unlink shm and signal exit")
    unlink_shm()  # make sure shared memory is unlinked after termination of all workers
    buffer.set_exit_flag()
    [worker.join() for worker in workers]
    print("********************************************************************************")
    buffer_server.terminate()


if __name__ == '__main__':
    main()
