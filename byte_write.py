import os
import platform
import queue
import sys
import time
import struct

import numpy as np
import multiprocessing as mp
from multiprocessing.managers import BaseManager


def main():
    """
    Results: shape is a tuple, write must match (which we will not use, sine we only need to support single
    element write) read don't need to match, causing extra 0s being read
    """

    # prepare buffer
    shm = mp.shared_memory.SharedMemory(create=True, size=32, name="MyShm")

    # format
    # print(int.from_bytes(bytes=bytes(v[0:4]), byteorder=sys.byteorder, signed=False))
    # print((1000).to_bytes(4, byteorder=sys.byteorder))
    # write (l inclusive, r exclusive)
    # TODO: float, int has length 8 on linux. Test speed
    if platform.system() == "Windows":
        # print(struct.pack("d", 123.5))
        shm.buf[0:4] = (1000).to_bytes(4, byteorder=sys.byteorder, signed=True)
        shm.buf[4:8] = (-15).to_bytes(4, byteorder=sys.byteorder, signed=True)
        shm.buf[0:8] = struct.pack("d", 123.5)
        mm_array1 = np.ndarray(shape=(4, ), dtype=float, buffer=shm.buf)
        print(mm_array1)
    elif platform.system() == "Linux":

        s_time = time.time()
        for i in range(20 * 1024 * 1024):
            shm.buf[0:8] = b"abcdefga"
        e_time = time.time()
        print(f"Loop time: {e_time - s_time}")

        s_time = time.time()
        for i in range(20 * 1024 * 1024):
            shm.buf[0:8] = i.to_bytes(8, byteorder=sys.byteorder, signed=True)
        e_time = time.time()
        print(f"Int time: {e_time - s_time}")

        s_time = time.time()
        for i in range(20 * 1024 * 1024):
            shm.buf[0:8] = struct.pack("d", 123.5)
        e_time = time.time()
        print(f"Float time: {e_time - s_time}")

        # shm.buf[0:8] = struct.pack("d", 123.5)
        mm_array1 = np.ndarray(shape=(4, ), dtype=float, buffer=shm.buf)
        print(mm_array1)
    else:
        print("OS not supported")

    # clean up
    shm.close()
    shm.unlink()


if __name__ == '__main__':
    main()
