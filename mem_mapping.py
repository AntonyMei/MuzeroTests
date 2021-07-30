import os
import platform
import queue
import time

import numpy as np
import multiprocessing as mp
from multiprocessing.managers import BaseManager


def main():
    """
    Results: shape is a tuple, write must match (which we will not use, sine we only need to support single
    element write) read don't need to match, causing extra 0s being read
    """
    shm = mp.shared_memory.SharedMemory(create=True, size=100, name="MyShm")
    # w_array1 = np.array([[[1, 2], [3, 4], [5, 6]], [[7, 8], [9, 10], [11, 12]]])
    w_array1 = np.array([1, 2, 3, 4])
    mm_array1 = np.ndarray(shape=(4, ), dtype=w_array1.dtype, buffer=shm.buf)
    mm_array1[:] = w_array1[:]
    mm_array1 = np.ndarray(shape=(5, ), dtype=w_array1.dtype, buffer=shm.buf)
    print(mm_array1)
    print(w_array1.dtype)
    print(w_array1.shape)
    print(bytes(shm.buf))
    shm.close()
    shm.unlink()


if __name__ == '__main__':
    main()
