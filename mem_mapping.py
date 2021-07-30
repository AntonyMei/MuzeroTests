import os
import platform
import queue
import time

import numpy as np
import multiprocessing as mp
from multiprocessing.managers import BaseManager


def main():
    shm = mp.shared_memory.SharedMemory(create=True, size=100, name="MyShm")
    w_array1 = np.array([1, 2, 3, 4, 5], [6, 7, 8, 9, 10])
    mm_array1 = np.ndarray(shape=w_array1.shape, dtype=w_array1.dtype, buffer=shm.buf)
    mm_array1[:] = w_array1[:]
    print(w_array1.dtype)
    print(bytes(shm.buf))
    shm.close()
    shm.unlink()


if __name__ == '__main__':
    main()
