"""
This file is used for testing memory speed
"""

import os
import multiprocessing as mp
import random
import time
from multiprocessing.managers import BaseManager
from multiprocessing import Lock
import numpy as np
import math
import platform
import copy

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


if __name__ == '__main__':

    buffer = Buffer()
    # do some work
    game = np.ones(entry_size)
    for i in range(int(0.01 * entry_size)):
        game[random.randint(0, entry_size - 1)] = random.random()

    # write
    s_time = time.time()
    buffer.set(copy.deepcopy(game))
    e_time = time.time()
    print("write time", e_time - s_time)

    # read
    s_time = time.time()
    array = copy.deepcopy(buffer.get(0))
    e_time = time.time()
    print("read time", e_time - s_time)
