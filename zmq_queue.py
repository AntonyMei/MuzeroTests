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


if __name__ == '__main__':

    # do some work
    game = np.ones(entry_size)

    # write
    s_time = time.time()
    game_new = game[:]
    e_time = time.time()
    print("write time", e_time - s_time)
