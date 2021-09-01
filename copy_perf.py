"""
This file is used for testing memory speed
"""

import os
import random
import time
import numpy as np
import math
import platform
import copy

entry_size = 40*1024*1024
repeat = 100


if __name__ == '__main__':

    # do some work
    game = np.ascontiguousarray(np.ones(entry_size))
    # game = bytearray(entry_size)

    # write
    timer = 0
    for i in range(repeat):
        game_new = np.ascontiguousarray(np.empty(entry_size))
        # game_new = bytearray(entry_size)
        s_time = time.time()
        # assert game_new.flags['C_CONTIGUOUS']
        # game_new.ravel()[:len(game)] = game
        # game_new = np.copy(game)
        game_new[:] = game[:]
        # game_new = np.zeros(entry_size)
        # game_new = copy.deepcopy(game)
        e_time = time.time()
        timer += (e_time - s_time)
        print(f"iter {i}: time {e_time - s_time}")
    print(f"avg write time: {timer/repeat}")
    print(f"avg speed: {(entry_size * 8 / 1024 / 1024 / 1024) / (timer / repeat)} GB/s")
