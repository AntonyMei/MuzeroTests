"""
This file is used for testing python multiprocessing
"""

import os
import multiprocessing as mp


class TestObj:
    def __init__(self, idx):
        self.idx = idx

    def display(self):
        print(self.idx, os.getpid())


def run_test_obj(idx):
    test_obj = TestObj(idx)
    test_obj.display()


if __name__ == '__main__':
    # fork only works on linux (unix with posix)
    ctx = mp.get_context('spawn')
    p_list = [ctx.Process(target=run_test_obj, args=(i,)) for i in range(10)]
    [proc.start() for proc in p_list]
    [proc.join() for proc in p_list]
    print("Finished")
