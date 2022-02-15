import sys
import time
import pickle
import numpy as np
import zlib


def main():
    # generate data
    start = time.time()
    data = np.ones((1024 ** 2))
    end = time.time()
    print(f"Data creation time {end - start}")

    # serialize
    start = time.time()
    stream = pickle.dumps(data)
    end = time.time()
    print(f"Serialize time {end - start}, size {sys.getsizeof(stream)}")

    # compress
    start = time.time()
    compressed_stream = zlib.compress(stream)
    end = time.time()
    print(f"Serialize time {end - start}, size {sys.getsizeof(compressed_stream)}")


if __name__ == '__main__':
    main()
