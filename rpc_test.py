from buffer.buffer_base import Buffer
import multiprocessing
import numpy as np
import time
import os


class SharedMemoryBuffer(Buffer):
    def __init__(self, max_size, max_hit):
        super().__init__(max_size)

        # config
        self.max_hit = max_hit

        # Data storage
        self.storage = []
        self.remaining_hits = []
        self.size = 0
        self.stored = 0

        # Control signals used for a balanced Reader-Writer model
        self.reader_counter = 0
        self.reader_counter_lock = multiprocessing.Lock()
        self.writer_lock = multiprocessing.Lock()
        self.readwrite_lock = multiprocessing.Lock()
        self.reader_lock = multiprocessing.Lock()

    # Writer
    def Put(self, segs):

        # Init
        # Wait for other writers and reader queue
        self.writer_lock.acquire()
        self.readwrite_lock.acquire()

        # Writer starts here

        # Append
        original_length = len(self.storage)
        self.storage += segs
        for i in range(len(segs)):
            self.remaining_hits.append(self.max_hit)
        self.size += len(segs)

        # Truncate
        if self.size > self.max_size:
            self.storage = self.storage[-self.max_size:]
            self.remaining_hits = self.remaining_hits[-self.max_size:]
            self.size = self.max_size
        self.stored += len(segs)
        assert (len(self.storage) == len(self.remaining_hits))

        # Debug
        print(f"writer: before {str(original_length)}, "
              f"write {str(len(segs))}, after {str(len(self.storage))}\n")

        # Clear
        self.readwrite_lock.release()
        self.writer_lock.release()

    # Reader
    def Get(self, batch_size):

        # Init
        # Wait with the writers until chosen
        self.writer_lock.acquire()
        # Critical region: reader_counter
        self.reader_counter_lock.acquire()
        if self.reader_counter == 0:
            self.readwrite_lock.acquire()
        self.reader_counter += 1
        self.reader_counter_lock.release()
        # Leaving critical region: reader_counter
        self.writer_lock.release()

        # Reader starts here
        samples = []
        deleted = 0
        original_length = len(self.storage)

        # Critical region: sampling from buffer
        self.reader_lock.acquire()

        # Check if buffer have enough resource
        if self.size < batch_size:

            # Not enough resources
            pass

        else:

            # Sampling
            indexes = np.random.choice(self.size, batch_size, replace=False)
            indexes = sorted(indexes, reverse=True)
            for i in indexes:
                if self.remaining_hits[i] == 1:
                    self.remaining_hits.pop(i)
                    samples.append(self.storage.pop(i))
                    deleted += 1
                else:
                    self.remaining_hits[i] -= 1
                    samples.append(self.storage[i])
            assert (len(self.storage) == len(self.remaining_hits))
            self.size = len(self.storage)

        # Debug
        print(f"reader: before {str(original_length)}, "
              f"read delete {str(deleted)}, after {str(len(self.storage))}\n")

        self.reader_lock.release()
        # Leaving critical region: sampling from buffer

        # Clear
        # Critical region: reader_counter
        self.reader_counter_lock.acquire()
        self.reader_counter -= 1
        if self.reader_counter == 0:
            self.readwrite_lock.release()
        self.reader_counter_lock.release()
        # Leaving critical region: reader_counter

        # Return
        return samples

    def Info(self):
        return round(float(self.size)/self.max_size, 2), self.stored
