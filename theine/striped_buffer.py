from threading import Lock
from typing import List, Callable
from random import getrandbits
from theine.utils import round_up_power_of_2
import os


# a striped lossy buffer
class StripedBuffer:

    def __init__(self, clear_buffer: Callable):
        self.buffer_count = 4 * round_up_power_of_2(os.cpu_count())
        self.buffers: List[List[int]] = [[] for _ in range(self.buffer_count)]
        self.mutexes: List[Lock] = [Lock() for _ in range(self.buffer_count)]
        self.buffer_size = 16
        self.clear_buffer = clear_buffer

    def add(self, hash_value: int):
        index = getrandbits(32) & (self.buffer_count - 1)
        waiting = []

        # skip if acquire lock failed, lossy1
        if self.mutexes[index].acquire(blocking=False):
            bs = len(self.buffers[index])
            # skip if buffer items count >= buffer size, lossy2
            if bs >= self.buffer_size:
                self.mutexes[index].release()
                return
            self.buffers[index].append(hash_value)
            if bs == self.buffer_size - 1:
                # move entries from current buffer to waiting buffer, then clear current buffer,
                # so current buffer can continue accept new entry when waiting buffer is processing
                waiting = self.buffers[index]
                self.buffers[index] = []
            self.mutexes[index].release()

        # clear will hold global policy mutex
        if waiting:
            self.clear_buffer(waiting)