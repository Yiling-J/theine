from threading import Lock
from typing import List, Callable
from random import getrandbits
from theine.utils import round_up_power_of_2
from contextlib import nullcontext
import os


# a striped lossy buffer
class StripedBuffer:

    def __init__(self, clear_buffer: Callable[[List[int]], None], nolock: bool = False):
        self.buffer_count = 4 * round_up_power_of_2(os.cpu_count() or 4)
        self.buffers: List[List[int]] = [[] for _ in range(self.buffer_count)]
        self.mutexes = [Lock() for _ in range(self.buffer_count)]
        self.buffer_size = 16
        self.clear_buffer = clear_buffer
        self.nolock = nolock

    def add(self, hash_value: int) -> None:
        index = getrandbits(32) & (self.buffer_count - 1)
        waiting: List[int] = []

        # skip if acquire lock failed, lossy1
        if self.nolock or self.mutexes[index].acquire(blocking=False):
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
            if not self.nolock:
                self.mutexes[index].release()

        # clear will hold global policy mutex
        if waiting:
            self.clear_buffer(waiting)
