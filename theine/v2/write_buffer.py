from threading import Lock
from typing import List, Tuple, Callable


BufferSize = 16


class WriteBuffer:
    def __init__(self, clear_buffer: Callable[[List[Tuple[int, int]]], None]):
        # tuple: (key_hash, ttl)
        self.buffer: List[Tuple[int, int]] = []
        self.waiting: List[Tuple[int, int]] = []
        self.processing = False
        self.mutex = Lock()
        self.swap_mutex = Lock()
        self.clear_buffer = clear_buffer

    def add(self, key_hash: int, ttl: int) -> None:
        should_clear = False
        with self.mutex:
            self.buffer.append((key_hash, ttl))
            if len(self.buffer) == BufferSize:
                should_clear = True
                with self.swap_mutex:
                    self.waiting = self.buffer
                    self.buffer = self.buffer[:0]

        if should_clear:
            # use spinlock to trigger write buffer processing as fast as possible
            while True:
                if self.swap_mutex.acquire(blocking=False):
                    self.clear_buffer(self.waiting)
                    self.swap_mutex.release()
                    break
