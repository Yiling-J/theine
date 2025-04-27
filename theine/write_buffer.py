from threading import Lock
from typing import List, Tuple, Callable


BufferSize = 16


class WriteBuffer:
    def __init__(
        self,
        send_to_core: Callable[[List[Tuple[int, int]]], List[int]],
        remove_keys: Callable[[List[int]], None],
        core_mutex: Lock,
        nolock: bool = False,
    ):
        # tuple: (key_hash, ttl)
        self.buffer: List[Tuple[int, int]] = [(0, 0) for _ in range(BufferSize)]
        self.mutex = Lock()
        self.core_mutex = core_mutex
        self.send_to_core = send_to_core
        self.remove_keys = remove_keys
        self.tail = 0
        self.nolock = nolock

    def add(self, key_hash: int, ttl: int) -> None:
        self.nolock or self.mutex.acquire()
        current = self.tail
        self.tail += 1
        if current < BufferSize:
            self.buffer[current] = (key_hash, ttl)
        if self.nolock or self.core_mutex.acquire(current >= BufferSize):
            wb = self.buffer[: self.tail]
            self.tail = 0
            if not self.nolock:
                self.mutex.release()
            evicted = self.send_to_core(wb)
            if not self.nolock:
                self.core_mutex.release()
            self.remove_keys(evicted)
        else:
            if not self.nolock:
                self.mutex.release()
