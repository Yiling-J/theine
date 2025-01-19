from typing import TypeVar, Generic, Hashable

KT = TypeVar("KT", bound=Hashable)
VT = TypeVar("VT")


class CacheStats:
    def __init__(self, total: int, hit: int):
        self.request_count = total
        self.hit_count = hit
        self.miss_count = self.request_count - self.hit_count
        self.hit_rate = self.hit_count / self.request_count


class Entry(Generic[VT]):
    __slots__ = ("value", "expire")
    value: VT
    expire: int

    def __init__(self, value: VT, expire: int) -> None:
        self.value = value
        self.expire = expire
