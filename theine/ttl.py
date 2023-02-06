import time
from collections import OrderedDict
from typing import Dict, List
from threading import Lock
from collections import defaultdict

from theine.models import CachedValue


class FakeTimer:
    def __init__(self):
        return

    def set(self, key: str, value: CachedValue, ttl: float):
        return

    def delete(self, key: str, value: CachedValue):
        return

    def expire(self, ttl: float, now: float) -> List[str]:
        return []


class BucketTimer:
    def __init__(self):
        self.buckets: Dict[float, OrderedDict] = defaultdict(OrderedDict)
        self.metadata = {}
        self.locks = defaultdict(Lock)

    def set(self, key: str, value: CachedValue, ttl: float):
        bucket = self.buckets[ttl]
        bucket[key] = value
        bucket.move_to_end(key)
        value.bucket = bucket

    def delete(self, key: str, value: CachedValue):
        bucket = value.bucket
        if bucket is not None:
            bucket.pop(key, None)

    def expire(self, ttl: float, now: float) -> List[str]:
        with self.locks[ttl]:
            expire = self.metadata.setdefault(ttl, now + ttl + 0.01)
            if now <= expire:
                return []
            bucket = self.buckets[ttl]
            remain = 20  # limit maxium proecess size, avoid long blocking
            expiry = time.time()
            expired = []
            self.wait_expire = -1
            for key, item in bucket.items():
                if remain > 0 and item.expire <= expiry:
                    expired.append(key)
                    remain -= 1
                else:  # already expire N items or reach a not expired one
                    self.metadata[ttl] = item.expire + 0.01
                    break
            for key in expired:
                bucket.pop(key, None)
            return expired
