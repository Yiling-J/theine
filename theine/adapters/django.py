from threading import Lock
import time
from datetime import timedelta
from typing import Optional, cast
from theine import Cache as Theine
from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache


class Cache(BaseCache):
    def __init__(self, name, params):
        super().__init__(params)
        self.cache = Theine("tlfu", self._max_entries)

    def _timeout_seconds(self, timeout) -> Optional[float]:
        if timeout == DEFAULT_TIMEOUT:
            return self.default_timeout
        return timeout

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version)
        if key in self.cache._cache and self.cache._cache[key].expire > time.time():
            return False
        self.cache.set(
            key,
            value,
            timedelta(seconds=cast(float, timeout))
            if timeout is not DEFAULT_TIMEOUT
            else None,
        )
        return True

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version)
        return self.cache.get(key, default)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        to = self._timeout_seconds(timeout)
        if to is not None and to <= 0:
            self.delete(key)
            return
        key = self.make_key(key, version)
        print("setttt", key, to)
        self.cache.set(
            key,
            value,
            timedelta(seconds=to) if to is not None else None,
        )

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        nkey = self.make_key(key, version)
        if nkey in self.cache._cache:
            with Lock():
                self.set(key, self.cache._cache[nkey].data, timeout, version)
            return True
        return False

    def delete(self, key, version=None):
        key = self.make_key(key, version)
        return self.cache.delete(key)

    def clear(self):
        for key in self.cache._cache:
            self.cache.core.remove(cast(str, key))
        self.cache._cache = {}
