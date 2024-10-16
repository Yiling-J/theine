from datetime import timedelta
from threading import Lock
from typing import Optional, cast

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

from theine import Cache as Theine
from theine.theine import sentinel


class Cache(BaseCache):
    def __init__(self, name, params):
        super().__init__(params)
        options = params.get("OPTIONS", {})
        policy = options.get("POLICY", "tlfu")
        self.cache = Theine(policy, self._max_entries)

    def _timeout_seconds(self, timeout) -> Optional[float]:
        if timeout == DEFAULT_TIMEOUT:
            return self.default_timeout
        return timeout

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        data = self.get(key, sentinel, version)
        if data is not sentinel:
            return False
        key = self.make_key(key, version)
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
        self.cache.set(
            key,
            value,
            timedelta(seconds=to) if to is not None else None,
        )

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        data = self.get(key, sentinel, version)
        if data is sentinel:
            return False
        nkey = self.make_key(key, version)
        if (
            timeout is not DEFAULT_TIMEOUT
            and timeout is not None
            and cast(float, timeout) <= 0
        ):
            self.cache.delete(nkey)
            return True
        to = self._timeout_seconds(timeout)
        with Lock():
            self.cache._access(nkey, timedelta(seconds=to) if to is not None else None)
        return True

    def delete(self, key, version=None):
        key = self.make_key(key, version)
        return self.cache.delete(key)

    def clear(self):
        self.cache.clear()
