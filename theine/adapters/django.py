from datetime import timedelta
from threading import Lock
from typing import Any, Callable, Optional, Union

from django.core.cache.backends.base import BaseCache, DEFAULT_TIMEOUT

from theine import Cache as Theine
from theine.theine import sentinel

KEY_TYPE = Union[str, Callable[..., str]]
VALUE_TYPE = Any
VERSION_TYPE = Optional[int]


class Cache(BaseCache):
    def __init__(self, name: str, params: dict[str, Any]):
        super().__init__(params)
        options = params.get("OPTIONS", {})
        policy = options.get("POLICY", "tlfu")
        self.cache = Theine(policy, self._max_entries)

    def add(self, key: KEY_TYPE, value: VALUE_TYPE, timeout: Optional[float] = DEFAULT_TIMEOUT,
            version: VERSION_TYPE = None) -> bool:
        data = self.get(key, sentinel, version)
        if data is not sentinel:
            return False
        key = self.make_key(key, version)
        backend_timeout = self.get_backend_timeout(timeout)
        self.cache.set(
            key,
            value,
            timedelta(seconds=backend_timeout) if backend_timeout else None,
        )
        return True

    def get(self, key: KEY_TYPE, default: Optional[VALUE_TYPE] = None, version: VERSION_TYPE = None) -> Optional[
        VALUE_TYPE]:
        key = self.make_key(key, version)
        return self.cache.get(key, default)

    def set(self, key: KEY_TYPE, value: VALUE_TYPE, timeout: Optional[float] = DEFAULT_TIMEOUT,
            version: VERSION_TYPE = None) -> None:
        to = self.get_backend_timeout(timeout)
        if to is not None and to <= 0:
            self.delete(key)
            return
        key = self.make_key(key, version)
        self.cache.set(
            key,
            value,
            timedelta(seconds=to) if to is not None else None,
        )

    def touch(self, key: KEY_TYPE, timeout: Optional[float] = DEFAULT_TIMEOUT, version: VERSION_TYPE = None) -> bool:
        data = self.get(key, sentinel, version)
        if data is sentinel:
            return False
        nkey = self.make_key(key, version)
        if (
            timeout is not DEFAULT_TIMEOUT
            and timeout is not None
            and timeout <= 0
        ):
            self.cache.delete(nkey)
            return True
        to = self.get_backend_timeout(timeout)
        with Lock():
            self.cache._access(nkey, timedelta(seconds=to) if to is not None else None)
        return True

    def delete(self, key: KEY_TYPE, version: VERSION_TYPE = None) -> bool:
        key = self.make_key(key, version)
        return self.cache.delete(key)

    def clear(self) -> None:
        self.cache.clear()
