from datetime import timedelta
from threading import Lock
from typing import Any, Callable, Dict, Optional, Union, cast

from django.core.cache.backends.base import BaseCache, DEFAULT_TIMEOUT

from theine import Cache as Theine
from theine.theine import sentinel

KEY_TYPE = Union[str, Callable[..., str]]
VALUE_TYPE = Any
VERSION_TYPE = Optional[int]


class Cache(BaseCache):
    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(params)
        self.cache = Theine[Any, Any](self._max_entries)

    def _timeout_seconds(
        self, timeout: "Optional[Union[float, DEFAULT_TIMEOUT]]"
    ) -> float:
        if timeout == DEFAULT_TIMEOUT:
            return cast(float, self.default_timeout)
        return cast(float, timeout)

    def add(
        self,
        key: KEY_TYPE,
        value: VALUE_TYPE,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        version: VERSION_TYPE = None,
    ) -> bool:
        data = self.get(key, sentinel, version)
        if data is not sentinel:
            return False
        key = self.make_key(key, version)
        self.cache.set(
            key,
            value,
            (
                timedelta(seconds=cast(float, timeout))
                if timeout is not DEFAULT_TIMEOUT
                else None
            ),
        )
        return True

    def get(
        self,
        key: KEY_TYPE,
        default: Optional[VALUE_TYPE] = None,
        version: VERSION_TYPE = None,
    ) -> Optional[VALUE_TYPE]:
        key = self.make_key(key, version)
        v, ok = self.cache.get(key)
        if not ok:
            return default
        return v

    def set(
        self,
        key: KEY_TYPE,
        value: VALUE_TYPE,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        version: VERSION_TYPE = None,
    ) -> None:
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

    def touch(
        self,
        key: KEY_TYPE,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        version: VERSION_TYPE = None,
    ) -> bool:
        data = self.get(key, sentinel, version)
        if data is sentinel:
            return False
        nkey = self.make_key(key, version)
        if timeout is not DEFAULT_TIMEOUT and timeout is not None and timeout <= 0:
            self.cache.delete(nkey)
            return True
        to = self._timeout_seconds(timeout)
        with Lock():
            self.cache._access(nkey, timedelta(seconds=to) if to is not None else None)
        return True

    def delete(self, key: KEY_TYPE, version: VERSION_TYPE = None) -> bool:
        key = self.make_key(key, version)
        return self.cache.delete(key)

    def clear(self) -> None:
        self.cache.clear()
