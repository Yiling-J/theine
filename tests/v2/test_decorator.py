import asyncio
import concurrent.futures
from datetime import timedelta
from random import randint
from threading import Thread
from time import sleep
from typing import Any, Dict, List
from unittest.mock import Mock

import pytest
from bounded_zipf import Zipf  # type: ignore[import]

from theine.theine.v2 import Memoize


@Memoize(1000, None)
def foo(id: int, m: Mock) -> Dict[str, int]:
    m(id)
    return {"id": id}


@foo.key
def _(id: int, m: Mock) -> str:
    return f"id-{id}"


@Memoize(1000, None)
def foo_empty() -> Dict:
    return {"id": "empty"}


@foo_empty.key
def _() -> str:
    return "empty"


@Memoize(1000, None)
async def async_foo(id: int, m: Mock) -> Dict:
    m(id)
    await asyncio.sleep(1)
    return {"id": id}


@async_foo.key
def _(id: int, m: Mock) -> str:
    return f"id-{id}"


class Bar:
    @Memoize(1000, None)
    def foo(self, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @foo.key
    def _(self, id: int, m: Mock) -> str:
        return f"id-{id}"

    @Memoize(1000, None)
    async def async_foo(self, id: int, m: Mock) -> Dict:
        m(id)
        await asyncio.sleep(1)
        return {"id": id}

    @async_foo.key
    def _(self, id: int, m: Mock) -> str:
        return f"id-{id}"

    @Memoize(1000, None)
    def foo_empty(self) -> str:
        return "empty"

    @foo_empty.key
    def _(self) -> str:
        return "empty"

    @Memoize(1000, None)
    @classmethod
    def foo_class(cls, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @foo_class.key
    def _(cls: Any, id: int, m: Mock) -> str:
        m(id)
        return f"id-{id}"

    @Memoize(1000, None)
    def foo_auto(self, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @Memoize(1000, None)
    async def async_foo_auto(self, id: int, m: Mock) -> Dict:
        m(id)
        await asyncio.sleep(1)
        return {"id": id}


def test_sync_decorator() -> None:
    mock = Mock()
    threads: List[Thread] = []
    assert foo.__name__ == "foo"  # type: ignore

    def assert_id(id: int, m: Mock):
        try:
            v = foo(id, m)
        except Exception as e:
            print(e)
        assert v["id"] == id

    for _ in range(500):
        t = Thread(target=assert_id, args=[randint(0, 5), mock])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


def test_sync_decorator_empty() -> None:
    threads: List[Thread] = []

    def assert_id():
        assert foo_empty()["id"] == "empty"

    for _ in range(500):
        t = Thread(target=assert_id, args=[])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


@pytest.mark.asyncio
async def test_async_decorator() -> None:
    mock = Mock()
    assert async_foo.__name__ == "async_foo"  # type: ignore

    async def assert_id(id: int, m: Mock):
        data = await async_foo(id, m)
        assert data["id"] == id

    await asyncio.gather(*[assert_id(randint(0, 5), mock) for _ in range(500)])

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


def test_instance_method_sync() -> None:
    mock = Mock()
    threads: List[Thread] = []
    bar = Bar()
    assert bar.foo.__name__ == "foo"  # type: ignore

    def assert_id(id: int, m: Mock):
        assert bar.foo(id, m)["id"] == id

    for _ in range(500):
        t = Thread(target=assert_id, args=[randint(0, 5), mock])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


@pytest.mark.asyncio
async def test_instance_method_async() -> None:
    mock = Mock()
    bar = Bar()
    assert bar.async_foo.__name__ == "async_foo"  # type: ignore

    async def assert_id(id: int, m: Mock):
        data = await bar.async_foo(id, m)
        assert data["id"] == id

    await asyncio.gather(*[assert_id(randint(0, 5), mock) for _ in range(500)])

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


@Memoize(1000, None)
def foo_auto_key(a: int, b: int, c: int = 5) -> Dict:
    return {"a": a, "b": b, "c": c}


def test_auto_key() -> None:
    tests = [
        ([1, 2, 3], {}, (1, 2, 3)),
        ([1, 2], {}, (1, 2, 5)),
        ([1], {"b": 2}, (1, 2, 5)),
        ([], {"a": 1, "b": 2}, (1, 2, 5)),
        ([], {"a": 1, "b": 2, "c": 3}, (1, 2, 3)),
    ]

    def assert_data(args, kwargs, expected):
        result = foo_auto_key(*args, **kwargs)
        assert result["a"] == expected[0]
        assert result["b"] == expected[1]
        assert result["c"] == expected[2]

    for case in tests:
        assert_data(*case)


@Memoize(1000, None)
async def async_foo_auto_key(a: int, b: int, c: int = 5) -> Dict:
    return {"a": a, "b": b, "c": c}


@pytest.mark.asyncio
async def test_auto_key_async() -> None:
    tests = [
        ([1, 2, 3], {}, (1, 2, 3)),
        ([1, 2], {}, (1, 2, 5)),
        ([1], {"b": 2}, (1, 2, 5)),
        ([], {"a": 1, "b": 2}, (1, 2, 5)),
        ([], {"a": 1, "b": 2, "c": 3}, (1, 2, 3)),
    ]

    async def assert_data(args, kwargs, expected):
        result = await async_foo_auto_key(*args, **kwargs)
        assert result["a"] == expected[0]
        assert result["b"] == expected[1]
        assert result["c"] == expected[2]

    for case in tests:
        await assert_data(*case)


def test_instance_method_auto_key_sync() -> None:
    mock = Mock()
    threads: List[Thread] = []
    bar = Bar()

    def assert_id(id: int, m: Mock):
        assert bar.foo_auto(id, m)["id"] == id

    for _ in range(500):
        t = Thread(target=assert_id, args=[randint(0, 5), mock])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


@pytest.mark.asyncio
async def test_instance_method_auto_key_async() -> None:
    mock = Mock()
    bar = Bar()

    async def assert_id(id: int, m: Mock):
        data = await bar.async_foo_auto(id, m)
        assert data["id"] == id

    await asyncio.gather(*[assert_id(randint(0, 5), mock) for _ in range(500)])

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


@Memoize(1000, timedelta(seconds=1))
def foo_to(id: int) -> Dict:
    return {"id": id}


@foo_to.key
def _(id: int) -> str:
    return f"id-{id}"


def test_timeout() -> None:
    for i in range(30):
        result = foo_to(i)
        assert result["id"] == i
    assert len(foo_to._cache) == 30
    foo_to._cache._force_drain_write()
    current = 30
    counter = 0
    while True:
        sleep(5)
        counter += 1
        assert len(foo_to._cache) < current
        current = len(foo_to._cache)
        if current == 0:
            break


@Memoize(1000, timedelta(seconds=1))
def foo_to_auto(id: int, m: Mock) -> Dict:
    m(id)
    return {"id": id}


def test_timeout_auto_key() -> None:
    mock = Mock()
    for i in range(30):
        result = foo_to_auto(i, mock)
        assert result["id"] == i
    assert len(foo_to_auto._cache) == 30
    foo_to_auto._cache._force_drain_write()
    current = 30
    counter = 0
    while True:
        sleep(5)
        counter += 1
        assert len(foo_to._cache) < current
        current = len(foo_to._cache)
        if current == 0:
            break


def test_cache_full_evict() -> None:
    mock = Mock()
    for i in range(30, 1500):
        result = foo_to_auto(i, mock)
        assert result["id"] == i
    foo_to_auto._cache._force_drain_write()
    assert len(foo_to_auto._cache) == 1000


def test_cache_full_auto_key_sync_multi() -> None:
    mock = Mock()
    threads: List[Thread] = []

    def assert_id(id: int, m: Mock):
        assert foo_to_auto(id, m)["id"] == id

    for i in range(2000, 3500):
        t = Thread(target=assert_id, args=[i, mock])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    foo_to_auto._cache._force_drain_write()
    assert len(foo_to_auto._cache) == 1000


@Memoize(1000, timeout=None)
def read_auto_key(key: str) -> str:
    return key


def assert_read_key(n: int) -> None:
    key = f"key:{n}"
    v = read_auto_key(key)
    assert v == key
    assert len(read_auto_key._cache) < 2000
    print(".", end="")


def test_cocurrency_load() -> None:
    z = Zipf(1.0001, 10, 5000000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
        for _ in range(200000):
            future = executor.submit(assert_read_key, z.get())
            exception = future.exception()
            if exception:
                raise exception
    print("==== done ====", len(read_auto_key._cache))
