import asyncio
import concurrent.futures
from datetime import timedelta
from random import randint
from threading import Thread
from time import sleep
from typing import Any, Dict, List
from unittest.mock import Mock

import pytest

from benchmarks.zipf import Zipf
from theine import Cache, Memoize


@Memoize(Cache("tlfu", 1000), None)
def foo(id: int, m: Mock) -> Dict:
    m(id)
    return {"id": id}


@foo.key
def _(id: int, m: Mock) -> str:
    return f"id-{id}"


@Memoize(Cache("tlfu", 1000), None)
def foo_empty() -> Dict:
    return {"id": "empty"}


@foo_empty.key
def _() -> str:
    return "empty"


@Memoize(Cache("tlfu", 1000), None)
async def async_foo(id: int, m: Mock) -> Dict:
    m(id)
    return {"id": id}


@async_foo.key
def _(id: int, m: Mock) -> str:
    return f"id-{id}"


class Bar:
    @Memoize(Cache("tlfu", 1000), None)
    def foo(self, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @foo.key
    def _(self, id: int, m: Mock) -> str:
        return f"id-{id}"

    @Memoize(Cache("tlfu", 1000), None)
    async def async_foo(self, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @async_foo.key
    def _(self, id: int, m: Mock) -> str:
        return f"id-{id}"

    @Memoize(Cache("tlfu", 1000), None)
    def foo_empty(self) -> str:
        return "empty"

    @foo_empty.key
    def _(self) -> str:
        return "empty"

    @Memoize(Cache("tlfu", 1000), None)
    @classmethod
    def foo_class(cls, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @foo_class.key
    def _(cls: Any, id: int, m: Mock) -> str:
        m(id)
        return f"id-{id}"

    @Memoize(Cache("tlfu", 1000), None)
    def foo_auto(self, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}

    @Memoize(Cache("tlfu", 1000), None)
    async def async_foo_auto(self, id: int, m: Mock) -> Dict:
        m(id)
        return {"id": id}


def test_sync_decorator():
    mock = Mock()
    threads: List[Thread] = []
    assert foo.__name__ == "foo"  # type: ignore

    def assert_id(id: int, m: Mock):
        assert foo(id, m)["id"] == id

    for _ in range(500):
        t = Thread(target=assert_id, args=[randint(0, 5), mock])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


def test_sync_decorator_empty():
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
async def test_async_decorator():
    mock = Mock()
    assert async_foo.__name__ == "async_foo"  # type: ignore

    async def assert_id(id: int, m: Mock):
        data = await async_foo(id, m)
        assert data["id"] == id

    await asyncio.gather(*[assert_id(randint(0, 5), mock) for _ in range(500)])

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


def test_instance_method_sync():
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
async def test_instance_method_async():
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


@Memoize(Cache("tlfu", 1000), None)
def foo_auto_key(a: int, b: int, c: int = 5) -> Dict:
    return {"a": a, "b": b, "c": c}


def test_auto_key():

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


@Memoize(Cache("tlfu", 1000), None)
async def async_foo_auto_key(a: int, b: int, c: int = 5) -> Dict:
    return {"a": a, "b": b, "c": c}


@pytest.mark.asyncio
async def test_auto_key_async():

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


def test_instance_method_auto_key_sync():
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
async def test_instance_method_auto_key_async():
    mock = Mock()
    bar = Bar()

    async def assert_id(id: int, m: Mock):
        data = await bar.async_foo_auto(id, m)
        assert data["id"] == id

    await asyncio.gather(*[assert_id(randint(0, 5), mock) for _ in range(500)])

    assert mock.call_count == 6
    ints = [i[0][0] for i in mock.call_args_list]
    assert set(ints) == {0, 1, 2, 3, 4, 5}


@Memoize(Cache("tlfu", 1000), timedelta(seconds=1))
def foo_to(id: int) -> Dict:
    return {"id": id}


@foo_to.key
def _(id: int) -> str:
    return f"id-{id}"


def test_timeout():
    for i in range(30):
        result = foo_to(i)
        assert result["id"] == i
    assert len(foo_to._cache) == 30
    current = 30
    counter = 0
    while True:
        sleep(5)
        counter += 1
        assert len(foo_to._cache) < current
        current = len(foo_to._cache)
        if current == 0:
            break


@Memoize(Cache("tlfu", 1000), timedelta(seconds=1))
def foo_to_auto(id: int, m: Mock) -> Dict:
    m(id)
    return {"id": id}


def test_timeout_auto_key():
    mock = Mock()
    for i in range(30):
        result = foo_to_auto(i, mock)
        assert result["id"] == i
    assert len(foo_to_auto._cache) == 30
    assert foo_to_auto._cache.key_gen.len() == 30
    current = 30
    counter = 0
    while True:
        sleep(5)
        counter += 1
        assert len(foo_to._cache) < current
        current = len(foo_to._cache)
        if current == 0:
            break
    assert foo_to_auto._cache.key_gen.len() == 0


def test_cache_full_evict():
    mock = Mock()
    for i in range(30, 1500):
        result = foo_to_auto(i, mock)
        assert result["id"] == i
    assert len(foo_to_auto._cache) == 1000
    assert foo_to_auto._cache.key_gen.len() == 1000


def test_cache_full_auto_key_sync_multi():
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

    assert len(foo_to_auto._cache) == 1000
    assert foo_to_auto._cache.key_gen.len() == 1000


@Memoize(Cache("tlfu", 1000), timeout=None, lock=True)
def read_auto_key(key: str):
    return key


def assert_read_key(n: int):
    key = f"key:{n}"
    v = read_auto_key(key)
    assert v == key
    assert len(read_auto_key._cache) < 2000
    assert foo_to_auto._cache.key_gen.len() < 2000
    print(".", end="")


def test_cocurrency_load():
    z = Zipf(1.0001, 10, 5000000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
        for _ in range(200000):
            future = executor.submit(assert_read_key, z.get())
            exception = future.exception()
            if exception:
                raise exception
    print("==== done ====", len(read_auto_key._cache), foo_to_auto._cache.key_gen.len())
