import asyncio
import pytest
from typing import Dict, List, Any
from unittest.mock import Mock
from threading import Thread
from random import randint
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
    return {"id": id}


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
