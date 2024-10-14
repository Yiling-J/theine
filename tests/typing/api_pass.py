import asyncio
from typing import Any, Dict, List, Optional
from theine import Memoize, Cache


@Memoize(1000, None)
def foo(id: int) -> Dict[str, int]:
    return {"id": id}


@foo.key
def _(id: int) -> str:
    return f"id-{id}"


class Bar:

    @Memoize(1000, None)
    def foo(self, id: int) -> Dict[str, int]:
        return {"id": id}

    @foo.key
    def _(self, id: int) -> str:
        return f"id-{id}"

    @Memoize(1000, None)
    async def async_foo(self, id: int) -> Dict[str, int]:
        await asyncio.sleep(1)
        return {"id": id}

    @async_foo.key
    def _(self, id: int) -> str:
        return f"id-{id}"


@Memoize(1000, None)
async def async_foo(id: int) -> Dict[str, int]:
    await asyncio.sleep(1)
    return {"id": id}


def run() -> None:
    v: Dict[str, int] = foo(12)
    bar = Bar()
    b: Dict[str, int] = bar.foo(13)

    client = Cache[int, int](1000)
    client.set(1, 1)
    v2, ok = client.get(1)
    vt: Optional[int] = v2
    okk: bool = ok


async def run_async() -> None:
    v: Dict[str, int] = await async_foo(12)
    bar = Bar()
    b: Dict[str, int] = await bar.async_foo(13)
