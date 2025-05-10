from typing import Any, Dict, List, Optional
from theine import Memoize, Cache


@Memoize(1000, None)
def foo(id: int) -> Dict[str, int]:
    return {"id": id}


@foo.key
def _(id: int, name: str) -> str:
    return f"id-{id}"


class Bar:

    @Memoize(1000, None)
    def foo(self, id: int) -> Dict[str, int]:
        return {"id": id}

    @foo.key
    def _(self, id: int, name: str) -> str:
        return f"id-{id}"


def run() -> None:
    v: Dict[str, int] = foo(12, 13)
    bar = Bar()
    b: Dict[str, int] = bar.foo(12, 13)

    client = Cache[int, int](1000)
    client.set("a", 1)
    client.set(1, "a")
    v2, ok = client.get(1)
    vt: Optional[str] = v2
    okk: str = ok
