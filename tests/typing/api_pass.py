from typing import Any, Dict, List
from theine import Memoize, Cache


@Memoize(Cache("tlfu", 1000), None)
def foo(id: int) -> Dict[str, int]:
    return {"id": id}


@foo.key
def _(id: int) -> str:
    return f"id-{id}"


class Bar:

    @Memoize(Cache("tlfu", 1000), None)
    def foo(self, id: int) -> Dict[str, int]:
        return {"id": id}

    @foo.key
    def _(self, id: int) -> str:
        return f"id-{id}"


def run() -> None:
    v: Dict[str, int] = foo(12)
    bar = Bar()
    b: Dict[str, int] = bar.foo(13)
