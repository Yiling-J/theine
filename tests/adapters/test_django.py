import datetime
import time
from typing import Iterable, List, cast

import pytest
from django.core.cache import BaseCache
from django.core.cache import cache as default_cache

from theine.adapters.django import Cache as Theine


@pytest.fixture
def cache() -> Iterable[BaseCache]:
    yield default_cache
    default_cache.clear()


class TestTheineCache:
    def test_settings(self, cache: BaseCache) -> None:
        assert cache._max_entries == 1000
        assert cache.default_timeout == 60

    def test_unicode_keys(self, cache: BaseCache) -> None:
        cache.set("ключ", "value")
        res = cache.get("ключ")
        assert res == "value"

    def test_save_and_integer(self, cache: BaseCache) -> None:
        cache.set("test_key", 2)
        res = cache.get("test_key", "Foo")

        assert isinstance(res, int)
        assert res == 2

    def test_save_string(self, cache: BaseCache) -> None:
        cache.set("test_key", "hello" * 1000)
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "hello" * 1000

        cache.set("test_key", "2")
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "2"

    def test_save_unicode(self, cache: BaseCache) -> None:
        cache.set("test_key", "heló")
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "heló"

    def test_save_dict(self, cache: BaseCache) -> None:
        now_dt = datetime.datetime.now()
        test_dict = {"id": 1, "date": now_dt, "name": "Foo"}

        cache.set("test_key", test_dict)
        res = cache.get("test_key")

        assert isinstance(res, dict)
        assert res["id"] == 1
        assert res["name"] == "Foo"
        assert res["date"] == now_dt

    def test_save_float(self, cache: BaseCache) -> None:
        float_val = 1.345620002

        cache.set("test_key", float_val)
        res = cache.get("test_key")

        assert isinstance(res, float)
        assert res == float_val

    def test_timeout(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=3)
        time.sleep(4)

        res = cache.get("test_key")
        assert res is None

    def test_timeout_0(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=0)
        res = cache.get("test_key")
        assert res is None

    def test_timeout_parameter_as_positional_argument(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, -1)
        res = cache.get("test_key")
        assert res is None

        cache.set("test_key", 222, 1)
        res1 = cache.get("test_key")
        time.sleep(2)
        res2 = cache.get("test_key")
        assert res1 == 222
        assert res2 is None

    def test_timeout_negative(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=-1)
        res = cache.get("test_key")
        assert res is None

        cache.set("test_key", 222, timeout=None)
        cache.set("test_key", 222, timeout=-1)
        res = cache.get("test_key")
        assert res is None

    def test_timeout_tiny(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=0.00001)
        res = cache.get("test_key")
        assert res in (None, 222)

    def test_set_add(self, cache: BaseCache) -> None:
        cache.set("add_key", "Initial value")
        assert cache.add("add_key", "New value") is False

        assert cache.get("add_key") == "Initial value"
        assert cache.add("other_key", "New value") is True

    def test_get_many(self, cache: BaseCache) -> None:
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_get_many_unicode(self, cache: BaseCache) -> None:
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("c", "3")

        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": "1", "b": "2", "c": "3"}

    def test_set_many(self, cache: BaseCache) -> None:
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_delete(self, cache: BaseCache) -> None:
        cache.set_many({"a": 1, "b": 2, "c": 3})
        assert cache.delete("a") is True
        assert cache.get_many(["a", "b", "c"]) == {"b": 2, "c": 3}
        assert cache.delete("a") is False

    def test_delete_many(self, cache: BaseCache) -> None:
        cache.set_many({"a": 1, "b": 2, "c": 3})
        cache.delete_many(["a", "b"])
        assert cache.get_many(["a", "b", "c"]) == {"c": 3}

    def test_delete_many_generator(self, cache: BaseCache) -> None:
        cache.set_many({"a": 1, "b": 2, "c": 3})
        cache.delete_many(key for key in ["a", "b"])
        res = cache.get_many(["a", "b", "c"])
        assert res == {"c": 3}

    def test_delete_many_empty_generator(self, cache: BaseCache) -> None:
        cache.delete_many(key for key in cast(List[str], []))

    def test_incr(self, cache: BaseCache) -> None:
        cache.set("num", 1)
        cache.incr("num")
        res = cache.get("num")
        assert res == 2

        cache.incr("num", 10)
        res = cache.get("num")
        assert res == 12

        # max 64 bit signed int
        cache.set("num", 9223372036854775807)

        cache.incr("num")
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3)

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 5

    def test_incr_no_timeout(self, cache: BaseCache) -> None:
        cache.set("num", 1, timeout=None)

        cache.incr("num")
        res = cache.get("num")
        assert res == 2

        cache.incr("num", 10)
        res = cache.get("num")
        assert res == 12

        # max 64 bit signed int
        cache.set("num", 9223372036854775807, timeout=None)

        cache.incr("num")
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3, timeout=None)

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 5

    def test_get_set_bool(self, cache: BaseCache) -> None:
        cache.set("bool", True)
        res = cache.get("bool")

        assert isinstance(res, bool)
        assert res is True

        cache.set("bool", False)
        res = cache.get("bool")

        assert isinstance(res, bool)
        assert res is False

    def test_version(self, cache: BaseCache) -> None:
        cache.set("keytest", 2, version=2)
        res = cache.get("keytest")
        assert res is None

        res = cache.get("keytest", version=2)
        assert res == 2

    def test_incr_version(self, cache: BaseCache) -> None:
        cache.set("keytest", 2)
        cache.incr_version("keytest")

        res = cache.get("keytest")
        assert res is None

        res = cache.get("keytest", version=2)
        assert res == 2

    def test_ttl_incr_version_no_timeout(self, cache: BaseCache) -> None:
        cache.set("my_key", "hello world!", timeout=None)

        cache.incr_version("my_key")

        my_value = cache.get("my_key", version=2)

        assert my_value == "hello world!"

    def test_touch_zero_timeout(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", 0) is True
        res = cache.get("test_key")
        assert res is None

    def test_touch_positive_timeout(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", 2) is True
        assert cache.get("test_key") == 222
        time.sleep(3)
        assert cache.get("test_key") is None

    def test_touch_negative_timeout(self, cache: BaseCache) -> None:
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", -1) is True
        res = cache.get("test_key")
        assert res is None

    def test_touch_missed_key(self, cache: BaseCache) -> None:
        assert cache.touch("test_key_does_not_exist", 1) is False

    def test_touch_forever(self, cache: Theine) -> None:
        cache.set("test_key", "foo", timeout=1)
        result = cache.touch("test_key", None)
        assert result is True
        time.sleep(2)
        assert cache.get("test_key") == "foo"

    def test_touch_forever_nonexistent(self, cache: BaseCache) -> None:
        result = cache.touch("test_key_does_not_exist", None)
        assert result is False

    def test_touch_default_timeout(self, cache: BaseCache) -> None:
        cache.set("test_key", "foo", timeout=1)
        result = cache.touch("test_key")
        assert result is True
        time.sleep(2)
        assert cache.get("test_key") == "foo"

    def test_clear(self, cache: BaseCache) -> None:
        cache.set("foo", "bar")
        value_from_cache = cache.get("foo")
        assert value_from_cache == "bar"
        cache.clear()
        value_from_cache_after_clear = cache.get("foo")
        assert value_from_cache_after_clear is None
