# Theine

High performance in-memory cache inspired by [Caffeine](https://github.com/ben-manes/caffeine).

- High performance [Rust core](https://github.com/Yiling-J/theine-core)
- High hit ratio with adaptive [W-TinyLFU](https://arxiv.org/pdf/1512.00727.pdf) eviction policy
- Expired data are removed automatically using [hierarchical timer wheel](http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf)
- Fully typed
- Thread-safe with Free-Threading support (tested on Python 3.13.6t)
- Django cache backend

## Theine V2 Migration Guide

Theine V2 is a major refactor and rewrite of V1, focused on thread safety and scalability. Below are the key changes:

#### Cache Class and Memoize Decorator
In V2, the `Cache` class and `Memoize` decorator now accept capacity as the first parameter. We have simplified the design by consolidating to a single policy: adaptive W-Tinylfu.

Old:
```python
cache = Cache("tlfu", 10000)

@Memoize(Cache("tlfu", 10000), timedelta(seconds=100))
...
```

New:
```python
cache = Cache(10000)

@Memoize(10000, timedelta(seconds=100))
...
```

#### Making `Cache.get` return explicit existence status

The `Cache.get` method now returns a tuple `(value, exists)` instead of only the value. This makes it explicit whether the requested key was found in the cache. In the old API, a missing key returned `None` (or a provided default value), which could cause ambiguity if `None` was also a valid cached value.

Old:

```python
# Without default, returns None if key is missing
v = cache.get("key")

# With default, returns default if key is missing
sentinel = object()
v = cache.get("key", sentinel)
```

New:

```python
# Returns (value, exists), where exists is True if the key was found
v, ok = cache.get("key")
if ok:
    ...
```

#### Renaming `timeout` to `ttl`
The `timeout` parameter in the `Cache` class’s `set` method has been renamed to `ttl` (Time-to-Live). This is more commonly used in caching and is clearer in meaning. In V1, the term `timeout` was used for consistency with Django, but `ttl` is now the preferred naming convention in V2. The Django adapter settings still uses `TIMEOUT` for compatibility.

Old:
```python
cache.set("key", {"foo": "bar"}, timeout=timedelta(seconds=100))

@Memoize(Cache("tlfu", 10000), timeout=timedelta(seconds=100))
...
```

New:
```python
cache.set("key", {"foo": "bar"}, ttl=timedelta(seconds=100))

@Memoize(10000, ttl=timedelta(seconds=100))
```

#### Thread Safety by Default
In V2, both the `Cache` class and the `Memoize` decorator are thread-safe by default. However, if you're not using Theine in a multi-threaded environment, you can disable the locking mechanism. **However for free-threaded Python build, `nolock` will always be `False`, even if set to `True` here.** This is because Theine internally uses an extra thread for proactive expiry, so at least two threads are active, and thus `nolock` must remain `False`.
```python
cache = Cache(10000, nolock=True)

@Memoize(10000, timedelta(seconds=100), nolock=True)
...
```
In V1, there was a `lock` parameter used to prevent cache stampede. In V2, this protection is enabled by default, so the `lock` parameter is no longer required. Setting `nolock` to `True` will disable the cache stampede protection, as cache stampede is not possible in a single-threaded environment.

#### Single Expiration Handling Thread for All Cache Instances
In V2, instead of each cache instance using a separate thread for proactive expiration (as in V1), a single thread will be used to handle expirations for all cache instances via `asyncio`. This improves efficiency and scalability.

#### Improved Adaptive Cache Eviction Policy
The improved adaptive cache eviction policy automatically switches between LRU and LFU strategies to achieve a higher hit ratio across diverse workloads. See [hit ratios](#hit-ratios) for results based on many widely used trace data.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Design](#design)
- [API](#api)
- [Metadata Memory Overhead](#metadata-memory-overhead)
- [Benchmarks](#benchmarks)
  * [throughput](#throughput)
  * [hit ratios](#hit-ratios)
- [Support](#support)

## Requirements
Python 3.9+

For use with **free-threaded Python**, the recommended version is **Python 3.13.6+**. See https://github.com/python/cpython/issues/133136 for more details.

## Installation
```
pip install theine
```
## Design

Theine consists of two main components: a core cache policy implemented in Rust, and a Python interface that manages the storage of cached key-value pairs. The Python layer uses the Rust policy to ensure that the total size of the cache does not exceed its maximum limit and evicts entries based on the policy's decisions.

The Rust core and the Python layer are decoupled through the use of key hashes(int). The core operates solely on key hashes, without any direct knowledge of Python objects. This separation simplifies the core implementation and avoids the complexity of handling Python-specific behavior in Rust.

All interactions with the Rust core are protected by a mutex, meaning there is no concurrency within the core itself. This is an intentional design decision: since Rust is significantly faster than Python, adding concurrency at the core level would introduce unnecessary complexity with little performance gain.

On the Python side, key-value pairs are stored in a sharded dictionary. Each shard has its own mutex (currently a standard threading `Lock`) to improve scalability. A reader-writer lock is not used because Python does not provide one in the standard library.

Each shard actually maintains two dictionaries:

1. The primary key-value dictionary, used to retrieve values directly by key.
2. A keyhash-to-key dictionary, used to map the hashes (which are what the Rust core operates on) back to the original keys. This is necessary because when the Rust core decides to evict an item based on its hash, the Python side must be able to identify and remove the corresponding key from the primary dictionary.

## API

Key should be a **Hashable** object, and value can be any **Python object**.

#### Cache Client

```Python
from theine import Cache
from datetime import timedelta

cache = Cache(10000)
# get value by key, returns (value, exists) where exists indicates if the key was found
v, ok = cache.get("key")

# set with ttl
cache.set("key", {"foo": "bar"}, timedelta(seconds=100))

# delete from cache
cache.delete("key")

# close cache, stop timing wheel thread
cache.close()

# clear cache
cache.clear()

# get current cache stats, please call stats() again if you need updated stats
stats = cache.stats()
print(stats.request_count, stats.hit_count, stats.hit_rate)

# get cache max size
cache.max_size

# get cache current size
len(cache)

```

#### Cache Decorator

Theine's Decorator is designed with following:

- Both sync and async support.
- Explicitly control how key is generated. Most remote cache(redis, memcached...) only allow string keys, return a string in key function make it easier when you want to use remote cache later.
- [Thundering herd/Cache stampede](https://en.wikipedia.org/wiki/Cache_stampede) protection.
- Type checked. Mypy can check key function to make sure it has same input signature as original function and return a hashable.

Theine support hashable keys, so to use a decorator, a function to convert input signatures to hashable is necessary. **The recommended way is specifying the function explicitly**, this is approach 1, Theine also support generating key automatically, this is approach 2.

**- explicit key function**

```python
from theine import Cache, Memoize
from datetime import timedelta

@Memoize(10000, timedelta(seconds=100))
def foo(a:int) -> int:
    return a

@foo.key
def _(a:int) -> str:
    return f"a:{a}"

foo(1)

# asyncio
@Memoize(10000, timedelta(seconds=100))
async def foo_a(a:int) -> int:
    return a

@foo_a.key
def _(a:int) -> str:
    return f"a:{a}"

await foo_a(1)

# get current cache stats, please call stats() again if you need updated stats
stats = foo.cache_stats()
print(stats.request_count, stats.hit_count, stats.hit_rate)

```

**- auto key function**

```python
from theine import Cache, Memoize
from datetime import timedelta

@Memoize(10000, timedelta(seconds=100), typed=True)
def foo(a:int) -> int:
    return a

foo(1)

# asyncio
@Memoize(10000, timedelta(seconds=100), typed=True)
async def foo_a(a:int) -> int:
    return a

await foo_a(1)

```

**Important:** The auto key function use same methods as Python's lru_cache. And there are some issues relatedto the memory usage in this way, take a look [this issue](https://github.com/python/cpython/issues/88476) or [this one](https://github.com/python/cpython/issues/64058).


#### Django Cache Backend

```Python
CACHES = {
    "default": {
        "BACKEND": "theine.adapters.django.Cache",
        "TIMEOUT": 300,
        "OPTIONS": {"MAX_ENTRIES": 10000},
    },
}
```

## Core Metadata Memory Overhead
The Rust core uses only the key hash, so the actual key size does not affect memory usage. Each metadata entry in Rust consumes 64 bytes of memory.

## Benchmarks

### hit ratios

Source Code: [trace_bench.py](benchmarks/trace_bench.py)

**zipf**

![hit ratios](benchmarks/zipf.png)
**s3**

![hit ratios](benchmarks/s3.png)
**ds1**

![hit ratios](benchmarks/ds1.png)
**oltp**

![hit ratios](benchmarks/oltp.png)
**wiki CDN**

![hit ratios](benchmarks/wikicdn.png)
**Twitter Cache**

![hit ratios](benchmarks/twitter-c52s10.png)

### throughput

Source Code: [throughput_bench.py](benchmarks/throughput_bench.py)

CPU: aliyun, 32 vCPU, Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz

Python 3.13.3t

reads=100%, 1-32 threads

![throughput](benchmarks/read_throughput.png)

## Support
Feel free to open an issue or ask question in discussions.
