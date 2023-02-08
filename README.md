# theine
high performance in-memory cache

## API

```Python
from theine import Cache
from datetime import timedelta

cache = Cache("tlfu", 10000)
# without default, return None on miss
v = cache.get("key")

# with default, return default on miss
sentinel = object()
v = cache.get(key, sentinel)

# set with ttl
cache.set("key", {"foo": "bar"}, timedelta(seconds=100))

# delete from cache
cache.delete("key")
```
