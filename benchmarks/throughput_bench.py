from threading import Thread, Lock
from theine import Cache
from bounded_zipf import Zipf
import time
import random
from datetime import timedelta

# The multi-threading benchmark results are not meaningful for a GIL-enabled build
# because cache Get/Set operations are CPU-intensive, and multi-threading performance
# is heavily constrained by the GIL.
# Currently, the multi-threading benchmark is also of limited value for a no GIL build,
# as the scalability of CPython without the GIL remains unclear. For instance, will
# there be contention caused by reference counting when accessing the global variable
# CACHE_SIZE across threads? The same concern applies to functions and methods.
# Accurate multi-threading throughput benchmarks cannot be achieved through assumptions
# and guesses; proper evaluation will require official documentation from CPython
# on no GIL scalability.


CACHE_SIZE = 100_000
CPU = 6
DATA_LEN = 2 << 14


def key_gen():
    z = Zipf(1.0001, 9, CACHE_SIZE * 100)
    keys = []
    i = 0
    while i < DATA_LEN:
        keys.append(z.get())
        i += 1

    return keys


def bench_run(client, tp, reporter=None):
    keys = key_gen()

    start = random.randint(0, DATA_LEN - 1)
    s = time.monotonic_ns()

    for i in range(500000):
        if tp == 0:
            client.get(keys[(i + start) & DATA_LEN - 1])
        else:
            client.set(keys[(i + start) & DATA_LEN - 1], 1, timedelta(hours=1))

    dt = time.monotonic_ns() - s
    if reporter is None:
        nop = dt / 500000
        print(f"single thread {'read' if tp==0 else 'write'}: {nop:.2f} ns/op")
    else:
        reporter(500000)


def bench_run_parallel(r, w, runner):
    tl = []

    count_sum = 0
    mu = Lock()

    def report(count):
        nonlocal count_sum

        with mu:
            count_sum += count

    s = time.monotonic_ns()
    for i in range(r + w):
        t = Thread(target=bench_run, args=[client, 0 if i < r else 1, report])
        tl.append(t)
        t.start()

    for t in tl:
        t.join()
    dt = time.monotonic_ns() - s

    nop = dt / count_sum
    print(f"read-{r} write-{w}: {nop:.2f} ns/op")


if __name__ == "__main__":
    client = Cache(CACHE_SIZE)
    keys = key_gen()
    for k in keys:
        client.set(k, 1, timedelta(hours=1))

    # read only single thread
    bench_run(client, 0)

    # write only single thread
    bench_run(client, 1)

    # read only multi threaded
    bench_run_parallel(CPU, 0, client)

    # write only multi threaded
    bench_run_parallel(0, CPU, client)

    # 75% read multi threaded
    rc = int(CPU * 0.75)
    bench_run_parallel(rc, CPU - rc, client)
