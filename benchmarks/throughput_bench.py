import random
import time
from threading import Lock, Thread

import matplotlib.pyplot as plt
import numpy as np
from bounded_zipf import Zipf
from cachetools import TTLCache, cached
from scipy.interpolate import PchipInterpolator

from theine import Memoize

CPU = 6
DATA_LEN = 2 << 15


def key_gen():
    z = Zipf(1.0001, 9, DATA_LEN)
    keys = []
    i = 0
    while i < DATA_LEN:
        keys.append(z.get())
        i += 1

    return keys


def bench_run(index, fn, tp, reporter=None, keys=[]):
    if len(keys) == 0:
        keys = key_gen()
    else:
        keys = keys[:]

    start = random.randint(0, DATA_LEN - 1)
    s = time.monotonic_ns()

    for i in range(DATA_LEN):
        if tp == 0:
            fn(keys[(i + start) & DATA_LEN - 1])
        else:
            fn(index * DATA_LEN + i)

    dt = time.monotonic_ns() - s
    if reporter is None:
        nop = dt / DATA_LEN
        print(f"single thread {'read' if tp==0 else 'write'}: {nop:.2f} ns/op")
    else:
        reporter(DATA_LEN)


def bench_run_parallel(r, w, tp):
    if tp == "theine":
            @Memoize(DATA_LEN * 2, None)
            def get(key):
                return key

            @get.key
            def k(key):
                return str(key)
    elif tp == "cachetools":
        @cached(cache=TTLCache(maxsize=DATA_LEN * 2, ttl=20000), lock=Lock())
        def get(key):
            return key

    keys = key_gen()
    for k in keys:
        get(k)
        get(k)
        get(k)

    tl = []

    count_sum = 0
    mu = Lock()

    def report(count):
        nonlocal count_sum

        with mu:
            count_sum += count

    s = time.monotonic_ns()
    for i in range(r + w):
        t = Thread(target=bench_run, args=[i, get, 0 if i < r else 1, report, keys])
        tl.append(t)
        t.start()

    for t in tl:
        t.join()
    dt = time.monotonic_ns() - s

    nop = dt / count_sum
    print(f"read-{r} write-{w}: {nop:.2f} ns/op")
    return 1 / nop * 1e9


def bench_run_nolock(tp):
    if tp == "theine":
            @Memoize(DATA_LEN * 2, None, nolock=True)
            def get(key):
                return key

            @get.key
            def k(key):
                return str(key)
    elif tp == "cachetools":
        @cached(cache=TTLCache(maxsize=DATA_LEN * 2, ttl=20000), info=True)
        def get(key):
            return key

    keys = key_gen()
    for k in keys:
        get(k)
        get(k)
        get(k)

    count_sum = 0
    mu = Lock()

    def report(count):
        nonlocal count_sum

        with mu:
            count_sum += count

    s = time.monotonic_ns()
    bench_run(0, get, 0, report, keys)
    dt = time.monotonic_ns() - s
    nop = dt / count_sum
    ops = 1 / nop * 1e9
    print(f"{tp} 100% read: {nop} ns/op {ops} ops/s")
    return 1 / nop * 1e9

def init_plot(title):
    fig, ax = plt.subplots()
    ax.set_xlabel("threads")
    ax.set_ylabel("ops/s")
    ax.set_title(title)
    fig.set_figwidth(12)
    return fig, ax

def bench_and_plot(parallel, name):
    x = []
    y_theine = []
    y_cachetools = []

    for p in parallel:
        x.append(p)
        y_theine.append(bench_run_parallel(p, 0, "theine"))
        y_cachetools.append(bench_run_parallel(p, 0, "cachetools"))

    fig, ax = init_plot("Read Throughput")
    # Smooth the lines using np.interp for interpolation
    x_new = np.linspace(min(x), max(x), 300)

    theine_spl = PchipInterpolator(x, y_theine)
    cachetools_spl = PchipInterpolator(x, y_cachetools)
    y_theine_smooth = theine_spl(x_new)
    y_cachetools_smooth = cachetools_spl(x_new)

    ax.plot(x_new, y_theine_smooth, "b-", label="theine")
    ax.plot(x_new, y_cachetools_smooth, "r-", label="cachetools")
    ax.plot(x, y_theine, "bo")
    ax.plot(x, y_cachetools, "ro")

    ax.legend()
    fig.savefig(f"benchmarks/{name.lower()}.png", dpi=200)


def bench_and_plot_nolock(name):
    bench_run_nolock("theine")
    bench_run_nolock("cachetools")

if __name__ == "__main__":
    bench_and_plot_nolock("read_throughput")
    bench_and_plot([1,2,4,8,12,16,20,26,32], "read_throughput")
