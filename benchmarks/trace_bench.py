import csv
from datetime import timedelta
from random import randint
from typing import Callable, Iterator

import matplotlib.pyplot as plt
from bounded_zipf import Zipf
from theine import Cache


plt.style.use("ggplot")


GET = "GET"
SET = "SET"


def zipf_key_gen() -> Iterator:
    z = Zipf(1.001, 10, 50000000)
    for _ in range(1000000):
        yield f"{z.get()}", GET


def ucb_key_gen() -> Iterator:
    with open(f"benchmarks/trace/ucb", "rb") as f:
        for line in f:
            vb = line.split(b" ")[-2]
            try:
                v = vb.decode()
            except:
                v = "failed"
            yield v, GET


def ds1_key_gen() -> Iterator:
    with open(f"benchmarks/trace/ds1", "r") as f:
        for line in f:
            tmp = line.split(" ")
            base = int(tmp[0])
            count = int(tmp[1])
            for i in range(count):
                yield str(base + i), GET


def s3_key_gen() -> Iterator:
    with open(f"benchmarks/trace/s3", "r") as f:
        for line in f:
            tmp = line.split(" ")
            base = int(tmp[0])
            count = int(tmp[1])
            for i in range(count):
                yield str(base + i), GET


def scarab_key_gen() -> Iterator:
    with open("benchmarks/trace/sc.trace", "rb") as f:
        while True:
            raw = f.read(8)
            if not raw:
                break
            yield str(int.from_bytes(raw, "big")), GET


def scarab_1h_key_gen() -> Iterator:
    with open("benchmarks/trace/sc2", "rb") as f:
        while True:
            raw = f.read(8)
            if not raw:
                break
            yield str(int.from_bytes(raw, "big")), GET


def fb_key_gen() -> Iterator:
    with open("benchmarks/trace/fb.csv", "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield str(row["key"]), row["op"]


def fb2_key_gen() -> Iterator:
    with open("benchmarks/trace/fb2.csv", "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield str(row["key"]), row["op"]


def fb3_key_gen() -> Iterator:
    with open("benchmarks/trace/fb3.csv", "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield str(row["key"]), row["op"]


def ytb_key_gen() -> Iterator:
    with open("benchmarks/trace/ytb.dat", "r", newline="") as f:
        for line in f:
            tmp = line.split(" ")
            vid = tmp[4].split("&")[0]
            yield vid, GET


def bench_theine(policy: str, cap: int, gen: Callable[..., Iterator]):
    counter = 0
    miss = 0

    cache = Cache(policy, cap)

    for key, op in gen():
        counter += 1
        if op == GET:
            v = cache.get(key)
            if v is not None:
                assert key == v
            else:
                miss += 1
                cache.set(key, key)
        else:
            cache.set(key, key)

        if counter % 100000 == 0:
            print(".", end="", flush=True)
    print("")
    hr = (counter - miss) / counter
    print(f"---- theine({policy}) hit ratio: {hr:.3f}")
    return hr


def infinit_run(cap: int):
    z = Zipf(1.001, 10, 100000000)
    client = Cache("tlfu", cap)
    count = 0
    hit = 0
    while True:
        key = f"key:{z.get()}"
        data = client.get(key, None)
        if data is None:
            client.set(key, key, timedelta(seconds=randint(30, 20000)))
        else:
            hit += 1
            assert data == key
        count += 1
        if count % 100000 == 0:
            print(f"finish {count // 100000}, hit ratio: {hit / count}")


def init_plot(title):
    fig, ax = plt.subplots()
    ax.set_xlabel("capacity")
    ax.set_ylabel("hit ratio")
    ax.set_title(title)
    fig.set_figwidth(12)
    return fig, ax


def bench_and_plot(caps, key_gen, name):
    x = []
    y_tlfu = []
    y_clockpro = []
    y_lru = []

    for cap in caps:
        print(f"======= {name} cache size: {cap} =======")
        x.append(cap)
        y_tlfu.append(bench_theine("tlfu", cap, key_gen))
        y_clockpro.append(bench_theine("clockpro", cap, key_gen))
        y_lru.append(bench_theine("lru", cap, key_gen))

    fig, ax = init_plot(f"Hit Ratios - {name}")
    ax.plot(x, y_tlfu, "bo-", label="w-tlfu")
    ax.plot(x, y_clockpro, "r*-", label="clockpro")
    ax.plot(x, y_lru, "yh-", label="lru")
    ax.legend()
    fig.savefig(f"benchmarks/{name.lower()}.png", dpi=200)


# infinit_run(50000)

bench_and_plot([100, 200, 500, 1000, 2000, 5000, 10000, 20000], zipf_key_gen, "Zipf")
bench_and_plot(
    [50000, 100000, 200000, 300000, 500000, 800000, 1000000], ucb_key_gen, "UCB"
)
bench_and_plot(
    [1000000, 2000000, 3000000, 5000000, 6000000, 8000000], ds1_key_gen, "DS1"
)
bench_and_plot(
    [50000, 100000, 200000, 300000, 500000, 800000, 1000000], s3_key_gen, "S3"
)
bench_and_plot(
    [1000, 2000, 5000, 10000, 20000, 50000, 100000], scarab_key_gen, "SCARAB"
)

bench_and_plot([25000, 50000, 75000, 100000], scarab_1h_key_gen, "SCARAB1H")

bench_and_plot([10000, 20000, 50000, 80000, 100000], fb_key_gen, "FB")
