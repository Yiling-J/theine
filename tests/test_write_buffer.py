import threading
import random
from theine.write_buffer import WriteBuffer
from threading import Lock
from time import sleep


def test_write_buffer() -> None:
    cq = []
    buffer = WriteBuffer(lambda s: cq.append(s), lambda s: s, Lock(), False)
    for i in range(40):
        buffer.add(i, 0)
    assert cq == [[(i, 0)] for i in range(40)]


def test_write_buffer_busy_lock() -> None:
    cq = []
    lock = Lock()
    buffer = WriteBuffer(lambda s: cq.append(s), lambda s: s, lock, False)
    lc = 0
    lock.acquire()
    for i in range(40):
        if lc == 6:
            lock.release()
            lc = 0
            buffer.add(i, 0)
            lock.acquire()
        else:
            buffer.add(i, 0)
            lc += 1
    lock.release()
    buffer.add(999, 999)
    assert cq == [
        [(i, 0) for i in range(7)],
        [(i + 7, 0) for i in range(7)],
        [(i + 14, 0) for i in range(7)],
        [(i + 21, 0) for i in range(7)],
        [(i + 28, 0) for i in range(7)],
        [(i + 35, 0) for i in range(5)] + [(999, 999)],
    ]


def test_write_buffer_multithreaded() -> None:
    cq = []
    buffer = WriteBuffer(lambda s: cq.append(s), lambda s: s, Lock(), False)

    def add_items(start):
        for i in range(10):
            sleep(random.randint(5, 30) / 1000)
            buffer.add(start * 10 + i, 0)

    threads = []
    for start in range(4):
        thread = threading.Thread(target=add_items, args=[start])
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    m = {}
    for i in cq:
        for j in i:
            m[j[0]] = True
    assert len(m) == 40
    l = list(m.keys())
    l.sort()
    assert l == list(range(40))


def test_write_buffer_busy_lock_multithreaded() -> None:
    cq = []
    lock = Lock()
    buffer = WriteBuffer(lambda s: cq.append(s), lambda s: s, lock, False)

    def add_items(start):
        for i in range(10):
            sleep(random.randint(5, 30) / 1000)
            buffer.add(start * 10 + i, 0)

    done = False

    def locking():
        nonlocal done
        while not done:
            lock.acquire()
            sleep(5 / 1000)
            lock.release()
            sleep(2 / 1000)

    ls = threading.Thread(target=locking, args=[])
    ls.start()
    threads = []
    for start in range(4):
        thread = threading.Thread(target=add_items, args=[start])
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    done = True
    ls.join()
    buffer.add(999, 999)

    m = {}
    for i in cq:
        for j in i:
            m[j[0]] = True
    assert len(m) == 41
    l = list(m.keys())
    l.sort()
    assert l == list(range(40)) + [999]
