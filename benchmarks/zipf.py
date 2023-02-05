import math
import random


class Zipf:
    def __init__(self, s: float, v: float, imax: int):
        if s <= 1 or v < 1:
            raise
        self.imax = float(imax)
        self.v = v
        self.q = s
        self.oneminus_q = 1.0 - self.q
        self.oneminus_qinv = 1.0 / self.oneminus_q
        self.hxm = self.h(self.imax + 0.5)
        self.hx0minus_hxm = (
            self.h(0.5) - math.exp(math.log(self.v) * (-self.q)) - self.hxm
        )
        self.s = 1 - self.hinv(self.h(1.5) - math.exp(-self.q * math.log(self.v + 1.0)))

    def h(self, x: float) -> float:
        return math.exp(self.oneminus_q * math.log(self.v + x)) * self.oneminus_qinv

    def hinv(self, x: float) -> float:
        return math.exp(self.oneminus_qinv * math.log(self.oneminus_q * x)) - self.v

    def get(self) -> int:
        k = 0
        while True:
            r = random.random()
            ur = self.hxm + r * self.hx0minus_hxm
            x = self.hinv(ur)
            k = math.floor(x + 0.5)
            if k - x <= self.s:
                break
            if ur >= self.h(k + 0.5) - math.exp(-math.log(k + self.v) * self.q):
                break
        return int(k)
