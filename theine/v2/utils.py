def round_up_power_of_2(v: int) -> int:
    if v == 0:
        return 1
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v += 1
    return v
