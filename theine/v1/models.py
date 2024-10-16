class CacheStats:
    def __init__(self, total: int, hit: int):
        self.request_count = total
        self.hit_count = hit
        self.miss_count = self.request_count - self.hit_count
        self.hit_rate = self.hit_count / self.request_count
