import json
from redis.exceptions import ConnectionError
from flight_service.cache import FlightCache


def test_cache_has_ttl_and_redis_failure_is_nonfatal():
    class Redis:
        value = None
        def get(self, key):
            return self.value
        def set(self, key, value, ex):
            assert ex == 300
            self.value = value
    redis = Redis()
    cache = FlightCache(redis)
    assert cache.get("key") is None
    cache.put("key", {"price_minor": 500000})
    assert cache.get("key") == {"price_minor": 500000}
    redis.value = "invalid json"
    assert cache.get("key") is None
    def broken(*args, **kwargs):
        raise ConnectionError("redis unavailable")
    redis.get = redis.set = broken
    assert cache.get("key") is None
    cache.put("key", {})
