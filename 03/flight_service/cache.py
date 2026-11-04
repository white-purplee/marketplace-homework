import json
import logging
import os
from redis.sentinel import Sentinel
from redis.exceptions import RedisError
from redis.backoff import NoBackoff
from redis.retry import Retry

log = logging.getLogger(__name__)


class FlightCache:
    def __init__(self, client=None, ttl=None):
        self.ttl = int(ttl or os.getenv("CACHE_TTL_SECONDS", "300"))
        if not 1 <= self.ttl <= 600:
            raise ValueError("CACHE_TTL_SECONDS must be between 1 and 600")
        if client is None:
            addresses = [item.rsplit(":", 1) for item in os.getenv(
                "REDIS_SENTINELS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
            ).split(",")]
            sentinel = Sentinel(
                [(host, int(port)) for host, port in addresses],
                # Six failed contacts (get + put across three Sentinels) must
                # fit comfortably inside the one-second gRPC attempt budget.
                socket_timeout=0.05, socket_connect_timeout=0.05,
                retry=Retry(NoBackoff(), 0),
                sentinel_kwargs={"socket_timeout": 0.05, "socket_connect_timeout": 0.05,
                                 "retry": Retry(NoBackoff(), 0)},
            )
            client = sentinel.master_for(
                os.getenv("REDIS_MASTER_NAME", "flight-cache"), decode_responses=True,
                socket_timeout=0.05, socket_connect_timeout=0.05,
                retry=Retry(NoBackoff(), 0),
            )
        self.client = client

    def get(self, key):
        try:
            data = self.client.get(key)
            if data is not None:
                log.info("cache_hit key=%s", key)
                return json.loads(data)
            log.info("cache_miss key=%s", key)
        except (RedisError, ValueError, TypeError):
            log.warning("cache_error operation=get key=%s", key)
        return None

    def put(self, key, value):
        try:
            self.client.set(key, json.dumps(value), ex=self.ttl)
        except RedisError:
            log.warning("cache_error operation=set key=%s", key)
