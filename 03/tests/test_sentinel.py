"""Opt-in real Redis/Sentinel check, without Docker or PostgreSQL.

Set TEST_REDIS_SERVER to a redis-server executable. All five processes use
temporary directories and loopback ports; no existing Redis data is touched.
"""
import json
import os
import socket
import subprocess
import time

import pytest
from redis import Redis
from redis.exceptions import RedisError

from flight_service.cache import FlightCache


def eventually(check, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (RedisError, OSError):
            pass
        time.sleep(0.1)
    raise AssertionError("Redis/Sentinel condition did not become true")


@pytest.mark.sentinel
def test_real_sentinel_failover_and_rejoin(tmp_path, monkeypatch, caplog):
    binary = os.getenv("TEST_REDIS_SERVER")
    if not binary:
        pytest.skip("Set TEST_REDIS_SERVER to run real Redis/Sentinel processes")
    caplog.set_level("INFO", logger="flight_service.cache")
    sockets = [socket.socket() for _ in range(5)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        ports = [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()
    processes, logs, configs = {}, {}, {}
    nodes = [Redis(host="127.0.0.1", port=p, decode_responses=True,
                   socket_timeout=2, socket_connect_timeout=0.2) for p in ports]

    def start(index):
        args = [binary, str(configs[index])]
        if index >= 2:
            args.append("--sentinel")
        processes[index] = subprocess.Popen(args, stdout=logs[index], stderr=subprocess.STDOUT)

    def stop(index):
        process = processes[index]
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    try:
        for index, port in enumerate(ports):
            folder = tmp_path / str(index)
            folder.mkdir()
            config = f"bind 127.0.0.1\nport {port}\ndir {folder}\nprotected-mode no\n"
            if index < 2:
                config += "appendonly yes\n"
                if index == 1:
                    config += f"replicaof 127.0.0.1 {ports[0]}\n"
            else:
                config += (f"sentinel monitor flight-cache 127.0.0.1 {ports[0]} 2\n"
                           "sentinel down-after-milliseconds flight-cache 1000\n"
                           "sentinel failover-timeout flight-cache 10000\n"
                           "sentinel parallel-syncs flight-cache 1\n")
            configs[index] = folder / "redis.conf"
            configs[index].write_text(config)
            logs[index] = (folder / "redis.log").open("w")
            start(index)
            eventually(nodes[index].ping, timeout=10)

        eventually(lambda: nodes[1].info("replication")["master_link_status"] == "up")
        for sentinel in nodes[2:]:
            eventually(lambda: sentinel.sentinel_masters()["flight-cache"]["num-other-sentinels"] == 2)
        monkeypatch.setenv("REDIS_SENTINELS", ",".join(f"127.0.0.1:{p}" for p in ports[2:]))
        monkeypatch.setenv("REDIS_MASTER_NAME", "flight-cache")
        cache = FlightCache(ttl=300)  # Keep this same client throughout failover.
        try:
            flight_key, search_key = "v1:flight:test", "v1:search:SVO:LED:all"
            cache.put(flight_key, {"available_seats": 10})
            cache.put(search_key, [{"available_seats": 10}])
            assert cache.get(flight_key) == {"available_seats": 10}
            assert cache.get(search_key) == [{"available_seats": 10}]
            assert 0 < cache.client.ttl(flight_key) <= 300
            assert 0 < cache.client.ttl(search_key) <= 300
            assert cache.client.wait(1, 5000) == 1

            expiring = FlightCache(client=cache.client, ttl=1)
            expiring.put("expiry-probe", {"temporary": True})
            assert expiring.get("expiry-probe") == {"temporary": True}
            eventually(lambda: expiring.get("expiry-probe") is None, timeout=5)

            stop(0)
            eventually(lambda: nodes[1].info("replication")["role"] == "master")
            eventually(lambda: all(s.sentinel_masters()["flight-cache"]["port"] == ports[1]
                                   for s in nodes[2:]))

            def write_after_failover():
                cache.put("v2:flight:test", {"available_seats": 9})
                return cache.get("v2:flight:test") == {"available_seats": 9}

            eventually(write_after_failover)
            assert json.loads(nodes[1].get("v2:flight:test")) == {"available_seats": 9}
            assert 0 < nodes[1].ttl("v2:flight:test") <= 300
            assert cache.client.connection_pool.master_address == ("127.0.0.1", ports[1])

            start(0)  # Use its persisted, writable config, as in Compose.

            def rejoined():
                info = nodes[0].info("replication")
                return (info["role"] == "slave" and info["master_link_status"] == "up"
                        and info["master_port"] == ports[1])

            eventually(rejoined)
            cache.put("rejoin-probe", {"replicated": True})
            assert cache.client.wait(1, 5000) == 1
            assert nodes[0].get("rejoin-probe") == nodes[1].get("rejoin-probe")
            stop(0)
            start(0)
            eventually(rejoined)  # CONFIG REWRITE must preserve its new role.
            stop(0)
            stop(1)
            assert cache.get("v2:flight:test") is None
            cache.put("unavailable-probe", {})  # Cache failure must be nonfatal.
            assert all(event in caplog.text for event in ("cache_hit", "cache_miss", "cache_error"))
        finally:
            cache.client.close()
    finally:
        for index, process in processes.items():
            if process.poll() is None:
                stop(index)
        for node in nodes:
            node.close()
        for log in logs.values():
            log.close()
