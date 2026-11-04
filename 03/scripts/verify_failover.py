"""Host-side Sentinel check. Requires Docker Compose and a running, idle stack."""
import argparse
import json
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICES = {"172.30.87.10": "redis-primary", "172.30.87.11": "redis-replica"}
FLIGHT_ID = "11111111-1111-4111-8111-111111111111"


def compose(*args):
    result = subprocess.run(["docker", "compose", *args], cwd=ROOT,
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"Compose {args} failed:\n{result.stderr}")
    return result.stdout.strip()


def redis(service, *args):
    return compose("exec", "-T", service, "redis-cli", "--raw", *args)


def master():
    lines = compose("exec", "-T", "sentinel-1", "redis-cli", "-p", "26379", "--raw",
                    "SENTINEL", "get-master-addr-by-name", "flight-cache").splitlines()
    return lines[0] if len(lines) == 2 else None


def api(url, path):
    with urllib.request.urlopen(url + path, timeout=15) as response:
        assert response.status == 200
        return json.load(response)


def eventually(check, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.5)
    raise AssertionError("Timed out waiting for Redis replication/failover")


def check_application_cache(url, primary):
    # Only inspect PostgreSQL here. Cache writes MUST come from the already
    # running REST -> gRPC service, not a newly constructed FlightCache client.
    probe = """import json,os,sqlalchemy as sa
from common.db import make_engine
from flight_service.models import cache_state
engine=make_engine()
with engine.connect() as c: version=c.execute(sa.select(cache_state.c.version)).scalar_one()
print(json.dumps({'version':version,'ttl':int(os.getenv('CACHE_TTL_SECONDS','300'))}))
engine.dispose()
"""
    settings = json.loads(compose("exec", "-T", "flight-service", "python", "-c", probe))
    keys = [f"v{settings['version']}:flight:{FLIGHT_ID}",
            f"v{settings['version']}:search:SVO:LED:all"]
    redis(primary, "DEL", *keys)
    flight = api(url, "/flights/" + FLIGHT_ID)
    search = api(url, "/flights?origin=SVO&destination=LED")["items"]
    cached_flight, cached_search = [json.loads(redis(primary, "GET", key)) for key in keys]
    for field in ("id", "available_seats", "price_minor", "status"):
        assert cached_flight[field] == flight[field], "Cached flight differs from REST response"
    fields = ("id", "available_seats", "price_minor", "status")
    assert [[row[f] for f in fields] for row in cached_search] == [[row[f] for f in fields] for row in search]
    for key in keys:
        assert 0 < int(redis(primary, "TTL", key)) <= settings["ttl"], "Cache TTL is missing/incorrect"
    # A second request also exercises the warm-cache path in the live service.
    assert api(url, "/flights/" + FLIGHT_ID) == flight
    return keys


def replica_connected(service, primary_ip):
    info = dict(line.split(":", 1) for line in redis(service, "INFO", "replication").splitlines()
                if ":" in line)
    return (info.get("role") == "slave" and info.get("master_link_status") == "up"
            and info.get("master_host") == primary_ip and info.get("master_port") == "6379")


def wait_for_application_cache(url, primary):
    # Sentinel may announce the new primary before the live client's next
    # discovery. Preserve the original bounded allowance for reconnection.
    deadline = time.monotonic() + 15
    while True:
        try:
            return check_application_cache(url, primary)
        except (AssertionError, json.JSONDecodeError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()
    before = master()
    if before not in SERVICES:
        raise SystemExit("Unexpected Sentinel primary: " + str(before))
    stopped = SERVICES[before]
    replica = next(service for ip, service in SERVICES.items() if ip != before)
    eventually(lambda: replica_connected(replica, before))
    keys = check_application_cache(args.url, stopped)
    eventually(lambda: all(redis(replica, "GET", key) == redis(stopped, "GET", key) for key in keys))
    try:
        compose("stop", stopped)
        started = time.monotonic()
        after = before
        while time.monotonic() - started < 45:
            api(args.url, "/flights?origin=SVO&destination=LED")
            after = master()
            if after and after != before:
                break
            time.sleep(0.5)
        assert after != before and after in SERVICES, "Sentinel did not promote a replica"
        # This live client was connected to the previous primary before its stop.
        wait_for_application_cache(args.url, SERVICES[after])
    finally:
        compose("start", stopped)
    # Role alone is insufficient: require a working replication link to the
    # promoted primary and an actual replicated cache entry.
    eventually(lambda: replica_connected(stopped, after))
    keys = check_application_cache(args.url, SERVICES[after])
    eventually(lambda: all(redis(stopped, "GET", key) == redis(SERVICES[after], "GET", key) for key in keys))
    print(json.dumps({"result": "passed", "previous_primary": before, "new_primary": after,
                      "api_during_failover": "available", "live_service_cache": "reconnected",
                      "previous_primary_rejoined": "replica with link up and replicated keys"}, indent=2))


if __name__ == "__main__":
    main()
