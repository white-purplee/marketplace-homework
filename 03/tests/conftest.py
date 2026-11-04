import os
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from common.db import make_engine


class MemoryCache:
    def __init__(self):
        self.data, self.hits, self.misses = {}, 0, 0
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            value = self.data.get(key)
            if value is None:
                self.misses += 1
            else:
                self.hits += 1
            return value

    def put(self, key, value):
        with self.lock:
            self.data[key] = value


@pytest.fixture(scope="session")
def databases():
    urls = [os.getenv("TEST_FLIGHT_DATABASE_URL"), os.getenv("TEST_BOOKING_DATABASE_URL")]
    if not all(urls):
        pytest.skip("Set TEST_FLIGHT_DATABASE_URL and TEST_BOOKING_DATABASE_URL to disposable PostgreSQL databases")
    if any(not url.rsplit("/", 1)[-1].endswith("_test") for url in urls):
        pytest.fail("Integration database names must end with _test")
    for service, url in zip(["flight_service", "booking_service"], urls):
        env = {**os.environ, "DATABASE_URL": url, "PYTHONPATH": os.pathsep.join([str(Path.cwd()), str(Path.cwd() / 'generated')])}
        subprocess.run([sys.executable, "-m", "alembic", "-c", f"{service}/alembic.ini", "upgrade", "head"], env=env, check=True)
    return urls


@pytest.fixture
def stack(databases):
    from flight_service.service import FlightService
    from flight_service.models import flights
    flight_engine, booking_engine = [make_engine(url) for url in databases]
    lock_engine = make_engine(databases[1], locks=True)
    cache = MemoryCache()
    flight_service = FlightService(flight_engine, cache)
    flight_id = uuid4()
    start = datetime.now(timezone.utc) + timedelta(days=30)
    with flight_engine.begin() as conn:
        conn.execute(flights.insert().values(
            id=flight_id, flight_number=uuid4().hex[:12], airline="Test Airline", origin="SVO", destination="LED",
            departure_at=start, arrival_at=start + timedelta(hours=2), total_seats=10, available_seats=10,
            price_minor=500000, status="SCHEDULED"))
    yield flight_service, flight_id, booking_engine, lock_engine, cache
    for engine in (flight_engine, booking_engine, lock_engine):
        engine.dispose()
