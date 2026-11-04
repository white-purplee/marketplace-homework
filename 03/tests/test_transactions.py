from concurrent.futures import ThreadPoolExecutor
from threading import Event
from uuid import uuid4

import grpc
import pytest
import sqlalchemy as sa
from common.errors import DomainError
from flight_service.models import flights, reservations, cache_state

pytestmark = pytest.mark.integration


def test_last_seat_is_reserved_once(stack):
    service, flight_id, *_ = stack
    with service.engine.begin() as conn:
        conn.execute(flights.update().where(flights.c.id == flight_id).values(available_seats=1, total_seats=1))
    def reserve(_):
        try:
            service.reserve(flight_id, 1, uuid4())
            return "ok"
        except DomainError as exc:
            return exc.code
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(8)))
    assert results.count("ok") == 1
    assert results.count(grpc.StatusCode.RESOURCE_EXHAUSTED) == 7
    assert service.get(flight_id)["available_seats"] == 0


def test_concurrent_duplicate_reserve_and_release(stack):
    service, flight_id, *_ = stack
    booking_id = uuid4()
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: service.reserve(flight_id, 2, booking_id), range(6)))
    assert len({r["id"] for r in results}) == 1
    assert service.get(flight_id)["available_seats"] == 8
    with pytest.raises(DomainError) as error:
        service.reserve(flight_id, 3, booking_id)
    assert error.value.code == grpc.StatusCode.ALREADY_EXISTS
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda _: service.release(booking_id), range(6)))
    assert service.get(flight_id)["available_seats"] == 10
    with pytest.raises(DomainError) as error:
        service.reserve(flight_id, 2, booking_id)
    assert error.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_compensation_fences_a_late_reserve(stack):
    service, flight_id, *_ = stack
    booking_id = uuid4()
    service.release(booking_id)
    with pytest.raises(DomainError):
        service.reserve(flight_id, 1, booking_id)
    assert service.get(flight_id)["available_seats"] == 10


def test_failed_reserve_rolls_back_all_changes(stack):
    service, flight_id, *_ = stack
    booking_id = uuid4()
    with service.engine.connect() as conn:
        before = conn.execute(sa.select(cache_state.c.version)).scalar_one()
    with pytest.raises(DomainError):
        service.reserve(flight_id, 11, booking_id)
    with service.engine.connect() as conn:
        assert conn.execute(sa.select(reservations).where(reservations.c.booking_id == booking_id)).first() is None
        assert conn.execute(sa.select(cache_state.c.version)).scalar_one() == before


def test_database_constraints_prevent_invalid_inventory(stack):
    from sqlalchemy.exc import IntegrityError
    service, flight_id, *_ = stack
    for values in [{"available_seats": -1}, {"available_seats": 11}, {"price_minor": 0}, {"total_seats": 0}]:
        with pytest.raises(IntegrityError), service.engine.begin() as conn:
            conn.execute(flights.update().where(flights.c.id == flight_id).values(**values))


def test_cache_version_rejects_late_population(stack):
    service, flight_id, _, _, cache = stack
    read_version, resume = Event(), Event()
    original_get = cache.get
    paused = [False]
    def blocking_get(key):
        result = original_get(key)
        if not paused[0]:
            paused[0] = True
            read_version.set()
            assert resume.wait(5)
        return result
    cache.get = blocking_get
    with ThreadPoolExecutor(max_workers=1) as pool:
        old = pool.submit(service.get, flight_id)
        assert read_version.wait(5)
        service.update(flight_id, 750000)
        resume.set()
        assert old.result(timeout=5)["price_minor"] == 500000
    assert service.get(flight_id)["price_minor"] == 750000
    service.get(flight_id)
    assert cache.hits >= 1


def test_mutations_invalidate_search_results(stack):
    service, flight_id, *_ = stack
    first = next(f for f in service.search("SVO", "LED") if f["id"] == str(flight_id))
    assert first["available_seats"] == 10
    booking_id = uuid4()
    service.reserve(flight_id, 2, booking_id)
    assert next(f for f in service.search("SVO", "LED") if f["id"] == str(flight_id))["available_seats"] == 8
    service.release(booking_id)
    assert next(f for f in service.search("SVO", "LED") if f["id"] == str(flight_id))["available_seats"] == 10
