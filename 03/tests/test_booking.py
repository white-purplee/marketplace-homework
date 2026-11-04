from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import grpc
import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from booking_service.api import create_app
from booking_service.models import operations, bookings
from booking_service.service import ApiError, BookingService
from booking_service.resilience import LocalRpcError
from common.errors import DomainError

pytestmark = pytest.mark.integration


class DirectClient:
    """Use actual Flight PostgreSQL while selecting failure boundaries precisely."""
    def __init__(self, service):
        self.service = service

    def _call(self, action, *args):
        try:
            return action(*args)
        except DomainError as exc:
            raise LocalRpcError(exc.code, exc.message) from exc

    def get(self, flight_id):
        return self._call(self.service.get, flight_id)

    def search(self, *args):
        return self._call(self.service.search, *args)

    def reserve(self, *args):
        return self._call(self.service.reserve, *args)

    def release(self, booking_id):
        return self._call(self.service.release, booking_id)


def setup_booking(stack, client_type=DirectClient):
    flight, flight_id, engine, locks, _ = stack
    service = BookingService(engine, locks, client_type(flight))
    body = {"user_id": "user-" + uuid4().hex, "flight_id": str(flight_id), "passenger_name": "Test Passenger",
            "passenger_email": "passenger@example.com", "seat_count": 2}
    return service, body


def test_rest_workflow_and_idempotency(stack):
    service, body = setup_booking(stack)
    key = uuid4().hex
    with TestClient(create_app(service, recovery=False)) as client:
        search = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
        assert search.status_code == 200
        response = client.post("/bookings", json=body, headers={"Idempotency-Key": key})
        assert response.status_code == 201, response.text
        booking = response.json()
        assert booking["status"] == "CONFIRMED" and booking["total_price_minor"] == 1000000
        retry = client.post("/bookings", json=body, headers={"Idempotency-Key": key})
        assert retry.status_code == 201 and retry.json()["id"] == booking["id"]
        conflict = client.post("/bookings", json={**body, "seat_count": 3}, headers={"Idempotency-Key": key})
        assert conflict.status_code == 409
        assert client.get(f"/bookings/{booking['id']}").status_code == 200
        assert len(client.get("/bookings", params={"user_id": body["user_id"]}).json()["items"]) == 1
        stack[0].update(stack[1], 700000)
        assert client.get(f"/bookings/{booking['id']}").json()["total_price_minor"] == 1000000
        for _ in range(2):
            cancelled = client.post(f"/bookings/{booking['id']}/cancel")
            assert cancelled.status_code == 200 and cancelled.json()["status"] == "CANCELLED"
        assert stack[0].get(stack[1])["available_seats"] == 10
        assert client.get("/health/ready").status_code == 200


def test_rest_validation_and_not_found(stack):
    service, body = setup_booking(stack)
    with TestClient(create_app(service, recovery=False)) as client:
        for change in [{"seat_count": 0}, {"seat_count": True}, {"passenger_email": "bad"}, {"user_id": " "}]:
            result = client.post("/bookings", json={**body, **change}, headers={"Idempotency-Key": uuid4().hex})
            assert result.status_code == 422
            assert result.json()["code"] == "INVALID_ARGUMENT"
        assert client.post("/bookings", json=body).status_code == 422
        assert client.get(f"/bookings/{uuid4()}").status_code == 404
        assert client.get("/flights?origin=bad&destination=LED").status_code == 422
        assert client.get("/flights?origin=SVO&destination=LED&date=not-a-date").status_code == 422


def test_insufficient_seats_does_not_create_booking(stack):
    service, body = setup_booking(stack)
    body["seat_count"] = 11
    with pytest.raises(ApiError) as error:
        service.create(body, uuid4().hex)
    assert error.value.status == 409
    assert service.list(body["user_id"]) == []
    assert service.operation(error.value.operation_id)["state"] == "FAILED"
    assert stack[0].get(stack[1])["available_seats"] == 10


def test_process_interruption_after_reserve_is_compensated(stack):
    class CrashAfterReserve(DirectClient):
        def reserve(self, *args):
            super().reserve(*args)
            raise SystemExit("simulated process exit")
    service, body = setup_booking(stack, CrashAfterReserve)
    key = uuid4().hex
    with pytest.raises(SystemExit):
        service.create(body, key)
    assert stack[0].get(stack[1])["available_seats"] == 8
    restarted = BookingService(service.engine, service.lock_engine, DirectClient(stack[0]))
    restarted.recover_once()
    assert stack[0].get(stack[1])["available_seats"] == 10
    assert restarted.list(body["user_id"]) == []
    with pytest.raises(ApiError):
        restarted.create(body, key)


def test_failed_compensation_survives_until_next_recovery(stack):
    class AmbiguousClient(DirectClient):
        def reserve(self, *args):
            super().reserve(*args)
            raise LocalRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "response lost")
        def release(self, booking_id):
            raise LocalRpcError(grpc.StatusCode.UNAVAILABLE, "dependency unavailable")
    service, body = setup_booking(stack, AmbiguousClient)
    with pytest.raises(ApiError) as error:
        service.create(body, uuid4().hex)
    op_id = error.value.operation_id
    assert service.operation(op_id)["state"] == "COMPENSATING"
    service.client = DirectClient(stack[0])
    with service.engine.begin() as conn:
        conn.execute(operations.update().where(operations.c.id == op_id).values(next_attempt_at=sa.func.now()))
    service.recover_once()
    assert service.operation(op_id)["state"] == "FAILED"
    assert stack[0].get(stack[1])["available_seats"] == 10


def test_cancellation_recovers_after_release_before_db_update(stack):
    service, body = setup_booking(stack)
    booking = service.create(body, uuid4().hex)
    class CrashAfterRelease(DirectClient):
        def release(self, booking_id):
            super().release(booking_id)
            raise SystemExit("simulated process exit")
    service.client = CrashAfterRelease(stack[0])
    with pytest.raises(SystemExit):
        service.cancel(booking["id"])
    assert service.get(booking["id"])["status"] == "CONFIRMED"
    service.client = DirectClient(stack[0])
    service.recover_once()
    assert service.get(booking["id"])["status"] == "CANCELLED"
    assert stack[0].get(stack[1])["available_seats"] == 10


def test_operation_lock_prevents_http_worker_race(stack):
    service, body = setup_booking(stack)
    op = service._new_operation("create:" + uuid4().hex, body, "CREATE")
    with service._lock(op["booking_id"], op["id"]):
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(service.recover_once).result(timeout=5) is None
        assert service.operation(op["id"])["state"] == "PREPARED"
    service.recover_once()
    assert service.operation(op["id"])["state"] == "SUCCEEDED"


def test_http_open_circuit_is_503(stack):
    service, _ = setup_booking(stack)
    def unavailable(_):
        raise LocalRpcError(grpc.StatusCode.UNAVAILABLE, "circuit open")
    service.client.get = unavailable
    with TestClient(create_app(service, recovery=False)) as client:
        result = client.get(f"/flights/{stack[1]}")
    assert result.status_code == 503


def test_booking_insert_failure_leaves_durable_compensation(stack):
    from sqlalchemy.exc import OperationalError
    service, body = setup_booking(stack)
    def fail_insert(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("INSERT INTO bookings"):
            raise OperationalError(statement, {}, Exception("Simulated booking write failure"))
    sa.event.listen(service.engine, "before_cursor_execute", fail_insert)
    try:
        with pytest.raises(ApiError) as error:
            service.create(body, uuid4().hex)
        op_id = error.value.operation_id
        assert service.operation(op_id)["state"] == "COMMITTING"
        assert service.list(body["user_id"]) == []
    finally:
        sa.event.remove(service.engine, "before_cursor_execute", fail_insert)
    service.recover_once()
    assert service.operation(op_id)["state"] == "FAILED"
    assert stack[0].get(stack[1])["available_seats"] == 10


def test_rest_to_real_grpc_and_database(stack):
    from flight_service.server import build_server
    from booking_service.flight_client import FlightClient
    from booking_service.resilience import ResilienceInterceptor
    server, port = build_server(stack[0], "test-secret", "127.0.0.1:0")
    server.start()
    remote = FlightClient(f"127.0.0.1:{port}", ResilienceInterceptor("test-secret"))
    service, body = setup_booking(stack)
    service.client = remote
    try:
        with TestClient(create_app(service, recovery=False)) as client:
            result = client.post("/bookings", json=body, headers={"Idempotency-Key": uuid4().hex})
            assert result.status_code == 201, result.text
            booking_id = result.json()["id"]
            assert client.get(f"/flights/{stack[1]}").json()["available_seats"] == 8
            assert client.post(f"/bookings/{booking_id}/cancel").status_code == 200
            assert client.get(f"/flights/{stack[1]}").json()["available_seats"] == 10
    finally:
        remote.close()
        server.stop(0).wait()
