import hashlib
import json
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import grpc
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from booking_service.models import bookings, operations

log = logging.getLogger(__name__)
TERMINAL = ("SUCCEEDED", "FAILED")
HTTP_CODES = {
    "INVALID_ARGUMENT": 422, "NOT_FOUND": 404, "RESOURCE_EXHAUSTED": 409,
    "FAILED_PRECONDITION": 409, "ALREADY_EXISTS": 409,
    "UNAVAILABLE": 503, "DEADLINE_EXCEEDED": 504, "UNAUTHENTICATED": 502,
    "PERMISSION_DENIED": 502, "INTERNAL": 502, "UNKNOWN": 502,
}


class ApiError(Exception):
    def __init__(self, status, code, message, operation_id=None):
        super().__init__(message)
        self.status, self.code, self.message, self.operation_id = status, code, message, operation_id


def rpc_error(exc, operation_id=None):
    code = exc.code().name
    message = exc.details() if code in {"NOT_FOUND", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ALREADY_EXISTS", "INVALID_ARGUMENT"} else "Flight service request failed; retry with the same idempotency key"
    return ApiError(HTTP_CODES.get(code, 502), code, message, operation_id)


def request_hash(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class BookingService:
    def __init__(self, engine, lock_engine, client):
        self.engine, self.lock_engine, self.client = engine, lock_engine, client

    @contextmanager
    def _lock(self, booking_id, operation_id):
        # Separate pool: a lock held during RPC cannot starve normal transactions.
        key = booking_id.int % (2 ** 63)
        with self.lock_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            locked = conn.execute(sa.text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar_one()
            if not locked:
                raise ApiError(503, "OPERATION_IN_PROGRESS", "Operation is already being processed", operation_id)
            try:
                yield
            finally:
                try:
                    conn.execute(sa.text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                except SQLAlchemyError:
                    conn.invalidate()
                    log.warning("operation_lock_connection_lost operation_id=%s", operation_id)

    def get(self, booking_id):
        with self.engine.connect() as conn:
            row = conn.execute(sa.select(bookings).where(bookings.c.id == booking_id)).mappings().first()
        if row is None:
            raise ApiError(404, "NOT_FOUND", "Booking not found")
        return dict(row)

    def list(self, user_id):
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(sa.select(bookings).where(bookings.c.user_id == user_id)
                    .order_by(bookings.c.created_at, bookings.c.id)).mappings()]

    def operation(self, operation_id):
        with self.engine.connect() as conn:
            row = conn.execute(sa.select(operations).where(operations.c.id == operation_id)).mappings().first()
        if row is None:
            raise ApiError(404, "NOT_FOUND", "Operation not found")
        return dict(row)

    def _new_operation(self, key, payload, kind, booking_id=None):
        digest = request_hash(payload)
        with self.engine.begin() as conn:
            conn.execute(insert(operations).values(
                id=uuid4(), booking_id=booking_id or uuid4(), kind=kind, idempotency_key=key,
                request_hash=digest, payload=payload, state="PREPARED" if kind == "CREATE" else "RELEASING",
            ).on_conflict_do_nothing(index_elements=[operations.c.idempotency_key]))
            row = conn.execute(sa.select(operations).where(operations.c.idempotency_key == key)).mappings().one()
            if row["request_hash"] != digest:
                raise ApiError(409, "IDEMPOTENCY_CONFLICT", "Idempotency key was used with a different request", row["id"])
            return dict(row)

    def _transition(self, op_id, expected, state, **values):
        with self.engine.begin() as conn:
            result = conn.execute(sa.update(operations).where(operations.c.id == op_id, operations.c.state.in_(expected))
                                  .values(state=state, updated_at=sa.func.now(), **values))
            if result.rowcount != 1:
                raise ApiError(503, "OPERATION_CHANGED", "Operation state changed; check its result", op_id)
        log.info("saga_transition operation_id=%s from=%s to=%s", op_id, expected, state)

    def _failure(self, op):
        code = op["error_code"] or "UNAVAILABLE"
        return ApiError(HTTP_CODES.get(code, 503), code, op["error_message"] or "Operation failed", op["id"])

    def _compensate(self, op):
        try:
            self.client.release(op["booking_id"])
        except grpc.RpcError:
            self._schedule(op["id"])
            log.warning("compensation_pending operation_id=%s", op["id"])
            return
        self._transition(op["id"], ["COMPENSATING"], "FAILED")

    def _schedule(self, op_id):
        with self.engine.begin() as conn:
            conn.execute(sa.update(operations).where(operations.c.id == op_id, operations.c.state.not_in(TERMINAL)).values(
                next_attempt_at=datetime.now(timezone.utc) + timedelta(seconds=3), updated_at=sa.func.now()))

    def create(self, payload, key):
        op = self._new_operation("create:" + key, payload, "CREATE")
        with self._lock(op["booking_id"], op["id"]):
            return self._create_locked(self.operation(op["id"]))

    def _create_locked(self, op, *, recovering=False):
        if op["state"] == "SUCCEEDED":
            return self.get(op["booking_id"])
        if op["state"] == "FAILED":
            raise self._failure(op)
        if op["state"] == "COMPENSATING":
            self._compensate(op)
            raise self._failure(op)
        if recovering and op["state"] in ("RESERVING", "COMMITTING"):
            self._transition(op["id"], [op["state"]], "COMPENSATING",
                             error_code="UNAVAILABLE", error_message="Interrupted booking was compensated")
            op = self.operation(op["id"])
            self._compensate(op)
            raise self._failure(op)
        if op["state"] == "PREPARED":
            try:
                flight = self.client.get(op["payload"]["flight_id"])
            except grpc.RpcError as exc:
                error = rpc_error(exc, op["id"])
                self._transition(op["id"], ["PREPARED"], "FAILED", error_code=error.code, error_message=error.message)
                raise error
            payload = {**op["payload"], "price_minor": flight["price_minor"]}
            total = payload["seat_count"] * payload["price_minor"]
            if not 1 <= total <= 9223372036854775807:
                self._transition(op["id"], ["PREPARED"], "FAILED", error_code="INVALID_ARGUMENT",
                                 error_message="Total price exceeds supported money range")
                raise self._failure(self.operation(op["id"]))
            self._transition(op["id"], ["PREPARED"], "RESERVING", payload=payload)
            op = self.operation(op["id"])
        elif op["state"] in ("RESERVING", "COMMITTING"):
            # A repeated HTTP request can also encounter an interrupted operation.
            return self._create_locked(op, recovering=True)
        try:
            self.client.reserve(op["payload"]["flight_id"], op["payload"]["seat_count"], op["booking_id"])
        except grpc.RpcError as exc:
            error = rpc_error(exc, op["id"])
            self._transition(op["id"], ["RESERVING"], "COMPENSATING", error_code=error.code, error_message=error.message)
            self._compensate(self.operation(op["id"]))
            raise error
        self._transition(op["id"], ["RESERVING"], "COMMITTING")
        try:
            with self.engine.begin() as conn:
                state = conn.execute(sa.select(operations.c.state).where(operations.c.id == op["id"]).with_for_update()).scalar_one()
                if state != "COMMITTING":
                    raise ApiError(503, "OPERATION_CHANGED", "Booking is being compensated", op["id"])
                body = op["payload"]
                from uuid import UUID
                result = conn.execute(sa.insert(bookings).values(
                    id=op["booking_id"], user_id=body["user_id"], flight_id=UUID(body["flight_id"]),
                    passenger_name=body["passenger_name"], passenger_email=body["passenger_email"],
                    seat_count=body["seat_count"], total_price_minor=body["seat_count"] * body["price_minor"], status="CONFIRMED",
                ).returning(bookings)).mappings().one()
                conn.execute(sa.update(operations).where(operations.c.id == op["id"]).values(state="SUCCEEDED", updated_at=sa.func.now()))
        except SQLAlchemyError as exc:
            # Commit may have succeeded even if its acknowledgement was lost.
            # Recovery checks the durable journal instead of issuing a blind release.
            raise ApiError(503, "UNAVAILABLE", "Booking result is uncertain; repeat with the same key", op["id"]) from exc
        log.info("saga_succeeded operation_id=%s", op["id"])
        return dict(result)

    def cancel(self, booking_id):
        booking = self.get(booking_id)
        if booking["status"] == "CANCELLED":
            return booking
        op = self._new_operation(f"cancel:{booking_id}", {"booking_id": str(booking_id)}, "CANCEL", booking_id)
        with self._lock(booking_id, op["id"]):
            return self._cancel_locked(self.operation(op["id"]))

    def _cancel_locked(self, op):
        if op["state"] == "SUCCEEDED":
            return self.get(op["booking_id"])
        try:
            self.client.release(op["booking_id"])
        except grpc.RpcError as exc:
            self._schedule(op["id"])
            raise rpc_error(exc, op["id"])
        with self.engine.begin() as conn:
            state = conn.execute(sa.select(operations.c.state).where(operations.c.id == op["id"]).with_for_update()).scalar_one()
            if state != "RELEASING":
                raise ApiError(503, "OPERATION_CHANGED", "Cancellation state changed", op["id"])
            result = conn.execute(sa.update(bookings).where(bookings.c.id == op["booking_id"]).values(
                status="CANCELLED", updated_at=sa.func.now()).returning(bookings)).mappings().one()
            conn.execute(sa.update(operations).where(operations.c.id == op["id"]).values(state="SUCCEEDED", updated_at=sa.func.now()))
        return dict(result)

    def recover_once(self):
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(operations.c.id, operations.c.booking_id).where(
                operations.c.state.not_in(TERMINAL), operations.c.next_attempt_at <= sa.func.now()
            ).order_by(operations.c.next_attempt_at, operations.c.id).limit(50)).all()
        for op_id, booking_id in rows:
            try:
                with self._lock(booking_id, op_id):
                    op = self.operation(op_id)
                    if op["state"] in TERMINAL:
                        continue
                    log.info("saga_recover operation_id=%s state=%s", op_id, op["state"])
                    if op["kind"] == "CREATE":
                        self._create_locked(op, recovering=True)
                    else:
                        self._cancel_locked(op)
            except ApiError as exc:
                log.info("saga_recovery_result operation_id=%s code=%s", op_id, exc.code)
            except SQLAlchemyError:
                log.exception("saga_recovery_database_error operation_id=%s", op_id)


class RecoveryWorker:
    def __init__(self, service, interval=2):
        if interval <= 0:
            raise ValueError("Recovery interval must be positive")
        self.service, self.interval = service, interval
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="saga-recovery", daemon=True)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.service.recover_once()
            except Exception:
                log.exception("saga_recovery_cycle_error")
            self.stop_event.wait(self.interval)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=10)
