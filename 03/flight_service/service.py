import logging
import re
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

import grpc
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from common.errors import DomainError, invalid, uuid_value
from flight_service.models import flights, reservations, guards, cache_state

log = logging.getLogger(__name__)


def flight_json(row):
    data = dict(row)
    for key in ("id",):
        data[key] = str(data[key])
    for key in ("departure_at", "arrival_at", "created_at", "updated_at"):
        data[key] = data[key].astimezone(timezone.utc).isoformat()
    return data


class FlightService:
    def __init__(self, engine, cache):
        self.engine, self.cache = engine, cache

    def _read(self, suffix, query, *, single=False):
        # Version and data share one MVCC snapshot, including a cache-miss race.
        with self.engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
            with conn.begin():
                conn.execute(sa.text("SET TRANSACTION READ ONLY"))
                version = conn.execute(sa.select(cache_state.c.version)).scalar_one()
                key = f"v{version}:{suffix}"
                result = self.cache.get(key)
                if result is not None:
                    return result
                rows = conn.execute(query).mappings().all()
                if single and not rows:
                    raise DomainError(grpc.StatusCode.NOT_FOUND, "Flight not found")
                result = flight_json(rows[0]) if single else [flight_json(r) for r in rows]
        self.cache.put(key, result)
        return result

    def get(self, flight_id):
        flight_id = uuid_value(flight_id)
        return self._read(f"flight:{flight_id}", sa.select(flights).where(flights.c.id == flight_id), single=True)

    def search(self, origin, destination, departure_date=None):
        if not re.fullmatch(r"[A-Z]{3}", origin) or not re.fullmatch(r"[A-Z]{3}", destination):
            invalid("origin and destination must be three uppercase IATA letters")
        if origin == destination:
            invalid("origin and destination must differ")
        query = sa.select(flights).where(
            flights.c.origin == origin, flights.c.destination == destination, flights.c.status == "SCHEDULED"
        )
        if departure_date:
            if not isinstance(departure_date, date):
                invalid("Invalid departure date")
            start = datetime.combine(departure_date, time.min, timezone.utc)
            query = query.where(flights.c.departure_at >= start, flights.c.departure_at < start + timedelta(days=1))
        query = query.order_by(flights.c.departure_at, flights.c.id)
        return self._read(f"search:{origin}:{destination}:{departure_date or 'all'}", query)

    @staticmethod
    def _guard(conn, booking_id):
        conn.execute(insert(guards).values(booking_id=booking_id).on_conflict_do_nothing())
        return conn.execute(sa.select(guards).where(guards.c.booking_id == booking_id).with_for_update()).mappings().one()

    @staticmethod
    def _bump(conn):
        version = conn.execute(sa.update(cache_state).where(cache_state.c.id == 1)
                               .values(version=cache_state.c.version + 1).returning(cache_state.c.version)).scalar_one()
        log.info("cache_version_changed version=%s", version)

    def reserve(self, flight_id, seat_count, booking_id):
        flight_id, booking_id = uuid_value(flight_id), uuid_value(booking_id)
        if not 1 <= seat_count <= 2147483647:
            invalid("seat_count must be a positive int32")
        with self.engine.begin() as conn:
            guard = self._guard(conn, booking_id)
            if guard["release_requested"]:
                raise DomainError(grpc.StatusCode.FAILED_PRECONDITION, "Reservation has been released")
            old = conn.execute(sa.select(reservations).where(reservations.c.booking_id == booking_id)).mappings().first()
            if old:
                if old["flight_id"] != flight_id or old["seat_count"] != seat_count:
                    raise DomainError(grpc.StatusCode.ALREADY_EXISTS, "booking_id has different reservation parameters")
                return dict(old)
            flight = conn.execute(sa.select(flights).where(flights.c.id == flight_id).with_for_update()).mappings().first()
            if not flight:
                raise DomainError(grpc.StatusCode.NOT_FOUND, "Flight not found")
            if flight["status"] != "SCHEDULED" or flight["departure_at"] <= datetime.now(timezone.utc):
                raise DomainError(grpc.StatusCode.FAILED_PRECONDITION, "Flight is not available for booking")
            if flight["available_seats"] < seat_count:
                raise DomainError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Not enough available seats")
            conn.execute(sa.update(flights).where(flights.c.id == flight_id).values(
                available_seats=flights.c.available_seats - seat_count, updated_at=sa.func.now()))
            result = conn.execute(sa.insert(reservations).values(
                id=uuid4(), booking_id=booking_id, flight_id=flight_id, seat_count=seat_count, status="ACTIVE"
            ).returning(reservations)).mappings().one()
            self._bump(conn)
            return dict(result)

    def release(self, booking_id):
        booking_id = uuid_value(booking_id)
        with self.engine.begin() as conn:
            self._guard(conn, booking_id)
            conn.execute(sa.update(guards).where(guards.c.booking_id == booking_id).values(release_requested=True))
            old = conn.execute(sa.select(reservations).where(reservations.c.booking_id == booking_id)).mappings().first()
            if old and old["status"] == "ACTIVE":
                conn.execute(sa.select(flights.c.id).where(flights.c.id == old["flight_id"]).with_for_update()).one()
                conn.execute(sa.update(flights).where(flights.c.id == old["flight_id"]).values(
                    available_seats=flights.c.available_seats + old["seat_count"], updated_at=sa.func.now()))
                conn.execute(sa.update(reservations).where(reservations.c.id == old["id"]).values(
                    status="RELEASED", updated_at=sa.func.now()))
                self._bump(conn)
        return {"booking_id": str(booking_id), "released": True}

    def update(self, flight_id, price_minor):
        flight_id = uuid_value(flight_id)
        if not 1 <= price_minor <= 9223372036854775807:
            invalid("price_minor must be a positive int64")
        with self.engine.begin() as conn:
            result = conn.execute(sa.update(flights).where(flights.c.id == flight_id).values(
                price_minor=price_minor, updated_at=sa.func.now()).returning(flights)).mappings().first()
            if not result:
                raise DomainError(grpc.StatusCode.NOT_FOUND, "Flight not found")
            self._bump(conn)
            return flight_json(result)
