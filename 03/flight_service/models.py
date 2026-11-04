import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

metadata = sa.MetaData()
flights = sa.Table(
    "flights", metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("flight_number", sa.String(12), nullable=False),
    sa.Column("airline", sa.String(120), nullable=False),
    sa.Column("origin", sa.String(3), nullable=False),
    sa.Column("destination", sa.String(3), nullable=False),
    sa.Column("departure_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("arrival_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("total_seats", sa.Integer, nullable=False),
    sa.Column("available_seats", sa.Integer, nullable=False),
    sa.Column("price_minor", sa.BigInteger, nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.CheckConstraint("total_seats > 0 AND available_seats >= 0 AND available_seats <= total_seats"),
    sa.CheckConstraint("price_minor > 0"),
    sa.CheckConstraint("arrival_at > departure_at"),
    sa.CheckConstraint("origin <> destination AND origin ~ '^[A-Z]{3}$' AND destination ~ '^[A-Z]{3}$'"),
    sa.CheckConstraint("status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED')"),
    sa.CheckConstraint("length(trim(flight_number)) > 0 AND length(trim(airline)) > 0"),
)
sa.Index("uq_flight_number_date", flights.c.flight_number,
         sa.text("((departure_at AT TIME ZONE 'UTC')::date)"), unique=True)
sa.Index("ix_flight_search", flights.c.origin, flights.c.destination, flights.c.departure_at,
         postgresql_where=flights.c.status == "SCHEDULED")
guards = sa.Table(
    "reservation_guards", metadata,
    sa.Column("booking_id", UUID(as_uuid=True), primary_key=True),
    sa.Column("release_requested", sa.Boolean, nullable=False, server_default=sa.false()),
)
reservations = sa.Table(
    "seat_reservations", metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("booking_id", UUID(as_uuid=True), sa.ForeignKey("reservation_guards.booking_id"), nullable=False, unique=True),
    sa.Column("flight_id", UUID(as_uuid=True), sa.ForeignKey("flights.id"), nullable=False),
    sa.Column("seat_count", sa.Integer, nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.CheckConstraint("seat_count > 0"),
    sa.CheckConstraint("status IN ('ACTIVE', 'RELEASED', 'EXPIRED')"),
)
cache_state = sa.Table(
    "cache_state", metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("version", sa.BigInteger, nullable=False),
    sa.CheckConstraint("id = 1 AND version >= 0"),
)
