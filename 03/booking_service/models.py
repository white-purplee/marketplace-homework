import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

metadata = sa.MetaData()
bookings = sa.Table(
    "bookings", metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("user_id", sa.String(128), nullable=False),
    sa.Column("flight_id", UUID(as_uuid=True), nullable=False),
    sa.Column("passenger_name", sa.String(200), nullable=False),
    sa.Column("passenger_email", sa.String(254), nullable=False),
    sa.Column("seat_count", sa.Integer, nullable=False),
    sa.Column("total_price_minor", sa.BigInteger, nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.CheckConstraint("seat_count > 0 AND total_price_minor > 0"),
    sa.CheckConstraint("status IN ('CONFIRMED', 'CANCELLED')"),
    sa.CheckConstraint("length(trim(user_id)) > 0 AND length(trim(passenger_name)) > 0"),
)
sa.Index("ix_bookings_user", bookings.c.user_id, bookings.c.created_at, bookings.c.id)
operations = sa.Table(
    "booking_operations", metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("booking_id", UUID(as_uuid=True), nullable=False),
    sa.Column("kind", sa.String(8), nullable=False),
    sa.Column("idempotency_key", sa.String(160), nullable=False, unique=True),
    sa.Column("request_hash", sa.String(64), nullable=False),
    sa.Column("state", sa.String(20), nullable=False),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("error_code", sa.String(40)),
    sa.Column("error_message", sa.String(500)),
    sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.UniqueConstraint("booking_id", "kind"),
    sa.CheckConstraint("kind IN ('CREATE', 'CANCEL')"),
    sa.CheckConstraint("state IN ('PREPARED', 'RESERVING', 'COMMITTING', 'COMPENSATING', 'RELEASING', 'SUCCEEDED', 'FAILED')"),
)
sa.Index("ix_operations_recovery", operations.c.state, operations.c.next_attempt_at)
