"""Seed two future flights on the first installation only."""
from alembic import op

revision = "flight_0002"
down_revision = "flight_0001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        INSERT INTO flights(id, flight_number, airline, origin, destination,
                            departure_at, arrival_at, total_seats, available_seats, price_minor, status)
        VALUES
          ('11111111-1111-4111-8111-111111111111', 'SU1234', 'Aeroflot', 'SVO', 'LED',
           date_trunc('day', now()) + interval '30 days 10 hours',
           date_trunc('day', now()) + interval '30 days 11 hours 30 minutes', 10, 10, 500000, 'SCHEDULED'),
          ('22222222-2222-4222-8222-222222222222', 'SU1235', 'Aeroflot', 'LED', 'SVO',
           date_trunc('day', now()) + interval '31 days 12 hours',
           date_trunc('day', now()) + interval '31 days 13 hours 30 minutes', 20, 20, 450000, 'SCHEDULED')
    """)
    op.execute("UPDATE cache_state SET version = version + 1 WHERE id = 1")


def downgrade():
    op.execute("DELETE FROM flights WHERE id IN ('11111111-1111-4111-8111-111111111111', '22222222-2222-4222-8222-222222222222')")
    op.execute("UPDATE cache_state SET version = version + 1 WHERE id = 1")
