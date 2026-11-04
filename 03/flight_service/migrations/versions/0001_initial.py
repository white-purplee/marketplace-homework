"""Initial flight schema. Frozen DDL; independent of future model changes."""
from alembic import op
revision = "flight_0001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.execute('\nCREATE TABLE cache_state (\n\tid SERIAL NOT NULL, \n\tversion BIGINT NOT NULL, \n\tPRIMARY KEY (id), \n\tCHECK (id = 1 AND version >= 0)\n)\n\n')
    op.execute("\nCREATE TABLE flights (\n\tid UUID NOT NULL, \n\tflight_number VARCHAR(12) NOT NULL, \n\tairline VARCHAR(120) NOT NULL, \n\torigin VARCHAR(3) NOT NULL, \n\tdestination VARCHAR(3) NOT NULL, \n\tdeparture_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tarrival_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\ttotal_seats INTEGER NOT NULL, \n\tavailable_seats INTEGER NOT NULL, \n\tprice_minor BIGINT NOT NULL, \n\tstatus VARCHAR(16) NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (id), \n\tCHECK (total_seats > 0 AND available_seats >= 0 AND available_seats <= total_seats), \n\tCHECK (price_minor > 0), \n\tCHECK (arrival_at > departure_at), \n\tCHECK (origin <> destination AND origin ~ '^[A-Z]{3}$' AND destination ~ '^[A-Z]{3}$'), \n\tCHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED')), \n\tCHECK (length(trim(flight_number)) > 0 AND length(trim(airline)) > 0)\n)\n\n")
    op.execute("CREATE INDEX ix_flight_search ON flights (origin, destination, departure_at) WHERE status = 'SCHEDULED'")
    op.execute("CREATE UNIQUE INDEX uq_flight_number_date ON flights (flight_number, ((departure_at AT TIME ZONE 'UTC')::date))")
    op.execute('\nCREATE TABLE reservation_guards (\n\tbooking_id UUID NOT NULL, \n\trelease_requested BOOLEAN DEFAULT false NOT NULL, \n\tPRIMARY KEY (booking_id)\n)\n\n')
    op.execute("\nCREATE TABLE seat_reservations (\n\tid UUID NOT NULL, \n\tbooking_id UUID NOT NULL, \n\tflight_id UUID NOT NULL, \n\tseat_count INTEGER NOT NULL, \n\tstatus VARCHAR(16) NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (id), \n\tCHECK (seat_count > 0), \n\tCHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')), \n\tUNIQUE (booking_id), \n\tFOREIGN KEY(booking_id) REFERENCES reservation_guards (booking_id), \n\tFOREIGN KEY(flight_id) REFERENCES flights (id)\n)\n\n")
    op.execute("INSERT INTO cache_state(id, version) VALUES (1, 0)")

def downgrade():
    op.execute("DROP TABLE seat_reservations")
    op.execute("DROP TABLE reservation_guards")
    op.execute("DROP TABLE flights")
    op.execute("DROP TABLE cache_state")
