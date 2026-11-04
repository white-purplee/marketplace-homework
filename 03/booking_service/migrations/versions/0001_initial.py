"""Initial booking schema. Frozen DDL; independent of future model changes."""
from alembic import op
revision = "booking_0001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.execute("\nCREATE TABLE booking_operations (\n\tid UUID NOT NULL, \n\tbooking_id UUID NOT NULL, \n\tkind VARCHAR(8) NOT NULL, \n\tidempotency_key VARCHAR(160) NOT NULL, \n\trequest_hash VARCHAR(64) NOT NULL, \n\tstate VARCHAR(20) NOT NULL, \n\tpayload JSONB NOT NULL, \n\terror_code VARCHAR(40), \n\terror_message VARCHAR(500), \n\tnext_attempt_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (id), \n\tUNIQUE (booking_id, kind), \n\tCHECK (kind IN ('CREATE', 'CANCEL')), \n\tCHECK (state IN ('PREPARED', 'RESERVING', 'COMMITTING', 'COMPENSATING', 'RELEASING', 'SUCCEEDED', 'FAILED')), \n\tUNIQUE (idempotency_key)\n)\n\n")
    op.execute('CREATE INDEX ix_operations_recovery ON booking_operations (state, next_attempt_at)')
    op.execute("\nCREATE TABLE bookings (\n\tid UUID NOT NULL, \n\tuser_id VARCHAR(128) NOT NULL, \n\tflight_id UUID NOT NULL, \n\tpassenger_name VARCHAR(200) NOT NULL, \n\tpassenger_email VARCHAR(254) NOT NULL, \n\tseat_count INTEGER NOT NULL, \n\ttotal_price_minor BIGINT NOT NULL, \n\tstatus VARCHAR(16) NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tPRIMARY KEY (id), \n\tCHECK (seat_count > 0 AND total_price_minor > 0), \n\tCHECK (status IN ('CONFIRMED', 'CANCELLED')), \n\tCHECK (length(trim(user_id)) > 0 AND length(trim(passenger_name)) > 0)\n)\n\n")
    op.execute('CREATE INDEX ix_bookings_user ON bookings (user_id, created_at, id)')

def downgrade():
    op.execute("DROP TABLE bookings")
    op.execute("DROP TABLE booking_operations")
