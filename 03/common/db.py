import os
from sqlalchemy import create_engine


def make_engine(url=None, *, locks=False):
    # A separate small pool for session locks prevents starvation of DB workers.
    return create_engine(
        url or os.environ["DATABASE_URL"],
        pool_pre_ping=True, pool_size=8 if locks else 12, max_overflow=4,
        connect_args={"connect_timeout": 3, "options": "-c statement_timeout=3000 -c lock_timeout=2000"},
    )
