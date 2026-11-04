"""Create disposable *_test databases and run all tests inside Compose."""
import os
import subprocess
import sys
from pathlib import Path
import sqlalchemy as sa


def main():
    for name in ("TEST_FLIGHT_DATABASE_URL", "TEST_BOOKING_DATABASE_URL"):
        url = sa.engine.make_url(os.environ[name])
        if not url.database.endswith("_test"):
            raise SystemExit("Test database names must end with _test")
        engine = sa.create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            if conn.execute(sa.text("SELECT 1 FROM pg_database WHERE datname=:name"), {"name": url.database}).first() is None:
                quoted = conn.dialect.identifier_preparer.quote(url.database)
                conn.execute(sa.text("CREATE DATABASE " + quoted))
        engine.dispose()
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", "-q"]))


if __name__ == "__main__":
    main()
