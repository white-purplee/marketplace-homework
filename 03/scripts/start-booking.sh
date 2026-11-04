#!/bin/sh
set -eu
alembic -c booking_service/alembic.ini upgrade head
exec uvicorn booking_service.api:app --host 0.0.0.0 --port 8000
