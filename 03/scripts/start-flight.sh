#!/bin/sh
set -eu
alembic -c flight_service/alembic.ini upgrade head
exec python -m flight_service.server
