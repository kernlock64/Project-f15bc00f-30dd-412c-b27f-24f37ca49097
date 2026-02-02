#!/bin/sh

echo "Preparing for starting..."
set -e

if [ -z "$(ls -A alembic/versions)" ]; then
    echo "No migrations found. Generating initial migration..."
    alembic revision --autogenerate -m "Initial migration"
fi

echo "Checking for any migrations..."
alembic upgrade head

echo "Starting application..."
exec "$@"