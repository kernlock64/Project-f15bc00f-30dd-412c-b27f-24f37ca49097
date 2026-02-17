#!/bin/sh

echo "Preparing for starting..."
set -e

echo "Checking for any migrations..."
alembic upgrade head

echo "Starting application..."
exec "$@"