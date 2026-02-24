#!/bin/sh

echo "Preparing for starting..."
set -e

echo "Starting application..."
exec "$@"